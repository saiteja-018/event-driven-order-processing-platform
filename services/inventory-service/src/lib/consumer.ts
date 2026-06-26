import { consumer, producer } from './kafka';
import { validateEvent } from './schemaValidator';
import pool from './db';
import redis from './redis';
import { logger } from '../logger';
import { v4 as uuidv4 } from 'uuid';
import AWS from 'aws-sdk';
import dotenv from 'dotenv';
dotenv.config();

const sns = new AWS.SNS({
  endpoint: process.env.LOCALSTACK_URL || 'http://localstack:4566',
  region: 'us-east-1',
  accessKeyId: 'test',
  secretAccessKey: 'test'
});

const INVENTORY_ALERTS_TOPIC_ARN =
  process.env.INVENTORY_ALERTS_TOPIC_ARN ||
  'arn:aws:sns:us-east-1:000000000000:inventory-alerts-topic';

function delay(ms: number) { return new Promise(r => setTimeout(r, ms)); }

// ─────────────────────────────────────────────────────────────
// checkReorderAlerts: publish SNS alert if stock is low
// ─────────────────────────────────────────────────────────────
async function checkReorderAlerts(productId: string) {
  try {
    const { rows } = await pool.query(
      'SELECT sku, available_stock, reorder_threshold FROM inventory_service.products WHERE id=$1',
      [productId]
    );
    if (rows.length === 0) return;
    const p = rows[0];
    if (p.available_stock <= p.reorder_threshold) {
      const alert = {
        alertType: 'LOW_STOCK',
        productId,
        sku: p.sku,
        availableStock: p.available_stock,
        reorderThreshold: p.reorder_threshold,
        timestamp: new Date().toISOString()
      };
      await sns.publish({
        TopicArn: INVENTORY_ALERTS_TOPIC_ARN,
        Message: JSON.stringify(alert),
        MessageAttributes: {
          alertType: { DataType: 'String', StringValue: 'LOW_STOCK' }
        }
      }).promise();
      logger.info({ message: 'low_stock_alert_sent', productId, sku: p.sku, availableStock: p.available_stock });
    }
  } catch (err) {
    logger.error({ message: 'check_reorder_alerts_error', productId, error: String(err) });
  }
}

// ─────────────────────────────────────────────────────────────
// reserveStock: triggered by order.created event
// Uses optimistic locking on the version column.
// All items must be reserved atomically; if any fail, release all.
// ─────────────────────────────────────────────────────────────
async function reserveStock(event: any) {
  const { orderId, userId, items } = event;
  if (!orderId || !Array.isArray(items) || items.length === 0) {
    throw new Error('invalid_order_created_event');
  }

  // Check for zero-stock items early (before acquiring DB client)
  // We check available_stock directly from DB — not from Redis cache
  for (const item of items) {
    const { rows } = await pool.query(
      'SELECT id, available_stock FROM inventory_service.products WHERE id=$1',
      [item.productId]
    );
    if (rows.length === 0) {
      logger.warn({ message: 'product_not_found', productId: item.productId });
      await publishReservationFailed(orderId, userId, items, `product_not_found:${item.productId}`);
      return;
    }
    if (rows[0].available_stock < item.quantity) {
      logger.warn({ message: 'insufficient_stock', productId: item.productId, available: rows[0].available_stock, requested: item.quantity });
      await publishReservationFailed(orderId, userId, items, `insufficient_stock:${item.productId}`);
      return;
    }
  }

  // Attempt to reserve each item with optimistic locking
  const reservedItems: Array<{ productId: string; quantity: number; reservationId: string }> = [];

  for (const item of items) {
    let reserved = false;
    for (let attempt = 0; attempt < 3; attempt++) {
      const client = await pool.connect();
      try {
        await client.query('BEGIN');

        // Read current version
        const { rows: prodRows } = await client.query(
          'SELECT id, version, available_stock, reserved_stock FROM inventory_service.products WHERE id=$1 FOR UPDATE',
          [item.productId]
        );
        if (prodRows.length === 0) {
          await client.query('ROLLBACK');
          throw new Error(`product_not_found:${item.productId}`);
        }
        const prod = prodRows[0];
        if (prod.available_stock < item.quantity) {
          await client.query('ROLLBACK');
          throw new Error(`insufficient_stock:${item.productId}`);
        }

        // Optimistic lock update using version column
        const updateRes = await client.query(
          `UPDATE inventory_service.products
           SET reserved_stock = reserved_stock + $1,
               version = version + 1,
               updated_at = now()
           WHERE id = $2 AND version = $3`,
          [item.quantity, item.productId, prod.version]
        );

        if (updateRes.rowCount === 0) {
          // Concurrent modification — retry
          await client.query('ROLLBACK');
          client.release();
          if (attempt < 2) {
            await delay(50);
            continue;
          }
          throw new Error(`optimistic_lock_exhausted:${item.productId}`);
        }

        // Insert stock_reservations row
        const reservationId = uuidv4();
        const expiresAt = new Date(Date.now() + 15 * 60 * 1000).toISOString();
        await client.query(
          `INSERT INTO inventory_service.stock_reservations
             (id, order_id, product_id, quantity, status, expires_at, created_at)
           VALUES ($1, $2, $3, $4, 'ACTIVE', $5, now())`,
          [reservationId, orderId, item.productId, item.quantity, expiresAt]
        );

        // Insert stock_movements row
        await client.query(
          `INSERT INTO inventory_service.stock_movements
             (product_id, movement_type, quantity_delta, reference_id, reference_type, created_at)
           VALUES ($1, 'RESERVATION', $2, $3, 'ORDER', now())`,
          [item.productId, -item.quantity, orderId]
        );

        await client.query('COMMIT');
        client.release();

        // Invalidate Redis cache for this product
        await redis.del(`product:stock:${item.productId}`);

        reservedItems.push({ productId: item.productId, quantity: item.quantity, reservationId });
        reserved = true;
        break;
      } catch (err: any) {
        try { await client.query('ROLLBACK'); } catch (e) { /* ignore */ }
        client.release();

        if (err.message.startsWith('insufficient_stock') || err.message.startsWith('product_not_found')) {
          // Non-retriable: release already-reserved items and fail
          await rollbackReservations(orderId, reservedItems);
          await publishReservationFailed(orderId, userId, items, err.message);
          return;
        }

        if (attempt < 2) {
          await delay(50);
          continue;
        }

        // Exhausted retries for this item
        await rollbackReservations(orderId, reservedItems);
        await publishReservationFailed(orderId, userId, items, err.message);
        return;
      }
    }

    if (!reserved) {
      await rollbackReservations(orderId, reservedItems);
      await publishReservationFailed(orderId, userId, items, `reservation_failed:${item.productId}`);
      return;
    }
  }

  // All items reserved successfully — publish inventory.reserved
  const reservedEvent = {
    eventId: uuidv4(),
    eventType: 'INVENTORY_RESERVED',
    occurredAt: new Date().toISOString(),
    orderId,
    userId: userId || null,
    reservations: reservedItems.map(r => ({ productId: r.productId, quantity: r.quantity, reservationId: r.reservationId })),
    items: reservedItems.map(r => ({ productId: r.productId, quantity: r.quantity, reservationId: r.reservationId })),
    schemaVersion: '1.0'
  };

  await producer.send({
    topic: 'inventory.reserved',
    messages: [{ key: orderId, value: JSON.stringify(reservedEvent) }]
  });

  // Check reorder thresholds for all affected products
  for (const item of items) {
    await checkReorderAlerts(item.productId);
  }

  logger.info({ message: 'stock_reserved', orderId, itemCount: items.length });
}

// ─────────────────────────────────────────────────────────────
// rollbackReservations: release already-reserved items on partial failure
// ─────────────────────────────────────────────────────────────
async function rollbackReservations(
  orderId: string,
  reservedItems: Array<{ productId: string; quantity: number; reservationId: string }>
) {
  for (const r of reservedItems) {
    try {
      await pool.query(
        "UPDATE inventory_service.stock_reservations SET status='RELEASED' WHERE id=$1",
        [r.reservationId]
      );
      await pool.query(
        `UPDATE inventory_service.products
         SET reserved_stock = GREATEST(0, reserved_stock - $1), version = version + 1, updated_at = now()
         WHERE id = $2`,
        [r.quantity, r.productId]
      );
      await redis.del(`product:stock:${r.productId}`);
    } catch (err) {
      logger.error({ message: 'rollback_reservation_error', reservationId: r.reservationId, error: String(err) });
    }
  }
}

// ─────────────────────────────────────────────────────────────
// publishReservationFailed: send inventory.reservation_failed event
// ─────────────────────────────────────────────────────────────
async function publishReservationFailed(orderId: string, userId: string, items: any[], reason: string) {
  const event = {
    eventId: uuidv4(),
    eventType: 'INVENTORY_RESERVATION_FAILED',
    occurredAt: new Date().toISOString(),
    orderId,
    userId: userId || null,
    reason,
    items,
    schemaVersion: '1.0'
  };
  await producer.send({
    topic: 'inventory.reservation_failed',
    messages: [{ key: orderId, value: JSON.stringify(event) }]
  });
  logger.info({ message: 'reservation_failed', orderId, reason });
}

// ─────────────────────────────────────────────────────────────
// releaseStock: triggered by order.cancelled
// Sets reservation status to RELEASED, decrements reserved_stock
// ─────────────────────────────────────────────────────────────
async function releaseStock(orderId: string) {
  const { rows: reservations } = await pool.query(
    "SELECT id, product_id, quantity FROM inventory_service.stock_reservations WHERE order_id=$1 AND status='ACTIVE'",
    [orderId]
  );

  for (const r of reservations) {
    try {
      await pool.query(
        "UPDATE inventory_service.stock_reservations SET status='RELEASED' WHERE id=$1",
        [r.id]
      );
      await pool.query(
        `UPDATE inventory_service.products
         SET reserved_stock = GREATEST(0, reserved_stock - $1), version = version + 1, updated_at = now()
         WHERE id = $2`,
        [r.quantity, r.product_id]
      );
      await pool.query(
        `INSERT INTO inventory_service.stock_movements
           (product_id, movement_type, quantity_delta, reference_id, reference_type, created_at)
         VALUES ($1, 'RELEASE', $2, $3, 'ORDER', now())`,
        [r.product_id, r.quantity, orderId]
      );
      await redis.del(`product:stock:${r.product_id}`);
      await checkReorderAlerts(r.product_id);
    } catch (err) {
      logger.error({ message: 'release_stock_error', reservationId: r.id, error: String(err) });
    }
  }

  const event = {
    eventId: uuidv4(),
    eventType: 'INVENTORY_RELEASED',
    occurredAt: new Date().toISOString(),
    orderId,
    schemaVersion: '1.0'
  };
  await producer.send({ topic: 'inventory.released', messages: [{ key: orderId, value: JSON.stringify(event) }] });
  logger.info({ message: 'stock_released', orderId });
}

// ─────────────────────────────────────────────────────────────
// fulfillStock: triggered by order.completed
// Sets reservation FULFILLED, deducts total_stock (permanent sale)
// ─────────────────────────────────────────────────────────────
async function fulfillStock(orderId: string) {
  const { rows: reservations } = await pool.query(
    "SELECT id, product_id, quantity FROM inventory_service.stock_reservations WHERE order_id=$1 AND status='ACTIVE'",
    [orderId]
  );

  for (const r of reservations) {
    try {
      await pool.query(
        "UPDATE inventory_service.stock_reservations SET status='FULFILLED' WHERE id=$1",
        [r.id]
      );
      // Deduct both total_stock and reserved_stock (permanent sale)
      await pool.query(
        `UPDATE inventory_service.products
         SET total_stock = GREATEST(0, total_stock - $1),
             reserved_stock = GREATEST(0, reserved_stock - $1),
             version = version + 1,
             updated_at = now()
         WHERE id = $2`,
        [r.quantity, r.product_id]
      );
      await pool.query(
        `INSERT INTO inventory_service.stock_movements
           (product_id, movement_type, quantity_delta, reference_id, reference_type, created_at)
         VALUES ($1, 'SALE', $2, $3, 'ORDER', now())`,
        [r.product_id, -r.quantity, orderId]
      );
      await redis.del(`product:stock:${r.product_id}`);
      await checkReorderAlerts(r.product_id);
    } catch (err) {
      logger.error({ message: 'fulfill_stock_error', reservationId: r.id, error: String(err) });
    }
  }
  logger.info({ message: 'stock_fulfilled', orderId });
}

// ─────────────────────────────────────────────────────────────
// startConsumers: subscribe to relevant Kafka topics
// ─────────────────────────────────────────────────────────────
export async function startConsumers() {
  await consumer.subscribe({ topic: 'order.created', fromBeginning: false });
  await consumer.subscribe({ topic: 'order.cancelled', fromBeginning: false });
  await consumer.subscribe({ topic: 'order.completed', fromBeginning: false });

  await consumer.run({
    autoCommit: false,
    eachMessage: async ({ topic, partition, message }) => {
      const retryDelays = [1000, 2000, 4000];
      let lastErr: any = null;

      for (let attempt = 0; attempt < 3; attempt++) {
        try {
          const value = message.value?.toString();
          if (!value) {
            await consumer.commitOffsets([{ topic, partition, offset: (Number(message.offset) + 1).toString() }]);
            return;
          }

          let event: any;
          try { event = JSON.parse(value); } catch { throw new Error('invalid_json'); }

          // Schema validation (best-effort — don't block on schema not found)
          const validation = validateEvent(topic, event);
          if (!validation.valid) {
            const errors = validation.errors || [];
            const isSchemaMissing = errors.length === 1 && String(errors[0]).startsWith('schema_not_found');
            if (!isSchemaMissing) {
              logger.warn({ message: 'schema_invalid', topic, errors: validation.errors });
              throw new Error(`schema_invalid:${JSON.stringify(validation.errors)}`);
            }
          }

          // Consumer idempotency check
          const eventId = event.eventId;
          if (eventId) {
            const consumerId = `inventory-service-group:${topic}`;
            const processedKey = `processed_event:${consumerId}:${eventId}`;
            if (await redis.get(processedKey)) {
              await consumer.commitOffsets([{ topic, partition, offset: (Number(message.offset) + 1).toString() }]);
              return;
            }
          }

          if (topic === 'order.created') {
            await reserveStock(event);
          } else if (topic === 'order.cancelled') {
            await releaseStock(event.orderId);
          } else if (topic === 'order.completed') {
            await fulfillStock(event.orderId);
          }

          // Mark as processed
          if (eventId) {
            const consumerId = `inventory-service-group:${topic}`;
            await redis.set(`processed_event:${consumerId}:${eventId}`, '1', 'EX', 3600);
          }

          await consumer.commitOffsets([{ topic, partition, offset: (Number(message.offset) + 1).toString() }]);
          return;
        } catch (err: any) {
          lastErr = err;
          logger.error({ message: 'consumer_error', topic, attempt, error: String(err) });
          if (attempt < 2) await delay(retryDelays[attempt]);
        }
      }

      // All retries exhausted — publish to DLQ
      try {
        const dlqEnvelope = {
          originalTopic: topic,
          originalPartition: partition,
          originalOffset: String(message.offset),
          failedAt: new Date().toISOString(),
          errorMessage: String(lastErr),
          retryCount: 3,
          payload: message.value?.toString() ? JSON.parse(message.value.toString()) : null
        };
        await producer.send({
          topic: 'dead.letter.queue',
          messages: [{ key: `${topic}-${partition}-${message.offset}`, value: JSON.stringify(dlqEnvelope) }]
        });
        logger.error({ message: 'message_sent_to_dlq', topic, partition, offset: message.offset });
      } catch (dlqErr) {
        logger.error({ message: 'dlq_publish_failed', error: String(dlqErr) });
      }

      await consumer.commitOffsets([{ topic, partition, offset: (Number(message.offset) + 1).toString() }]);
    }
  });
}
