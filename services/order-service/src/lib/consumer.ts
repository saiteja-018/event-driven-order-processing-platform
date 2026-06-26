import { Kafka } from 'kafkajs';
import dotenv from 'dotenv';
dotenv.config();
import { updateOrderStatus } from '../services/orderStatus';
import { logger } from '../logger';
import { producer } from './kafka';
import redis from '../lib/redis';
import { validateEvent } from './schemaValidator';
import { publishNotificationDispatch, publishOrderLifecycleSns } from './notifications';
import { v4 as uuidv4 } from 'uuid';
import pool from '../lib/db';

const kafka = new Kafka({ brokers: [process.env.KAFKA_BROKER || 'kafka:9092'], clientId: 'order-service-consumer' });
const consumer = kafka.consumer({ groupId: 'order-service-group' });

function delay(ms: number) { return new Promise(res => setTimeout(res, ms)); }

// Fire-and-forget for non-critical async operations (SNS, notifications)
function fireAndForget(fn: () => Promise<any>, label: string) {
  fn().catch(err => logger.warn({ message: `${label}_non_critical_error`, error: String(err) }));
}

export async function startConsumers() {
  await consumer.connect();
  await consumer.subscribe({ topic: 'inventory.reserved', fromBeginning: true });
  await consumer.subscribe({ topic: 'inventory.reservation_failed', fromBeginning: true });
  await consumer.subscribe({ topic: 'payment.initiated', fromBeginning: true });
  await consumer.subscribe({ topic: 'payment.succeeded', fromBeginning: true });
  await consumer.subscribe({ topic: 'payment.failed', fromBeginning: true });

  await consumer.run({ autoCommit: false, eachMessage: async ({ topic, partition, message }) => {
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
        try { event = JSON.parse(value); } catch (e) { throw new Error('invalid_json'); }

        // Schema validation (best-effort — don't block on schema not found)
        const validation = validateEvent(topic, event);
        if (!validation.valid) {
          // Check if it's a missing schema or actual validation failure
          const errors = validation.errors || [];
          const isSchemaMissing = errors.length === 1 && String(errors[0]).startsWith('schema_not_found');
          if (!isSchemaMissing) {
            throw new Error(`schema_invalid:${JSON.stringify(errors)}`);
          }
        }

        // Consumer idempotency using eventId (if present)
        const consumerId = `order-service-group:${topic}`;
        if (event.eventId) {
          const processedKey = `processed_event:${consumerId}:${event.eventId}`;
          if (await redis.get(processedKey)) {
            logger.info({ message: 'idempotent_skip', topic, eventId: event.eventId });
            await consumer.commitOffsets([{ topic, partition, offset: (Number(message.offset) + 1).toString() }]);
            return;
          }
        }

        if (topic === 'inventory.reserved') {
          // Use no-op safe updateOrderStatus (won't throw on already-CONFIRMED)
          try {
            await updateOrderStatus(event.orderId, 'CONFIRMED');
          } catch (e: any) {
            // If already CONFIRMED, that's fine — idempotent
            if (!String(e).includes('illegal_transition') && !String(e).includes('no-op')) throw e;
          }
          const orderRes = await pool.query('SELECT user_id, total_amount, currency FROM order_service.orders WHERE id=$1', [event.orderId]);
          const order = orderRes.rows[0];
          const userId = order ? order.user_id : (event.userId || 'unknown');
          const totalAmount = order ? Number(order.total_amount) : 0;
          const currency = order ? order.currency : 'USD';

          const outEvent = {
            eventId: uuidv4(),
            eventType: 'ORDER_CONFIRMED',
            occurredAt: new Date().toISOString(),
            orderId: event.orderId,
            userId,
            totalAmount,
            currency,
            schemaVersion: '1.0'
          };
          await producer.send({ topic: 'order.confirmed', messages: [{ key: event.orderId, value: JSON.stringify(outEvent) }] });
          fireAndForget(() => publishNotificationDispatch({ userId, orderId: event.orderId, channel: 'EMAIL', templateKey: 'ORDER_CONFIRMED', payload: { orderId: event.orderId } }), 'notify_confirmed');
          fireAndForget(() => publishOrderLifecycleSns('ORDER_CONFIRMED', { orderId: event.orderId }), 'sns_confirmed');
        } else if (topic === 'inventory.reservation_failed') {
          try {
            await updateOrderStatus(event.orderId, 'CANCELLED', 'inventory_reservation_failed');
          } catch (e: any) {
            if (!String(e).includes('illegal_transition') && !String(e).includes('no-op')) throw e;
          }
          const outEvent = { eventId: uuidv4(), eventType: 'ORDER_CANCELLED', occurredAt: new Date().toISOString(), orderId: event.orderId, reason: 'inventory_reservation_failed', schemaVersion: '1.0' };
          await producer.send({ topic: 'order.cancelled', messages: [{ key: event.orderId, value: JSON.stringify(outEvent) }] });
          fireAndForget(() => publishNotificationDispatch({ userId: event.userId || 'unknown', orderId: event.orderId, channel: 'EMAIL', templateKey: 'ORDER_CANCELLED', payload: { orderId: event.orderId, reason: 'inventory_reservation_failed' } }), 'notify_reservation_failed');
          fireAndForget(() => publishOrderLifecycleSns('ORDER_CANCELLED', { orderId: event.orderId, reason: 'inventory_reservation_failed' }), 'sns_reservation_failed');
        } else if (topic === 'payment.initiated') {
          try {
            await updateOrderStatus(event.orderId, 'PAYMENT_PROCESSING');
          } catch (e: any) {
            if (!String(e).includes('illegal_transition') && !String(e).includes('no-op')) throw e;
          }
        } else if (topic === 'payment.succeeded') {
          try {
            await updateOrderStatus(event.orderId, 'COMPLETED');
          } catch (e: any) {
            if (!String(e).includes('illegal_transition') && !String(e).includes('no-op')) throw e;
          }
          const outEvent = { eventId: uuidv4(), eventType: 'ORDER_COMPLETED', occurredAt: new Date().toISOString(), orderId: event.orderId, schemaVersion: '1.0' };
          await producer.send({ topic: 'order.completed', messages: [{ key: event.orderId, value: JSON.stringify(outEvent) }] });
          fireAndForget(() => publishNotificationDispatch({ userId: event.userId || 'unknown', orderId: event.orderId, channel: 'EMAIL', templateKey: 'ORDER_COMPLETED', payload: { orderId: event.orderId } }), 'notify_completed');
          fireAndForget(() => publishOrderLifecycleSns('ORDER_COMPLETED', { orderId: event.orderId }), 'sns_completed');
        } else if (topic === 'payment.failed') {
          const retry = event.retryCount || 0;
          try {
            await updateOrderStatus(event.orderId, 'PAYMENT_FAILED', 'payment_failed');
          } catch (e: any) {
            if (!String(e).includes('illegal_transition') && !String(e).includes('no-op')) throw e;
          }
          fireAndForget(() => publishNotificationDispatch({ userId: event.userId || 'unknown', orderId: event.orderId, channel: 'EMAIL', templateKey: 'PAYMENT_FAILED', payload: { orderId: event.orderId } }), 'notify_payment_failed');
          if (retry < 3) {
            try {
              await updateOrderStatus(event.orderId, 'PAYMENT_PROCESSING', 'payment_retry');
            } catch (e: any) {
              if (!String(e).includes('illegal_transition') && !String(e).includes('no-op')) throw e;
            }
            const retryEvent = { eventId: uuidv4(), eventType: 'PAYMENT_INITIATED', occurredAt: new Date().toISOString(), transactionId: event.transactionId, orderId: event.orderId, amount: event.amount, currency: event.currency, userId: event.userId, idempotencyKey: event.idempotencyKey || event.transactionId, retryCount: retry + 1, schemaVersion: '1.0' };
            await producer.send({ topic: 'payment.initiated', messages: [{ key: event.orderId, value: JSON.stringify(retryEvent) }] });

            // Re-queue to SQS for actual payment re-processing
            try {
              const sqsClient = new (await import('aws-sdk')).default.SQS({
                endpoint: process.env.LOCALSTACK_URL || 'http://localstack:4566',
                region: 'us-east-1',
                accessKeyId: 'test',
                secretAccessKey: 'test'
              });
              const qUrl = await sqsClient.getQueueUrl({ QueueName: 'payment-processing-queue' }).promise().then(r => r.QueueUrl as string);
              await sqsClient.sendMessage({
                QueueUrl: qUrl,
                MessageBody: JSON.stringify({
                  transactionId: event.transactionId,
                  orderId: event.orderId,
                  amount: event.amount,
                  currency: event.currency,
                  userId: event.userId,
                  idempotencyKey: event.idempotencyKey || event.transactionId,
                  retryCount: retry + 1
                })
              }).promise();
            } catch (sqsErr: any) {
              logger.error({ message: 'payment_retry_sqs_error', orderId: event.orderId, error: String(sqsErr) });
            }
          } else {
            try {
              await updateOrderStatus(event.orderId, 'CANCELLED', 'payment_failed');
            } catch (e: any) {
              if (!String(e).includes('illegal_transition') && !String(e).includes('no-op')) throw e;
            }
            const outEvent = { eventId: uuidv4(), eventType: 'ORDER_CANCELLED', occurredAt: new Date().toISOString(), orderId: event.orderId, reason: 'payment_failed', schemaVersion: '1.0' };
            await producer.send({ topic: 'order.cancelled', messages: [{ key: event.orderId, value: JSON.stringify(outEvent) }] });
            fireAndForget(() => publishNotificationDispatch({ userId: event.userId || 'unknown', orderId: event.orderId, channel: 'EMAIL', templateKey: 'ORDER_CANCELLED', payload: { orderId: event.orderId, reason: 'payment_failed' } }), 'notify_order_cancelled_payment');
            fireAndForget(() => publishOrderLifecycleSns('ORDER_CANCELLED', { orderId: event.orderId, reason: 'payment_failed' }), 'sns_order_cancelled_payment');
          }
        }

        // Mark as processed (idempotency)
        if (event.eventId) {
          await redis.set(`processed_event:${consumerId}:${event.eventId}`, '1', 'EX', 3600);
        }
        await consumer.commitOffsets([{ topic, partition, offset: (Number(message.offset) + 1).toString() }]);
        return;
      } catch (err:any) {
        lastErr = err;
        if (attempt < 2) await delay(retryDelays[attempt]);
      }
    }
    // publish to dead letter queue after retries exhausted
    try {
      let rawPayload: string | null = null;
      try { rawPayload = message.value?.toString() || null; } catch { /* ignore */ }
      let parsedPayload: any = rawPayload;
      try { if (rawPayload) parsedPayload = JSON.parse(rawPayload); } catch { /* ignore */ }
      await producer.send({
        topic: 'dead.letter.queue',
        messages: [{
          key: `${topic}-${partition}-${message.offset}`,
          value: JSON.stringify({
            originalTopic: topic,
            originalPartition: partition,
            originalOffset: String(message.offset),
            failedAt: new Date().toISOString(),
            errorMessage: String(lastErr),
            retryCount: 3,
            payload: parsedPayload
          })
        }]
      });
      await consumer.commitOffsets([{ topic, partition, offset: (Number(message.offset) + 1).toString() }]);
    } catch (e) {
      logger.error({ message: 'dlq_publish_failed', error: String(e) });
      // Still commit to avoid infinite loop
      try { await consumer.commitOffsets([{ topic, partition, offset: (Number(message.offset) + 1).toString() }]); } catch { /* ignore */ }
    }
  } });
}
