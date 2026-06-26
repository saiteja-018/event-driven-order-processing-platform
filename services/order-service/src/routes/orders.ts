import { Router } from 'express';
import pool from '../lib/db';
import redis from '../lib/redis';
import { v4 as uuidv4 } from 'uuid';
import { producer } from '../lib/kafka';
import { logger } from '../logger';
import { updateOrderStatus } from '../services/orderStatus';
import { publishNotificationDispatch, publishOrderLifecycleSns } from '../lib/notifications';

const router = Router();

// Non-blocking fire-and-forget for non-critical async operations
function fireAndForget(fn: () => Promise<any>, label: string) {
  fn().catch(err => logger.warn({ message: `${label}_non_critical_error`, error: String(err) }));
}

router.post('/internal/orders', async (req, res) => {
  const reqId = (req as any).requestId || uuidv4();
  try {
    const body = req.body;
    // Basic validation
    if (!body.userId || !Array.isArray(body.items) || body.items.length === 0 || !body.idempotencyKey) {
      return res.status(422).json({ error: 'validation_failed' });
    }
    if (typeof body.currency === 'string' && body.currency.length !== 3) return res.status(422).json({ error: 'validation_failed' });
    if (body.idempotencyKey && body.idempotencyKey.length > 255) return res.status(422).json({ error: 'validation_failed' });
    for (const it of body.items) {
      if (!it.productId || !Number.isInteger(it.quantity) || it.quantity <= 0 || typeof it.unitPrice !== 'number' || it.unitPrice <= 0) {
        return res.status(422).json({ error: 'validation_failed' });
      }
    }
    const idemKey = body.idempotencyKey;
    const idemRedisKey = `idempotency:${idemKey}`;
    const cached = await redis.get(idemRedisKey);
    if (cached) {
      logger.info({ requestId: reqId, message: 'idempotent hit', idempotencyKey: idemKey });
      return res.status(200).json(JSON.parse(cached));
    }

    const client = await pool.connect();
    try {
      await client.query('BEGIN');
      const orderId = uuidv4();
      const insertOrderText = `INSERT INTO order_service.orders (id, user_id, status, total_amount, currency, metadata, idempotency_key) VALUES ($1,$2,$3,$4,$5,$6,$7)`;
      await client.query(insertOrderText, [orderId, body.userId, 'PENDING', body.totalAmount, body.currency || 'USD', body.metadata || {}, idemKey]);
      const insertItemText = `INSERT INTO order_service.order_items (order_id, product_id, quantity, unit_price) VALUES ($1,$2,$3,$4)`;
      for (const it of body.items) {
        await client.query(insertItemText, [orderId, it.productId, it.quantity, it.unitPrice]);
      }
      const historyText = `INSERT INTO order_service.order_status_history (order_id, from_status, to_status) VALUES ($1,$2,$3)`;
      await client.query(historyText, [orderId, null, 'PENDING']);
      await client.query('COMMIT');

      const orderObj = { id: orderId, userId: body.userId, status: 'PENDING', totalAmount: body.totalAmount, currency: body.currency || 'USD', items: body.items, idempotencyKey: idemKey };

      // store idempotency response in Redis immediately
      await redis.set(idemRedisKey, JSON.stringify(orderObj), 'EX', parseInt(process.env.IDEMPOTENCY_TTL || '86400'));

      logger.info({ requestId: reqId, message: 'order_created', orderId });

      // Return 201 IMMEDIATELY — all downstream publishing is fire-and-forget
      res.status(201).json(orderObj);

      // Publish order.created event (non-blocking after response)
      const event = {
        eventId: uuidv4(),
        eventType: 'ORDER_CREATED',
        occurredAt: new Date().toISOString(),
        orderId: orderId,
        userId: body.userId,
        items: body.items,
        totalAmount: body.totalAmount,
        currency: body.currency || 'USD',
        idempotencyKey: idemKey,
        schemaVersion: '1.0'
      };

      fireAndForget(() => producer.send({ topic: 'order.created', messages: [{ key: orderId, value: JSON.stringify(event) }] }), 'order_created_kafka');
      fireAndForget(() => publishNotificationDispatch({ userId: body.userId, orderId, channel: 'EMAIL', templateKey: 'ORDER_CREATED', payload: { orderId, totalAmount: body.totalAmount, currency: body.currency || 'USD' } }), 'order_created_notification');
      fireAndForget(() => publishOrderLifecycleSns('ORDER_CREATED', { orderId, userId: body.userId, totalAmount: body.totalAmount, currency: body.currency || 'USD' }), 'order_created_sns');

      return;
    } catch (err) {
      await client.query('ROLLBACK');
      logger.error({ requestId: reqId, message: 'order_create_failed', error: String(err) });
      return res.status(500).json({ error: 'internal_error' });
    } finally {
      client.release();
    }
  } catch (err) {
    logger.error({ message: 'request_failed', error: String(err) });
    return res.status(500).json({ error: 'internal_error' });
  }
});

router.get('/internal/orders/:orderId', async (req, res) => {
  const orderId = req.params.orderId;
  try {
    const cacheKey = `order:${orderId}`;
    const cached = await redis.get(cacheKey);
    if (cached) return res.json(JSON.parse(cached));
    const orderRes = await pool.query('SELECT * FROM order_service.orders WHERE id=$1', [orderId]);
    if (orderRes.rowCount === 0) return res.status(404).json({ error: 'not_found' });
    const itemsRes = await pool.query('SELECT product_id, quantity, unit_price FROM order_service.order_items WHERE order_id=$1', [orderId]);
    const order = orderRes.rows[0];
    const payload = { id: order.id, userId: order.user_id, status: order.status, totalAmount: order.total_amount, currency: order.currency, items: itemsRes.rows.map(r=>({ productId:r.product_id, quantity:r.quantity, unitPrice:r.unit_price })) };
    const ttl = ['COMPLETED', 'CANCELLED'].includes(order.status) ? 60 : 2;
    await redis.set(cacheKey, JSON.stringify(payload), 'EX', ttl);
    return res.json(payload);
  } catch (err) {
    logger.error({ message: 'get_order_failed', error: String(err) });
    return res.status(500).json({ error: 'internal_error' });
  }
});

router.get('/internal/orders/user/:userId', async (req, res) => {
  const userId = req.params.userId;
  const status = req.query.status as string | undefined;
  const page = parseInt((req.query.page as string) || '1');
  const limit = parseInt((req.query.limit as string) || '20');
  const offset = (page - 1) * limit;
  try {
    const params: any[] = [userId];
    let sql = 'SELECT * FROM order_service.orders WHERE user_id=$1';
    let idx = 2;
    if (status) { sql += ` AND status=$${idx}`; params.push(status); idx++; }
    sql += ` ORDER BY created_at DESC LIMIT $${idx} OFFSET $${idx + 1}`;
    params.push(limit, offset);
    const ordersRes = await pool.query(sql, params);
    return res.json(ordersRes.rows.map(o => ({ id: o.id, userId: o.user_id, status: o.status, totalAmount: o.total_amount, currency: o.currency })));
  } catch (err) {
    logger.error({ message: 'list_orders_failed', error: String(err) });
    return res.status(500).json({ error: 'internal_error' });
  }
});

router.patch('/internal/orders/:orderId/cancel', async (req, res) => {
  const orderId = req.params.orderId;
  try {
    const orderRes = await pool.query('SELECT user_id FROM order_service.orders WHERE id=$1', [orderId]);
    const userId = orderRes.rows[0]?.user_id || 'unknown';
    await updateOrderStatus(orderId, 'CANCELLED', 'user_cancelled');
    const event = { eventId: uuidv4(), eventType: 'ORDER_CANCELLED', occurredAt: new Date().toISOString(), orderId, reason: 'user_cancelled', schemaVersion: '1.0' };

    res.json({ status: 'ok' });

    fireAndForget(() => producer.send({ topic: 'order.cancelled', messages: [{ key: orderId, value: JSON.stringify(event) }] }), 'order_cancelled_kafka');
    fireAndForget(() => publishNotificationDispatch({ userId, orderId, channel: 'EMAIL', templateKey: 'ORDER_CANCELLED', payload: { orderId, reason: 'user_cancelled' } }), 'order_cancelled_notification');
    fireAndForget(() => publishOrderLifecycleSns('ORDER_CANCELLED', { orderId, reason: 'user_cancelled' }), 'order_cancelled_sns');

    return;
  } catch (err) {
    logger.error({ message: 'cancel_order_failed', error: String(err) });
    return res.status(409).json({ error: 'conflict' });
  }
});

export default router;
