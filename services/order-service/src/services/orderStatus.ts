import pool from '../lib/db';
import redis from '../lib/redis';
import { logger } from '../logger';

export const validTransitions: Record<string, string[]> = {
  'PENDING': ['CONFIRMED','CANCELLED'],
  'CONFIRMED': ['PAYMENT_PROCESSING','CANCELLED'],
  'PAYMENT_PROCESSING': ['COMPLETED','PAYMENT_FAILED'],
  'PAYMENT_FAILED': ['PAYMENT_PROCESSING','CANCELLED']
};

export async function updateOrderStatus(orderId: string, toStatus: string, reason?: string) {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const { rows } = await client.query('SELECT status FROM order_service.orders WHERE id=$1 FOR UPDATE', [orderId]);
    if (rows.length === 0) throw new Error('order_not_found');
    const current = rows[0].status;
    if (current === toStatus) {
      await client.query('COMMIT');
      return { ok: true, message: 'no-op' };
    }
    // terminal states
    if (['COMPLETED','CANCELLED'].includes(current)) throw new Error('illegal_transition');
    const allowed = validTransitions[current] || [];
    if (!allowed.includes(toStatus)) throw new Error('illegal_transition');
    await client.query('UPDATE order_service.orders SET status=$1, updated_at=now() WHERE id=$2', [toStatus, orderId]);
    await client.query('INSERT INTO order_service.order_status_history (order_id, from_status, to_status, reason) VALUES ($1,$2,$3,$4)', [orderId, current, toStatus, reason || null]);
    await client.query('COMMIT');
    // invalidate cache
    await redis.del(`order:${orderId}`);
    return { ok: true };
  } catch (err) {
    await client.query('ROLLBACK');
    logger.error({ message: 'update_order_status_failed', orderId, toStatus, error: String(err) });
    throw err;
  } finally { client.release(); }
}
