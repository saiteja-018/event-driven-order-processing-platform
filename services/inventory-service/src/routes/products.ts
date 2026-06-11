import { Router } from 'express';
import pool from '../lib/db';
import redis from '../lib/redis';
import { logger } from '../logger';

const router = Router();

router.get('/internal/products', async (req, res) => {
  try {
    const { rows } = await pool.query('SELECT id, sku, name, total_stock, reserved_stock, available_stock, reorder_threshold FROM inventory_service.products LIMIT 100');
    return res.json(rows);
  } catch (err) {
    logger.error({ message: 'list_products_failed', error: String(err) });
    return res.status(500).json({ error: 'internal_error' });
  }
});

router.get('/internal/products/:productId', async (req, res) => {
  const productId = req.params.productId;
  try {
    const cacheKey = `product:stock:${productId}`;
    const cached = await redis.get(cacheKey);
    if (cached) return res.json(JSON.parse(cached));
    const { rows } = await pool.query('SELECT id, sku, name, total_stock, reserved_stock, available_stock, reorder_threshold FROM inventory_service.products WHERE id=$1', [productId]);
    if (rows.length === 0) return res.status(404).json({ error: 'not_found' });
    const p = rows[0];
    const payload = { id: p.id, sku: p.sku, name: p.name, totalStock: p.total_stock, reservedStock: p.reserved_stock, availableStock: p.available_stock };
    await redis.set(cacheKey, JSON.stringify(payload), 'EX', 60);
    return res.json(payload);
  } catch (err) {
    logger.error({ message: 'get_product_failed', error: String(err) });
    return res.status(500).json({ error: 'internal_error' });
  }
});

router.post('/internal/products/:productId/adjust-stock', async (req, res) => {
  const productId = req.params.productId;
  const { delta } = req.body;
  try {
    await pool.query('BEGIN');
    await pool.query('UPDATE inventory_service.products SET total_stock = total_stock + $1, updated_at = now() WHERE id=$2', [delta, productId]);
    await pool.query('INSERT INTO inventory_service.stock_movements (product_id, movement_type, quantity_delta, created_at) VALUES ($1,$2,$3,now())', [productId, 'MANUAL_ADJUST', delta]);
    await pool.query('COMMIT');
    // invalidate cache
    await redis.del(`product:stock:${productId}`);
    return res.json({ status: 'ok' });
  } catch (err) {
    await pool.query('ROLLBACK');
    logger.error({ message: 'adjust_stock_failed', error: String(err) });
    return res.status(500).json({ error: 'internal_error' });
  }
});

export default router;
