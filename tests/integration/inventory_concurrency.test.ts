import axios from 'axios';
import { uuid, queryDb, waitFor, API_BASE } from './setup';

describe('inventory concurrency', () => {
  jest.setTimeout(180000);

  test('test_concurrent_reservations', async () => {
    // Find or create a product with available_stock >= 5
    let prod = await queryDb(
      'SELECT id, total_stock, reserved_stock FROM inventory_service.products WHERE (total_stock - reserved_stock) >= 5 LIMIT 1'
    );

    if (prod.rowCount === 0) {
      const id = uuid();
      await queryDb(
        'INSERT INTO inventory_service.products (id, sku, name, total_stock, reserved_stock, reorder_threshold, version) VALUES ($1,$2,$3,5,0,10,1)',
        [id, `SKU-CONC${Date.now()}`, 'Concurrency Test Product']
      );
      prod = await queryDb('SELECT id, total_stock, reserved_stock FROM inventory_service.products WHERE id=$1', [id]);
    }

    const product = prod.rows[0];
    const initialAvailable = product.total_stock - product.reserved_stock;

    // Use exactly 5 as the available stock for the test (take snapshot)
    // Send 10 concurrent order creation requests each requesting quantity 1
    const tasks = [];
    for (let i = 0; i < 10; i++) {
      const payload = {
        userId: uuid(),
        items: [{ productId: product.id, quantity: 1, unitPrice: 10 }],
        totalAmount: 10,
        currency: 'USD',
        idempotencyKey: uuid()
      };
      tasks.push(
        axios.post(`${API_BASE}/api/v1/orders`, payload, { timeout: 15000 }).catch(e => e.response)
      );
    }

    const results = await Promise.all(tasks);
    const orderIds = results
      .filter(r => r && (r.status === 200 || r.status === 201))
      .map(r => r.data?.id || r.data?.orderId)
      .filter(Boolean);

    expect(orderIds.length).toBeGreaterThan(0);

    // Wait for all orders to reach a terminal state (COMPLETED, CANCELLED, or CONFIRMED)
    await waitFor(async () => {
      const statusRes = await queryDb(
        `SELECT status FROM order_service.orders WHERE id = ANY($1)`,
        [orderIds]
      );
      const statuses = statusRes.rows.map((r: any) => r.status);
      const terminal = statuses.filter((s: any) =>
        ['COMPLETED', 'CANCELLED', 'CONFIRMED'].includes(s)
      );
      return terminal.length === orderIds.length;
    }, 120000, 2000);

    // Count confirmed/completed vs cancelled
    const ordersRes = await queryDb(
      `SELECT o.status FROM order_service.orders o
       JOIN order_service.order_items i ON i.order_id = o.id
       WHERE i.product_id = $1 AND o.id = ANY($2)`,
      [product.id, orderIds]
    );
    const statuses = ordersRes.rows.map((r: any) => r.status);
    const succeeded = statuses.filter((s: any) => ['CONFIRMED', 'COMPLETED'].includes(s)).length;
    const cancelled = statuses.filter((s: any) => s === 'CANCELLED').length;

    // Verify stock integrity — available_stock must never go negative
    const prodRow = await queryDb(
      'SELECT total_stock, reserved_stock FROM inventory_service.products WHERE id=$1',
      [product.id]
    );
    const available = prodRow.rows[0].total_stock - prodRow.rows[0].reserved_stock;
    expect(available).toBeGreaterThanOrEqual(0);

    // At most initialAvailable orders should succeed
    expect(succeeded).toBeLessThanOrEqual(initialAvailable);
    expect(succeeded + cancelled).toBe(orderIds.length);

    console.log(`Concurrency test: ${succeeded} succeeded, ${cancelled} cancelled, available=${available}`);
  });
});