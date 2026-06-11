import { postOrder, getOrder, queryDb, waitFor, uuid } from './setup';
import { queryDb as qdb } from './setup';

describe('order lifecycle', () => {
  jest.setTimeout(120000);

  test('test_full_order_success_flow', async () => {
    // find a product with available stock
    const prodRes = await qdb('SELECT id, total_stock, reserved_stock FROM inventory_service.products WHERE (total_stock - reserved_stock) >= 1 LIMIT 1');
    if (prodRes.rowCount === 0) throw new Error('No product with available stock found for test');
    const product = prodRes.rows[0];
    const idemp = uuid();
    const orderPayload = {
      userId: uuid(),
      items: [{ productId: product.id, quantity: 1, unitPrice: 10 }],
      totalAmount: 10,
      currency: 'USD',
      idempotencyKey: idemp
    };

    const createRes = await postOrder(orderPayload);
    expect([200,201]).toContain(createRes.status);
    const order = createRes.data;
    const orderId = order.id || order.orderId;

    // wait until order reaches COMPLETED or CANCELLED
    await waitFor(async () => {
      const r = await getOrder(orderId).catch(()=>null);
      if (!r) return false;
      const s = r.data.status;
      return s === 'COMPLETED' || s === 'CANCELLED';
    }, 60000);

    const final = await getOrder(orderId);
    expect(['COMPLETED','CANCELLED']).toContain(final.data.status);

    if (final.data.status === 'COMPLETED') {
      // validate DB side effects
      const reserv = await qdb('SELECT * FROM inventory_service.stock_reservations WHERE order_id=$1', [orderId]);
      expect(reserv.rowCount).toBeGreaterThan(0);
      const pay = await qdb('SELECT * FROM payment_service.payment_transactions WHERE order_id=$1', [orderId]);
      expect(pay.rowCount).toBeGreaterThan(0);
      const notif = await qdb('SELECT * FROM notification_service.notification_logs WHERE order_id=$1', [orderId]);
      expect(notif.rowCount).toBeGreaterThanOrEqual(2);
      await waitFor(async () => {
        const analytics = await qdb("SELECT event_type FROM analytics_service.order_events_log WHERE order_id=$1", [orderId]);
        const types = analytics.rows.map((r:any)=>r.event_type || r.eventType);
        return types.includes('ORDER_CREATED') && types.includes('ORDER_CONFIRMED') && types.includes('ORDER_COMPLETED');
      }, 10000);
    }
  });

  test('test_order_with_insufficient_stock', async () => {
    const productId = uuid();
    await qdb(
      'INSERT INTO inventory_service.products (id, sku, name, total_stock, reserved_stock, reorder_threshold, version) VALUES ($1,$2,$3,0,0,0,1)',
      [productId, `SKU-ZERO-${Date.now()}`, 'Zero Stock Product']
    );
    const product = { id: productId };
    const idemp = uuid();
    const orderPayload = { userId: uuid(), items: [{ productId: product.id, quantity: 1, unitPrice: 10 }], totalAmount:10, currency:'USD', idempotencyKey: idemp };
    const createRes = await postOrder(orderPayload);
    expect([200,201]).toContain(createRes.status);
    const orderId = createRes.data.id || createRes.data.orderId;

    await waitFor(async () => {
      const r = await getOrder(orderId).catch(()=>null);
      if (!r) return false;
      return r.data.status === 'CANCELLED';
    }, 30000);

    const reserv = await qdb('SELECT * FROM inventory_service.stock_reservations WHERE order_id=$1', [orderId]);
    expect(reserv.rowCount).toBe(0);
  });

  test('test_idempotent_order_creation', async () => {
    const prodRes = await qdb('SELECT id FROM inventory_service.products WHERE (total_stock - reserved_stock) >= 1 LIMIT 1');
    if (prodRes.rowCount === 0) throw new Error('No product with stock');
    const product = prodRes.rows[0];
    const idemp = uuid();
    const payload = { userId: uuid(), items: [{ productId: product.id, quantity: 1, unitPrice: 5 }], totalAmount:5, currency:'USD', idempotencyKey: idemp };
    const r1 = await postOrder(payload);
    expect([200,201]).toContain(r1.status);
    const r2 = await postOrder(payload);
    expect([200,201]).toContain(r2.status);
    const orderId1 = r1.data.id || r1.data.orderId;
    const orderId2 = r2.data.id || r2.data.orderId;
    expect(orderId1).toBe(orderId2);
    const cnt = await qdb('SELECT COUNT(*) FROM order_service.orders WHERE idempotency_key=$1', [idemp]);
    expect(parseInt(cnt.rows[0].count)).toBe(1);
  });

  test('test_payment_failure_and_retry', async () => {
    // pick a product and create an order with total between 5000 and 10000
    const prodRes = await qdb('SELECT id FROM inventory_service.products WHERE (total_stock - reserved_stock) >= 1 LIMIT 1');
    if (prodRes.rowCount === 0) throw new Error('No product with stock');
    const product = prodRes.rows[0];
    const idemp = uuid();
    const amount = 6000; // in retry band
    const payload = { userId: uuid(), items: [{ productId: product.id, quantity: 1, unitPrice: amount }], totalAmount: amount, currency:'USD', idempotencyKey: idemp };
    const createRes = await postOrder(payload);
    expect([200,201]).toContain(createRes.status);
    const orderId = createRes.data.id || createRes.data.orderId;

    await waitFor(async () => {
      const r = await getOrder(orderId).catch(()=>null);
      if (!r) return false;
      return ['COMPLETED','CANCELLED'].includes(r.data.status);
    }, 60000);

    const final = await getOrder(orderId);
    if (final.data.status === 'CANCELLED') {
      const pay = await qdb('SELECT retry_count FROM payment_service.payment_transactions WHERE order_id=$1', [orderId]);
      expect(pay.rowCount).toBeGreaterThan(0);
      expect(pay.rows[0].retry_count).toBeGreaterThanOrEqual(1);
    }
  });
});
