exports.handler = async function(event) {
  console.log('compute-hourly-metrics lambda invoked');
  const { Client } = require('pg');
  const dbUrl = process.env.DATABASE_URL || 'postgres://postgres:postgres@postgres:5432/app';
  const client = new Client({ connectionString: dbUrl });
  await client.connect();
  try {
    const now = new Date();
    const end = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate(), now.getUTCHours(), 0, 0, 0));
    const start = new Date(end.getTime() - 60 * 60 * 1000);

    const { rows } = await client.query(
      `SELECT event_type, payload FROM analytics_service.order_events_log WHERE processed_at >= $1 AND processed_at < $2 AND event_type IN ('ORDER_CREATED','ORDER_COMPLETED','ORDER_CANCELLED')`,
      [start.toISOString(), end.toISOString()]
    );

    let totalOrders = 0;
    let successfulOrders = 0;
    let failedOrders = 0;
    let totalRevenue = 0;

    for (const r of rows) {
      if (r.event_type === 'ORDER_CREATED') totalOrders++;
      if (r.event_type === 'ORDER_COMPLETED') {
        successfulOrders++;
        const amount = r.payload?.totalAmount || r.payload?.total_amount || 0;
        totalRevenue += Number(amount || 0);
      }
      if (r.event_type === 'ORDER_CANCELLED') failedOrders++;
    }
    const avgOrderValue = successfulOrders > 0 ? totalRevenue / successfulOrders : null;

    await client.query(
      `INSERT INTO analytics_service.hourly_order_metrics (hour_bucket, total_orders, total_revenue, successful_orders, failed_orders, avg_order_value, computed_at)
       VALUES ($1,$2,$3,$4,$5,$6, now())
       ON CONFLICT (hour_bucket) DO UPDATE SET total_orders=EXCLUDED.total_orders, total_revenue=EXCLUDED.total_revenue, successful_orders=EXCLUDED.successful_orders, failed_orders=EXCLUDED.failed_orders, avg_order_value=EXCLUDED.avg_order_value, computed_at=now()`,
      [start.toISOString(), totalOrders, totalRevenue, successfulOrders, failedOrders, avgOrderValue]
    );
  } catch (err) {
    console.error('compute-hourly-metrics failed', err);
  } finally {
    await client.end();
  }
  return {status: 'ok'};
};
