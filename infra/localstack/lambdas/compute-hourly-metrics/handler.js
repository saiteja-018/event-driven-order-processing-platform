exports.handler = async function(event) {
  console.log('compute-hourly-metrics lambda invoked', JSON.stringify(event));
  const { Client } = require('pg');
  const dbUrl = process.env.DATABASE_URL || 'postgres://postgres:postgres@postgres:5432/app';
  const client = new Client({ connectionString: dbUrl });

  try {
    await client.connect();
  } catch (connErr) {
    console.error('Failed to connect to database:', connErr.message);
    return { status: 'error', error: connErr.message };
  }

  try {
    const now = new Date();

    // Compute metrics for the CURRENT partial hour (from the start of this hour to now)
    // AND the previous complete hour — whichever has data
    const bucketsToCompute = [];

    // Current hour bucket
    const currentHourStart = new Date(Date.UTC(
      now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate(),
      now.getUTCHours(), 0, 0, 0
    ));
    bucketsToCompute.push({ start: currentHourStart, end: now });

    // Previous complete hour bucket
    const prevHourEnd = currentHourStart;
    const prevHourStart = new Date(prevHourEnd.getTime() - 60 * 60 * 1000);
    bucketsToCompute.push({ start: prevHourStart, end: prevHourEnd });

    for (const bucket of bucketsToCompute) {
      const { rows } = await client.query(
        `SELECT event_type, payload FROM analytics_service.order_events_log
         WHERE processed_at >= $1 AND processed_at < $2
           AND event_type IN ('ORDER_CREATED','ORDER_COMPLETED','ORDER_CANCELLED')`,
        [bucket.start.toISOString(), bucket.end.toISOString()]
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

      console.log(`Bucket ${bucket.start.toISOString()}: orders=${totalOrders}, completed=${successfulOrders}, cancelled=${failedOrders}, revenue=${totalRevenue}`);

      await client.query(
        `INSERT INTO analytics_service.hourly_order_metrics
           (hour_bucket, total_orders, total_revenue, successful_orders, failed_orders, avg_order_value, computed_at)
         VALUES ($1,$2,$3,$4,$5,$6,now())
         ON CONFLICT (hour_bucket) DO UPDATE SET
           total_orders = EXCLUDED.total_orders,
           total_revenue = EXCLUDED.total_revenue,
           successful_orders = EXCLUDED.successful_orders,
           failed_orders = EXCLUDED.failed_orders,
           avg_order_value = EXCLUDED.avg_order_value,
           computed_at = now()`,
        [bucket.start.toISOString(), totalOrders, totalRevenue, successfulOrders, failedOrders, avgOrderValue]
      );
    }

    console.log('compute-hourly-metrics completed successfully');
    return { status: 'ok', bucketsComputed: bucketsToCompute.length };
  } catch (err) {
    console.error('compute-hourly-metrics failed:', err.message || err);
    return { status: 'error', error: err.message };
  } finally {
    try { await client.end(); } catch (e) { /* ignore */ }
  }
};
