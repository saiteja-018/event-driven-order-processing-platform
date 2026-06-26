import express from 'express';
import dotenv from 'dotenv';
import { Kafka } from 'kafkajs';
import Redis from 'ioredis';
import pool from './lib/db';
import { logger } from './logger';

dotenv.config();

const app = express();
app.use(express.json());

const kafka = new Kafka({ brokers: [process.env.KAFKA_BROKER || 'kafka:9092'], clientId: 'analytics-service' });
const consumer = kafka.consumer({ groupId: 'analytics-service-group' });
const admin = kafka.admin();
const redis = new Redis(process.env.REDIS_URL || 'redis://redis:6379');

// ── Health check ──────────────────────────────────────────────────────────────
async function checkDependency(fn: () => Promise<any>): Promise<string> {
  const start = Date.now();
  try {
    await Promise.race([fn(), new Promise((_, rej) => setTimeout(() => rej(new Error('timeout')), 5000))]);
    return (Date.now() - start) > 1000 ? 'degraded' : 'ok';
  } catch {
    return 'unreachable';
  }
}

const healthHandler = async (req: any, res: any) => {
  const postgresStatus = await checkDependency(() => pool.query('SELECT 1'));
  const redisStatus = await checkDependency(() => redis.ping());
  const kafkaStatus = await checkDependency(() => admin.listTopics());
  const deps = { postgres: postgresStatus, redis: redisStatus, kafka: kafkaStatus };
  const anyUnreachable = Object.values(deps).some(v => v === 'unreachable');
  const anyDegraded = Object.values(deps).some(v => v === 'degraded');
  const overall = anyUnreachable ? 503 : (anyDegraded ? 207 : 200);
  res.status(overall).json({
    status: anyUnreachable ? 'unreachable' : (anyDegraded ? 'degraded' : 'ok'),
    service: 'analytics-service',
    timestamp: new Date().toISOString(),
    dependencies: deps
  });
};

app.get('/health', healthHandler);
app.get('/internal/health', healthHandler);

// ── Analytics HTTP Endpoints ──────────────────────────────────────────────────

// GET /internal/analytics/metrics?from=<ISO8601>&to=<ISO8601>
// Returns hourly_order_metrics rows within the given time range. Max 7 days.
app.get('/internal/analytics/metrics', async (req, res) => {
  try {
    const { from, to } = req.query as { from?: string; to?: string };
    if (!from || !to) {
      return res.status(400).json({ error: 'Missing required query params: from, to' });
    }

    const fromDate = new Date(from);
    const toDate = new Date(to);
    if (isNaN(fromDate.getTime()) || isNaN(toDate.getTime())) {
      return res.status(400).json({ error: 'Invalid date format for from/to params' });
    }

    const rangeMs = toDate.getTime() - fromDate.getTime();
    const sevenDaysMs = 7 * 24 * 60 * 60 * 1000;
    if (rangeMs > sevenDaysMs) {
      return res.status(400).json({ error: 'Time range must not exceed 7 days' });
    }
    if (rangeMs < 0) {
      return res.status(400).json({ error: 'from must be before to' });
    }

    const { rows } = await pool.query(
      `SELECT id, hour_bucket, total_orders, total_revenue, successful_orders, failed_orders, avg_order_value, computed_at
       FROM analytics_service.hourly_order_metrics
       WHERE hour_bucket >= $1 AND hour_bucket <= $2
       ORDER BY hour_bucket ASC`,
      [fromDate.toISOString(), toDate.toISOString()]
    );

    return res.json(rows);
  } catch (err) {
    logger.error({ message: 'analytics_metrics_error', error: String(err) });
    return res.status(500).json({ error: 'internal_error' });
  }
});

// GET /internal/analytics/events?orderId=<uuid>
// Returns all event log rows for a given orderId ordered by processed_at ascending.
app.get('/internal/analytics/events', async (req, res) => {
  try {
    const { orderId } = req.query as { orderId?: string };
    if (!orderId) {
      return res.status(400).json({ error: 'Missing required query param: orderId' });
    }

    const { rows } = await pool.query(
      `SELECT id, event_type, order_id, user_id, payload, kafka_offset, kafka_partition, kafka_topic, processed_at
       FROM analytics_service.order_events_log
       WHERE order_id = $1
       ORDER BY processed_at ASC`,
      [orderId]
    );

    return res.json(rows);
  } catch (err) {
    logger.error({ message: 'analytics_events_error', error: String(err) });
    return res.status(500).json({ error: 'internal_error' });
  }
});

// ── computeHourlyMetrics (also callable via Lambda and HTTP) ──────────────────
export async function computeHourlyMetrics() {
  const now = new Date();
  // Previous complete hour
  const end = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate(), now.getUTCHours(), 0, 0, 0));
  const start = new Date(end.getTime() - 60 * 60 * 1000);

  const { rows } = await pool.query(
    `SELECT event_type, payload
     FROM analytics_service.order_events_log
     WHERE processed_at >= $1 AND processed_at < $2
       AND event_type IN ('ORDER_CREATED','ORDER_COMPLETED','ORDER_CANCELLED')`,
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
      totalRevenue += Number(r.payload?.totalAmount || r.payload?.total_amount || 0);
    }
    if (r.event_type === 'ORDER_CANCELLED') failedOrders++;
  }

  const avgOrderValue = successfulOrders > 0 ? totalRevenue / successfulOrders : null;

  await pool.query(
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
    [start.toISOString(), totalOrders, totalRevenue, successfulOrders, failedOrders, avgOrderValue]
  );

  logger.info({ message: 'hourly_metrics_computed', hourBucket: start.toISOString(), totalOrders, successfulOrders, failedOrders, totalRevenue });
  return { hourBucket: start.toISOString(), totalOrders, successfulOrders, failedOrders, totalRevenue, avgOrderValue };
}

// POST /internal/analytics/compute-metrics
// Manually trigger hourly metrics computation (also invokable by Lambda)
app.post('/internal/analytics/compute-metrics', async (req, res) => {
  try {
    const result = await computeHourlyMetrics();
    return res.json({ status: 'ok', ...result });
  } catch (err) {
    logger.error({ message: 'compute_metrics_error', error: String(err) });
    return res.status(500).json({ error: 'internal_error' });
  }
});

// ── Startup ───────────────────────────────────────────────────────────────────
async function startKafkaWithRetry(maxAttempts = 20, delayMs = 3000) {
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      await admin.connect();
      await consumer.connect();

      // Subscribe to all application topics as a regex (avoiding internal binary topics)
      await consumer.subscribe({ topic: /^(order|inventory|payment|notification|dead\.letter\.queue).*/, fromBeginning: false });

      await consumer.run({
        autoCommit: false,
        eachMessage: async ({ topic, partition, message }) => {
          try {
            const value = message.value?.toString() || '{}';
            let event: any = {};
            try { event = JSON.parse(value); } catch { event = { raw: value }; }

            // Handle DLQ messages — persist to dead_letter_messages
            if (topic === 'dead.letter.queue') {
              try {
                await pool.query(
                  `INSERT INTO public.dead_letter_messages
                     (original_topic, original_partition, original_offset, failed_at, error_message, retry_count, payload, created_at)
                   VALUES ($1,$2,$3,$4,$5,$6,$7,now())`,
                  [
                    event.originalTopic || null,
                    event.originalPartition != null ? event.originalPartition : null,
                    event.originalOffset || null,
                    event.failedAt ? new Date(event.failedAt) : null,
                    event.errorMessage || null,
                    event.retryCount || 0,
                    event.payload != null ? (typeof event.payload === 'string' ? event.payload : JSON.stringify(event.payload)) : null
                  ]
                );
              } catch (dlqErr) {
                logger.error({ message: 'dlq_insert_error', error: String(dlqErr) });
              }
            }

            // Always insert into order_events_log for every topic
            await pool.query(
              `INSERT INTO analytics_service.order_events_log
                 (event_type, order_id, user_id, payload, kafka_offset, kafka_partition, kafka_topic, processed_at)
               VALUES ($1,$2,$3,$4,$5,$6,$7,now())`,
              [
                event.eventType || event.event_type || topic,
                event.orderId || event.order_id || null,
                event.userId || event.user_id || null,
                event,
                message.offset ? parseInt(message.offset) : null,
                partition,
                topic
              ]
            );

            await consumer.commitOffsets([{ topic, partition, offset: (Number(message.offset) + 1).toString() }]);
          } catch (err) {
            logger.error({ message: 'analytics_consume_error', topic, error: String(err) });
            // Still commit to avoid getting stuck
            try {
              await consumer.commitOffsets([{ topic, partition, offset: (Number(message.offset) + 1).toString() }]);
            } catch { /* ignore */ }
          }
        }
      });

      logger.info({ message: 'analytics-service kafka started', attempt });
      return;
    } catch (err) {
      logger.warn({ message: 'analytics_kafka_init_failed', attempt, error: String(err) });
      if (attempt < maxAttempts) {
        await new Promise(r => setTimeout(r, delayMs));
      }
    }
  }
  logger.error({ message: 'analytics_kafka_init_exhausted_retries' });
}

async function start() {
  // Start Kafka with retry (non-blocking after initial connection)
  await startKafkaWithRetry();

  // Run computeHourlyMetrics on startup and every hour
  setTimeout(async () => {
    try {
      await computeHourlyMetrics();
      logger.info({ message: 'initial_hourly_metrics_computed' });
    } catch (err) {
      logger.warn({ message: 'initial_hourly_metrics_failed', error: String(err) });
    }
  }, 5000);

  setInterval(async () => {
    try {
      await computeHourlyMetrics();
    } catch (err) {
      logger.warn({ message: 'periodic_hourly_metrics_failed', error: String(err) });
    }
  }, 60 * 60 * 1000); // every hour

  logger.info({ message: 'analytics-service started' });
}

// Start HTTP server immediately
const PORT = process.env.PORT || 3005;
app.listen(PORT, () => logger.info({ message: 'analytics-service listening', port: PORT }));

start().catch(err => {
  logger.error({ message: 'startup_error', error: String(err) });
  // Don't exit — HTTP server stays up
});
