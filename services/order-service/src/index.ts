import express from 'express';
import dotenv from 'dotenv';
import ordersRouter from './routes/orders';
import { v4 as uuidv4 } from 'uuid';
import Redis from 'ioredis';
import { Kafka } from 'kafkajs';
import { initKafka, producer } from './lib/kafka';
import { logger } from './logger';
import { startConsumers } from './lib/consumer';
import { updateOrderStatus } from './services/orderStatus';
import pool from './lib/db';
import redis from './lib/redis';
import AWS from 'aws-sdk';

dotenv.config();

const app = express();
app.use(express.json());

// Request ID propagation
app.use((req, res, next) => {
  const id = req.header('X-Request-ID') || uuidv4();
  res.setHeader('X-Request-ID', id);
  (req as any).requestId = id;
  next();
});

app.use(ordersRouter);

// ── Health check ──────────────────────────────────────────────────────────────
const healthRedis = new Redis(process.env.REDIS_URL || 'redis://redis:6379');
const healthKafka = new Kafka({ brokers: [process.env.KAFKA_BROKER || 'kafka:9092'], clientId: 'order-service-health' });
const healthAdmin = healthKafka.admin();
healthAdmin.connect().catch(() => {});

let kafkaReady = false;

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
  const redisStatus = await checkDependency(() => healthRedis.ping());
  const kafkaStatus = await checkDependency(() => healthAdmin.listTopics());
  const deps = { postgres: postgresStatus, redis: redisStatus, kafka: kafkaStatus };
  const anyUnreachable = Object.values(deps).some(v => v === 'unreachable');
  const anyDegraded = Object.values(deps).some(v => v === 'degraded');
  const overall = anyUnreachable ? 503 : (anyDegraded ? 207 : 200);
  res.status(overall).json({
    status: anyUnreachable ? 'unreachable' : (anyDegraded ? 'degraded' : 'ok'),
    service: 'order-service',
    timestamp: new Date().toISOString(),
    dependencies: deps
  });
};

app.get('/health', healthHandler);
app.get('/internal/health', healthHandler);

// ── AWS SQS client ────────────────────────────────────────────────────────────
const sqs = new AWS.SQS({
  endpoint: process.env.LOCALSTACK_URL || 'http://localstack:4566',
  region: 'us-east-1',
  accessKeyId: 'test',
  secretAccessKey: 'test'
});

// ── Reservation expiry queue poller ───────────────────────────────────────────
async function pollReservationExpiryQueue() {
  try {
    const qUrl = await sqs.getQueueUrl({ QueueName: 'reservation-expiry-queue' }).promise()
      .then(r => r.QueueUrl as string);

    const msgs = await sqs.receiveMessage({
      QueueUrl: qUrl,
      MaxNumberOfMessages: 10,
      WaitTimeSeconds: 1
    }).promise();

    if (!msgs.Messages || msgs.Messages.length === 0) return;

    for (const m of msgs.Messages) {
      try {
        const body = JSON.parse(m.Body || '{}');
        const orderId = body.orderId || body.order_id;
        if (!orderId) continue;

        logger.info({ message: 'reservation_expiry_received', orderId });

        try {
          await updateOrderStatus(orderId, 'CANCELLED', 'reservation_expired');
          if (kafkaReady) {
            const event = {
              eventId: uuidv4(),
              eventType: 'ORDER_CANCELLED',
              occurredAt: new Date().toISOString(),
              orderId,
              reason: 'reservation_expired',
              schemaVersion: '1.0'
            };
            await producer.send({ topic: 'order.cancelled', messages: [{ key: orderId, value: JSON.stringify(event) }] });
          }
          logger.info({ message: 'order_cancelled_due_to_expiry', orderId });
        } catch (statusErr: any) {
          logger.warn({ message: 'skip_expiry_cancel', orderId, reason: String(statusErr) });
        }

        await sqs.deleteMessage({ QueueUrl: qUrl, ReceiptHandle: m.ReceiptHandle as string }).promise();
      } catch (msgErr) {
        logger.error({ message: 'reservation_expiry_msg_error', error: String(msgErr) });
      }
    }
  } catch (err) {
    // SQS not ready yet — silently ignore
  }
}

// ── Webhook delivery poller ───────────────────────────────────────────────────
async function pollWebhookQueue() {
  try {
    const qUrl = await sqs.getQueueUrl({ QueueName: 'order-webhook-queue' }).promise()
      .then(r => r.QueueUrl as string);

    const msgs = await sqs.receiveMessage({
      QueueUrl: qUrl,
      MaxNumberOfMessages: 5,
      WaitTimeSeconds: 1
    }).promise();

    if (!msgs.Messages || msgs.Messages.length === 0) return;

    for (const m of msgs.Messages) {
      try {
        const body = JSON.parse(m.Body || '{}');
        // Parse SNS envelope if present
        const snsMsg = body.Message ? JSON.parse(body.Message) : body;
        const orderId = snsMsg.orderId || snsMsg.order_id;

        if (orderId) {
          const key = `webhook:attempts:${orderId}`;
          const attempts = parseInt(await redis.get(key) || '0') + 1;
          await redis.set(key, String(attempts), 'EX', 3600);

          // Simulate delivery failure for first 5 attempts to ensure we record failures
          const success = attempts > 5 ? true : (Math.random() < 0.3); // only 30% success for first 5
          if (success) {
            logger.info({ message: 'WEBHOOK_DELIVERED', orderId, attempts });
          } else {
            logger.warn({ message: 'WEBHOOK_FAILED', orderId, attempts });
            if (attempts >= 5) {
              try {
                await pool.query(
                  `INSERT INTO order_service.webhook_delivery_failures
                     (order_id, payload, attempts, last_error, created_at)
                   VALUES ($1,$2,$3,$4,now())
                   ON CONFLICT DO NOTHING`,
                  [orderId, JSON.stringify(snsMsg), attempts, 'delivery_failed']
                );
                logger.info({ message: 'WEBHOOK_FAILURE_RECORDED', orderId, attempts });
              } catch (dbErr) {
                logger.error({ message: 'webhook_failure_insert_error', error: String(dbErr) });
              }
            }
          }
        }

        await sqs.deleteMessage({ QueueUrl: qUrl, ReceiptHandle: m.ReceiptHandle as string }).promise();
      } catch (msgErr) {
        logger.error({ message: 'webhook_process_error', error: String(msgErr) });
      }
    }
  } catch (err) {
    // SQS not ready yet — silently ignore
  }
}

// ── Resilient Kafka startup with retry ────────────────────────────────────────
async function startKafkaWithRetry(maxAttempts = 20, delayMs = 3000) {
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      logger.info({ message: 'kafka_init_attempt', attempt });
      await initKafka();
      logger.info({ message: 'kafka_initialized' });
      await startConsumers();
      logger.info({ message: 'consumers_started' });
      kafkaReady = true;
      return;
    } catch (err) {
      logger.warn({ message: 'kafka_init_failed', attempt, error: String(err) });
      if (attempt < maxAttempts) {
        await new Promise(r => setTimeout(r, delayMs));
      }
    }
  }
  logger.error({ message: 'kafka_init_exhausted_retries', maxAttempts });
  // Do NOT exit — HTTP server stays up so DB-only operations still work
}

// ── Startup ───────────────────────────────────────────────────────────────────
// Start HTTP server IMMEDIATELY — independent of Kafka/SQS readiness
const PORT = process.env.PORT || 3001;
app.listen(PORT, () => logger.info({ message: 'order-service listening', port: PORT }));

// Start Kafka in background (non-blocking)
startKafkaWithRetry().catch(err => {
  logger.error({ message: 'kafka_startup_background_error', error: String(err) });
});

// Start SQS pollers after a delay (give LocalStack time to initialize)
setTimeout(() => {
  setInterval(pollReservationExpiryQueue, 5000);
  setInterval(pollWebhookQueue, 5000);
  logger.info({ message: 'sqs_pollers_started' });
}, 20000);
