import express from 'express';
import dotenv from 'dotenv';
import { Kafka } from 'kafkajs';
import Redis from 'ioredis';
import AWS from 'aws-sdk';
import { v4 as uuidv4 } from 'uuid';
import pool from './lib/db';
import { logger } from './logger';

dotenv.config();

const app = express();
app.use(express.json());

const sqs = new AWS.SQS({
  endpoint: process.env.LOCALSTACK_URL || 'http://localstack:4566',
  region: 'us-east-1',
  accessKeyId: 'test',
  secretAccessKey: 'test'
});

const redis = new Redis(process.env.REDIS_URL || 'redis://redis:6379');

const kafka = new Kafka({ brokers: [process.env.KAFKA_BROKER || 'kafka:9092'], clientId: 'notification-service' });
const consumer = kafka.consumer({ groupId: 'notification-service-group' });
const admin = kafka.admin();

// ── Template resolver ─────────────────────────────────────────────────────────
export function resolveTemplate(templateKey: string, payload: Record<string, any>): string {
  switch (templateKey) {
    case 'ORDER_CREATED':
      return `Your order ${payload.orderId} has been placed successfully for ${payload.totalAmount} ${payload.currency}.`;
    case 'ORDER_CONFIRMED':
      return `Your order ${payload.orderId} has been confirmed and is being processed.`;
    case 'ORDER_COMPLETED':
      return `Your order ${payload.orderId} has been delivered. Thank you for shopping with us.`;
    case 'ORDER_CANCELLED':
      return `Your order ${payload.orderId} has been cancelled. Reason: ${payload.reason}.`;
    case 'PAYMENT_FAILED':
      return `Payment for your order ${payload.orderId} failed. We will retry automatically.`;
    case 'LOW_STOCK_ALERT':
      return `Product ${payload.sku} is running low on stock. Available: ${payload.availableStock}.`;
    default:
      return `Notification for ${templateKey}`;
  }
}

// ── processNotification ───────────────────────────────────────────────────────
async function processNotification(job: {
  userId: string;
  orderId?: string | null;
  channel: string;
  templateKey: string;
  payload: Record<string, any>;
}) {
  const { userId, orderId, channel, templateKey, payload } = job;

  // Deduplication: skip if already processed within 60s
  const dedupKey = `notif:dedup:${orderId || 'none'}:${templateKey}`;
  if (await redis.get(dedupKey)) {
    logger.info({ message: 'notification_dedup_skip', orderId, templateKey });
    return;
  }

  let logId: string | null = null;
  try {
    // Insert notification log with PROCESSING status
    const insertRes = await pool.query(
      `INSERT INTO notification_service.notification_logs
         (id, user_id, order_id, channel, template_key, payload, status, created_at)
       VALUES ($1,$2,$3,$4,$5,$6,'PROCESSING',now())
       RETURNING id`,
      [uuidv4(), userId, orderId || null, channel, templateKey, payload]
    );
    logId = insertRes.rows[0]?.id;

    const text = resolveTemplate(templateKey, payload);
    logger.info({ message: 'sending_notification', channel, templateKey, orderId, text });

    // Update to SENT
    await pool.query(
      "UPDATE notification_service.notification_logs SET status='SENT', sent_at=now() WHERE id=$1",
      [logId]
    );

    // Set dedup key
    await redis.set(dedupKey, '1', 'EX', 60);
  } catch (err) {
    logger.error({ message: 'notification_process_error', error: String(err), orderId, templateKey });
    if (logId) {
      try {
        await pool.query(
          "UPDATE notification_service.notification_logs SET status='FAILED' WHERE id=$1",
          [logId]
        );
      } catch (updateErr) { /* ignore */ }
    }
  }
}

// ── SQS queue polling helper ──────────────────────────────────────────────────
async function pollSqsQueue(queueName: string, channel: string) {
  try {
    const qUrl = await sqs.getQueueUrl({ QueueName: queueName }).promise().then(r => r.QueueUrl as string);
    const result = await sqs.receiveMessage({
      QueueUrl: qUrl,
      MaxNumberOfMessages: 10,
      WaitTimeSeconds: 1
    }).promise();

    if (!result.Messages || result.Messages.length === 0) return;

    for (const m of result.Messages) {
      try {
        const body = JSON.parse(m.Body || '{}');
        // Handle SNS envelope
        const parsed = body.Message ? JSON.parse(body.Message) : body;

        await processNotification({
          userId: parsed.userId || parsed.user_id || 'system',
          orderId: parsed.orderId || parsed.order_id || null,
          channel,
          templateKey: parsed.templateKey || parsed.template_key || 'ORDER_CREATED',
          payload: parsed.payload || parsed
        });

        await sqs.deleteMessage({ QueueUrl: qUrl, ReceiptHandle: m.ReceiptHandle as string }).promise();
      } catch (msgErr) {
        logger.error({ message: 'sqs_message_error', queue: queueName, error: String(msgErr) });
      }
    }
  } catch (err) {
    logger.error({ message: 'sqs_poll_error', queue: queueName, error: String(err) });
  }
}

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
    service: 'notification-service',
    timestamp: new Date().toISOString(),
    dependencies: deps
  });
};

app.get('/health', healthHandler);
app.get('/internal/health', healthHandler);

// ── Startup ───────────────────────────────────────────────────────────────────
async function start() {
  await admin.connect();
  await consumer.connect();
  await consumer.subscribe({ topic: 'notification.dispatch', fromBeginning: false });

  await consumer.run({
    autoCommit: false,
    eachMessage: async ({ topic, partition, message }) => {
      try {
        const value = message.value?.toString();
        if (!value) {
          await consumer.commitOffsets([{ topic, partition, offset: (Number(message.offset) + 1).toString() }]);
          return;
        }

        const payload = JSON.parse(value);

        // Consumer idempotency
        if (payload.eventId) {
          const consumerId = 'notification-service-group:notification.dispatch';
          const processedKey = `processed_event:${consumerId}:${payload.eventId}`;
          if (await redis.get(processedKey)) {
            await consumer.commitOffsets([{ topic, partition, offset: (Number(message.offset) + 1).toString() }]);
            return;
          }
          await processNotification({
            userId: payload.userId || 'system',
            orderId: payload.orderId || null,
            channel: payload.channel || 'EMAIL',
            templateKey: payload.templateKey || 'ORDER_CREATED',
            payload: payload.payload || {}
          });
          await redis.set(processedKey, '1', 'EX', 3600);
        } else {
          await processNotification({
            userId: payload.userId || 'system',
            orderId: payload.orderId || null,
            channel: payload.channel || 'EMAIL',
            templateKey: payload.templateKey || 'ORDER_CREATED',
            payload: payload.payload || {}
          });
        }

        await consumer.commitOffsets([{ topic, partition, offset: (Number(message.offset) + 1).toString() }]);
      } catch (err) {
        logger.error({ message: 'kafka_notification_error', error: String(err) });
        await consumer.commitOffsets([{ topic, partition, offset: (Number(message.offset) + 1).toString() }]);
      }
    }
  });

  // Poll SQS queues every 2 seconds
  setInterval(() => pollSqsQueue('notification-email-queue', 'EMAIL'), 2000);
  setInterval(() => pollSqsQueue('notification-sms-queue', 'SMS'), 2000);

  logger.info({ message: 'notification-service started' });
}

// Start HTTP server IMMEDIATELY
const PORT = process.env.PORT || 3004;
app.listen(PORT, () => logger.info({ message: 'notification-service listening', port: PORT }));

// Start Kafka + SQS pollers in background with retry
async function startWithRetry(maxAttempts = 20, delayMs = 3000) {
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      await start();
      return;
    } catch (err) {
      logger.warn({ message: 'notification_service_start_failed', attempt, error: String(err) });
      if (attempt < maxAttempts) {
        await new Promise(r => setTimeout(r, delayMs));
      }
    }
  }
  logger.error({ message: 'notification_service_start_exhausted_retries' });
}

startWithRetry().catch(err => {
  logger.error({ message: 'startup_background_error', error: String(err) });
});
