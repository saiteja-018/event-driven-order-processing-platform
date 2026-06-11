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

const kafka = new Kafka({ brokers: [process.env.KAFKA_BROKER || 'kafka:9092'], clientId: 'payment-service' });
const consumer = kafka.consumer({ groupId: 'payment-service-group' });
const producer = kafka.producer();
const admin = kafka.admin();

const redis = new Redis(process.env.REDIS_URL || 'redis://redis:6379');

const sqs = new AWS.SQS({
  endpoint: process.env.LOCALSTACK_URL || 'http://localstack:4566',
  region: 'us-east-1',
  accessKeyId: 'test',
  secretAccessKey: 'test'
});

function delay(ms: number) { return new Promise(r => setTimeout(r, ms)); }

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
    service: 'payment-service',
    timestamp: new Date().toISOString(),
    dependencies: deps
  });
};

app.get('/health', healthHandler);
app.get('/internal/health', healthHandler);

// ── Main startup ──────────────────────────────────────────────────────────────
async function start() {
  await admin.connect();
  await producer.connect();
  await consumer.connect();
  await consumer.subscribe({ topic: 'order.confirmed', fromBeginning: false });

  await consumer.run({
    autoCommit: false,
    eachMessage: async ({ topic, partition, message }) => {
      const retryDelays = [1000, 2000, 4000];
      let lastErr: any = null;

      for (let attempt = 0; attempt < 3; attempt++) {
        try {
          const v = message.value?.toString();
          if (!v) {
            await consumer.commitOffsets([{ topic, partition, offset: (Number(message.offset) + 1).toString() }]);
            return;
          }

          const event = JSON.parse(v);

          // Consumer idempotency
          const eventId = event.eventId;
          if (eventId) {
            const consumerId = `payment-service-group:${topic}`;
            const processedKey = `processed_event:${consumerId}:${eventId}`;
            if (await redis.get(processedKey)) {
              await consumer.commitOffsets([{ topic, partition, offset: (Number(message.offset) + 1).toString() }]);
              return;
            }
          }

          // Idempotency: check whether a non-failed/cancelled transaction already exists
          const { rows } = await pool.query(
            "SELECT id, status FROM payment_service.payment_transactions WHERE order_id=$1 AND status NOT IN ('FAILED','CANCELLED')",
            [event.orderId]
          );
          if (rows.length === 0) {
            const txId = uuidv4();
            const idempotencyKey = event.idempotencyKey || txId;

            await pool.query(
              `INSERT INTO payment_service.payment_transactions
                 (id, order_id, amount, currency, status, idempotency_key)
               VALUES ($1,$2,$3,$4,'INITIATED',$5)
               ON CONFLICT (idempotency_key) DO NOTHING`,
              [txId, event.orderId, event.totalAmount || 0, event.currency || 'USD', idempotencyKey]
            );

            const msgBody = {
              transactionId: txId,
              orderId: event.orderId,
              amount: event.totalAmount || 0,
              currency: event.currency || 'USD',
              userId: event.userId || null,
              idempotencyKey
            };

            // Send to payment-processing-queue
            const qUrl = await sqs.getQueueUrl({ QueueName: 'payment-processing-queue' }).promise().then(r => r.QueueUrl as string);
            await sqs.sendMessage({ QueueUrl: qUrl, MessageBody: JSON.stringify(msgBody) }).promise();

            // Publish payment.initiated to Kafka
            const initiatedEvent = {
              eventId: uuidv4(),
              eventType: 'PAYMENT_INITIATED',
              occurredAt: new Date().toISOString(),
              transactionId: txId,
              orderId: event.orderId,
              amount: event.totalAmount || 0,
              currency: event.currency || 'USD',
              userId: event.userId || null,
              idempotencyKey,
              schemaVersion: '1.0'
            };
            await producer.send({ topic: 'payment.initiated', messages: [{ key: event.orderId, value: JSON.stringify(initiatedEvent) }] });
          }

          // Mark as processed
          if (eventId) {
            const consumerId = `payment-service-group:${topic}`;
            await redis.set(`processed_event:${consumerId}:${eventId}`, '1', 'EX', 3600);
          }

          await consumer.commitOffsets([{ topic, partition, offset: (Number(message.offset) + 1).toString() }]);
          return;
        } catch (err: any) {
          lastErr = err;
          logger.error({ message: 'consumer_error', topic, attempt, error: String(err) });
          if (attempt < 2) await delay(retryDelays[attempt]);
        }
      }

      // DLQ routing
      try {
        await producer.send({
          topic: 'dead.letter.queue',
          messages: [{
            key: `${topic}-${partition}-${message.offset}`,
            value: JSON.stringify({
              originalTopic: topic,
              originalPartition: partition,
              originalOffset: String(message.offset),
              failedAt: new Date().toISOString(),
              errorMessage: String(lastErr),
              retryCount: 3,
              payload: message.value?.toString() ? JSON.parse(message.value.toString()) : null
            })
          }]
        });
      } catch (dlqErr) {
        logger.error({ message: 'dlq_publish_failed', error: String(dlqErr) });
      }

      await consumer.commitOffsets([{ topic, partition, offset: (Number(message.offset) + 1).toString() }]);
    }
  });

  // ── SQS payment-processing-queue poller ────────────────────────────────────
  // Processes payments directly instead of relying on the LocalStack Lambda
  // which may not have reliable Kafka connectivity.
  setTimeout(() => {
    setInterval(pollPaymentProcessingQueue, 3000);
    logger.info({ message: 'payment_processing_sqs_poller_started' });
  }, 10000);

  logger.info({ message: 'payment-service started' });
}

// ── Payment Processing SQS Poller ─────────────────────────────────────────────
async function pollPaymentProcessingQueue() {
  try {
    const qUrl = await sqs.getQueueUrl({ QueueName: 'payment-processing-queue' }).promise()
      .then(r => r.QueueUrl as string);

    const result = await sqs.receiveMessage({
      QueueUrl: qUrl,
      MaxNumberOfMessages: 5,
      WaitTimeSeconds: 1
    }).promise();

    if (!result.Messages || result.Messages.length === 0) return;

    for (const m of result.Messages) {
      try {
        const body = typeof m.Body === 'string' ? JSON.parse(m.Body) : m.Body;
        const txId = body.transactionId || body.transaction_id;
        const orderId = body.orderId || body.order_id;
        const amount = parseFloat(body.amount || 0);
        const currency = body.currency || 'USD';
        const userId = body.userId || body.user_id || null;
        const idempotencyKey = body.idempotencyKey || body.idempotency_key || txId;

        if (!txId || !orderId) {
          logger.error({ message: 'payment_processing_missing_fields', body });
          await sqs.deleteMessage({ QueueUrl: qUrl, ReceiptHandle: m.ReceiptHandle as string }).promise();
          continue;
        }

        // Determine payment outcome based on amount
        let success = true;
        if (amount > 10000) {
          success = false;
        } else if (amount >= 5000 && amount <= 10000) {
          success = Math.random() >= 0.3; // 70% success rate
        }

        if (success) {
          const providerRef = 'PAY-' + Math.random().toString(36).substring(2, 10).toUpperCase();
          await pool.query(
            "UPDATE payment_service.payment_transactions SET status='SUCCEEDED', provider_reference=$1, updated_at=now() WHERE id=$2",
            [providerRef, txId]
          );

          const evt = {
            eventId: uuidv4(),
            eventType: 'PAYMENT_SUCCEEDED',
            occurredAt: new Date().toISOString(),
            transactionId: txId,
            orderId,
            userId,
            amount,
            currency,
            providerReference: providerRef,
            idempotencyKey,
            schemaVersion: '1.0'
          };

          await producer.send({ topic: 'payment.succeeded', messages: [{ key: orderId, value: JSON.stringify(evt) }] });
          logger.info({ message: 'payment_succeeded', orderId, txId });
        } else {
          await pool.query(
            "UPDATE payment_service.payment_transactions SET status='FAILED', failure_reason=$1, retry_count = retry_count + 1, updated_at=now() WHERE id=$2",
            ['simulated_failure', txId]
          );

          const { rows } = await pool.query(
            'SELECT retry_count FROM payment_service.payment_transactions WHERE id=$1',
            [txId]
          );
          const retryCount = rows[0] ? rows[0].retry_count : 1;

          const evt = {
            eventId: uuidv4(),
            eventType: 'PAYMENT_FAILED',
            occurredAt: new Date().toISOString(),
            transactionId: txId,
            orderId,
            userId,
            amount,
            currency,
            failureReason: 'simulated_failure',
            retryCount,
            idempotencyKey,
            schemaVersion: '1.0'
          };

          await producer.send({ topic: 'payment.failed', messages: [{ key: orderId, value: JSON.stringify(evt) }] });
          logger.info({ message: 'payment_failed', orderId, txId, retryCount });
        }

        await sqs.deleteMessage({ QueueUrl: qUrl, ReceiptHandle: m.ReceiptHandle as string }).promise();
      } catch (msgErr) {
        logger.error({ message: 'payment_processing_msg_error', error: String(msgErr) });
      }
    }
  } catch (err) {
    // SQS not ready yet or other error - will retry on next interval
    logger.error({ message: 'payment_processing_poll_error', error: String(err) });
  }
}

start().catch(err => {
  logger.error({ message: 'startup_error', error: String(err) });
  process.exit(1);
});

const PORT = process.env.PORT || 3003;
app.listen(PORT, () => logger.info({ message: 'payment-service listening', port: PORT }));
