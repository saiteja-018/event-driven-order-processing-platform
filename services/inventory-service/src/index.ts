import express from 'express';
import dotenv from 'dotenv';
import { v4 as uuidv4 } from 'uuid';
import { Kafka } from 'kafkajs';
import Redis from 'ioredis';
import AWS from 'aws-sdk';
import pool from './lib/db';
import { initKafka } from './lib/kafka';
import { startConsumers } from './lib/consumer';
import productsRouter from './routes/products';
import { logger } from './logger';

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

app.use(productsRouter);

// ── Health check ──────────────────────────────────────────────────────────────
const healthRedis = new Redis(process.env.REDIS_URL || 'redis://redis:6379');
const healthKafka = new Kafka({ brokers: [process.env.KAFKA_BROKER || 'kafka:9092'], clientId: 'inventory-service-health' });
const healthAdmin = healthKafka.admin();
healthAdmin.connect().catch(() => {});

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
    service: 'inventory-service',
    timestamp: new Date().toISOString(),
    dependencies: deps
  });
};

app.get('/health', healthHandler);
app.get('/internal/health', healthHandler);

// ── Reservation-expiry-queue SQS poller ──────────────────────────────────────
// (This is also handled by the order-service, but inventory-service updates the
//  reservation status. The SQS message is consumed by order-service for cancellation.)

// Start HTTP server IMMEDIATELY
const PORT = process.env.PORT || 3002;
app.listen(PORT, () => logger.info({ message: 'inventory-service listening', port: PORT }));

// Start Kafka consumers in background with retry
async function startWithRetry(maxAttempts = 20, delayMs = 3000) {
  for (let attempt = 1; attempt <= maxAttempts; attempt++) {
    try {
      await initKafka();
      logger.info({ message: 'inventory-service kafka initialized', attempt });
      await startConsumers();
      logger.info({ message: 'inventory-service consumers started', attempt });
      return;
    } catch (err) {
      logger.warn({ message: 'inventory_service_start_failed', attempt, error: String(err) });
      if (attempt < maxAttempts) {
        await new Promise(r => setTimeout(r, delayMs));
      }
    }
  }
  logger.error({ message: 'inventory_service_start_exhausted_retries' });
}

startWithRetry().catch(err => {
  logger.error({ message: 'startup_background_error', error: String(err) });
});
