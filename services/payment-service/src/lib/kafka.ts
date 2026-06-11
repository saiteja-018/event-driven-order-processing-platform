import { Kafka } from 'kafkajs';
import dotenv from 'dotenv';
import { logger } from '../logger';
dotenv.config();

const kafka = new Kafka({ brokers: [process.env.KAFKA_BROKER || 'kafka:9092'], clientId: 'payment-service' });
const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: 'payment-service-group' });
const admin = kafka.admin();

export async function initKafka() {
  await admin.connect();
  const topics = [
    { topic: 'payment.initiated', numPartitions: 6, replicationFactor: 1 },
    { topic: 'payment.succeeded', numPartitions: 6, replicationFactor: 1 },
    { topic: 'payment.failed', numPartitions: 6, replicationFactor: 1 },
    { topic: 'order.confirmed', numPartitions: 6, replicationFactor: 1 },
    { topic: 'dead.letter.queue', numPartitions: 3, replicationFactor: 1 }
  ];
  try { await admin.createTopics({ topics, waitForLeaders: true }); logger.info({ message: 'Kafka topics created or already exist' }); } catch (err:any) { logger.error({ message: 'Kafka topic creation failed', error: String(err) }); }
  await producer.connect();
  await consumer.connect();
}

export { producer, consumer };
