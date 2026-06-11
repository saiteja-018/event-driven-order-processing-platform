import { Kafka } from 'kafkajs';
import dotenv from 'dotenv';
import { logger } from '../logger';
dotenv.config();

const kafka = new Kafka({ brokers: [process.env.KAFKA_BROKER || 'kafka:9092'], clientId: 'inventory-service' });
const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: 'inventory-service-group' });
const admin = kafka.admin();

export async function initKafka() {
  await admin.connect();
  // Topics are created by order-service; we just ensure consumers can connect
  const topics = [
    { topic: 'inventory.reserved', numPartitions: 6, replicationFactor: 1, configEntries: [{ name: 'retention.ms', value: '604800000' }] },
    { topic: 'inventory.reservation_failed', numPartitions: 6, replicationFactor: 1, configEntries: [{ name: 'retention.ms', value: '604800000' }] },
    { topic: 'inventory.released', numPartitions: 6, replicationFactor: 1, configEntries: [{ name: 'retention.ms', value: '604800000' }] },
    { topic: 'order.created', numPartitions: 6, replicationFactor: 1, configEntries: [{ name: 'retention.ms', value: '604800000' }] },
    { topic: 'order.cancelled', numPartitions: 6, replicationFactor: 1, configEntries: [{ name: 'retention.ms', value: '604800000' }] },
    { topic: 'order.completed', numPartitions: 6, replicationFactor: 1, configEntries: [{ name: 'retention.ms', value: '604800000' }] },
    { topic: 'dead.letter.queue', numPartitions: 3, replicationFactor: 1, configEntries: [{ name: 'retention.ms', value: '2592000000' }] }
  ];
  try {
    await admin.createTopics({ topics, waitForLeaders: true });
    logger.info({ message: 'Kafka topics created or already exist' });
  } catch (err: any) {
    logger.error({ message: 'Kafka topic creation failed', error: String(err) });
  }
  await producer.connect();
  await consumer.connect();
}

export { producer, consumer, admin };
