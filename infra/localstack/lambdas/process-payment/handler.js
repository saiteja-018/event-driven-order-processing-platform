exports.handler = async function (event) {
  console.log('process-payment lambda invoked', JSON.stringify(event));
  const { Client } = require('pg');
  const { Kafka } = require('kafkajs');
  const AWS = require('aws-sdk');

  const dbUrl = process.env.DATABASE_URL || 'postgres://postgres:postgres@postgres:5432/app';
  const kafkaBrokers = (process.env.KAFKA_BROKERS || 'kafka:9092').split(',');
  const localstackUrl = process.env.LOCALSTACK_URL || 'http://localstack:4566';

  const client = new Client({ connectionString: dbUrl });
  await client.connect();

  const kafka = new Kafka({ brokers: kafkaBrokers, clientId: 'process-payment-lambda' });
  const producer = kafka.producer();
  await producer.connect();

  const sns = new AWS.SNS({
    endpoint: localstackUrl,
    region: 'us-east-1',
    accessKeyId: 'test',
    secretAccessKey: 'test'
  });

  try {
    const records = event.Records || [];
    for (const r of records) {
      let body = r.body || '{}';
      if (typeof body === 'string') {
        try { body = JSON.parse(body); } catch (e) { console.error('failed to parse body', r.body); continue; }
      }

      const txId = body.transactionId || body.transaction_id;
      const orderId = body.orderId || body.order_id;
      const amount = parseFloat(body.amount || 0);
      const currency = body.currency || 'USD';
      const userId = body.userId || body.user_id || null;
      const idempotencyKey = body.idempotencyKey || body.idempotency_key || txId;

      if (!txId || !orderId) {
        console.error('Missing txId or orderId in message', body);
        continue;
      }

      // Determine payment outcome
      let success = true;
      if (amount > 10000) {
        success = false;
      } else if (amount >= 5000 && amount <= 10000) {
        success = Math.random() >= 0.3; // 70% success
      }

      if (success) {
        const providerRef = 'PAY-' + Math.random().toString(36).substring(2, 10).toUpperCase();
        await client.query(
          "UPDATE payment_service.payment_transactions SET status='SUCCEEDED', provider_reference=$1, updated_at=now() WHERE id=$2",
          [providerRef, txId]
        );

        const evt = {
          eventId: require('crypto').randomUUID(),
          eventType: 'PAYMENT_SUCCEEDED',
          occurredAt: new Date().toISOString(),
          transactionId: txId,
          orderId: orderId,
          userId: userId,
          amount: amount,
          currency: currency,
          providerReference: providerRef,
          idempotencyKey: idempotencyKey,
          schemaVersion: '1.0'
        };

        await producer.send({ topic: 'payment.succeeded', messages: [{ key: orderId, value: JSON.stringify(evt) }] });

        // Publish to SNS payment-events-topic
        try {
          await sns.publish({
            TopicArn: 'arn:aws:sns:us-east-1:000000000000:payment-events-topic',
            Message: JSON.stringify(evt),
            MessageAttributes: {
              eventType: { DataType: 'String', StringValue: 'PAYMENT_SUCCEEDED' }
            }
          }).promise();
        } catch (snsErr) {
          console.error('SNS publish error (non-fatal)', snsErr.message);
        }

        console.log('Payment succeeded for order', orderId);
      } else {
        // Increment retry_count then read back the current value
        await client.query(
          "UPDATE payment_service.payment_transactions SET status='FAILED', failure_reason=$1, retry_count = retry_count + 1, updated_at=now() WHERE id=$2",
          ['simulated_failure', txId]
        );

        const { rows } = await client.query(
          'SELECT retry_count FROM payment_service.payment_transactions WHERE id=$1',
          [txId]
        );
        const retryCount = rows[0] ? rows[0].retry_count : 1;

        const evt = {
          eventId: require('crypto').randomUUID(),
          eventType: 'PAYMENT_FAILED',
          occurredAt: new Date().toISOString(),
          transactionId: txId,
          orderId: orderId,
          userId: userId,
          amount: amount,
          currency: currency,
          failureReason: 'simulated_failure',
          retryCount: retryCount,
          idempotencyKey: idempotencyKey,
          schemaVersion: '1.0'
        };

        await producer.send({ topic: 'payment.failed', messages: [{ key: orderId, value: JSON.stringify(evt) }] });

        console.log('Payment failed for order', orderId, 'retry_count:', retryCount);
      }
    }
  } catch (err) {
    console.error('process-payment lambda error', err);
    throw err;
  } finally {
    try { await producer.disconnect(); } catch (e) { /* ignore */ }
    try { await client.end(); } catch (e) { /* ignore */ }
  }

  return { status: 'ok' };
};
