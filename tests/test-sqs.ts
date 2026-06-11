import AWS from 'aws-sdk';

const sqs = new AWS.SQS({
  endpoint: 'http://127.0.0.1:4566',
  region: 'us-east-1',
  accessKeyId: 'test',
  secretAccessKey: 'test',
  httpOptions: { timeout: 2000 }
});

async function run() {
  try {
    console.log('Fetching queue URL for reservation-expiry-queue...');
    const res = await sqs.getQueueUrl({ QueueName: 'reservation-expiry-queue' }).promise();
    console.log('Success!', res);
  } catch (err: any) {
    console.error('Error occurred:', err.message);
    if (err.stack) console.error(err.stack);
  }
}

run();
