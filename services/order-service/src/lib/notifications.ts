import AWS from 'aws-sdk';
import { v4 as uuidv4 } from 'uuid';
import { producer } from './kafka';

const sns = new AWS.SNS({
	endpoint: process.env.LOCALSTACK_URL || 'http://localstack:4566',
	region: 'us-east-1',
	accessKeyId: 'test',
	secretAccessKey: 'test'
});
const orderTopicArn = process.env.ORDER_LIFECYCLE_TOPIC_ARN || 'arn:aws:sns:us-east-1:000000000000:order-lifecycle-topic';

export async function publishNotificationDispatch(params: { userId: string; orderId?: string; channel: string; templateKey: string; payload: any; }) {
	const evt = {
		eventId: uuidv4(),
		eventType: 'NOTIFICATION_DISPATCH',
		occurredAt: new Date().toISOString(),
		userId: params.userId,
		orderId: params.orderId,
		channel: params.channel,
		templateKey: params.templateKey,
		payload: params.payload,
		schemaVersion: '1.0'
	};
	await producer.send({ topic: 'notification.dispatch', messages: [{ key: params.orderId || params.userId, value: JSON.stringify(evt) }] });
}

export async function publishOrderLifecycleSns(eventType: string, payload: any) {
	await sns.publish({
		TopicArn: orderTopicArn,
		Message: JSON.stringify({ eventType, ...payload }),
		MessageAttributes: {
			eventType: { DataType: 'String', StringValue: eventType }
		}
	}).promise();
}
