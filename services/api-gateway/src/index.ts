import express from 'express';
import bodyParser from 'body-parser';
import Redis from 'ioredis';
import { v4 as uuidv4 } from 'uuid';
import axios from 'axios';
import { logger } from './logger';
import { rateLimiter } from './rateLimiter';
import { circuitBreakerMiddleware } from './circuitBreaker';
import { circuitBreaker } from './circuitBreaker';

const app = express();
app.use(bodyParser.json());

const redis = new Redis(process.env.REDIS_URL || 'redis://redis:6379');

// requestId middleware
app.use((req, res, next) => {
	const id = req.header('X-Request-ID') || uuidv4();
	res.setHeader('X-Request-ID', id);
	(req as any).requestId = id;
	next();
});

// rate limiter
app.use(rateLimiter(redis));

// circuit breaker applied per downstream call via helper middleware

const orderCircuit = circuitBreaker(redis, 'order-service');
const inventoryCircuit = circuitBreaker(redis, 'inventory-service');
const paymentCircuit = circuitBreaker(redis, 'payment-service');
const notificationCircuit = circuitBreaker(redis, 'notification-service');
const analyticsCircuit = circuitBreaker(redis, 'analytics-service');

function mapDownstreamError(err: any, res: any) {
	if (err && err.message === 'circuit_open') return res.status(503).json({ error: 'Service unavailable (circuit open)' });
	const status = err?.response?.status;
	if (status === 404 || status === 409 || status === 422) {
		return res.status(status).json(err.response.data || { error: 'downstream_error' });
	}
	return res.status(502).json({ error: 'downstream_error' });
}

// Orders
app.post('/api/v1/orders', circuitBreakerMiddleware(redis, 'order-service'), async (req, res) => {
	try {
		const r = await orderCircuit(() => axios.post('http://order-service:3001/internal/orders', req.body, { timeout: 10000, headers: { 'X-Request-ID': (req as any).requestId } }), req);
		return res.status(r.status).json(r.data);
	} catch (err:any) { return mapDownstreamError(err, res); }
});

app.get('/api/v1/orders/:orderId', circuitBreakerMiddleware(redis, 'order-service'), async (req, res) => {
	try {
		const r = await orderCircuit(() => axios.get(`http://order-service:3001/internal/orders/${req.params.orderId}`, { timeout: 10000, headers: { 'X-Request-ID': (req as any).requestId } }), req);
		return res.status(r.status).json(r.data);
	} catch (err:any) { return mapDownstreamError(err, res); }
});

app.get('/api/v1/orders/user/:userId', circuitBreakerMiddleware(redis, 'order-service'), async (req, res) => {
	try {
		const r = await orderCircuit(() => axios.get(`http://order-service:3001/internal/orders/user/${req.params.userId}`, { timeout: 10000, headers: { 'X-Request-ID': (req as any).requestId }, params: req.query }), req);
		return res.status(r.status).json(r.data);
	} catch (err:any) { return mapDownstreamError(err, res); }
});

app.delete('/api/v1/orders/:orderId', circuitBreakerMiddleware(redis, 'order-service'), async (req, res) => {
	try {
		const r = await orderCircuit(() => axios.patch(`http://order-service:3001/internal/orders/${req.params.orderId}/cancel`, {}, { timeout: 10000, headers: { 'X-Request-ID': (req as any).requestId } }), req);
		return res.status(r.status).json(r.data);
	} catch (err:any) { return mapDownstreamError(err, res); }
});

// Products
app.get('/api/v1/products', circuitBreakerMiddleware(redis, 'inventory-service'), async (req, res) => {
	try {
		const r = await inventoryCircuit(() => axios.get('http://inventory-service:3002/internal/products', { timeout: 10000, headers: { 'X-Request-ID': (req as any).requestId }, params: req.query }), req);
		return res.status(r.status).json(r.data);
	} catch (err:any) { return mapDownstreamError(err, res); }
});

app.get('/api/v1/products/:productId', circuitBreakerMiddleware(redis, 'inventory-service'), async (req, res) => {
	try {
		const r = await inventoryCircuit(() => axios.get(`http://inventory-service:3002/internal/products/${req.params.productId}`, { timeout: 10000, headers: { 'X-Request-ID': (req as any).requestId } }), req);
		return res.status(r.status).json(r.data);
	} catch (err:any) { return mapDownstreamError(err, res); }
});

// Analytics
app.get('/api/v1/analytics/metrics', circuitBreakerMiddleware(redis, 'analytics-service'), async (req, res) => {
	try {
		const r = await analyticsCircuit(() => axios.get('http://analytics-service:3005/internal/analytics/metrics', { timeout: 10000, headers: { 'X-Request-ID': (req as any).requestId }, params: req.query }), req);
		return res.status(r.status).json(r.data);
	} catch (err:any) { return mapDownstreamError(err, res); }
});

app.get('/api/v1/analytics/events', circuitBreakerMiddleware(redis, 'analytics-service'), async (req, res) => {
	try {
		const r = await analyticsCircuit(() => axios.get('http://analytics-service:3005/internal/analytics/events', { timeout: 10000, headers: { 'X-Request-ID': (req as any).requestId }, params: req.query }), req);
		return res.status(r.status).json(r.data);
	} catch (err:any) { return mapDownstreamError(err, res); }
});

app.get('/api/v1/health', async (req, res) => {
	const services = [
		{ name: 'order-service', url: `http://order-service:3001/health` },
		{ name: 'inventory-service', url: `http://inventory-service:3002/health` },
		{ name: 'payment-service', url: `http://payment-service:3003/health` },
		{ name: 'notification-service', url: `http://notification-service:3004/health` },
		{ name: 'analytics-service', url: `http://analytics-service:3005/health` }
	];

	const results = await Promise.all(services.map(async s => {
		try {
			const r = await axios.get(s.url, { timeout: 1500, headers: { 'X-Request-ID': (req as any).requestId } });
			return { service: s.name, status: r.data?.status || 'ok' };
		} catch (err: any) {
			return { service: s.name, status: 'unreachable' };
		}
	}));

	const statuses = results.map(r => r.status);
	let httpStatus = 200;
	if (statuses.includes('unreachable')) httpStatus = 503;
	else if (statuses.includes('degraded')) httpStatus = 207;

	res.status(httpStatus).json({ status: httpStatus === 200 ? 'ok' : (httpStatus === 207 ? 'degraded' : 'unreachable'), service: 'api-gateway', timestamp: new Date().toISOString(), dependencies: results.reduce((acc:any,r)=>{acc[r.service]=r.status;return acc;},{}) });
});

app.listen(process.env.PORT || 3000, ()=> logger.info({service:'api-gateway','message':'listening','port':process.env.PORT||3000}));

