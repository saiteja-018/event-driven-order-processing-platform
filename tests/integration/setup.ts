import { Pool } from 'pg';
import axios from 'axios';
import { randomUUID } from 'crypto';

export const API_BASE = process.env.API_BASE || 'http://127.0.0.1:3000';
export const DATABASE_URL = process.env.DATABASE_URL || 'postgres://postgres:postgres@127.0.0.1:5432/app';

export const dbPool = new Pool({ connectionString: DATABASE_URL, connectionTimeoutMillis: 5000 });

export async function queryDb(text: string, params?: any[]) {
  const res = await dbPool.query(text, params || []);
  return res;
}

export function uuid() { return randomUUID(); }

export async function waitFor(fn: () => Promise<boolean>, timeout = 60000, interval = 1000) {
  const start = Date.now();
  while (Date.now() - start < timeout) {
    try {
      if (await fn()) return true;
    } catch (e) {
      console.error('Error in waitFor:', e);
    }
    await new Promise(r => setTimeout(r, interval));
  }
  throw new Error('waitFor timeout');
}

export async function postOrder(payload: any) {
  return axios.post(`${API_BASE}/api/v1/orders`, payload, { timeout: 10000 });
}

export async function getOrder(orderId: string) {
  return axios.get(`${API_BASE}/api/v1/orders/${orderId}`, { timeout: 10000 });
}
