import { Redis } from 'ioredis';
import { Request, Response, NextFunction } from 'express';

export function circuitBreakerMiddleware(redis: Redis, serviceName: string) {
  return async (req: Request, res: Response, next: NextFunction) => {
    const stateKey = `circuit:${serviceName}:state`;
    const failuresKey = `circuit:${serviceName}:failures`;
    const openTimerKey = `circuit:${serviceName}:open_timer`;
    const failureThreshold = 5;
    const resetTimeout = 30; // seconds

    let state = (await redis.get(stateKey)) || 'CLOSED';
    if (state === 'OPEN') {
      const exists = await redis.exists(openTimerKey);
      if (exists === 0) {
        state = 'HALF_OPEN';
        await redis.set(stateKey, 'HALF_OPEN');
      } else {
        return res.status(503).json({ error: 'Service unavailable (circuit open)' });
      }
    }
    if (state === 'HALF_OPEN') {
      // allow one probe, but mark blocked until result
    }
    // attach helper to record failure or success
    (req as any).circuit = {
      async success() {
        await redis.set(stateKey, 'CLOSED');
        await redis.del(failuresKey);
        await redis.del(openTimerKey);
      },
      async failure() {
        const failures = await redis.incr(failuresKey);
        if (failures >= failureThreshold) {
          await redis.set(stateKey, 'OPEN');
          await redis.set(openTimerKey, '1', 'EX', resetTimeout);
          // schedule transition to HALF_OPEN when key expires
          await redis.del(failuresKey);
        }
      }
    };
    next();
  };
}

export function circuitBreaker(redis: Redis, serviceName: string) {
  // helper to wrap calls
  return async function<T>(fn: ()=>Promise<T>, req:any) {
    const stateKey = `circuit:${serviceName}:state`;
    const state = (await redis.get(stateKey)) || 'CLOSED';
    if (state === 'OPEN') throw new Error('circuit_open');
    try {
      const result = await fn();
      if (req && req.circuit && req.circuit.success) await req.circuit.success();
      return result;
    } catch (err) {
      if (req && req.circuit && req.circuit.failure) await req.circuit.failure();
      throw err;
    }
  };
}
