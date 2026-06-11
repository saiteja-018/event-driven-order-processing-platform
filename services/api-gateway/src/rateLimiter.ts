import { Redis } from 'ioredis';
import { Request, Response, NextFunction } from 'express';

export function rateLimiter(redis: Redis) {
  return async function (req: Request, res: Response, next: NextFunction) {
    try {
      const userId = req.header('X-User-Id') || null;
      const ip = req.ip || (req.headers['x-forwarded-for'] as string) || req.socket.remoteAddress;
      const keyBase = userId ? `rate_limit:${userId}` : `rate_limit:ip:${ip}`;
      const windowSec = 60;
      const limit = 100;
      const windowTs = Math.floor(Date.now() / 1000 / windowSec) * windowSec;
      const key = `${keyBase}:${windowTs}`;
      const count = await redis.incr(key);
      if (count === 1) {
        await redis.expire(key, windowSec + 1);
      }
      if (count > limit) {
        const ttl = await redis.ttl(key);
        res.setHeader('Retry-After', String(ttl > 0 ? ttl : windowSec));
        return res.status(429).json({ error: 'Too Many Requests' });
      }
      next();
    } catch (err) {
      next();
    }
  };
}
