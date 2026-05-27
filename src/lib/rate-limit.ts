import { loadLua } from '../db/redis.js';
import { nanoid } from 'nanoid';
import { logger } from '../logger.js';
import type pg from 'pg';

export interface RateLimitResult {
  allowed: boolean;
  remaining: number;
  resetAt: number; // Unix timestamp ms
}

export interface RateLimitConfig {
  windowMs: number;
  maxRequests: number;
}

let _script: Awaited<ReturnType<typeof loadLua>> | null = null;

async function getScript(): Promise<Awaited<ReturnType<typeof loadLua>>> {
  if (!_script) {
    _script = await loadLua('sliding-window');
  }
  return _script;
}

export async function checkRateLimit(
  apiKey: string,
  config: RateLimitConfig,
): Promise<RateLimitResult> {
  const script = await getScript();
  const key = `rl:apikey:${apiKey}`;
  const now = Date.now();
  const reqId = nanoid(8);

  const result = (await script.evalsha(
    [key],
    [config.windowMs, config.maxRequests, now, reqId],
  )) as [number, number, number];

  logger.debug(
    { apiKey, allowed: result[0] === 1, remaining: result[1] },
    'rate limit check',
  );

  return {
    allowed: result[0] === 1,
    remaining: result[1],
    resetAt: result[2],
  };
}

// Cache of plan limits: planId → { windowMs, maxRequests }
const planLimitCache = new Map<
  string,
  { config: RateLimitConfig; cachedAt: number }
>();
const PLAN_CACHE_TTL = 60_000;

export async function getPlanRateLimit(
  planId: string,
  pgPool: pg.Pool,
): Promise<RateLimitConfig> {
  const cached = planLimitCache.get(planId);
  if (cached !== undefined && Date.now() - cached.cachedAt < PLAN_CACHE_TTL) {
    return cached.config;
  }

  const result = await pgPool.query<{ event_quota_per_month: number }>(
    'SELECT event_quota_per_month FROM plans WHERE id = $1',
    [planId],
  );

  if (result.rows[0] === undefined) {
    // Fallback: 1000/minute
    return { windowMs: 60_000, maxRequests: 1000 };
  }

  // Convert monthly quota to per-minute limit (assuming ~43200 minutes/month)
  const perMinute = Math.max(
    10,
    Math.ceil(result.rows[0].event_quota_per_month / 43200),
  );
  const config: RateLimitConfig = { windowMs: 60_000, maxRequests: perMinute };
  planLimitCache.set(planId, { config, cachedAt: Date.now() });
  return config;
}
