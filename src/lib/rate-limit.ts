import { nanoid } from 'nanoid';
import { logger } from '../logger.js';
import type { RedisDatabase } from '../db/redis.js';
import type { PostgresDatabase } from '../db/postgres.js';

export interface RateLimitResult {
  allowed: boolean;
  remaining: number;
  resetAt: number; // Unix timestamp ms
}

export interface RateLimitConfig {
  windowMs: number;
  maxRequests: number;
}

const PLAN_CACHE_TTL = 60_000;

export class RateLimitService {
  private readonly redis: RedisDatabase;
  private readonly pg: PostgresDatabase;

  private _script: Awaited<ReturnType<RedisDatabase['loadLua']>> | null = null;

  private readonly planLimitCache = new Map<
    string,
    { config: RateLimitConfig; cachedAt: number }
  >();

  constructor(redis: RedisDatabase, pg: PostgresDatabase) {
    this.redis = redis;
    this.pg = pg;
  }

  private async getScript(): Promise<Awaited<ReturnType<RedisDatabase['loadLua']>>> {
    if (!this._script) {
      this._script = await this.redis.loadLua('sliding-window');
    }
    return this._script;
  }

  async checkRateLimit(
    apiKey: string,
    config: RateLimitConfig,
  ): Promise<RateLimitResult> {
    const script = await this.getScript();
    const key = `rl:apikey:${apiKey}`;
    const now = Date.now();
    const reqId = nanoid(8);

    let result: [number, number, number];
    try {
      result = (await this.redis.client.evalsha(
        script.sha,
        1,
        key,
        String(config.windowMs),
        String(config.maxRequests),
        String(now),
        reqId,
      )) as [number, number, number];
    } catch (err) {
      // Redis down — fail open: allow the request and log a warning
      logger.warn({ err, apiKey }, 'rate limit check failed (Redis error) — failing open');
      return { allowed: true, remaining: -1, resetAt: now + config.windowMs };
    }

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

  async getPlanRateLimit(planId: string): Promise<RateLimitConfig> {
    const cached = this.planLimitCache.get(planId);
    if (cached !== undefined && Date.now() - cached.cachedAt < PLAN_CACHE_TTL) {
      return cached.config;
    }

    const result = await this.pg.query<{ event_quota_per_month: number }>(
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
    this.planLimitCache.set(planId, { config, cachedAt: Date.now() });
    return config;
  }
}
