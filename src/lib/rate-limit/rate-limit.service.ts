import { nanoid } from 'nanoid';
import type { RedisClient, LuaScript } from '../../db/redis/index.js';
import type { PostgresPool } from '../../db/postgres/index.js';
import { logger } from '../../utils/logger.js';
import type { RateLimitConfig, RateLimitResult } from './types.js';

export class RateLimitService {
  private _script: LuaScript | null = null;
  private readonly planLimitCache = new Map<string, { config: RateLimitConfig; cachedAt: number }>();
  private static readonly PLAN_CACHE_TTL = 60_000;

  constructor(
    private readonly redisClient: RedisClient,
    private readonly pool: PostgresPool,
  ) {}

  private async getScript(): Promise<LuaScript> {
    if (!this._script) {
      this._script = await this.redisClient.loadLua('sliding-window');
    }
    return this._script;
  }

  async check(apiKey: string, config: RateLimitConfig): Promise<RateLimitResult> {
    const script = await this.getScript();
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

  async getPlanLimit(planId: string): Promise<RateLimitConfig> {
    const cached = this.planLimitCache.get(planId);
    if (cached !== undefined && Date.now() - cached.cachedAt < RateLimitService.PLAN_CACHE_TTL) {
      return cached.config;
    }

    const result = await this.pool.query<{ event_quota_per_month: number }>(
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
    const planConfig: RateLimitConfig = { windowMs: 60_000, maxRequests: perMinute };
    this.planLimitCache.set(planId, { config: planConfig, cachedAt: Date.now() });
    return planConfig;
  }
}
