import { randomUUID } from 'node:crypto';
import type { PostgresPool } from '../../db/postgres/index.js';
import type { RedisClient } from '../../db/redis/index.js';
import type { RateLimitService } from '../../lib/rate-limit/index.js';
import { RateLimitError } from '../../utils/errors.js';
import { config } from '../../config.js';
import type { EventBody, IngestResult, BatchIngestResult } from './ingest.types.js';

export class IngestService {
  private readonly tenantPlanCache = new Map<string, { planId: string | null; cachedAt: number }>();
  private static readonly PLAN_CACHE_TTL_MS = 5 * 60 * 1000;

  constructor(
    private readonly pool: PostgresPool,
    private readonly redis: RedisClient,
    private readonly rateLimit: RateLimitService,
  ) {}

  private todayKey(): string {
    const now = new Date();
    return `${now.getUTCFullYear()}-${String(now.getUTCMonth() + 1).padStart(2, '0')}-${String(now.getUTCDate()).padStart(2, '0')}`;
  }

  async getTenantPlanId(tenantId: string): Promise<string | null> {
    const cached = this.tenantPlanCache.get(tenantId);
    if (cached !== undefined && Date.now() - cached.cachedAt < IngestService.PLAN_CACHE_TTL_MS) {
      return cached.planId;
    }
    const result = await this.pool.query<{ plan_id: string | null }>(
      'SELECT plan_id FROM tenants WHERE id = $1 LIMIT 1',
      [tenantId],
    );
    const planId = result.rows[0]?.plan_id ?? null;
    this.tenantPlanCache.set(tenantId, { planId, cachedAt: Date.now() });
    return planId;
  }

  async checkRateLimit(
    apiKey: string,
    tenantId: string,
  ): Promise<{ remaining: number; resetAt: number }> {
    const planId = await this.getTenantPlanId(tenantId);
    const rlConfig = await this.rateLimit.getPlanLimit(planId ?? 'default');
    const result = await this.rateLimit.check(apiKey, rlConfig);
    if (!result.allowed) {
      throw new RateLimitError(
        'Rate limit exceeded',
        Math.ceil((result.resetAt - Date.now()) / 1000),
      );
    }
    return { remaining: result.remaining, resetAt: result.resetAt };
  }

  async ingestOne(event: EventBody, projectId: string, tenantId: string): Promise<IngestResult> {
    const eventId = randomUUID();
    const traceId = randomUUID();
    const occurredAt = event.occurredAt ?? new Date().toISOString();
    const fingerprint = event.fingerprint ?? `${event.type}:${event.message}`;
    const dateKey = this.todayKey();

    const pipeline = this.redis.client.pipeline();
    pipeline.xadd(
      this.redis.STREAM_KEY,
      'MAXLEN',
      '~',
      config.ingest.streamMaxLen,
      '*',
      'eventId', eventId,
      'traceId', traceId,
      'projectId', projectId,
      'tenantId', tenantId,
      'type', event.type,
      'severity', event.severity,
      'message', event.message,
      'occurredAt', occurredAt,
      'fingerprint', fingerprint,
      'stackTrace', event.stackTrace !== undefined ? JSON.stringify(event.stackTrace) : '',
      'tags', event.tags !== undefined ? JSON.stringify(event.tags) : '',
      'userContext', event.userContext !== undefined ? JSON.stringify(event.userContext) : '',
      'deviceContext', event.deviceContext !== undefined ? JSON.stringify(event.deviceContext) : '',
      'payload', event.payload !== undefined ? JSON.stringify(event.payload) : '',
    );
    pipeline.zincrby(`leaderboard:${dateKey}`, 1, projectId);
    await pipeline.exec();

    return { eventId, traceId };
  }

  async ingestBatch(
    events: EventBody[],
    projectId: string,
    tenantId: string,
  ): Promise<BatchIngestResult> {
    const results = await Promise.all(events.map((e) => this.ingestOne(e, projectId, tenantId)));
    return { accepted: results.length, eventIds: results.map((r) => r.eventId) };
  }
}
