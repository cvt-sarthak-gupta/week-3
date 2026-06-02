import { randomUUID } from 'node:crypto';
import type { FastifyPluginAsync, FastifyReply } from 'fastify';
import fp from 'fastify-plugin';
import { AppContainer } from '../container.js';
import { RateLimitError } from '../errors.js';
import { config } from '../config.js';
import { logger } from '../logger.js';
import { STREAM_KEY } from '../db/redis.js';

interface StackFrame {
  file?: string;
  line?: number;
  column?: number;
  function?: string;
}

interface EventBody {
  type: 'error' | 'log' | 'metric' | 'custom';
  severity: 'debug' | 'info' | 'warn' | 'error' | 'fatal';
  message: string;
  stackTrace?: StackFrame[];
  tags?: Record<string, string>;
  userContext?: { userId?: string; email?: string; ip?: string };
  deviceContext?: { os?: string; browser?: string; version?: string };
  payload?: Record<string, unknown>;
  occurredAt?: string;
  fingerprint?: string;
}

// No module-level Map cache here — an in-process Map breaks under horizontal
// scaling because each API instance has its own copy and plan changes are only
// reflected after the local TTL expires. All caching goes through Redis instead.
const TENANT_PLAN_CACHE_TTL_SECONDS = 300;

export function ingestRoutes(container: AppContainer): FastifyPluginAsync {
  return fp(async (fastify) => {
    const apiKeyPreHandler = container.auth.apiKeyPreHandler();

    async function getTenantPlanId(tenantId: string): Promise<string | null> {
      const cacheKey = `tenant:plan:${tenantId}`;

      // 1. Redis hit — shared across all API instances
      const cached = await container.redis.client.get(cacheKey).catch(() => null);
      if (cached !== null) return cached === '' ? null : cached;

      // 2. PG fallback
      const result = await container.pg.query<{ plan_id: string | null }>(
        'SELECT plan_id FROM tenants WHERE id = $1 LIMIT 1',
        [tenantId],
      );
      const planId = result.rows[0]?.plan_id ?? null;

      // Store in Redis; use empty string to represent NULL so we can distinguish
      // a cache miss from a tenant with no plan.
      await container.redis.client
        .setex(cacheKey, TENANT_PLAN_CACHE_TTL_SECONDS, planId ?? '')
        .catch(() => {});

      return planId;
    }

    async function ingestSingleEvent(
      event: EventBody,
      projectId: string,
      tenantId: string,
    ): Promise<{ eventId: string; traceId: string }> {
      const eventId = randomUUID();
      const traceId = randomUUID();
      const occurredAt = event.occurredAt ?? new Date().toISOString();
      const fingerprint = event.fingerprint ?? `${event.type}:${event.message}`;

      const pipeline = container.redis.client.pipeline();
      pipeline.xadd(
        STREAM_KEY,
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
      const results = await pipeline.exec();

      // xadd is the first command in the pipeline (index 0).
      // If it failed (e.g. Redis OOM / stream MAXLEN), surface the error so the
      // HTTP handler returns 5xx instead of 202 Accepted for a dropped event.
      const xaddErr = results?.[0]?.[0];
      if (xaddErr instanceof Error) {
        throw xaddErr;
      }

      return { eventId, traceId };
    }

    fastify.post<{ Body: EventBody | EventBody[] }>(
      '/ingest',
      {
        preHandler: [apiKeyPreHandler],
        schema: {
          tags: ['ingest'],
          body: {
            oneOf: [
              {
                type: 'object',
                required: ['type', 'severity', 'message'],
                properties: {
                  type: { type: 'string', enum: ['error', 'log', 'metric', 'custom'] },
                  severity: { type: 'string', enum: ['debug', 'info', 'warn', 'error', 'fatal'] },
                  message: { type: 'string', minLength: 1 },
                  stackTrace: { type: 'array' },
                  tags: { type: 'object' },
                  userContext: { type: 'object' },
                  deviceContext: { type: 'object' },
                  payload: { type: 'object' },
                  occurredAt: { type: 'string' },
                  fingerprint: { type: 'string' },
                },
              },
              {
                type: 'array',
                items: {
                  type: 'object',
                  required: ['type', 'severity', 'message'],
                  properties: {
                    type: { type: 'string', enum: ['error', 'log', 'metric', 'custom'] },
                    severity: { type: 'string', enum: ['debug', 'info', 'warn', 'error', 'fatal'] },
                    message: { type: 'string', minLength: 1 },
                    stackTrace: { type: 'array' },
                    tags: { type: 'object' },
                    userContext: { type: 'object' },
                    deviceContext: { type: 'object' },
                    payload: { type: 'object' },
                    occurredAt: { type: 'string' },
                    fingerprint: { type: 'string' },
                  },
                },
              },
            ],
          },
        },
      },
      async (request, reply: FastifyReply): Promise<void> => {
        const project = request.project!;
        const { id: projectId, tenantId, apiKey } = project;

        const planId = await getTenantPlanId(tenantId);
        const rateLimitConfig = planId !== null
          ? await container.rateLimit.getPlanRateLimit(planId)
          : { windowMs: 60_000, maxRequests: 1_000 }; // safe fallback

        // For a batch request consume one slot per event so that batching
        // cannot be used to circumvent per-request rate limits.
        const batchSize = Array.isArray(request.body) ? (request.body as EventBody[]).length : 1;
        const rateResult = await container.rateLimit.checkRateLimit(apiKey, rateLimitConfig, batchSize);

        if (!rateResult.allowed) {
          // Log violation to MongoDB (fire-and-forget, never block the 429 response)
          container.mongo.rateLimitViolations()
            .insertOne({
              apiKeyTail: apiKey.slice(-8),
              projectId,
              tenantId,
              violatedAt: new Date(),
              resetAt: new Date(rateResult.resetAt),
            })
            .catch((err) => {
              logger.warn({ err, projectId }, 'Failed to log rate limit violation to MongoDB');
            });

          void reply
            .header('X-RateLimit-Remaining', '0')
            .header('X-RateLimit-Reset', String(rateResult.resetAt));
          throw new RateLimitError('Rate limit exceeded', Math.ceil((rateResult.resetAt - Date.now()) / 1000));
        }

        void reply
          .header('X-RateLimit-Remaining', String(rateResult.remaining))
          .header('X-RateLimit-Reset', String(rateResult.resetAt));

        const isBatch = Array.isArray(request.body);

        if (isBatch) {
          const events = request.body as EventBody[];
          const results = await Promise.all(
            events.map((event) => ingestSingleEvent(event, projectId, tenantId)),
          );

          void reply.status(202).send({
            accepted: results.length,
            eventIds: results.map((r) => r.eventId),
          });
          return;
        }

        const { eventId, traceId } = await ingestSingleEvent(
          request.body as EventBody,
          projectId,
          tenantId,
        );

        void reply.status(202).send({ eventId, traceId });
      },
    );
  }, { name: 'ingest-routes', fastify: '4.x' });
}
