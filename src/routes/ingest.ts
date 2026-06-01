import { randomUUID } from 'node:crypto';
import type { FastifyInstance, FastifyPluginAsync, FastifyReply } from 'fastify';
import fp from 'fastify-plugin';
import { pool } from '../db/postgres.js';
import { redis, STREAM_KEY } from '../db/redis.js';
import { rateLimitViolationsCollection } from '../db/mongo.js';
import { authenticateApiKey } from '../lib/auth.js';
import { checkRateLimit, getPlanRateLimit } from '../lib/rate-limit.js';
import { RateLimitError } from '../errors.js';
import { config } from '../config.js';
import { logger } from '../logger.js';

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

const tenantPlanCache = new Map<string, { planId: string | null; cachedAt: number }>();
const TENANT_PLAN_CACHE_TTL_MS = 5 * 60 * 1000;

async function getTenantPlanId(tenantId: string): Promise<string | null> {
  const cached = tenantPlanCache.get(tenantId);
  if (cached !== undefined && Date.now() - cached.cachedAt < TENANT_PLAN_CACHE_TTL_MS) {
    return cached.planId;
  }
  const result = await pool.query<{ plan_id: string | null }>(
    'SELECT plan_id FROM tenants WHERE id = $1 LIMIT 1',
    [tenantId],
  );
  const planId = result.rows[0]?.plan_id ?? null;
  tenantPlanCache.set(tenantId, { planId, cachedAt: Date.now() });
  return planId;
}

function todayKey(): string {
  const now = new Date();
  const yyyy = now.getUTCFullYear();
  const mm = (now.getUTCMonth() + 1).toString().padStart(2, '0');
  const dd = now.getUTCDate().toString().padStart(2, '0');
  return `${yyyy}-${mm}-${dd}`;
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

  const dateKey = todayKey();
  const leaderboardKey = `leaderboard:${dateKey}`;

  // Midnight UTC of the current day (= start of tomorrow)
  const nextMidnightUtc = new Date();
  nextMidnightUtc.setUTCHours(24, 0, 0, 0);
  const midnightUnixSeconds = Math.floor(nextMidnightUtc.getTime() / 1000);

  const pipeline = redis.pipeline();
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
  pipeline.zincrby(leaderboardKey, 1, projectId);
  // Expire at midnight UTC so yesterday's leaderboard is auto-cleaned
  pipeline.expireat(leaderboardKey, midnightUnixSeconds);
  await pipeline.exec();

  return { eventId, traceId };
}

const ingestPluginHandler: FastifyPluginAsync = async (fastify: FastifyInstance) => {
  const apiKeyPreHandler = authenticateApiKey(redis, pool);

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
      const rateLimitConfig = await getPlanRateLimit(planId ?? 'default', pool);

      const rateResult = await checkRateLimit(apiKey, rateLimitConfig);

      if (!rateResult.allowed) {
        // Log violation to MongoDB (fire-and-forget, never block the 429 response)
        rateLimitViolationsCollection()
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
};

export const ingestRoutes = fp(ingestPluginHandler, {
  name: 'ingest-routes',
  fastify: '4.x',
});
