import type { FastifyPluginAsync, FastifyReply, FastifyRequest } from 'fastify';
import fp from 'fastify-plugin';
import { AppContainer } from '../container.js';

const METRIC_KEYS = [
  'metrics:events:ingested:total',
  'metrics:events:ingested:errors',
  'metrics:events:stream:published',
  'metrics:events:worker:processed',
  'metrics:events:worker:failed',
  'metrics:events:worker:dlq',
  'metrics:alerts:triggered:total',
  'metrics:alerts:percolator:matched',
  'metrics:cache:hits:global',
  'metrics:cache:misses:global',
  'metrics:ratelimit:rejected:total',
  'metrics:api:requests:total',
  'metrics:api:requests:5xx',
  'metrics:api:requests:4xx',
] as const;

type MetricKey = (typeof METRIC_KEYS)[number];

export function metricsRoutes(container: AppContainer): FastifyPluginAsync {
  return fp(async (fastify) => {
    fastify.get(
      '/metrics',
      {
        schema: {
          tags: ['metrics'],
          description: 'Internal operational metrics — not authenticated, firewall-restricted',
        },
      },
      async (_request: FastifyRequest, reply: FastifyReply): Promise<void> => {
        const counterValues = await Promise.all(
          METRIC_KEYS.map(async (key) => {
            const val = await container.redis.client.get(key);
            const value = val === null ? 0 : parseInt(val, 10);
            return [key, value] as [MetricKey, number];
          }),
        );

        const metrics: Record<string, number> = {};
        for (const [key, value] of counterValues) {
          metrics[key] = value;
        }

        // Per-project cache hit/miss counters: scan for cache:hits:* and cache:misses:*
        const perProjectCache: Record<string, { hits: number; misses: number }> = {};
        try {
          let cursor = '0';
          const hitKeys: string[] = [];
          const missKeys: string[] = [];
          do {
            const [nextCursor, keys] = await container.redis.client.scan(cursor, 'MATCH', 'cache:hits:*', 'COUNT', 200);
            cursor = nextCursor;
            hitKeys.push(...keys);
          } while (cursor !== '0');
          cursor = '0';
          do {
            const [nextCursor, keys] = await container.redis.client.scan(cursor, 'MATCH', 'cache:misses:*', 'COUNT', 200);
            cursor = nextCursor;
            missKeys.push(...keys);
          } while (cursor !== '0');

          for (const key of hitKeys) {
            const projectId = key.slice('cache:hits:'.length);
            if (!perProjectCache[projectId]) perProjectCache[projectId] = { hits: 0, misses: 0 };
            const val = await container.redis.client.get(key);
            perProjectCache[projectId]!.hits = val !== null ? parseInt(val, 10) : 0;
          }
          for (const key of missKeys) {
            const projectId = key.slice('cache:misses:'.length);
            if (!perProjectCache[projectId]) perProjectCache[projectId] = { hits: 0, misses: 0 };
            const val = await container.redis.client.get(key);
            perProjectCache[projectId]!.misses = val !== null ? parseInt(val, 10) : 0;
          }
        } catch {
          // Redis error collecting per-project metrics — non-fatal, return empty
        }

        void reply.status(200).send({
          metrics,
          cacheByProject: perProjectCache,
          collectedAt: new Date().toISOString(),
        });
      },
    );
  }, { name: 'metrics-routes', fastify: '4.x' });
}
