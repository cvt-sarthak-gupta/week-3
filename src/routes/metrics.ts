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

        void reply.status(200).send({
          metrics,
          collectedAt: new Date().toISOString(),
        });
      },
    );
  }, { name: 'metrics-routes', fastify: '4.x' });
}
