import fp from 'fastify-plugin';
import type { FastifyInstance, FastifyPluginAsync, FastifyReply, FastifyRequest } from 'fastify';
import type { MetricsService } from './metrics.service.js';

export function createMetricsRoutes(metricsService: MetricsService): FastifyPluginAsync {
  return fp(
    async (fastify: FastifyInstance) => {
      // GET /metrics — no auth (firewall-restricted endpoint)

      fastify.get(
        '/metrics',
        {
          schema: {
            tags: ['metrics'],
            description:
              'Internal operational metrics — not authenticated, firewall-restricted',
          },
        },
        async (_request: FastifyRequest, reply: FastifyReply): Promise<void> => {
          const result = await metricsService.getMetrics();
          void reply.status(200).send(result);
        },
      );
    },
    { name: 'metrics-routes', fastify: '4.x' },
  );
}
