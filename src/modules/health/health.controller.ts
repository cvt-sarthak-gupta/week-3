import fp from 'fastify-plugin';
import type { FastifyInstance, FastifyPluginAsync, FastifyReply, FastifyRequest } from 'fastify';
import type { HealthService } from './health.service.js';

// ---------------------------------------------------------------------------
// Factory
// ---------------------------------------------------------------------------

export function createHealthRoutes(healthService: HealthService): FastifyPluginAsync {
  return fp(
    async (fastify: FastifyInstance) => {
      // -----------------------------------------------------------------------
      // GET /health — liveness probe (no dependency checks, always fast)
      // -----------------------------------------------------------------------

      fastify.get(
        '/health',
        { schema: { tags: ['health'] } },
        async (_request: FastifyRequest, reply: FastifyReply): Promise<void> => {
          void reply.status(200).send({ ok: true, timestamp: new Date().toISOString() });
        },
      );

      // -----------------------------------------------------------------------
      // GET /ready — readiness probe (checks all datastores concurrently)
      // -----------------------------------------------------------------------

      fastify.get(
        '/ready',
        { schema: { tags: ['health'] } },
        async (_request: FastifyRequest, reply: FastifyReply): Promise<void> => {
          const result = await healthService.check();
          const httpStatus = result.status === 'ok' ? 200 : 503;
          void reply.status(httpStatus).send({
            ok: result.status === 'ok',
            checks: result.checks,
            timestamp: result.timestamp,
          });
        },
      );
    },
    { name: 'health-routes', fastify: '4.x' },
  );
}
