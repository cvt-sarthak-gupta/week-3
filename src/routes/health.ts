import type { FastifyPluginAsync, FastifyReply, FastifyRequest } from 'fastify';
import fp from 'fastify-plugin';
import type { AppContainer } from '../container.js';

interface DatastoreChecks {
  postgres: boolean;
  mongo: boolean;
  elasticsearch: boolean;
  redis: boolean;
}

interface HealthResponse {
  ok: boolean;
  checks: DatastoreChecks;
  timestamp: string;
}

interface ReadyCheck {
  ok: boolean;
  latencyMs: number;
}

const HEALTH_TIMEOUT_MS = 200;

async function withTimeout<T>(
  promise: Promise<T>,
  fallback: T,
): Promise<T> {
  return Promise.race([
    promise,
    new Promise<T>((resolve) => setTimeout(() => resolve(fallback), HEALTH_TIMEOUT_MS)),
  ]);
}

export function healthRoutes(container: AppContainer): FastifyPluginAsync {
  return fp(async (fastify) => {
    fastify.get(
      '/health',
      { schema: { tags: ['health'] } },
      async (_request: FastifyRequest, reply: FastifyReply): Promise<void> => {
        void reply.status(200).send({ ok: true, timestamp: new Date().toISOString() });
      },
    );

    fastify.get(
      '/ready',
      { schema: { tags: ['health'] } },
      async (_request: FastifyRequest, reply: FastifyReply): Promise<void> => {
        const fallback: ReadyCheck = { ok: false, latencyMs: HEALTH_TIMEOUT_MS };

        const [pgResult, mongoResult, esResult, redisResult] = await Promise.allSettled([
          withTimeout(container.pg.healthCheck(), fallback),
          withTimeout(container.mongo.healthCheck(), fallback),
          withTimeout(container.es.healthCheck(), fallback),
          withTimeout(container.redis.healthCheck(), fallback),
        ]);

        const pg = pgResult.status === 'fulfilled' ? pgResult.value.ok : false;
        const mongo = mongoResult.status === 'fulfilled' ? mongoResult.value.ok : false;
        const es = esResult.status === 'fulfilled' ? esResult.value.ok : false;
        const redis = redisResult.status === 'fulfilled' ? redisResult.value.ok : false;

        const allOk = pg && mongo && es && redis;

        const body: HealthResponse = {
          ok: allOk,
          checks: {
            postgres: pg,
            mongo,
            elasticsearch: es,
            redis,
          },
          timestamp: new Date().toISOString(),
        };

        void reply.status(allOk ? 200 : 503).send(body);
      },
    );
  }, { name: 'health-routes', fastify: '4.x' });
}
