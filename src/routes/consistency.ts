import type { FastifyPluginAsync, FastifyReply, FastifyRequest } from 'fastify';
import fp from 'fastify-plugin';
import { AppContainer } from '../container.js';
import { ForbiddenError } from '../errors.js';

export function consistencyRoutes(container: AppContainer): FastifyPluginAsync {
  return fp(async (fastify) => {
    const userPreHandler = container.auth.userPreHandler();

    async function assertAdmin(userId: string): Promise<void> {
      const result = await container.pg.query<{ role: string }>(
        `SELECT role FROM users WHERE id = $1 AND role = 'admin' LIMIT 1`,
        [userId],
      );
      if (result.rows[0] === undefined) {
        throw new ForbiddenError('Admin access required');
      }
    }

    fastify.get(
      '/admin/consistency-audit',
      {
        preHandler: [userPreHandler],
        schema: {
          tags: ['admin'],
          description: 'Runs the cross-DB consistency audit across Postgres, MongoDB, and Elasticsearch',
        },
      },
      async (request: FastifyRequest, reply: FastifyReply): Promise<void> => {
        const userId = request.user.userId;

        await assertAdmin(userId);

        const result = await container.consistency.runAudit();

        void reply.status(200).send({
          inconsistencies: result.inconsistencies,
          summary: result.summary,
          ranAt: result.ranAt,
          durationMs: result.duration_ms,
          checked: result.checked,
        });
      },
    );
  }, { name: 'consistency-routes', fastify: '4.x' });
}
