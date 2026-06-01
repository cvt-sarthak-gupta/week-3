import fp from 'fastify-plugin';
import type { FastifyInstance, FastifyPluginAsync, FastifyReply, FastifyRequest } from 'fastify';
import { authenticateUser } from '../../lib/auth.js';
import { ForbiddenError } from '../../utils/errors.js';
import type { PostgresPool } from '../../db/postgres/index.js';
import type { ConsistencyService } from './consistency.service.js';

export function createConsistencyRoutes(
  consistencyService: ConsistencyService,
  pool: PostgresPool,
): FastifyPluginAsync {
  return fp(
    async (fastify: FastifyInstance) => {
      async function assertAdmin(userId: string): Promise<void> {
        const result = await pool.query<{ role: string }>(
          `SELECT role FROM users WHERE id = $1 AND role = 'admin' LIMIT 1`,
          [userId],
        );
        if (result.rows[0] === undefined) {
          throw new ForbiddenError('Admin access required');
        }
      }

      // POST /admin/consistency-audit

      fastify.post(
        '/admin/consistency-audit',
        {
          preHandler: [authenticateUser],
          schema: {
            tags: ['admin'],
            description:
              'Runs the cross-DB consistency audit across Postgres, MongoDB, and Elasticsearch',
          },
        },
        async (request: FastifyRequest, reply: FastifyReply): Promise<void> => {
          const userId = request.user.userId;

          await assertAdmin(userId);

          const result = await consistencyService.runAudit();

          void reply.status(200).send({
            inconsistencies: result.inconsistencies,
            summary: result.summary,
            ranAt: result.ranAt,
            durationMs: result.duration_ms,
            checked: result.checked,
          });
        },
      );
    },
    { name: 'consistency-routes', fastify: '4.x' },
  );
}
