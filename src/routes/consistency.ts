import type { FastifyInstance, FastifyPluginAsync, FastifyReply, FastifyRequest } from 'fastify';
import fp from 'fastify-plugin';
import { pool } from '../db/postgres.js';
import { authenticateUser } from '../lib/auth.js';
import { runAudit } from '../domain/consistency.js';
import { ForbiddenError } from '../errors.js';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

async function assertAdmin(userId: string): Promise<void> {
  const result = await pool.query<{ role: string }>(
    `SELECT role FROM users WHERE id = $1 AND role = 'admin' LIMIT 1`,
    [userId],
  );
  if (result.rows[0] === undefined) {
    throw new ForbiddenError('Admin access required');
  }
}

// ---------------------------------------------------------------------------
// Plugin
// ---------------------------------------------------------------------------

const consistencyPluginHandler: FastifyPluginAsync = async (fastify: FastifyInstance) => {
  fastify.get(
    '/admin/consistency-audit',
    {
      preHandler: [authenticateUser],
      schema: {
        tags: ['admin'],
        description: 'Runs the cross-DB consistency audit across Postgres, MongoDB, and Elasticsearch',
      },
    },
    async (request: FastifyRequest, reply: FastifyReply): Promise<void> => {
      const userId = request.user.userId;

      await assertAdmin(userId);

      const result = await runAudit();

      void reply.status(200).send({
        inconsistencies: result.inconsistencies,
        summary: result.summary,
        ranAt: result.ranAt,
        durationMs: result.duration_ms,
        checked: result.checked,
      });
    },
  );
};

export const consistencyRoutes = fp(consistencyPluginHandler, {
  name: 'consistency-routes',
  fastify: '4.x',
});
