import fp from 'fastify-plugin';
import type { FastifyInstance, FastifyPluginAsync, FastifyReply } from 'fastify';
import { authenticateUser } from '../../lib/auth.js';
import { ForbiddenError } from '../../utils/errors.js';
import type { PostgresPool } from '../../db/postgres/index.js';
import type { LeaderboardService } from './leaderboard.service.js';

interface TenantParams {
  tenantId: string;
}

interface MemberRow {
  user_id: string;
}

export function createLeaderboardRoutes(
  leaderboardService: LeaderboardService,
  pool: PostgresPool,
): FastifyPluginAsync {
  return fp(
    async (fastify: FastifyInstance) => {
      async function assertMember(userId: string, tenantId: string): Promise<void> {
        const result = await pool.query<MemberRow>(
          'SELECT user_id FROM tenant_members WHERE tenant_id = $1 AND user_id = $2 LIMIT 1',
          [tenantId, userId],
        );
        if (result.rows[0] === undefined) {
          throw new ForbiddenError('Not a member of this tenant');
        }
      }

      // GET /tenants/:tenantId/leaderboard

      fastify.get<{ Params: TenantParams }>(
        '/tenants/:tenantId/leaderboard',
        {
          preHandler: [authenticateUser],
          schema: {
            tags: ['leaderboard'],
            params: {
              type: 'object',
              properties: { tenantId: { type: 'string' } },
            },
          },
        },
        async (request, reply: FastifyReply): Promise<void> => {
          const { tenantId } = request.params;
          const userId = request.user.userId;

          await assertMember(userId, tenantId);

          const result = await leaderboardService.getLeaderboard(tenantId);
          void reply.status(200).send(result);
        },
      );
    },
    { name: 'leaderboard-routes', fastify: '4.x' },
  );
}
