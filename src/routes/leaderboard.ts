import type { FastifyPluginAsync, FastifyReply } from 'fastify';
import fp from 'fastify-plugin';
import { AppContainer } from '../container.js';
import { ForbiddenError } from '../errors.js';

const LEADERBOARD_TOP_N = 20;

interface TenantParams {
  tenantId: string;
}

interface MemberRow {
  user_id: string;
}

interface ProjectRow {
  id: string;
  name: string;
}

interface LeaderboardEntry {
  projectId: string;
  projectName: string;
  eventCount: number;
  rank: number;
}

function todayKey(): string {
  const now = new Date();
  const yyyy = now.getUTCFullYear();
  const mm = (now.getUTCMonth() + 1).toString().padStart(2, '0');
  const dd = now.getUTCDate().toString().padStart(2, '0');
  return `${yyyy}-${mm}-${dd}`;
}

export function leaderboardRoutes(container: AppContainer): FastifyPluginAsync {
  return fp(async (fastify) => {
    const userPreHandler = container.auth.userPreHandler();

    async function assertMember(userId: string, tenantId: string): Promise<void> {
      const result = await container.pg.query<MemberRow>(
        'SELECT user_id FROM tenant_members WHERE tenant_id = $1 AND user_id = $2 LIMIT 1',
        [tenantId, userId],
      );
      if (result.rows[0] === undefined) {
        throw new ForbiddenError('Not a member of this tenant');
      }
    }

    fastify.get<{ Params: TenantParams }>(
      '/tenants/:tenantId/leaderboard',
      {
        preHandler: [userPreHandler],
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

        const projectsResult = await container.pg.query<ProjectRow>(
          `SELECT id, name FROM projects WHERE tenant_id = $1 AND is_archived = false`,
          [tenantId],
        );
        const projectRows = projectsResult.rows;
        const tenantProjectIds = new Set(projectRows.map((p) => p.id));
        const nameByProjectId = new Map(projectRows.map((p) => [p.id, p.name]));

        const dateKey = todayKey();
        const redisKey = `leaderboard:${dateKey}`;

        // Fetch a bounded slice of the global sorted set.  Fetching 0..-1 (all)
        // loads every project across all tenants; cap at a multiple of LEADERBOARD_TOP_N
        // so we always have enough candidates to filter down to the tenant's top N
        // without scanning the entire set.  1000 is a safe upper bound for most deployments.
        const MAX_GLOBAL_FETCH = 1000;
        const raw = await container.redis.client.zrevrange(redisKey, 0, MAX_GLOBAL_FETCH - 1, 'WITHSCORES');

        const allEntries: Array<{ projectId: string; score: number }> = [];
        for (let i = 0; i < raw.length; i += 2) {
          const projectId = raw[i];
          const score = parseFloat(raw[i + 1] ?? '0');
          if (projectId !== undefined) {
            allEntries.push({ projectId, score });
          }
        }

        const tenantEntries = allEntries.filter((e) => tenantProjectIds.has(e.projectId));

        const leaderboard: LeaderboardEntry[] = tenantEntries
          .slice(0, LEADERBOARD_TOP_N)
          .map((entry, index) => ({
            projectId: entry.projectId,
            projectName: nameByProjectId.get(entry.projectId) ?? entry.projectId,
            eventCount: Math.round(entry.score),
            rank: index + 1,
          }));

        void reply.status(200).send({
          date: dateKey,
          leaderboard,
        });
      },
    );
  }, { name: 'leaderboard-routes', fastify: '4.x' });
}
