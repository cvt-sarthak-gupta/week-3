import type { FastifyInstance, FastifyPluginAsync, FastifyReply } from 'fastify';
import fp from 'fastify-plugin';
import { pool } from '../db/postgres.js';
import { redis } from '../db/redis.js';
import { authenticateUser } from '../lib/auth.js';
import { ForbiddenError } from '../errors.js';

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const LEADERBOARD_TOP_N = 20;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

async function assertMember(userId: string, tenantId: string): Promise<void> {
  const result = await pool.query<MemberRow>(
    'SELECT user_id FROM tenant_members WHERE tenant_id = $1 AND user_id = $2 LIMIT 1',
    [tenantId, userId],
  );
  if (result.rows[0] === undefined) {
    throw new ForbiddenError('Not a member of this tenant');
  }
}

function todayKey(): string {
  const now = new Date();
  const yyyy = now.getUTCFullYear();
  const mm = (now.getUTCMonth() + 1).toString().padStart(2, '0');
  const dd = now.getUTCDate().toString().padStart(2, '0');
  return `${yyyy}-${mm}-${dd}`;
}

// ---------------------------------------------------------------------------
// Plugin
// ---------------------------------------------------------------------------

const leaderboardPluginHandler: FastifyPluginAsync = async (fastify: FastifyInstance) => {
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

      const projectsResult = await pool.query<ProjectRow>(
        `SELECT id, name FROM projects WHERE tenant_id = $1 AND is_archived = false`,
        [tenantId],
      );
      const projectRows = projectsResult.rows;
      const tenantProjectIds = new Set(projectRows.map((p) => p.id));
      const nameByProjectId = new Map(projectRows.map((p) => [p.id, p.name]));

      const dateKey = todayKey();
      const redisKey = `leaderboard:${dateKey}`;

      // Returns flat array: [member1, score1, member2, score2, ...]
      const raw = await redis.zrevrange(redisKey, 0, -1, 'WITHSCORES');

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
};

export const leaderboardRoutes = fp(leaderboardPluginHandler, {
  name: 'leaderboard-routes',
  fastify: '4.x',
});
