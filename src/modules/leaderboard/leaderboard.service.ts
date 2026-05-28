import type { RedisClient } from '../../db/redis/index.js';
import type { PostgresPool } from '../../db/postgres/index.js';
import type { LeaderboardEntry, LeaderboardResult } from './leaderboard.types.js';

const LEADERBOARD_TOP_N = 20;

interface ProjectRow {
  id: string;
  name: string;
}

function todayKey(): string {
  const now = new Date();
  const yyyy = now.getUTCFullYear();
  const mm = (now.getUTCMonth() + 1).toString().padStart(2, '0');
  const dd = now.getUTCDate().toString().padStart(2, '0');
  return `${yyyy}-${mm}-${dd}`;
}

export class LeaderboardService {
  constructor(
    private readonly redis: RedisClient,
    private readonly pool: PostgresPool,
  ) {}

  // ---------------------------------------------------------------------------
  // getLeaderboard
  // ZREVRANGE with WITHSCORES, then join with project names from PG
  // ---------------------------------------------------------------------------

  async getLeaderboard(tenantId: string): Promise<LeaderboardResult> {
    // 1. Fetch active project IDs and names for this tenant from PG
    const projectsResult = await this.pool.query<ProjectRow>(
      `SELECT id, name FROM projects WHERE tenant_id = $1 AND is_archived = false`,
      [tenantId],
    );
    const projectRows = projectsResult.rows;
    const tenantProjectIds = new Set(projectRows.map((p) => p.id));
    const nameByProjectId = new Map(projectRows.map((p) => [p.id, p.name]));

    // 2. Get today's leaderboard key from Redis
    const dateKey = todayKey();
    const redisKey = `leaderboard:${dateKey}`;

    // Returns flat array: [member1, score1, member2, score2, ...]
    const raw = await this.redis.client.zrevrange(redisKey, 0, -1, 'WITHSCORES');

    // 3. Parse the flat array into scored entries
    const allEntries: Array<{ projectId: string; score: number }> = [];
    for (let i = 0; i < raw.length; i += 2) {
      const projectId = raw[i];
      const score = parseFloat(raw[i + 1] ?? '0');
      if (projectId !== undefined) {
        allEntries.push({ projectId, score });
      }
    }

    // 4. Filter to only this tenant's projects, take top N, assign ranks
    const tenantEntries = allEntries.filter((e) => tenantProjectIds.has(e.projectId));

    const leaderboard: LeaderboardEntry[] = tenantEntries
      .slice(0, LEADERBOARD_TOP_N)
      .map((entry, index) => ({
        projectId: entry.projectId,
        projectName: nameByProjectId.get(entry.projectId) ?? entry.projectId,
        eventCount: Math.round(entry.score),
        rank: index + 1,
      }));

    return { date: dateKey, leaderboard };
  }
}
