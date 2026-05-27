/**
 * seed-redis.ts
 * Pre-populates Redis with:
 *  - Sliding-window rate-limit sorted sets at 50% quota for each active project
 *  - Leaderboard (per-day ZINCRBY) for the last 7 days
 *
 * Run with: tsx seed/seed-redis.ts
 */

import pg from 'pg';
import { Redis } from 'ioredis';

// ---------------------------------------------------------------------------
// Connections (standalone)
// ---------------------------------------------------------------------------

const PG_HOST = process.env['PG_HOST'] ?? 'localhost';
const PG_PORT = Number(process.env['PG_PORT'] ?? 5432);
const PG_DATABASE = process.env['PG_DATABASE'] ?? 'pulseboard';
const PG_USER = process.env['PG_USER'] ?? 'postgres';
const PG_PASSWORD = process.env['PG_PASSWORD'] ?? 'postgres';

const REDIS_HOST = process.env['REDIS_HOST'] ?? 'localhost';
const REDIS_PORT = Number(process.env['REDIS_PORT'] ?? 6379);

// Rate limit key convention (matches src/lib/rate-limit.ts)
// rl:apikey:{apiKey}  → sliding-window sorted set
const RL_KEY_PREFIX = 'rl:apikey:';

// Leaderboard key convention (matches src/domain/ingestion.ts stage 7)
// leaderboard:{YYYY-MM-DD}  → sorted set of projectId → event count
const LEADERBOARD_KEY_PREFIX = 'leaderboard:';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function randInt(min: number, max: number): number {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

/**
 * Per-minute rate limit derived from monthly quota.
 * Logic mirrors src/lib/rate-limit.ts getPlanRateLimit().
 */
function quotaToPerMinute(eventQuotaPerMonth: number): number {
  return Math.max(10, Math.ceil(eventQuotaPerMonth / 43_200));
}

function formatDateKey(date: Date): string {
  const yyyy = date.getUTCFullYear();
  const mm = String(date.getUTCMonth() + 1).padStart(2, '0');
  const dd = String(date.getUTCDate()).padStart(2, '0');
  return `${yyyy}-${mm}-${dd}`;
}

// ---------------------------------------------------------------------------
// Main seeder
// ---------------------------------------------------------------------------

export async function seedRedis(): Promise<void> {
  // ── PostgreSQL: load active projects with their plan quota ────────────────
  const pgPool = new pg.Pool({
    host: PG_HOST,
    port: PG_PORT,
    database: PG_DATABASE,
    user: PG_USER,
    password: PG_PASSWORD,
    max: 3,
    idleTimeoutMillis: 10_000,
    connectionTimeoutMillis: 10_000,
  });

  console.log('[redis] Loading active projects from PostgreSQL…');

  const result = await pgPool.query<{
    id: string;
    api_key: string;
    event_quota_per_month: string;
  }>(`
    SELECT p.id, p.api_key, pl.event_quota_per_month
    FROM projects p
    JOIN tenants t ON t.id = p.tenant_id
    JOIN plans pl ON pl.id = t.plan_id
    WHERE p.is_archived = false
      AND t.is_active = true
  `);

  const projects = result.rows;
  console.log(`[redis] ${projects.length.toLocaleString()} active projects loaded.`);

  await pgPool.end();

  // ── Redis ─────────────────────────────────────────────────────────────────
  const redis = new Redis({
    host: REDIS_HOST,
    port: REDIS_PORT,
    lazyConnect: true,
    enableReadyCheck: true,
    maxRetriesPerRequest: 3,
  });

  await redis.connect();
  console.log('[redis] Connected.');

  const PIPELINE_BATCH = 200; // flush pipeline every N projects
  const now = Date.now();
  const WINDOW_MS = 60_000; // 1-minute sliding window

  let processed = 0;

  // ── Rate-limit sorted sets ────────────────────────────────────────────────
  console.log('[redis] Populating rate-limit sorted sets at 50% quota…');

  for (let i = 0; i < projects.length; i += PIPELINE_BATCH) {
    const batch = projects.slice(i, i + PIPELINE_BATCH);
    const pipeline = redis.pipeline();

    for (const project of batch) {
      const perMinute = quotaToPerMinute(Number(project.event_quota_per_month));
      const halfQuota = Math.floor(perMinute / 2);
      const rlKey = `${RL_KEY_PREFIX}${project.api_key}`;

      // Add halfQuota entries with timestamps spread across the last 60 seconds
      for (let entry = 0; entry < halfQuota; entry++) {
        // Spread timestamps evenly across the window
        const timestamp = now - Math.floor((entry / halfQuota) * WINDOW_MS);
        const member = `seed-${entry}-${Math.random().toString(36).slice(2, 8)}`;
        pipeline.zadd(rlKey, timestamp, member);
      }

      // Set TTL slightly beyond the window so old entries naturally expire
      pipeline.expire(rlKey, 120); // 2 minutes
    }

    await pipeline.exec();
    processed += batch.length;

    if (processed % 5000 === 0 || processed >= projects.length) {
      console.log(`[redis]   rate-limits: ${processed.toLocaleString()} / ${projects.length.toLocaleString()}`);
    }
  }

  // ── Leaderboard for last 7 days ────────────────────────────────────────────
  console.log('[redis] Populating 7-day leaderboard…');

  const DAYS = 7;

  for (let day = 0; day < DAYS; day++) {
    const date = new Date(now - day * 24 * 60 * 60 * 1000);
    const dateKey = `${LEADERBOARD_KEY_PREFIX}${formatDateKey(date)}`;

    // Seed leaderboard in batches
    for (let i = 0; i < projects.length; i += PIPELINE_BATCH) {
      const batch = projects.slice(i, i + PIPELINE_BATCH);
      const pipeline = redis.pipeline();

      for (const project of batch) {
        // Realistic daily event count: random fraction of per-minute quota × 1440 minutes
        const perMinute = quotaToPerMinute(Number(project.event_quota_per_month));
        const dailyEvents = randInt(
          Math.floor(perMinute * 60),      // low end: ~1 hour of traffic
          Math.floor(perMinute * 720),     // high end: ~half a day of traffic
        );
        pipeline.zincrby(dateKey, dailyEvents, project.id);
      }

      await pipeline.exec();
    }

    // Set TTL: keep leaderboard for 8 days
    await redis.expire(dateKey, 8 * 24 * 60 * 60);
    console.log(`[redis]   leaderboard day ${day + 1}/${DAYS}: ${formatDateKey(date)}`);
  }

  console.log('[redis] Done.');

  await redis.quit();
}

// ---------------------------------------------------------------------------
// Run directly
// ---------------------------------------------------------------------------

if (process.argv[1]?.endsWith('seed-redis.ts') || process.argv[1]?.endsWith('seed-redis.js')) {
  console.time('redis');
  seedRedis()
    .then(() => { console.timeEnd('redis'); process.exit(0); })
    .catch((err) => { console.error('[redis] FATAL:', err); process.exit(1); });
}
