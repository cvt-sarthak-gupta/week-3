/**
 * seed-postgres.ts
 * Seeds PostgreSQL with plans, tenants, users, tenant_members, projects,
 * and monthly_usage using UNNEST-based bulk INSERT for maximum throughput.
 *
 * Run with: tsx seed/seed-postgres.ts
 */

import pg from 'pg';
import { randomUUID } from 'node:crypto';

// ---------------------------------------------------------------------------
// DB connection (standalone — does NOT import src/config.ts)
// ---------------------------------------------------------------------------

const PG_HOST = process.env['PG_HOST'] ?? 'localhost';
const PG_PORT = Number(process.env['PG_PORT'] ?? 5432);
const PG_DATABASE = process.env['PG_DATABASE'] ?? 'pulseboard';
const PG_USER = process.env['PG_USER'] ?? 'postgres';
const PG_PASSWORD = process.env['PG_PASSWORD'] ?? 'postgres';

function createPool(): pg.Pool {
  return new pg.Pool({
    host: PG_HOST,
    port: PG_PORT,
    database: PG_DATABASE,
    user: PG_USER,
    password: PG_PASSWORD,
    max: 5,
    idleTimeoutMillis: 10_000,
    connectionTimeoutMillis: 10_000,
  });
}

// ---------------------------------------------------------------------------
// Word lists for realistic random names
// ---------------------------------------------------------------------------

const ADJECTIVES = [
  'rapid', 'swift', 'bright', 'smart', 'bold', 'calm', 'clean', 'clear',
  'crisp', 'deep', 'dense', 'deft', 'epic', 'fast', 'firm', 'flat', 'free',
  'full', 'good', 'grand', 'great', 'hard', 'high', 'huge', 'just', 'keen',
  'kind', 'large', 'lean', 'live', 'long', 'loud', 'main', 'mild', 'neat',
  'next', 'nice', 'open', 'peak', 'pure', 'real', 'rich', 'safe', 'sharp',
  'slim', 'soft', 'sure', 'tall', 'true', 'vast', 'warm', 'wide', 'wise',
  'prime', 'super', 'ultra', 'mega', 'apex', 'core', 'edge', 'flow', 'flux',
  'grid', 'hive', 'hub', 'ion', 'key', 'lux', 'max', 'neo', 'nova', 'omni',
];

const NOUNS = [
  'falcon', 'hawk', 'eagle', 'raven', 'wolf', 'tiger', 'lion', 'bear',
  'fox', 'lynx', 'shark', 'whale', 'crane', 'heron', 'finch', 'robin',
  'apex', 'base', 'beam', 'bolt', 'byte', 'chip', 'code', 'core', 'data',
  'deck', 'disk', 'edge', 'flow', 'flux', 'gate', 'gear', 'grid', 'hive',
  'hook', 'hub', 'icon', 'keys', 'lane', 'link', 'lock', 'loop', 'mesh',
  'mode', 'node', 'pack', 'path', 'peak', 'pipe', 'plug', 'port', 'rack',
  'rail', 'ring', 'root', 'rope', 'rune', 'sail', 'seed', 'ship', 'sign',
  'site', 'slot', 'span', 'spec', 'spin', 'star', 'stem', 'step', 'sync',
  'tank', 'tape', 'term', 'tide', 'tile', 'tool', 'tree', 'tube', 'tune',
  'unit', 'vault', 'view', 'wave', 'wire', 'work', 'yard', 'zone',
];

const FIRST_NAMES = [
  'alice', 'bob', 'carol', 'dave', 'eve', 'frank', 'grace', 'henry',
  'iris', 'jake', 'kate', 'leo', 'mia', 'neil', 'olivia', 'paul',
  'quinn', 'rachel', 'sam', 'tara', 'uma', 'victor', 'wendy', 'xander',
  'yara', 'zoe', 'alex', 'blake', 'casey', 'drew', 'eli', 'fiona',
  'gabe', 'hana', 'ivan', 'juno', 'kyle', 'luna', 'morgan', 'noel',
];

const LAST_NAMES = [
  'smith', 'jones', 'brown', 'davis', 'miller', 'wilson', 'moore',
  'taylor', 'anderson', 'thomas', 'jackson', 'white', 'harris', 'martin',
  'garcia', 'martinez', 'robinson', 'clark', 'rodriguez', 'lewis',
  'lee', 'walker', 'hall', 'allen', 'young', 'hernandez', 'king',
  'wright', 'lopez', 'hill', 'scott', 'green', 'adams', 'baker', 'nelson',
];

const DOMAINS = [
  'gmail.com', 'yahoo.com', 'outlook.com', 'protonmail.com', 'icloud.com',
  'fastmail.com', 'hey.com', 'tutanota.com', 'zoho.com', 'aol.com',
];

const VERBS = [
  'track', 'watch', 'monitor', 'log', 'audit', 'capture', 'analyze',
  'stream', 'process', 'index', 'query', 'report', 'alert', 'detect',
  'collect', 'forward', 'filter', 'route', 'parse', 'enrich',
];

const PROJECT_NOUNS = [
  'payments', 'events', 'users', 'sessions', 'orders', 'metrics',
  'logs', 'errors', 'alerts', 'reports', 'webhooks', 'api', 'jobs',
  'tasks', 'pipelines', 'streams', 'queues', 'cache', 'search', 'auth',
];

// ---------------------------------------------------------------------------
// Random helpers
// ---------------------------------------------------------------------------

function randInt(min: number, max: number): number {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

function pick<T>(arr: T[]): T {
  return arr[Math.floor(Math.random() * arr.length)]!;
}

let _tenantNameCounter = 0;
function tenantName(): string {
  _tenantNameCounter++;
  return `${pick(ADJECTIVES)} ${pick(NOUNS)} Co ${_tenantNameCounter}`;
}

function tenantSlug(name: string, index: number): string {
  return name.toLowerCase().replace(/\s+co\s+\d+$/, '').replace(/\s+/g, '-') + `-${index}`;
}

function userEmail(index: number): string {
  const first = pick(FIRST_NAMES);
  const last = pick(LAST_NAMES);
  return `${first}.${last}.${index}@${pick(DOMAINS)}`;
}

function userName(): string {
  const first = pick(FIRST_NAMES);
  const last = pick(LAST_NAMES);
  return `${first.charAt(0).toUpperCase()}${first.slice(1)} ${last.charAt(0).toUpperCase()}${last.slice(1)}`;
}

function projectName(tenantIndex: number, projectIndex: number): string {
  return `${pick(VERBS)}-${pick(PROJECT_NOUNS)}-${tenantIndex}-${projectIndex}`;
}

function projectSlug(name: string): string {
  return name.toLowerCase().replace(/[^a-z0-9-]/g, '-');
}

// Random date within last N months
function randomDate(monthsBack: number): Date {
  const now = Date.now();
  const past = now - monthsBack * 30 * 24 * 60 * 60 * 1000;
  return new Date(past + Math.random() * (now - past));
}

// ---------------------------------------------------------------------------
// Bulk INSERT helper (UNNEST approach)
// ---------------------------------------------------------------------------

const BATCH_SIZE = 500;

async function bulkInsert(
  client: pg.PoolClient,
  table: string,
  columns: string[],
  types: string[],
  rows: unknown[][],
): Promise<void> {
  if (rows.length === 0) return;

  // Build: INSERT INTO t (c1, c2, ...) SELECT unnest($1::type[]), unnest($2::type[]), ...
  const selects = columns.map((_, i) => `unnest($${i + 1}::${types[i]}[])`).join(', ');
  const sql = `INSERT INTO ${table} (${columns.join(', ')}) SELECT ${selects}`;

  // Transpose rows → columns
  const colArrays: unknown[][] = columns.map(() => []);
  for (const row of rows) {
    for (let c = 0; c < columns.length; c++) {
      colArrays[c]!.push(row[c]);
    }
  }

  await client.query(sql, colArrays);
}

// ---------------------------------------------------------------------------
// Main seeder
// ---------------------------------------------------------------------------

export async function seedPostgres(): Promise<void> {
  const pool = createPool();
  const client = await pool.connect();

  try {
    // ── 0. Disable session-level triggers to skip audit log during seeding ──
    await client.query('SET session_replication_role = replica');

    // ── 1. Plans ─────────────────────────────────────────────────────────────
    console.log('[postgres] Seeding plans…');

    // Clear existing data (order matters for FK constraints)
    await client.query('TRUNCATE monthly_usage, usage_dedup, alert_rules, projects, tenant_members, users, tenants, plans CASCADE');

    const freePlanId = randomUUID();
    const proPlanId = randomUUID();
    const enterprisePlanId = randomUUID();

    const plans = [
      [freePlanId, 'Free', 10_000, 30, 3, 0],
      [proPlanId, 'Pro', 500_000, 90, 20, 2900],
      [enterprisePlanId, 'Enterprise', 10_000_000, 365, 100, 29900],
    ];

    for (const plan of plans) {
      await client.query(
        `INSERT INTO plans (id, name, event_quota_per_month, retention_days, max_projects, price_cents)
         VALUES ($1, $2, $3, $4, $5, $6)`,
        plan,
      );
    }
    console.log('[postgres] Plans seeded: Free, Pro, Enterprise');

    // ── 2. Tenants ────────────────────────────────────────────────────────────
    console.log('[postgres] Seeding 10,000 tenants…');

    const TENANT_COUNT = 10_000;
    const tenantIds: string[] = [];
    const tenantPlanIds: string[] = [];

    // Plan distribution: 70% free, 25% pro, 5% enterprise
    const planDist = [freePlanId, proPlanId, enterprisePlanId];
    function pickPlan(): string {
      const r = Math.random();
      if (r < 0.70) return freePlanId;
      if (r < 0.95) return proPlanId;
      return enterprisePlanId;
    }

    for (let batch = 0; batch < TENANT_COUNT; batch += BATCH_SIZE) {
      const rows: unknown[][] = [];
      const end = Math.min(batch + BATCH_SIZE, TENANT_COUNT);

      for (let i = batch; i < end; i++) {
        const id = randomUUID();
        const name = tenantName();
        const slug = tenantSlug(name, i);
        const planId = pickPlan();
        tenantIds.push(id);
        tenantPlanIds.push(planId);
        rows.push([id, name, slug, planId, randomDate(12), true]);
      }

      await bulkInsert(client, 'tenants',
        ['id', 'name', 'slug', 'plan_id', 'created_at', 'is_active'],
        ['uuid', 'text', 'text', 'uuid', 'timestamptz', 'boolean'],
        rows,
      );

      if ((batch + BATCH_SIZE) % 2000 === 0 || end === TENANT_COUNT) {
        console.log(`[postgres]   tenants: ${end.toLocaleString()} / ${TENANT_COUNT.toLocaleString()}`);
      }
    }

    // ── 3. Users + tenant_members ─────────────────────────────────────────────
    console.log('[postgres] Seeding ~30,000 users and tenant_members…');

    // 3 users per tenant: 1 owner + 2 members
    // Insert users in batches, then tenant_members
    const USERS_PER_TENANT = 3;
    let userGlobalIndex = 0;

    for (let batch = 0; batch < TENANT_COUNT; batch += BATCH_SIZE) {
      const end = Math.min(batch + BATCH_SIZE, TENANT_COUNT);
      const userRows: unknown[][] = [];
      const memberRows: unknown[][] = [];
      const batchUserIds: string[] = [];

      for (let t = batch; t < end; t++) {
        const tenantId = tenantIds[t]!;
        const tenantUserIds: string[] = [];

        for (let u = 0; u < USERS_PER_TENANT; u++) {
          const userId = randomUUID();
          const email = userEmail(userGlobalIndex++);
          const fullName = userName();
          const createdAt = randomDate(12);
          tenantUserIds.push(userId);
          userRows.push([userId, email, '$2b$10$placeholder_hash_for_seeding', fullName, createdAt]);
        }

        batchUserIds.push(...tenantUserIds);

        // Insert tenant_members: first user is owner, rest are members
        const roles = ['owner', 'member', 'member'];
        for (let u = 0; u < USERS_PER_TENANT; u++) {
          memberRows.push([tenantId, tenantUserIds[u]!, roles[u]!, randomDate(12)]);
        }
      }

      await bulkInsert(client, 'users',
        ['id', 'email', 'password_hash', 'full_name', 'created_at'],
        ['uuid', 'text', 'text', 'text', 'timestamptz'],
        userRows,
      );

      await bulkInsert(client, 'tenant_members',
        ['tenant_id', 'user_id', 'role', 'joined_at'],
        ['uuid', 'uuid', 'text', 'timestamptz'],
        memberRows,
      );

      if ((batch + BATCH_SIZE) % 2000 === 0 || end === TENANT_COUNT) {
        console.log(`[postgres]   users/members: tenant ${end.toLocaleString()} / ${TENANT_COUNT.toLocaleString()} (${(end * USERS_PER_TENANT).toLocaleString()} users)`);
      }
    }

    // ── 4. Projects ───────────────────────────────────────────────────────────
    console.log('[postgres] Seeding ~30,000 projects…');

    const PROJECTS_PER_TENANT = 3;
    // Store project ids for redis seeder to use later
    const projectRows: Array<{ id: string; tenantId: string; planId: string }> = [];

    for (let batch = 0; batch < TENANT_COUNT; batch += BATCH_SIZE) {
      const end = Math.min(batch + BATCH_SIZE, TENANT_COUNT);
      const rows: unknown[][] = [];

      for (let t = batch; t < end; t++) {
        const tenantId = tenantIds[t]!;
        for (let p = 0; p < PROJECTS_PER_TENANT; p++) {
          const id = randomUUID();
          const name = projectName(t, p);
          const slug = projectSlug(name);
          const apiKey = randomUUID();
          const createdAt = randomDate(12);
          projectRows.push({ id, tenantId, planId: tenantPlanIds[t]! });
          rows.push([id, tenantId, name, slug, apiKey, createdAt, false]);
        }
      }

      await bulkInsert(client, 'projects',
        ['id', 'tenant_id', 'name', 'slug', 'api_key', 'created_at', 'is_archived'],
        ['uuid', 'uuid', 'text', 'text', 'uuid', 'timestamptz', 'boolean'],
        rows,
      );

      if ((batch + BATCH_SIZE) % 2000 === 0 || end === TENANT_COUNT) {
        console.log(`[postgres]   projects: tenant ${end.toLocaleString()} / ${TENANT_COUNT.toLocaleString()} (${(end * PROJECTS_PER_TENANT).toLocaleString()} projects)`);
      }
    }

    // ── 5. Monthly usage (12 months per tenant) ───────────────────────────────
    console.log('[postgres] Seeding 12 months of monthly_usage per tenant…');

    const now = new Date();
    const currentYear = now.getUTCFullYear();
    const currentMonth = now.getUTCMonth() + 1; // 1-indexed

    // Plan quotas
    const planQuotas: Record<string, number> = {
      [freePlanId]: 10_000,
      [proPlanId]: 500_000,
      [enterprisePlanId]: 10_000_000,
    };

    for (let batch = 0; batch < TENANT_COUNT; batch += BATCH_SIZE) {
      const end = Math.min(batch + BATCH_SIZE, TENANT_COUNT);
      const rows: unknown[][] = [];

      for (let t = batch; t < end; t++) {
        const tenantId = tenantIds[t]!;
        const quota = planQuotas[tenantPlanIds[t]!] ?? 10_000;

        for (let m = 0; m < 12; m++) {
          // Compute year/month going back m months from current
          let month = currentMonth - m;
          let year = currentYear;
          if (month <= 0) {
            month += 12;
            year -= 1;
          }
          // Random event count: 10%–95% of quota
          const eventCount = Math.floor(quota * (0.10 + Math.random() * 0.85));
          rows.push([tenantId, year, month, eventCount, randInt(0, 50)]);
        }
      }

      await bulkInsert(client, 'monthly_usage',
        ['tenant_id', 'year', 'month', 'event_count', 'alert_fires'],
        ['uuid', 'int4', 'int4', 'int8', 'int4'],
        rows,
      );

      if ((batch + BATCH_SIZE) % 2000 === 0 || end === TENANT_COUNT) {
        console.log(`[postgres]   monthly_usage: tenant ${end.toLocaleString()} / ${TENANT_COUNT.toLocaleString()}`);
      }
    }

    // Re-enable triggers
    await client.query('SET session_replication_role = DEFAULT');

    console.log('[postgres] Done.');
    console.log(`[postgres] Summary:`);
    console.log(`  - 3 plans`);
    console.log(`  - ${TENANT_COUNT.toLocaleString()} tenants`);
    console.log(`  - ${(TENANT_COUNT * USERS_PER_TENANT).toLocaleString()} users + tenant_members`);
    console.log(`  - ${(TENANT_COUNT * PROJECTS_PER_TENANT).toLocaleString()} projects`);
    console.log(`  - ${(TENANT_COUNT * 12).toLocaleString()} monthly_usage rows`);

  } finally {
    client.release();
    await pool.end();
  }
}

// ---------------------------------------------------------------------------
// Run directly
// ---------------------------------------------------------------------------

if (process.argv[1]?.endsWith('seed-postgres.ts') || process.argv[1]?.endsWith('seed-postgres.js')) {
  console.time('postgres');
  seedPostgres()
    .then(() => { console.timeEnd('postgres'); process.exit(0); })
    .catch((err) => { console.error('[postgres] FATAL:', err); process.exit(1); });
}
