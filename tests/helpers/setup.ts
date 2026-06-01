/**
 * tests/helpers/setup.ts
 * Vitest global setup file.
 *
 * Loaded via the `setupFiles` option in vitest.config.ts:
 *   setupFiles: ['./tests/helpers/setup.ts']
 *
 * Responsibilities:
 *  1. Force NODE_ENV=test
 *  2. Build a testConfig and create an AppContainer instance
 *  3. Initialize the container in beforeAll, close it in afterAll
 *  4. Export backward-compatible helpers: getPgPool, getMongoDb, getMongoClient,
 *     getRedisClient, getContainer, truncateAll, createTestTenant, createTestEvent,
 *     setupTestDatabases, teardownTestDatabases
 */

import { randomUUID } from 'node:crypto';
import { afterAll, beforeAll } from 'vitest';
import { AppContainer } from '../../src/container.js';
import { config } from '../../src/config.js';

// ---------------------------------------------------------------------------
// Force test environment
// ---------------------------------------------------------------------------

process.env['NODE_ENV'] = 'test';

// ---------------------------------------------------------------------------
// Test connection config (matches docker-compose.test.yml isolated ports)
// ---------------------------------------------------------------------------

const TEST_PG_HOST     = process.env['TEST_PG_HOST']     ?? 'localhost';
const TEST_PG_PORT     = Number(process.env['TEST_PG_PORT']     ?? 5433);
const TEST_PG_DATABASE = process.env['TEST_PG_DATABASE'] ?? 'pulseboard_test';
const TEST_PG_USER     = process.env['TEST_PG_USER']     ?? 'postgres';
const TEST_PG_PASSWORD = process.env['TEST_PG_PASSWORD'] ?? 'postgres';

const TEST_MONGO_URL = process.env['TEST_MONGO_URL'] ?? 'mongodb://localhost:27018/?replicaSet=rs0';
const TEST_MONGO_DB  = process.env['TEST_MONGO_DB']  ?? 'pulseboard_test';

const TEST_REDIS_HOST = process.env['TEST_REDIS_HOST'] ?? 'localhost';
const TEST_REDIS_PORT = Number(process.env['TEST_REDIS_PORT'] ?? 6380);

const TEST_ES_URL = process.env['TEST_ES_URL'] ?? 'http://localhost:9201';

// Build a testConfig that mirrors the shape of src/config.ts `config`
const testConfig: typeof config = {
  ...config,
  node: 'test',
  pg: Object.freeze({
    host: TEST_PG_HOST,
    port: TEST_PG_PORT,
    database: TEST_PG_DATABASE,
    user: TEST_PG_USER,
    password: TEST_PG_PASSWORD,
    poolMin: 1,
    poolMax: 5,
  }),
  mongo: Object.freeze({
    url: TEST_MONGO_URL,
    dbName: TEST_MONGO_DB,
  }),
  redis: Object.freeze({
    host: TEST_REDIS_HOST,
    port: TEST_REDIS_PORT,
  }),
  es: Object.freeze({
    url: TEST_ES_URL,
  }),
};

// ---------------------------------------------------------------------------
// Singleton container (initialised in beforeAll)
// ---------------------------------------------------------------------------

let container: AppContainer;

// ---------------------------------------------------------------------------
// Lifecycle — initialize / close
// ---------------------------------------------------------------------------

beforeAll(async () => {
  container = new AppContainer(testConfig);
  await container.initialize();
});

afterAll(async () => {
  await container?.close();
});

// ---------------------------------------------------------------------------
// Adapter exports — backward-compatible with tests that used raw pg.Pool,
// MongoClient, and ioredis.Redis directly.
// ---------------------------------------------------------------------------

/**
 * Returns an object whose .query() and .connect() signatures match pg.Pool,
 * delegating to PostgresDatabase which has identical method signatures.
 */
export function getPgPool(): Pick<import('../../src/db/postgres.js').PostgresDatabase, 'query' | 'connect'> {
  return container.pg;
}

/**
 * Returns the mongodb.Db instance from the container's MongoDatabase.
 */
export function getMongoDb(): ReturnType<AppContainer['mongo']['db']> {
  return container.mongo.db();
}

/**
 * Returns the underlying MongoClient from mongodb.Db.
 * mongodb.Db exposes .client on the Db instance.
 */
export function getMongoClient() {
  return (container.mongo.db() as import('mongodb').Db & { client: import('mongodb').MongoClient }).client;
}

/**
 * Returns the ioredis.Redis client from the container's RedisDatabase.
 */
export function getRedisClient(): import('ioredis').Redis {
  return container.redis.client;
}

/**
 * Returns the raw AppContainer for tests that need direct service access.
 */
export function getContainer(): AppContainer {
  return container;
}

// ---------------------------------------------------------------------------
// Helper: truncateAll
// Clears all test data from all stores.
// ---------------------------------------------------------------------------

export async function truncateAll(): Promise<void> {
  // PostgreSQL — truncate in dependency order
  await container.pg.query(`
    TRUNCATE
      audit_log,
      billing_events,
      monthly_usage,
      usage_dedup,
      alert_rules,
      projects,
      tenant_members,
      users,
      tenants
    RESTART IDENTITY CASCADE
  `);

  // MongoDB — clear all collections
  const db = container.mongo.db();
  const colls = await db.listCollections().toArray();
  for (const coll of colls) {
    await db.collection(coll.name).deleteMany({});
  }

  // Redis — flush all keys
  const keys = await container.redis.client.keys('*');
  if (keys.length > 0) {
    await container.redis.client.del(...keys);
  }
}

// ---------------------------------------------------------------------------
// Helper: createTestTenant
// Creates a minimal tenant + owner user + project for a test, returns IDs.
// ---------------------------------------------------------------------------

export interface TestTenantFixture {
  tenantId: string;
  userId: string;
  projectId: string;
  apiKey: string;
  planId: string;
}

export async function createTestTenant(): Promise<TestTenantFixture> {
  const client = await container.pg.connect();
  try {
    await client.query('BEGIN');

    // Ensure at least one plan exists (idempotent)
    const planRes = await client.query<{ id: string }>(
      `INSERT INTO plans (name, event_quota_per_month, retention_days, max_projects, price_cents)
       VALUES ('TestPlan', 10000, 30, 10, 0)
       ON CONFLICT DO NOTHING
       RETURNING id`,
    );
    let planId: string;
    if (planRes.rowCount && planRes.rowCount > 0) {
      planId = planRes.rows[0]!.id;
    } else {
      const existing = await client.query<{ id: string }>('SELECT id FROM plans LIMIT 1');
      planId = existing.rows[0]!.id;
    }

    // Tenant
    const tenantId = randomUUID();
    const slug = `test-tenant-${tenantId.slice(0, 8)}`;
    await client.query(
      `INSERT INTO tenants (id, name, slug, plan_id) VALUES ($1, $2, $3, $4)`,
      [tenantId, 'Test Tenant', slug, planId],
    );

    // User
    const userId = randomUUID();
    await client.query(
      `INSERT INTO users (id, email, password_hash, full_name)
       VALUES ($1, $2, $3, $4)`,
      [userId, `test-${userId.slice(0, 8)}@example.com`, '$2b$10$testhash', 'Test User'],
    );

    // Tenant member (owner)
    await client.query(
      `INSERT INTO tenant_members (tenant_id, user_id, role) VALUES ($1, $2, 'owner')`,
      [tenantId, userId],
    );

    // Project
    const projectId = randomUUID();
    const apiKey = randomUUID();
    const projectSlug = `test-project-${projectId.slice(0, 8)}`;
    await client.query(
      `INSERT INTO projects (id, tenant_id, name, slug, api_key)
       VALUES ($1, $2, $3, $4, $5)`,
      [projectId, tenantId, 'Test Project', projectSlug, apiKey],
    );

    await client.query('COMMIT');

    return { tenantId, userId, projectId, apiKey, planId };
  } catch (err) {
    await client.query('ROLLBACK');
    throw err;
  } finally {
    client.release();
  }
}

// ---------------------------------------------------------------------------
// Helper: createTestEvent
// Returns a minimal valid EventIngest payload (does NOT write to any DB).
// ---------------------------------------------------------------------------

export interface MinimalEventIngest {
  type: 'error' | 'log' | 'metric' | 'custom';
  severity: 'debug' | 'info' | 'warn' | 'error' | 'fatal';
  message: string;
}

export function createTestEvent(overrides?: Partial<MinimalEventIngest>): MinimalEventIngest {
  return {
    type: 'log',
    severity: 'info',
    message: 'Test event from vitest setup',
    ...overrides,
  };
}

// ---------------------------------------------------------------------------
// setupTestDatabases / teardownTestDatabases
// Convenience wrappers called explicitly in cross-DB integration test suites
// that do not rely on the global setupFiles lifecycle alone.
// ---------------------------------------------------------------------------

/**
 * Ensures all DB connections are alive and data is cleared.
 * Safe to call multiple times; re-uses the existing container singleton.
 */
export async function setupTestDatabases(): Promise<void> {
  // The container is initialised by the beforeAll in this same file (via setupFiles).
  // Poll briefly in case a suite-level call races against global setup.
  const deadline = Date.now() + 20_000;
  while (Date.now() < deadline) {
    if (container) break;
    await new Promise<void>((r) => setTimeout(r, 100));
  }
  if (!container) {
    throw new Error('setupTestDatabases: AppContainer did not initialise in time');
  }
  await truncateAll();
}

/**
 * Tears down connections opened for a specific test suite.
 * In practice the global afterAll handles closure; this is a no-op hook that
 * callers can await for symmetry.
 */
export async function teardownTestDatabases(): Promise<void> {
  // Intentionally a no-op: the module-level afterAll handles container teardown.
  // Individual test suites must NOT close the shared container early.
}

// ---------------------------------------------------------------------------
// Domain function wrappers — let tests call these directly rather than
// importing module-level functions that no longer exist after the class refactor
// ---------------------------------------------------------------------------

export type { IngestPayload } from '../../src/domain/ingestion.js';

export async function processEvent(
  payload: import('../../src/domain/ingestion.js').IngestPayload,
): Promise<void> {
  return getContainer().ingestion.processEvent(payload);
}

export async function onboardTenant(
  input: Parameters<import('../../src/domain/tenants.js').TenantService['onboardTenant']>[0],
): Promise<ReturnType<import('../../src/domain/tenants.js').TenantService['onboardTenant']>> {
  return getContainer().tenants.onboardTenant(input);
}

export async function runAudit(): Promise<import('../../src/domain/consistency.js').AuditResult> {
  return getContainer().consistency.runAudit();
}

export async function getOrFill<T>(
  key: string,
  ttlSeconds: number,
  fetcher: () => Promise<T>,
): Promise<T> {
  return getContainer().cache.getOrFill(key, ttlSeconds, fetcher);
}

export async function invalidatePattern(pattern: string): Promise<number> {
  return getContainer().cache.invalidatePattern(pattern);
}

export function getEsClient() {
  return getContainer().es.client;
}

export async function ensureIlmPolicies(): Promise<void> {
  return getContainer().es.ensureIlmPolicies();
}

export function resolveTierPolicy(retentionDays: number): string {
  // Policy name follows the pattern used by ensureIlmPolicy().
  return `logs-retention-${retentionDays}d`;
}

export async function applyPolicyForProject(projectId: string, retentionDays: number): Promise<void> {
  return getContainer().es.applyPolicyForProject(projectId, retentionDays);
}
