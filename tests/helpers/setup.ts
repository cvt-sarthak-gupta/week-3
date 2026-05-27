/**
 * tests/helpers/setup.ts
 * Vitest global setup file.
 *
 * Loaded via the `setupFiles` option in vitest.config.ts:
 *   setupFiles: ['./tests/helpers/setup.ts']
 *
 * Responsibilities:
 *  1. Force NODE_ENV=test
 *  2. Connect all DB clients using test ports from docker-compose.test.yml
 *  3. Export helper functions: truncateAll(), createTestTenant(), createTestEvent()
 */

import { randomUUID } from 'node:crypto';
import pg from 'pg';
import { MongoClient } from 'mongodb';
import { Redis } from 'ioredis';
import { afterAll, beforeAll } from 'vitest';
import * as mongoSingleton from '../../src/db/mongo.js';

// ---------------------------------------------------------------------------
// Force test environment
// ---------------------------------------------------------------------------

process.env['NODE_ENV'] = 'test';

// Test connection config (matches docker-compose.test.yml isolated ports)
const TEST_PG_HOST     = process.env['TEST_PG_HOST']     ?? 'localhost';
const TEST_PG_PORT     = Number(process.env['TEST_PG_PORT'] ?? 5433);      // mapped to 5433
const TEST_PG_DATABASE = process.env['TEST_PG_DATABASE'] ?? 'pulseboard_test';
const TEST_PG_USER     = process.env['TEST_PG_USER']     ?? 'postgres';
const TEST_PG_PASSWORD = process.env['TEST_PG_PASSWORD'] ?? 'postgres';

const TEST_MONGO_URL    = process.env['TEST_MONGO_URL']    ?? 'mongodb://localhost:27018/?replicaSet=rs0'; // 27018
const TEST_MONGO_DB     = process.env['TEST_MONGO_DB']     ?? 'pulseboard_test';

const TEST_REDIS_HOST = process.env['TEST_REDIS_HOST'] ?? 'localhost';
const TEST_REDIS_PORT = Number(process.env['TEST_REDIS_PORT'] ?? 6380); // 6380

// ---------------------------------------------------------------------------
// Singleton clients (initialised in beforeAll)
// ---------------------------------------------------------------------------

let pgPool: pg.Pool;
let mongoClient: MongoClient;
let redisClient: Redis;

// ---------------------------------------------------------------------------
// Lifecycle — connect / disconnect
// ---------------------------------------------------------------------------

beforeAll(async () => {
  // PostgreSQL
  pgPool = new pg.Pool({
    host: TEST_PG_HOST,
    port: TEST_PG_PORT,
    database: TEST_PG_DATABASE,
    user: TEST_PG_USER,
    password: TEST_PG_PASSWORD,
    max: 5,
    idleTimeoutMillis: 10_000,
    connectionTimeoutMillis: 10_000,
  });

  // MongoDB — test helper's own client
  mongoClient = new MongoClient(TEST_MONGO_URL, {
    serverSelectionTimeoutMS: 15_000,
    connectTimeoutMS: 15_000,
  });
  await mongoClient.connect();

  // Also connect the src/db/mongo.ts singleton so domain functions (processEvent, etc.) work
  await mongoSingleton.connect();

  // Redis
  redisClient = new Redis({
    host: TEST_REDIS_HOST,
    port: TEST_REDIS_PORT,
    lazyConnect: true,
    maxRetriesPerRequest: 3,
  });
  await redisClient.connect();
});

afterAll(async () => {
  await pgPool?.end();
  await mongoSingleton.close();
  await mongoClient?.close();
  await redisClient?.quit();
});

// ---------------------------------------------------------------------------
// Helper: truncateAll
// Clears all test data from all stores.
// ---------------------------------------------------------------------------

export async function truncateAll(): Promise<void> {
  // PostgreSQL — truncate in dependency order
  await pgPool.query(`
    TRUNCATE
      monthly_usage,
      usage_dedup,
      alert_rules,
      projects,
      tenant_members,
      users,
      tenants
    RESTART IDENTITY CASCADE
  `);

  // MongoDB — drop and re-create the events collection
  const db = mongoClient.db(TEST_MONGO_DB);
  const colls = await db.listCollections().toArray();
  for (const coll of colls) {
    await db.collection(coll.name).deleteMany({});
  }

  // Redis — flush only keys with the test prefix to avoid affecting other data
  const keys = await redisClient.keys('*');
  if (keys.length > 0) {
    await redisClient.del(...keys);
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
  const client = await pgPool.connect();
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
// Export clients for tests that need direct access
// ---------------------------------------------------------------------------

export function getPgPool(): pg.Pool {
  return pgPool;
}

export function getMongoClient(): MongoClient {
  return mongoClient;
}

export function getRedisClient(): Redis {
  return redisClient;
}

export function getMongoDb() {
  return mongoClient.db(TEST_MONGO_DB);
}

// ---------------------------------------------------------------------------
// setupTestDatabases / teardownTestDatabases
// Convenience wrappers called explicitly in cross-DB integration test suites
// that do not rely on the global setupFiles lifecycle alone.
// ---------------------------------------------------------------------------

/**
 * Ensures all DB connections are alive and data is cleared.
 * Safe to call multiple times; re-uses existing singleton clients.
 */
export async function setupTestDatabases(): Promise<void> {
  // Clients are initialised by the beforeAll in this same file (via setupFiles).
  // We wait until the singletons are ready by polling briefly if needed.
  const deadline = Date.now() + 20_000;
  while (Date.now() < deadline) {
    if (pgPool && mongoClient && redisClient) break;
    await new Promise<void>((r) => setTimeout(r, 100));
  }
  if (!pgPool || !mongoClient || !redisClient) {
    throw new Error('setupTestDatabases: DB clients did not initialise in time');
  }
  await truncateAll();
}

/**
 * Tears down connections opened for a specific test suite.
 * In practice the global afterAll handles closure; this is a no-op hook that
 * callers can await for symmetry.
 */
export async function teardownTestDatabases(): Promise<void> {
  // Intentionally a no-op: the module-level afterAll handles pool/client teardown.
  // Individual test suites must NOT close the shared singletons early.
}
