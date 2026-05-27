import { redis } from '../db/redis.js';
import { pool, withTenant } from '../db/postgres.js';
import { NotFoundError } from '../errors.js';
import { logger } from '../logger.js';

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export interface ProjectRecord {
  id: string;
  tenantId: string;
  planId: string;
  apiKey: string;
  retentionDays: number;
}

// ---------------------------------------------------------------------------
// Internal constants
// ---------------------------------------------------------------------------

const API_KEY_CACHE_PREFIX = 'apikey:';
const API_KEY_CACHE_TTL = 60; // seconds

// ---------------------------------------------------------------------------
// API key validation
// ---------------------------------------------------------------------------

/**
 * Validates an API key by checking Redis first, then falling back to PostgreSQL.
 * On a PG hit the result is cached in Redis for 60 seconds.
 * Throws NotFoundError if the key is unknown or the project is archived.
 */
export async function validateApiKey(apiKey: string): Promise<ProjectRecord> {
  const cacheKey = `${API_KEY_CACHE_PREFIX}${apiKey}`;

  // 1. Redis cache hit
  const cached = await redis.get(cacheKey);
  if (cached !== null) {
    logger.debug({ apiKey }, 'api key cache hit');
    return JSON.parse(cached) as ProjectRecord;
  }

  // 2. PG fallback — join projects → tenants → plans
  const result = await pool.query<{
    id: string;
    tenant_id: string;
    plan_id: string;
    api_key: string;
    retention_days: number;
  }>(
    `
    SELECT
      p.id,
      p.tenant_id,
      t.plan_id,
      p.api_key::text,
      pl.retention_days
    FROM projects p
    JOIN tenants t   ON t.id = p.tenant_id
    JOIN plans   pl  ON pl.id = t.plan_id
    WHERE p.api_key::text = $1
      AND p.is_archived = false
      AND t.is_active = true
    LIMIT 1
    `,
    [apiKey],
  );

  const row = result.rows[0];
  if (row === undefined) {
    throw new NotFoundError(`API key not found: ${apiKey}`);
  }

  const record: ProjectRecord = {
    id: row.id,
    tenantId: row.tenant_id,
    planId: row.plan_id,
    apiKey: row.api_key,
    retentionDays: row.retention_days,
  };

  // 3. Warm the cache
  await redis.setex(cacheKey, API_KEY_CACHE_TTL, JSON.stringify(record));
  logger.debug({ apiKey, projectId: record.id }, 'api key cached');

  return record;
}

// ---------------------------------------------------------------------------
// Get project by ID (tenant-scoped via RLS)
// ---------------------------------------------------------------------------

/**
 * Fetches a project by ID within the given tenant context.
 * Uses withTenant to set the RLS session variable.
 * Throws NotFoundError if the project does not exist or is not accessible.
 */
export async function getProjectById(
  projectId: string,
  tenantId: string,
): Promise<ProjectRecord> {
  return withTenant(tenantId, async (client) => {
    const result = await client.query<{
      id: string;
      tenant_id: string;
      plan_id: string;
      api_key: string;
      retention_days: number;
    }>(
      `
      SELECT
        p.id,
        p.tenant_id,
        t.plan_id,
        p.api_key::text,
        pl.retention_days
      FROM projects p
      JOIN tenants t   ON t.id = p.tenant_id
      JOIN plans   pl  ON pl.id = t.plan_id
      WHERE p.id = $1
        AND p.is_archived = false
      LIMIT 1
      `,
      [projectId],
    );

    const row = result.rows[0];
    if (row === undefined) {
      throw new NotFoundError(`Project not found: ${projectId}`);
    }

    return {
      id: row.id,
      tenantId: row.tenant_id,
      planId: row.plan_id,
      apiKey: row.api_key,
      retentionDays: row.retention_days,
    };
  });
}

// ---------------------------------------------------------------------------
// Cache invalidation
// ---------------------------------------------------------------------------

/**
 * Removes the Redis cache entry for the given API key.
 * Called on key rotation or project archival.
 */
export async function invalidateApiKeyCache(apiKey: string): Promise<void> {
  const cacheKey = `${API_KEY_CACHE_PREFIX}${apiKey}`;
  await redis.del(cacheKey);
  logger.debug({ apiKey }, 'api key cache invalidated');
}
