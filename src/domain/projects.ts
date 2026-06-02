import { NotFoundError } from '../errors.js';
import { logger } from '../logger.js';
import type { PostgresDatabase } from '../db/postgres.js';
import type { RedisDatabase } from '../db/redis.js';
import type { ProjectContext } from '../types/projects.js';

export type { ProjectContext };

const API_KEY_CACHE_PREFIX = 'apikey:';
const API_KEY_CACHE_TTL = 60; // seconds

export class ProjectService {
  private readonly pg: PostgresDatabase;
  private readonly redis: RedisDatabase;

  constructor(pg: PostgresDatabase, redis: RedisDatabase) {
    this.pg = pg;
    this.redis = redis;
  }

  /**
   * Validates an API key by checking Redis first, then falling back to PostgreSQL.
   * On a PG hit the result is cached in Redis for 60 seconds.
   * Throws NotFoundError if the key is unknown or the project is archived.
   */
  async validateApiKey(apiKey: string): Promise<ProjectContext> {
    const cacheKey = `${API_KEY_CACHE_PREFIX}${apiKey}`;

    // 1. Redis cache hit
    const cached = await this.redis.client.get(cacheKey);
    if (cached !== null) {
      logger.debug({ apiKey }, 'api key cache hit');
      return JSON.parse(cached) as ProjectContext;
    }

    // 2. PG fallback — join projects → tenants → plans
    const result = await this.pg.query<{
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

    const record: ProjectContext = {
      id: row.id,
      tenantId: row.tenant_id,
      planId: row.plan_id,
      apiKey: row.api_key,
      retentionDays: row.retention_days,
    };

    // 3. Warm the cache
    await this.redis.client.setex(cacheKey, API_KEY_CACHE_TTL, JSON.stringify(record));
    logger.debug({ apiKey, projectId: record.id }, 'api key cached');

    return record;
  }

  /**
   * Fetches a project by ID within the given tenant context.
   * Uses withTenant to set the RLS session variable.
   * Throws NotFoundError if the project does not exist or is not accessible.
   */
  async getProject(projectId: string, tenantId: string): Promise<unknown> {
    return this.pg.withTenant(tenantId, async (client) => {
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

      const record: ProjectContext = {
        id: row.id,
        tenantId: row.tenant_id,
        planId: row.plan_id,
        apiKey: row.api_key,
        retentionDays: row.retention_days,
      };

      return record;
    });
  }

  /**
   * Lists all non-archived projects for the given tenant.
   */
  async listProjects(tenantId: string): Promise<unknown[]> {
    return this.pg.withTenant(tenantId, async (client) => {
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
        WHERE p.tenant_id = $1
          AND p.is_archived = false
        ORDER BY p.created_at DESC
        `,
        [tenantId],
      );

      return result.rows.map((row) => ({
        id: row.id,
        tenantId: row.tenant_id,
        planId: row.plan_id,
        apiKey: row.api_key,
        retentionDays: row.retention_days,
      }));
    });
  }

  /**
   * Creates a new project for the given tenant.
   * Throws NotFoundError if the tenant does not exist or is inactive.
   */
  async createProject(tenantId: string, input: unknown): Promise<unknown> {
    return this.pg.withTenant(tenantId, async (client) => {
      const data = input as Record<string, unknown>;

      const result = await client.query<{
        id: string;
        tenant_id: string;
        plan_id: string;
        api_key: string;
        retention_days: number;
      }>(
        `
        INSERT INTO projects (tenant_id, name, api_key, is_archived)
        VALUES (
          $1,
          $2,
          gen_random_uuid()::text,
          false
        )
        RETURNING
          id,
          tenant_id,
          api_key::text
        `,
        [tenantId, data['name'] ?? null],
      );

      const row = result.rows[0];
      if (row === undefined) {
        throw new NotFoundError(`Tenant not found or inactive: ${tenantId}`);
      }

      logger.debug({ tenantId, projectId: row.id }, 'project created');

      // Re-fetch full record via getProject to include plan/retention data
      return this.getProject(row.id, tenantId);
    });
  }

  /**
   * Archives a project by ID within the given tenant context.
   * Invalidates the project's API key cache after archival.
   * Throws NotFoundError if the project does not exist.
   */
  async archiveProject(projectId: string, tenantId: string): Promise<void> {
    await this.pg.withTenant(tenantId, async (client) => {
      const result = await client.query<{ api_key: string }>(
        `
        UPDATE projects
        SET is_archived = true, updated_at = now()
        WHERE id = $1
          AND tenant_id = $2
          AND is_archived = false
        RETURNING api_key::text
        `,
        [projectId, tenantId],
      );

      const row = result.rows[0];
      if (row === undefined) {
        throw new NotFoundError(`Project not found: ${projectId}`);
      }

      await this.invalidateApiKeyCache(row.api_key);
      logger.debug({ tenantId, projectId }, 'project archived');
    });
  }

  /**
   * Rotates the API key for a project.
   * Invalidates the old key from the cache and returns the new key.
   * Throws NotFoundError if the project does not exist or is archived.
   */
  async rotateApiKey(projectId: string, tenantId: string): Promise<string> {
    return this.pg.withTenant(tenantId, async (client) => {
      const result = await client.query<{ old_key: string; new_key: string }>(
        `
        UPDATE projects
        SET
          api_key    = gen_random_uuid()::text,
          updated_at = now()
        WHERE id = $1
          AND tenant_id = $2
          AND is_archived = false
        RETURNING
          (SELECT api_key::text FROM projects WHERE id = $1) AS old_key,
          api_key::text AS new_key
        `,
        [projectId, tenantId],
      );

      const row = result.rows[0];
      if (row === undefined) {
        throw new NotFoundError(`Project not found: ${projectId}`);
      }

      if (row.old_key) {
        await this.invalidateApiKeyCache(row.old_key);
      }

      logger.debug({ tenantId, projectId }, 'api key rotated');
      return row.new_key;
    });
  }

  /**
   * Removes the Redis cache entry for the given API key.
   * Called on key rotation or project archival.
   */
  async invalidateApiKeyCache(apiKey: string): Promise<void> {
    const cacheKey = `${API_KEY_CACHE_PREFIX}${apiKey}`;
    await this.redis.client.del(cacheKey);
    logger.debug({ apiKey }, 'api key cache invalidated');
  }

  /**
   * Checks whether a user is a member of the given tenant and returns their role.
   * Throws NotFoundError if no membership record exists.
   */
  async checkMembership(tenantId: string, userId: string): Promise<{ role: string }> {
    const result = await this.pg.query<{ role: string }>(
      `
      SELECT role
      FROM tenant_memberships
      WHERE tenant_id = $1
        AND user_id   = $2
      LIMIT 1
      `,
      [tenantId, userId],
    );

    const row = result.rows[0];
    if (row === undefined) {
      throw new NotFoundError(`Membership not found for user ${userId} in tenant ${tenantId}`);
    }

    return { role: row.role };
  }
}
