import type { PostgresPool } from '../../db/postgres/index.js';
import type { ElasticClient } from '../../db/elastic/index.js';
import type { RedisClient } from '../../db/redis/index.js';
import { ForbiddenError, NotFoundError } from '../../utils/errors.js';
import { logger } from '../../utils/logger.js';
import type { ProjectRow, CreateProjectInput, RollKeyResult } from './projects.types.js';

export class ProjectService {
  constructor(
    private readonly pool: PostgresPool,
    private readonly es: ElasticClient,
    private readonly redis: RedisClient,
  ) {}

  async assertMember(userId: string, tenantId: string): Promise<{ user_id: string; role: string }> {
    const result = await this.pool.query<{ user_id: string; role: string }>(
      'SELECT user_id, role FROM tenant_members WHERE tenant_id = $1 AND user_id = $2 LIMIT 1',
      [tenantId, userId],
    );
    const member = result.rows[0];
    if (member === undefined) {
      throw new ForbiddenError('Not a member of this tenant');
    }
    return member;
  }

  async list(tenantId: string, userId: string): Promise<ProjectRow[]> {
    await this.assertMember(userId, tenantId);

    const result = await this.pool.query<ProjectRow>(
      `SELECT id, tenant_id, name, slug, api_key, is_archived, created_at, updated_at
       FROM projects
       WHERE tenant_id = $1 AND is_archived = false
       ORDER BY created_at DESC`,
      [tenantId],
    );

    return result.rows;
  }

  async create(
    tenantId: string,
    userId: string,
    data: CreateProjectInput,
  ): Promise<{ id: string; apiKey: string }> {
    const member = await this.assertMember(userId, tenantId);
    if (member.role !== 'owner' && member.role !== 'admin') {
      throw new ForbiddenError('Only owners or admins can create projects');
    }

    const result = await this.pool.withTenant(tenantId, async (client) => {
      return client.query<{ id: string; api_key: string }>(
        `INSERT INTO projects (tenant_id, name, slug, api_key)
         VALUES ($1, $2, $3, gen_random_uuid())
         RETURNING id, api_key`,
        [tenantId, data.name, data.slug],
      );
    });

    const project = result.rows[0];
    if (project === undefined) {
      throw new Error('Failed to create project');
    }

    await this.es.applyPolicyForProject(project.id, 30).catch((err) => {
      logger.warn({ err, projectId: project.id }, 'Failed to create ES index for new project');
    });

    return { id: project.id, apiKey: project.api_key };
  }

  async getById(tenantId: string, projectId: string, userId: string): Promise<ProjectRow> {
    await this.assertMember(userId, tenantId);

    const result = await this.pool.query<ProjectRow>(
      `SELECT id, tenant_id, name, slug, api_key, is_archived, created_at, updated_at
       FROM projects
       WHERE id = $1 AND tenant_id = $2
       LIMIT 1`,
      [projectId, tenantId],
    );

    const project = result.rows[0];
    if (project === undefined) {
      throw new NotFoundError('Project not found');
    }

    return project;
  }

  async archive(tenantId: string, projectId: string, userId: string): Promise<void> {
    const member = await this.assertMember(userId, tenantId);
    if (member.role !== 'owner' && member.role !== 'admin') {
      throw new ForbiddenError('Only owners or admins can delete projects');
    }

    const check = await this.pool.query<{ id: string }>(
      'SELECT id FROM projects WHERE id = $1 AND tenant_id = $2 LIMIT 1',
      [projectId, tenantId],
    );
    if (check.rows[0] === undefined) {
      throw new NotFoundError('Project not found');
    }

    await this.pool.withTenant(tenantId, async (client) => {
      await client.query(
        'UPDATE projects SET is_archived = true, updated_at = NOW() WHERE id = $1',
        [projectId],
      );
    });
  }

  async rollKey(tenantId: string, projectId: string, userId: string): Promise<RollKeyResult> {
    const member = await this.assertMember(userId, tenantId);
    if (member.role !== 'owner' && member.role !== 'admin') {
      throw new ForbiddenError('Only owners or admins can roll the API key');
    }

    const oldResult = await this.pool.query<{ api_key: string }>(
      'SELECT api_key FROM projects WHERE id = $1 AND tenant_id = $2 LIMIT 1',
      [projectId, tenantId],
    );
    const oldProject = oldResult.rows[0];
    if (oldProject === undefined) {
      throw new NotFoundError('Project not found');
    }

    const oldKey = oldProject.api_key;

    const newResult = await this.pool.withTenant(tenantId, async (client) => {
      return client.query<{ api_key: string }>(
        `UPDATE projects
         SET api_key = gen_random_uuid(), updated_at = NOW()
         WHERE id = $1
         RETURNING api_key`,
        [projectId],
      );
    });

    const newKey = newResult.rows[0]?.api_key;
    if (newKey === undefined) {
      throw new Error('Failed to roll API key');
    }

    // Invalidate old key in Redis cache
    await this.redis.client.del(`apikey:${oldKey}`).catch((err) => {
      logger.warn({ err, projectId }, 'Failed to invalidate old API key in Redis');
    });

    return { apiKey: newKey };
  }
}
