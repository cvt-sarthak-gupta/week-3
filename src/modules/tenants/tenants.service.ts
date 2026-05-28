import { randomUUID } from 'node:crypto';
import type { PostgresPool } from '../../db/postgres/index.js';
import type { MongoDatabase } from '../../db/mongo/index.js';
import type { ElasticClient } from '../../db/elastic/index.js';
import type { RedisClient } from '../../db/redis/index.js';
import { logger } from '../../utils/logger.js';
import { ForbiddenError, NotFoundError } from '../../utils/errors.js';
import type {
  OnboardTenantInput,
  OnboardTenantResult,
  TenantDetail,
  TenantQuotaReport,
  PatchTenantInput,
} from './tenants.types.js';

export class TenantService {
  constructor(
    private readonly pool: PostgresPool,
    private readonly mongo: MongoDatabase,
    private readonly es: ElasticClient,
    private readonly redis: RedisClient,
  ) {}

  async onboard(input: OnboardTenantInput): Promise<OnboardTenantResult> {
    const { tenantName, tenantSlug, planId, userId, projectName, projectSlug } = input;

    // Step 1: PG transaction — all inserts in one atomic block
    let tenantId!: string;
    let projectId!: string;
    let apiKey!: string;

    await this.pool.withClient(async (client) => {
      // INSERT tenant
      const tenantRes = await client.query<{ id: string }>(
        `INSERT INTO tenants (id, name, slug, plan_id, is_active, created_at)
         VALUES ($1, $2, $3, $4, true, NOW())
         RETURNING id`,
        [randomUUID(), tenantName, tenantSlug, planId],
      );
      const tenantRow = tenantRes.rows[0];
      if (tenantRow === undefined) {
        throw new Error('Failed to insert tenant');
      }
      tenantId = tenantRow.id;

      // INSERT project (api_key is a generated UUID)
      const projectRes = await client.query<{ id: string; api_key: string }>(
        `INSERT INTO projects (id, tenant_id, name, slug, api_key, is_archived, created_at)
         VALUES ($1, $2, $3, $4, $5, false, NOW())
         RETURNING id, api_key::text AS api_key`,
        [randomUUID(), tenantId, projectName, projectSlug, randomUUID()],
      );
      const projectRow = projectRes.rows[0];
      if (projectRow === undefined) {
        throw new Error('Failed to insert project');
      }
      projectId = projectRow.id;
      apiKey = projectRow.api_key;

      // INSERT tenant_member (owner)
      await client.query(
        `INSERT INTO tenant_members (tenant_id, user_id, role)
         VALUES ($1, $2, 'owner')`,
        [tenantId, userId],
      );

      // INSERT monthly_usage for current month
      const now = new Date();
      await client.query(
        `INSERT INTO monthly_usage (tenant_id, year, month, event_count)
         VALUES ($1, $2, $3, 0)
         ON CONFLICT (tenant_id, year, month) DO NOTHING`,
        [tenantId, now.getUTCFullYear(), now.getUTCMonth() + 1],
      );
    });

    // Step 2: MongoDB — insert project_configs document
    try {
      await this.mongo.projectConfigs().insertOne({
        _id: projectId,
        tenantId,
        name: projectName,
        retentionDays: 90,
        alertsEnabled: true,
        createdAt: new Date(),
        updatedAt: new Date(),
        settings: {
          samplingRate: 1.0,
          ignoredErrors: [] as string[],
          retentionDays: 90,
        },
      });
    } catch (mongoErr) {
      logger.error({ err: mongoErr, tenantId, projectId }, 'onboardTenant: Mongo insert failed, compensating PG');
      await this.deletePgRows(tenantId, projectId);
      throw mongoErr;
    }

    // Step 3: Elasticsearch — create index + alias via ILM policy
    try {
      await this.es.applyPolicyForProject(projectId, 90);
    } catch (esErr) {
      logger.error({ err: esErr, tenantId, projectId }, 'onboardTenant: ES index creation failed, compensating Mongo + PG');
      try {
        await this.mongo.projectConfigs().deleteOne({ _id: projectId });
      } catch (mongoCompErr) {
        logger.error(
          { err: mongoCompErr, projectId },
          'onboardTenant: compensation delete of Mongo config failed — manual cleanup required',
        );
      }
      await this.deletePgRows(tenantId, projectId);
      throw esErr;
    }

    // Step 4: Redis — init rate-limit key (best-effort, non-fatal)
    try {
      await this.redis.client.set(`ratelimit:init:${projectId}`, '1', 'EX', 3600);
    } catch (redisErr) {
      logger.warn(
        { err: redisErr, projectId },
        'onboardTenant: Redis rate-limit init failed (non-fatal, key is recreatable)',
      );
    }

    logger.info({ tenantId, projectId }, 'Tenant onboarded successfully');
    return { tenantId, projectId, apiKey };
  }

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

  async getById(tenantId: string, userId: string): Promise<TenantDetail> {
    await this.assertMember(userId, tenantId);

    const [tenantResult, countResult] = await Promise.all([
      this.pool.query<{
        id: string;
        name: string;
        slug: string;
        plan_id: string | null;
        is_active: boolean;
        created_at: Date;
        plan_name: string | null;
      }>(
        `SELECT t.id, t.name, t.slug, t.plan_id, t.is_active, t.created_at,
                p.name AS plan_name
         FROM tenants t
         LEFT JOIN plans p ON p.id = t.plan_id
         WHERE t.id = $1
         LIMIT 1`,
        [tenantId],
      ),
      this.pool.query<{ count: string }>(
        'SELECT COUNT(*)::text AS count FROM tenant_members WHERE tenant_id = $1',
        [tenantId],
      ),
    ]);

    const tenant = tenantResult.rows[0];
    if (tenant === undefined) {
      throw new NotFoundError('Tenant not found');
    }

    const memberCount = parseInt(countResult.rows[0]?.count ?? '0', 10);

    return {
      id: tenant.id,
      name: tenant.name,
      slug: tenant.slug,
      planId: tenant.plan_id,
      planName: tenant.plan_name,
      isActive: tenant.is_active,
      createdAt: tenant.created_at,
      memberCount,
    };
  }

  async update(tenantId: string, userId: string, data: PatchTenantInput): Promise<void> {
    const member = await this.assertMember(userId, tenantId);
    if (member.role !== 'owner' && member.role !== 'admin') {
      throw new ForbiddenError('Only owners or admins can update tenant settings');
    }

    const { name, isActive } = data;

    await this.pool.withTenant(tenantId, async (client) => {
      await client.query(
        `UPDATE tenants
         SET name      = COALESCE($1, name),
             is_active = COALESCE($2, is_active)
         WHERE id = $3`,
        [name ?? null, isActive ?? null, tenantId],
      );
    });
  }

  async getQuota(tenantId: string, userId: string): Promise<TenantQuotaReport> {
    await this.assertMember(userId, tenantId);

    const result = await this.pool.query<{
      tenant_id: string;
      plan_name: string | null;
      event_quota_per_month: number | null;
      events_this_month: number;
    }>(
      `SELECT
         t.id                                     AS tenant_id,
         p.name                                   AS plan_name,
         p.event_quota_per_month,
         COALESCE(mu.event_count, 0)::int         AS events_this_month
       FROM tenants t
       LEFT JOIN plans p        ON p.id = t.plan_id
       LEFT JOIN monthly_usage mu
              ON mu.tenant_id = t.id
             AND mu.year = EXTRACT(YEAR FROM NOW())
             AND mu.month = EXTRACT(MONTH FROM NOW())
       WHERE t.id = $1
       LIMIT 1`,
      [tenantId],
    );

    const row = result.rows[0];
    if (row === undefined) {
      return {
        tenantId,
        planName: null,
        eventQuotaPerMonth: null,
        eventsThisMonth: 0,
        percentUsed: 0,
      };
    }

    const quota = row.event_quota_per_month;
    const used = row.events_this_month;
    const percent = quota !== null && quota > 0 ? Math.round((used / quota) * 100) : 0;

    return {
      tenantId: row.tenant_id,
      planName: row.plan_name,
      eventQuotaPerMonth: quota,
      eventsThisMonth: used,
      percentUsed: percent,
    };
  }

  private async deletePgRows(tenantId: string, projectId: string): Promise<void> {
    try {
      await this.pool.withClient(async (client) => {
        await client.query(`DELETE FROM tenant_members WHERE tenant_id = $1`, [tenantId]);
        await client.query(`DELETE FROM monthly_usage WHERE tenant_id = $1`, [tenantId]);
        await client.query(`DELETE FROM projects WHERE id = $1`, [projectId]);
        await client.query(`DELETE FROM tenants WHERE id = $1`, [tenantId]);
      });
    } catch (compensationErr) {
      logger.error(
        { err: compensationErr, tenantId, projectId },
        'onboardTenant: compensation delete of PG rows failed — manual cleanup required',
      );
    }
  }
}
