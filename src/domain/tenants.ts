import { randomUUID } from 'node:crypto';
import type { PostgresDatabase } from '../db/postgres.js';
import type { MongoDatabase } from '../db/mongo.js';
import type { ElasticsearchDatabase } from '../db/elastic.js';
import type { RedisDatabase } from '../db/redis.js';
import { logger } from '../logger.js';
import type {
  OnboardTenantInput,
  OnboardTenantResult,
  TenantInfo,
  TenantQuotaReport,
  TenantQuotaReportRow,
} from '../types/tenants.js';

export type {
  OnboardTenantInput,
  OnboardTenantResult,
  TenantInfo,
  TenantQuotaReport,
  TenantQuotaReportRow,
};

export class TenantService {
  constructor(
    private readonly pg: PostgresDatabase,
    private readonly mongo: MongoDatabase,
    private readonly es: ElasticsearchDatabase,
    private readonly redis: RedisDatabase,
  ) {}

  private async deletePgRows(tenantId: string, projectId: string): Promise<void> {
    // Compensation must be atomic: if any DELETE fails we must roll back the
    // others so the database does not end up with orphaned rows (e.g. a project
    // row with no parent tenant). Without BEGIN/COMMIT each statement commits
    // independently and a mid-way failure leaves partial state.
    const client = await this.pg.connect();
    try {
      await client.query('BEGIN');
      await client.query(`DELETE FROM tenant_members WHERE tenant_id = $1`, [tenantId]);
      await client.query(`DELETE FROM monthly_usage  WHERE tenant_id = $1`, [tenantId]);
      await client.query(`DELETE FROM projects        WHERE id = $1`,        [projectId]);
      await client.query(`DELETE FROM tenants         WHERE id = $1`,        [tenantId]);
      await client.query('COMMIT');
    } catch (compensationErr) {
      await client.query('ROLLBACK').catch(() => undefined);
      logger.error(
        { err: compensationErr, tenantId, projectId },
        'onboardTenant: compensation delete of PG rows failed — manual cleanup required',
      );
    } finally {
      client.release();
    }
  }

  async onboardTenant(input: OnboardTenantInput): Promise<OnboardTenantResult> {
    const { tenantName, tenantSlug, planId, userId, projectName, projectSlug } = input;

    let tenantId!: string;
    let projectId!: string;
    let apiKey!: string;

    // New tenant creation has no prior tenant context — bypass RLS with a raw
    // transaction. withTenant requires an existing tenantId for SET LOCAL.
    const pgClient = await this.pg.connect();
    try {
      await pgClient.query('BEGIN');

      const tenantRes = await pgClient.query<{ id: string }>(
        `INSERT INTO tenants (id, name, slug, plan_id, is_active, created_at)
         VALUES ($1, $2, $3, $4, true, NOW())
         RETURNING id`,
        [randomUUID(), tenantName, tenantSlug, planId],
      );
      const tenantRow = tenantRes.rows[0];
      if (tenantRow === undefined) throw new Error('Failed to insert tenant');
      tenantId = tenantRow.id;

      const projectRes = await pgClient.query<{ id: string; api_key: string }>(
        `INSERT INTO projects (id, tenant_id, name, slug, api_key, is_archived, created_at)
         VALUES ($1, $2, $3, $4, $5, false, NOW())
         RETURNING id, api_key::text AS api_key`,
        [randomUUID(), tenantId, projectName, projectSlug, randomUUID()],
      );
      const projectRow = projectRes.rows[0];
      if (projectRow === undefined) throw new Error('Failed to insert project');
      projectId = projectRow.id;
      apiKey = projectRow.api_key;

      await pgClient.query(
        `INSERT INTO tenant_members (tenant_id, user_id, role) VALUES ($1, $2, 'owner')`,
        [tenantId, userId],
      );

      const now = new Date();
      await pgClient.query(
        `INSERT INTO monthly_usage (tenant_id, year, month, event_count)
         VALUES ($1, $2, $3, 0)
         ON CONFLICT (tenant_id, year, month) DO NOTHING`,
        [tenantId, now.getUTCFullYear(), now.getUTCMonth() + 1],
      );

      await pgClient.query('COMMIT');
    } catch (err) {
      await pgClient.query('ROLLBACK').catch(() => undefined);
      throw err;
    } finally {
      pgClient.release();
    }

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

  async getTenantQuotaReport(tenantId: string): Promise<TenantQuotaReport> {
    const result = await this.pg.query<{
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

  async getAllTenantsQuotaReport(): Promise<TenantQuotaReportRow[]> {
    const now = new Date();
    const prevMonth = new Date(now);
    prevMonth.setUTCMonth(prevMonth.getUTCMonth() - 1);

    const result = await this.pg.query<{
      tenant_id: string;
      tenant_name: string;
      plan_name: string | null;
      event_quota_per_month: number | null;
      this_month_events: string;
      prev_month_events: string;
      rank: string;
      usage_pct: string;
      exceeded_80pct: boolean;
      mom_growth_pct: string | null;
    }>(
      `WITH month_agg AS (
         SELECT tenant_id, SUM(event_count) AS this_month_events
         FROM monthly_usage
         WHERE year = $1 AND month = $2
         GROUP BY tenant_id
       ),
       prev_month_agg AS (
         SELECT tenant_id, SUM(event_count) AS prev_month_events
         FROM monthly_usage
         WHERE year = $3 AND month = $4
         GROUP BY tenant_id
       ),
       ranked AS (
         SELECT
           t.id                                   AS tenant_id,
           t.name                                 AS tenant_name,
           p.name                                 AS plan_name,
           p.event_quota_per_month,
           COALESCE(m.this_month_events, 0)       AS this_month_events,
           COALESCE(pm.prev_month_events, 0)      AS prev_month_events,
           RANK() OVER (
             ORDER BY COALESCE(m.this_month_events, 0) DESC
           )                                      AS rank,
           CASE WHEN p.event_quota_per_month > 0
             THEN ROUND(
               COALESCE(m.this_month_events, 0)::NUMERIC
                 / p.event_quota_per_month * 100, 2)
             ELSE 0
           END                                    AS usage_pct,
           CASE WHEN p.event_quota_per_month > 0
                THEN COALESCE(m.this_month_events, 0) > (p.event_quota_per_month * 0.8)
                ELSE false
           END                                    AS exceeded_80pct,
           CASE WHEN COALESCE(pm.prev_month_events, 0) > 0
             THEN ROUND(
               (COALESCE(m.this_month_events, 0) - pm.prev_month_events)::NUMERIC
                 / pm.prev_month_events * 100, 2)
             ELSE NULL
           END                                    AS mom_growth_pct
         FROM tenants t
         JOIN plans p ON p.id = t.plan_id
         LEFT JOIN month_agg m         ON m.tenant_id  = t.id
         LEFT JOIN prev_month_agg pm   ON pm.tenant_id = t.id
       )
       SELECT * FROM ranked ORDER BY rank`,
      [
        now.getUTCFullYear(),
        now.getUTCMonth() + 1,
        prevMonth.getUTCFullYear(),
        prevMonth.getUTCMonth() + 1,
      ],
    );

    return result.rows.map((row) => ({
      tenantId: row.tenant_id,
      tenantName: row.tenant_name,
      planName: row.plan_name,
      eventQuotaPerMonth: row.event_quota_per_month,
      eventsThisMonth: Number(row.this_month_events),
      // Use the SQL-computed percentage (2 dp, consistent with exceeded_80pct logic).
      percentUsed: Number(row.usage_pct),
      rank: Number(row.rank),
      exceeded80pct: row.exceeded_80pct,
      momGrowthPct: row.mom_growth_pct !== null ? Number(row.mom_growth_pct) : null,
    }));
  }

  async getTenantById(tenantId: string): Promise<unknown> {
    const result = await this.pg.query<TenantInfo>(
      `SELECT id, name, slug, plan_id, is_active, created_at
       FROM tenants
       WHERE id = $1
       LIMIT 1`,
      [tenantId],
    );

    return result.rows[0] ?? null;
  }

  async updateTenant(tenantId: string, input: unknown): Promise<unknown> {
    const updates = input as Record<string, unknown>;
    const setClauses: string[] = [];
    const values: unknown[] = [];
    let paramIndex = 1;

    const allowedFields: Record<string, string> = {
      name: 'name',
      slug: 'slug',
      planId: 'plan_id',
      isActive: 'is_active',
    };

    for (const [key, column] of Object.entries(allowedFields)) {
      if (key in updates) {
        setClauses.push(`${column} = $${paramIndex}`);
        values.push(updates[key]);
        paramIndex++;
      }
    }

    if (setClauses.length === 0) {
      return this.getTenantById(tenantId);
    }

    values.push(tenantId);

    const result = await this.pg.query<TenantInfo>(
      `UPDATE tenants
       SET ${setClauses.join(', ')}
       WHERE id = $${paramIndex}
       RETURNING id, name, slug, plan_id, is_active, created_at`,
      values,
    );

    return result.rows[0] ?? null;
  }

  async checkTenantMembership(tenantId: string, userId: string): Promise<{ role: string }> {
    const result = await this.pg.query<{ role: string }>(
      `SELECT role
       FROM tenant_members
       WHERE tenant_id = $1 AND user_id = $2
       LIMIT 1`,
      [tenantId, userId],
    );

    const row = result.rows[0];
    if (row === undefined) {
      throw new Error(`User ${userId} is not a member of tenant ${tenantId}`);
    }

    return { role: row.role };
  }
}
