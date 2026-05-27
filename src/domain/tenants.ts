import { randomUUID } from 'node:crypto';
import { pool, withClient } from '../db/postgres.js';
import { projectConfigsCollection } from '../db/mongo.js';
import { applyPolicyForProject } from '../db/elastic.js';
import { redis } from '../db/redis.js';
import { logger } from '../logger.js';

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export interface OnboardTenantInput {
  tenantName: string;
  tenantSlug: string;
  planId: string;
  userId: string;
  projectName: string;
  projectSlug: string;
}

export interface OnboardTenantResult {
  tenantId: string;
  projectId: string;
  apiKey: string;
}

export interface TenantInfo {
  id: string;
  name: string;
  slug: string;
  plan_id: string | null;
  is_active: boolean;
  created_at: Date;
}

export interface TenantQuotaReport {
  tenantId: string;
  planName: string | null;
  eventQuotaPerMonth: number | null;
  eventsThisMonth: number;
  percentUsed: number;
}

// ---------------------------------------------------------------------------
// Compensating delete helpers
// ---------------------------------------------------------------------------

async function deletePgRows(tenantId: string, projectId: string): Promise<void> {
  try {
    await withClient(async (client) => {
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

// ---------------------------------------------------------------------------
// Onboarding
// ---------------------------------------------------------------------------

export async function onboardTenant(input: OnboardTenantInput): Promise<OnboardTenantResult> {
  const { tenantName, tenantSlug, planId, userId, projectName, projectSlug } = input;

  // -------------------------------------------------------------------------
  // Step 1: PG transaction — all inserts in one atomic block
  // -------------------------------------------------------------------------
  let tenantId!: string;
  let projectId!: string;
  let apiKey!: string;

  await withClient(async (client) => {
    // INSERT tenant
    const tenantRes = await client.query<{ id: string }>(
      `INSERT INTO tenants (id, name, slug, plan_id, is_active, created_at)
       VALUES ($1, $2, $3, $4, true, NOW())
       RETURNING id`,
      [randomUUID(), tenantName, tenantSlug, planId],
    );
    const tenantRow = tenantRes.rows[0];
    if (tenantRow === undefined) throw new Error('Failed to insert tenant');
    tenantId = tenantRow.id;

    // INSERT project (api_key is a generated UUID)
    const projectRes = await client.query<{ id: string; api_key: string }>(
      `INSERT INTO projects (id, tenant_id, name, slug, api_key, is_archived, created_at)
       VALUES ($1, $2, $3, $4, $5, false, NOW())
       RETURNING id, api_key::text AS api_key`,
      [randomUUID(), tenantId, projectName, projectSlug, randomUUID()],
    );
    const projectRow = projectRes.rows[0];
    if (projectRow === undefined) throw new Error('Failed to insert project');
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

  // -------------------------------------------------------------------------
  // Step 2: MongoDB — insert project_configs document
  // -------------------------------------------------------------------------
  try {
    await projectConfigsCollection().insertOne({
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
    await deletePgRows(tenantId, projectId);
    throw mongoErr;
  }

  // -------------------------------------------------------------------------
  // Step 3: Elasticsearch — create index + alias via ILM policy
  // -------------------------------------------------------------------------
  try {
    await applyPolicyForProject(projectId, 90);
  } catch (esErr) {
    logger.error({ err: esErr, tenantId, projectId }, 'onboardTenant: ES index creation failed, compensating Mongo + PG');
    try {
      await projectConfigsCollection().deleteOne({ _id: projectId });
    } catch (mongoCompErr) {
      logger.error(
        { err: mongoCompErr, projectId },
        'onboardTenant: compensation delete of Mongo config failed — manual cleanup required',
      );
    }
    await deletePgRows(tenantId, projectId);
    throw esErr;
  }

  // -------------------------------------------------------------------------
  // Step 4: Redis — init rate-limit key (best-effort, non-fatal)
  // -------------------------------------------------------------------------
  try {
    await redis.set(`ratelimit:init:${projectId}`, '1', 'EX', 3600);
  } catch (redisErr) {
    logger.warn(
      { err: redisErr, projectId },
      'onboardTenant: Redis rate-limit init failed (non-fatal, key is recreatable)',
    );
  }

  logger.info({ tenantId, projectId }, 'Tenant onboarded successfully');
  return { tenantId, projectId, apiKey };
}

// ---------------------------------------------------------------------------
// Quota report (single tenant)
// ---------------------------------------------------------------------------

export async function getTenantQuota(tenantId: string): Promise<TenantQuotaReport> {
  const result = await pool.query<{
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

// ---------------------------------------------------------------------------
// Multi-tenant quota report (admin)
// ---------------------------------------------------------------------------

export interface TenantQuotaReportRow {
  tenantId: string;
  tenantName: string;
  planName: string | null;
  eventQuotaPerMonth: number | null;
  eventsThisMonth: number;
  percentUsed: number;
}

export async function getTenantQuotaReport(): Promise<TenantQuotaReportRow[]> {
  const result = await pool.query<{
    tenant_id: string;
    tenant_name: string;
    plan_name: string | null;
    event_quota_per_month: number | null;
    events_this_month: number;
  }>(
    `SELECT
       t.id                                     AS tenant_id,
       t.name                                   AS tenant_name,
       p.name                                   AS plan_name,
       p.event_quota_per_month,
       COALESCE(mu.event_count, 0)::int         AS events_this_month
     FROM tenants t
     LEFT JOIN plans p        ON p.id = t.plan_id
     LEFT JOIN monthly_usage mu
            ON mu.tenant_id = t.id
           AND mu.year = EXTRACT(YEAR FROM NOW())
           AND mu.month = EXTRACT(MONTH FROM NOW())
     ORDER BY events_this_month DESC`,
  );

  return result.rows.map((row) => {
    const quota = row.event_quota_per_month;
    const used = row.events_this_month;
    const percent = quota !== null && quota > 0 ? Math.round((used / quota) * 100) : 0;

    return {
      tenantId: row.tenant_id,
      tenantName: row.tenant_name,
      planName: row.plan_name,
      eventQuotaPerMonth: quota,
      eventsThisMonth: used,
      percentUsed: percent,
    };
  });
}
