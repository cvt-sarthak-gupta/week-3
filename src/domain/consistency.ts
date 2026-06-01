import { logger } from '../logger.js';
import type { PostgresDatabase } from '../db/postgres.js';
import type { MongoDatabase } from '../db/mongo.js';
import type { ElasticsearchDatabase } from '../db/elastic.js';

export interface Inconsistency {
  kind: 'missing_mongo_config' | 'missing_es_index' | 'count_drift' | 'orphan_mongo_config';
  projectId: string;
  details: string;
  suggestion: string;
}

export interface AuditResult {
  ranAt: Date;
  duration_ms: number;
  checked: number;
  inconsistencies: Inconsistency[];
  summary: {
    total: number;
    byKind: Record<string, number>;
  };
}

interface ProjectRow {
  id: string;
  tenant_id: string;
  name: string;
}

async function runConcurrent<T, R>(
  items: T[],
  concurrency: number,
  fn: (item: T) => Promise<R>,
): Promise<R[]> {
  const results: R[] = [];
  for (let i = 0; i < items.length; i += concurrency) {
    const batch = items.slice(i, i + concurrency);
    const batchResults = await Promise.all(batch.map(fn));
    results.push(...batchResults);
  }
  return results;
}

interface TenantProjectGroup {
  tenantId: string;
  projects: ProjectRow[];
}

export class ConsistencyService {
  private readonly pg: PostgresDatabase;
  private readonly mongo: MongoDatabase;
  private readonly es: ElasticsearchDatabase;

  constructor(pg: PostgresDatabase, mongo: MongoDatabase, es: ElasticsearchDatabase) {
    this.pg = pg;
    this.mongo = mongo;
    this.es = es;
  }

  /** Checks per-project: Mongo config exists + ES index exists. */
  private async auditProjectResources(project: ProjectRow): Promise<Inconsistency[]> {
    const issues: Inconsistency[] = [];
    const { id: projectId } = project;

    // (a) Check Mongo: project_configs document exists
    try {
      const mongoCount = await this.mongo.projectConfigs().countDocuments({ _id: projectId });
      if (mongoCount === 0) {
        issues.push({
          kind: 'missing_mongo_config',
          projectId,
          details: `No project_configs document found for project ${projectId}`,
          suggestion: `Re-run onboardTenant or manually insert the project_configs document for projectId=${projectId}`,
        });
      }
    } catch (err) {
      logger.warn({ err, projectId }, 'consistency audit: Mongo config check failed');
    }

    // (b) Check ES: index logs-${projectId}-* exists
    try {
      const pattern = `logs-${projectId}-*`;
      const indexExists = await this.es.client.indices.exists({ index: pattern });
      if (!indexExists) {
        issues.push({
          kind: 'missing_es_index',
          projectId,
          details: `No Elasticsearch index matching ${pattern}`,
          suggestion: `Call applyPolicyForProject('${projectId}', retentionDays) to create the index and alias`,
        });
      }
    } catch (err) {
      logger.warn({ err, projectId }, 'consistency audit: ES index check failed');
    }

    return issues;
  }

  /**
   * Checks count drift at the TENANT level.
   *
   * monthly_usage is keyed by (tenant_id, year, month) — not per project.
   * Comparing it against a single project's Mongo count would always show
   * drift for tenants with multiple projects.  The correct comparison is:
   *
   *   PG  : SUM(event_count) across all months for this tenant
   *   Mongo: COUNT of events across ALL projects belonging to this tenant
   *
   * A 1% tolerance accounts for in-flight events and usage_dedup lag.
   */
  private async auditTenantCountDrift(group: TenantProjectGroup): Promise<Inconsistency[]> {
    const issues: Inconsistency[] = [];
    const { tenantId, projects } = group;
    const projectIds = projects.map((p) => p.id);

    try {
      const pgResult = await this.pg.query<{ total: string }>(
        `SELECT COALESCE(SUM(event_count), 0)::text AS total
           FROM monthly_usage
          WHERE tenant_id = $1`,
        [tenantId],
      );
      const pgTotal = parseInt(pgResult.rows[0]?.total ?? '0', 10);

      // Sum Mongo events across all projects belonging to this tenant.
      const mongoTotal = await this.mongo.events().countDocuments({
        projectId: { $in: projectIds },
      });

      const tolerance = Math.ceil(Math.max(pgTotal, mongoTotal) * 0.01);
      const drift = Math.abs(pgTotal - mongoTotal);

      if (drift > tolerance && (pgTotal > 0 || mongoTotal > 0)) {
        // Report against the first project for surfacing purposes; details
        // include the full tenant context.
        issues.push({
          kind: 'count_drift',
          projectId: projectIds[0] ?? tenantId,
          details:
            `Tenant ${tenantId}: PG monthly_usage sum=${pgTotal}, ` +
            `Mongo events across ${projectIds.length} project(s)=${mongoTotal}, ` +
            `drift=${drift} (tolerance=${tolerance})`,
          suggestion:
            `Investigate dropped events or usage_dedup inconsistencies for tenantId=${tenantId}. ` +
            `Project IDs: ${projectIds.join(', ')}`,
        });
      }
    } catch (err) {
      logger.warn({ err, tenantId }, 'consistency audit: count drift check failed');
    }

    return issues;
  }

  async runAudit(): Promise<AuditResult> {
    const ranAt = new Date();
    const startMs = Date.now();

    // 1. Fetch all active projects
    const pgResult = await this.pg.query<ProjectRow>(
      `SELECT id, tenant_id, name FROM projects WHERE is_archived = false`,
    );
    const projects = pgResult.rows;

    // 2. Per-project resource checks (Mongo config + ES index) in batches of 10.
    const resourceIssues = (
      await runConcurrent(projects, 10, (p) => this.auditProjectResources(p))
    ).flat();

    // 3. Per-tenant count drift check.
    //    Group projects by tenant so the PG sum and Mongo count are compared
    //    at the same granularity (tenant total vs tenant total).
    const tenantMap = new Map<string, ProjectRow[]>();
    for (const project of projects) {
      const list = tenantMap.get(project.tenant_id) ?? [];
      list.push(project);
      tenantMap.set(project.tenant_id, list);
    }
    const tenantGroups: TenantProjectGroup[] = Array.from(tenantMap.entries()).map(
      ([tenantId, ps]) => ({ tenantId, projects: ps }),
    );
    const driftIssues = (
      await runConcurrent(tenantGroups, 10, (g) => this.auditTenantCountDrift(g))
    ).flat();

    const allIssues = [...resourceIssues, ...driftIssues];

    // 4. Check for orphaned Mongo configs (project_configs docs without a PG project)
    try {
      const projectIds = new Set(projects.map((p) => p.id));
      const mongoCursor = this.mongo.projectConfigs().find(
        {},
        { projection: { _id: 1 } },
      );
      const mongoDocs = await mongoCursor.toArray();
      for (const doc of mongoDocs) {
        if (!projectIds.has(doc._id)) {
          allIssues.push({
            kind: 'orphan_mongo_config',
            projectId: doc._id,
            details: `project_configs document exists for projectId=${doc._id} but no active PG project found`,
            suggestion: `Delete the orphaned project_configs document: db.project_configs.deleteOne({ _id: '${doc._id}' })`,
          });
        }
      }
    } catch (err) {
      logger.warn({ err }, 'consistency audit: orphan Mongo config check failed');
    }

    const duration_ms = Date.now() - startMs;

    // 5. Build summary
    const byKind: Record<string, number> = {};
    for (const issue of allIssues) {
      byKind[issue.kind] = (byKind[issue.kind] ?? 0) + 1;
    }

    const result: AuditResult = {
      ranAt,
      duration_ms,
      checked: projects.length,
      inconsistencies: allIssues,
      summary: {
        total: allIssues.length,
        byKind,
      },
    };

    logger.info(
      { checked: projects.length, total: allIssues.length, duration_ms, byKind },
      'Consistency audit complete',
    );

    return result;
  }
}
