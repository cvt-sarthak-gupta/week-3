import type { PostgresPool } from '../../db/postgres/index.js';
import type { MongoDatabase } from '../../db/mongo/index.js';
import type { ElasticClient } from '../../db/elastic/index.js';
import { logger } from '../../utils/logger.js';
import type { AuditMismatch, AuditResult } from './consistency.types.js';

// ---------------------------------------------------------------------------
// Concurrency-limited batch runner
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// ConsistencyService
// ---------------------------------------------------------------------------

interface ProjectRow {
  id: string;
  tenant_id: string;
  name: string;
}

export class ConsistencyService {
  constructor(
    private readonly pool: PostgresPool,
    private readonly mongo: MongoDatabase,
    private readonly es: ElasticClient,
  ) {}

  // ---------------------------------------------------------------------------
  // runAudit — cross-DB consistency audit across all active projects
  // ---------------------------------------------------------------------------

  async runAudit(): Promise<AuditResult> {
    const ranAt = new Date();
    const startMs = Date.now();

    // 1. Fetch all active projects
    const pgResult = await this.pool.query<ProjectRow>(
      `SELECT id, tenant_id, name FROM projects WHERE is_archived = false`,
    );
    const projects = pgResult.rows;

    // 2. Audit each project in batches of 10 in parallel
    const allIssues = (
      await runConcurrent(projects, 10, (project) => this.auditProject(project))
    ).flat();

    // 3. Check for orphaned Mongo configs (project_configs docs without a PG project)
    try {
      const projectIds = new Set(projects.map((p) => p.id));
      const mongoCursor = this.mongo.projectConfigs().find(
        {},
        { projection: { _id: 1 } },
      );
      const mongoDocs = await mongoCursor.toArray();
      for (const doc of mongoDocs) {
        if (!projectIds.has(String(doc._id))) {
          allIssues.push({
            kind: 'orphan_mongo_config',
            projectId: String(doc._id),
            details: `project_configs document exists for projectId=${String(doc._id)} but no active PG project found`,
            suggestion: `Delete the orphaned project_configs document: db.project_configs.deleteOne({ _id: '${String(doc._id)}' })`,
          });
        }
      }
    } catch (err) {
      logger.warn({ err }, 'consistency audit: orphan Mongo config check failed');
    }

    const duration_ms = Date.now() - startMs;

    // 4. Build summary
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

  // ---------------------------------------------------------------------------
  // auditProject — per-project checks
  // ---------------------------------------------------------------------------

  private async auditProject(project: ProjectRow): Promise<AuditMismatch[]> {
    const issues: AuditMismatch[] = [];
    const { id: projectId, tenant_id: tenantId } = project;

    // (a) Check Mongo: project_configs document exists
    try {
      const mongoCount = await this.mongo
        .projectConfigs()
        .countDocuments({ _id: projectId as unknown as never });
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

    // (c) Check count drift: PG monthly_usage SUM vs Mongo events count
    try {
      const pgResult = await this.pool.query<{ total: string }>(
        `SELECT COALESCE(SUM(event_count), 0)::text AS total FROM monthly_usage WHERE tenant_id = $1`,
        [tenantId],
      );
      const pgTotal = parseInt(pgResult.rows[0]?.total ?? '0', 10);
      const mongoTotal = await this.mongo.events().countDocuments({ projectId });

      // Allow 1% tolerance
      const tolerance = Math.ceil(Math.max(pgTotal, mongoTotal) * 0.01);
      const drift = Math.abs(pgTotal - mongoTotal);

      if (drift > tolerance && (pgTotal > 0 || mongoTotal > 0)) {
        issues.push({
          kind: 'count_drift',
          projectId,
          details: `PG monthly_usage sum=${pgTotal}, Mongo events count=${mongoTotal}, drift=${drift} (tolerance=${tolerance})`,
          suggestion: `Investigate dropped events or usage_dedup inconsistencies for projectId=${projectId} (tenantId=${tenantId})`,
        });
      }
    } catch (err) {
      logger.warn({ err, projectId }, 'consistency audit: count drift check failed');
    }

    return issues;
  }
}
