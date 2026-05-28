import type { PostgresPool } from '../../db/postgres/index.js';
import type { MongoDatabase } from '../../db/mongo/index.js';
import { logger } from '../../utils/logger.js';
import type { RetentionResult } from './retention.types.js';

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const BATCH_SIZE = 10_000;
const BATCH_SLEEP_MS = 50;
const PROJECT_BUDGET_MS = 30_000; // 30-second wall-clock budget per project

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

interface ProjectRetentionRow {
  project_id: string;
  retention_days: number;
}

// ---------------------------------------------------------------------------
// RetentionService
// ---------------------------------------------------------------------------

export class RetentionService {
  constructor(
    private readonly pool: PostgresPool,
    private readonly mongo: MongoDatabase,
  ) {}

  // ---------------------------------------------------------------------------
  // runRetention — batched deleteMany per project with per-project budget
  // ---------------------------------------------------------------------------

  async runRetention(): Promise<RetentionResult> {
    const startMs = Date.now();

    // 1. Query PG for all active projects with their retention_days from plan
    const pgResult = await this.pool.query<ProjectRetentionRow>(
      `SELECT p.id AS project_id, pl.retention_days
       FROM projects p
       JOIN tenants t   ON p.tenant_id = t.id
       JOIN plans pl    ON t.plan_id = pl.id
       WHERE p.is_archived = false
         AND t.is_active = true`,
    );

    const projects = pgResult.rows;

    // 2. Sort projects by "most stale first":
    //    approximate by smallest retention_days (data expires soonest)
    projects.sort((a, b) => a.retention_days - b.retention_days);

    let projectsProcessed = 0;
    let documentsDeleted = 0;
    let errors = 0;

    // 3. Process each project sequentially (no parallel IO saturation)
    for (const { project_id: projectId, retention_days: retentionDays } of projects) {
      const cutoff = new Date(Date.now() - retentionDays * 86_400_000);

      try {
        const deleted = await this.deleteProjectEvents(projectId, cutoff);
        documentsDeleted += deleted;
        projectsProcessed++;

        if (deleted > 0) {
          logger.info(
            { projectId, retentionDays, cutoff, deleted },
            'retention: project processed',
          );
        }
      } catch (err) {
        errors++;
        logger.error(
          { err, projectId, retentionDays, cutoff },
          'retention: error processing project',
        );
      }
    }

    const durationMs = Date.now() - startMs;

    logger.info(
      { projectsProcessed, documentsDeleted, errors, durationMs },
      'Retention job finished',
    );

    return { projectsProcessed, documentsDeleted, errors, durationMs };
  }

  // ---------------------------------------------------------------------------
  // deleteProjectEvents — batched delete loop for a single project
  // ---------------------------------------------------------------------------

  private async deleteProjectEvents(projectId: string, cutoff: Date): Promise<number> {
    let totalDeleted = 0;
    const projectStart = Date.now();

    while (true) {
      // Check wall-clock budget
      if (Date.now() - projectStart > PROJECT_BUDGET_MS) {
        logger.warn(
          { projectId, totalDeleted, budgetMs: PROJECT_BUDGET_MS },
          'retention: project budget exceeded, will resume next cycle',
        );
        break;
      }

      // Find a batch of IDs to delete (MongoDB doesn't support limit on deleteMany)
      const batch = await this.mongo
        .events()
        .find({ projectId, ingestedAt: { $lt: cutoff } }, { projection: { _id: 1 } })
        .limit(BATCH_SIZE)
        .toArray();

      if (batch.length === 0) {
        break;
      }

      const ids = batch.map((doc) => doc._id);
      const deleteResult = await this.mongo.events().deleteMany({ _id: { $in: ids } });
      const deleted = deleteResult.deletedCount ?? 0;
      totalDeleted += deleted;

      logger.debug(
        { projectId, batchDeleted: deleted, totalDeleted, cutoff },
        'retention: batch deleted',
      );

      if (deleted === 0) {
        // Safety guard — nothing was deleted, stop to avoid infinite loop
        break;
      }

      // Yield between batches to avoid saturating I/O
      await sleep(BATCH_SLEEP_MS);
    }

    return totalDeleted;
  }
}
