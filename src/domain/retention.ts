import { pool } from '../db/postgres.js';
import { eventsCollection } from '../db/mongo.js';
import { logger } from '../logger.js';

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export interface RetentionJobResult {
  projectsProcessed: number;
  documentsDeleted: number;
  errors: number;
  durationMs: number;
}

interface ProjectRetentionRow {
  project_id: string;
  retention_days: number;
}

// ---------------------------------------------------------------------------
// sleep helper
// ---------------------------------------------------------------------------

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// ---------------------------------------------------------------------------
// Delete loop for a single project
// ---------------------------------------------------------------------------

const BATCH_SIZE = 10_000;
const BATCH_SLEEP_MS = 50;
const PROJECT_BUDGET_MS = 30_000; // 30-second wall-clock budget per project

async function deleteProjectEvents(
  projectId: string,
  cutoff: Date,
): Promise<number> {
  let totalDeleted = 0;
  const projectStart = Date.now();

  // Loop in batches until nothing is left or budget is exhausted
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
    const batch = await eventsCollection()
      .find({ projectId, ingestedAt: { $lt: cutoff } }, { projection: { _id: 1 } })
      .limit(BATCH_SIZE)
      .toArray();

    if (batch.length === 0) {
      break;
    }

    const ids = batch.map((doc) => doc._id);
    const deleteResult = await eventsCollection().deleteMany({ _id: { $in: ids } });
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

// ---------------------------------------------------------------------------
// runRetentionJob
// ---------------------------------------------------------------------------

export async function runRetentionJob(): Promise<RetentionJobResult> {
  const startMs = Date.now();

  // 1. Query PG for all active projects with their retention_days from plan
  const pgResult = await pool.query<ProjectRetentionRow>(
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
      const deleted = await deleteProjectEvents(projectId, cutoff);
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
