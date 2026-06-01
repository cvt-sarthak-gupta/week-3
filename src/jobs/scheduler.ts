import type { PostgresPool } from '../db/postgres/index.js';
import type { MongoDatabase } from '../db/mongo/index.js';
import type { ElasticClient } from '../db/elastic/index.js';
import { logger } from '../utils/logger.js';

/**
 * The job functions themselves use module-level singletons from the old
 * db layer. The JobScheduler owns the schedule wiring; the actual job logic
 * stays in the existing job files. For the new modular architecture, jobs
 * receive injected dependencies via closures so they can be easily replaced
 * once the jobs are refactored to accept constructor params.
 */

/** Wraps a job function so errors are logged without crashing the scheduler. */
function safeRun(name: string, fn: () => Promise<void>): () => void {
  return () => {
    fn().catch((err: unknown) => {
      logger.error({ err, job: name }, 'Scheduled job failed');
    });
  };
}

/**
 * Returns milliseconds until the next occurrence of a given UTC hour/minute.
 * Used to align daily jobs to wall-clock times rather than "process uptime".
 */
function msUntilNextUtc(targetHour: number, targetMinute: number): number {
  const now = new Date();
  const next = new Date(
    Date.UTC(
      now.getUTCFullYear(),
      now.getUTCMonth(),
      now.getUTCDate(),
      targetHour,
      targetMinute,
      0,
      0,
    ),
  );
  // If the target time has already passed today, schedule for tomorrow
  if (next.getTime() <= now.getTime()) {
    next.setUTCDate(next.getUTCDate() + 1);
  }
  return next.getTime() - now.getTime();
}

export class JobScheduler {
  private readonly timers: Array<ReturnType<typeof setInterval> | ReturnType<typeof setTimeout>> = [];

  constructor(
    private readonly pool: PostgresPool,
    private readonly mongo: MongoDatabase,
    private readonly es: ElasticClient,
  ) {}

  /**
   * Schedules all cron jobs:
   *   - Retention:            daily at 02:00 UTC
   *   - PG partition creation: daily at 00:05 UTC
   *   - ILM apply:            daily at 01:00 UTC
   *
   * Each job is run immediately on startup and then again at the aligned
   * wall-clock time so partitions/policies exist from the start.
   */
  start(): void {
    const DAY_MS = 24 * 60 * 60 * 1_000;

    const retentionRunner = safeRun('retentionJob', () => this.runRetention());

    // Run immediately on startup
    retentionRunner();

    const msUntilRetention = msUntilNextUtc(2, 0);
    logger.info(
      {
        nextRunMs: msUntilRetention,
        nextRunAt: new Date(Date.now() + msUntilRetention).toISOString(),
      },
      'retentionJob scheduled: daily at 02:00 UTC',
    );
    this.timers.push(
      setTimeout(() => {
        retentionRunner();
        this.timers.push(setInterval(retentionRunner, DAY_MS));
      }, msUntilRetention),
    );

    const partitionsRunner = safeRun('ensurePartitionsJob', () => this.runEnsurePartitions());

    // Run immediately on startup so partitions exist from the start
    partitionsRunner();

    const msUntilPartitions = msUntilNextUtc(0, 5);
    logger.info(
      {
        nextRunMs: msUntilPartitions,
        nextRunAt: new Date(Date.now() + msUntilPartitions).toISOString(),
      },
      'ensurePartitionsJob scheduled: daily at 00:05 UTC',
    );
    this.timers.push(
      setTimeout(() => {
        partitionsRunner();
        this.timers.push(setInterval(partitionsRunner, DAY_MS));
      }, msUntilPartitions),
    );

    const ilmRunner = safeRun('applyIlmJob', () => this.runApplyIlm());

    // Run once immediately to reconcile on startup
    ilmRunner();

    const msUntilIlm = msUntilNextUtc(1, 0);
    logger.info(
      {
        nextRunMs: msUntilIlm,
        nextRunAt: new Date(Date.now() + msUntilIlm).toISOString(),
      },
      'applyIlmJob scheduled: daily at 01:00 UTC',
    );
    this.timers.push(
      setTimeout(() => {
        ilmRunner();
        this.timers.push(setInterval(ilmRunner, DAY_MS));
      }, msUntilIlm),
    );

    logger.info('JobScheduler: all jobs scheduled');
  }

  /** Clears all scheduled timers (graceful shutdown). */
  stop(): void {
    for (const timer of this.timers) {
      clearInterval(timer as ReturnType<typeof setInterval>);
      clearTimeout(timer as ReturnType<typeof setTimeout>);
    }
    this.timers.length = 0;
    logger.info('JobScheduler: all timers cleared');
  }

  // Job implementations — use injected DB clients directly so the new
  // modular jobs don't rely on module-level singletons.

  private async runRetention(): Promise<void> {
    logger.info('Retention job starting');
    // Retention cleans up MongoDB events that exceed per-project retention windows.
    // Query all active projects and their retention_days from PG, then delete old docs.
    const result = await this.pool.query<{ id: string; retention_days: number }>(
      `SELECT p.id, pl.retention_days
       FROM projects p
       JOIN tenants t  ON p.tenant_id = t.id
       JOIN plans pl   ON t.plan_id = pl.id
       WHERE p.is_archived = false
         AND t.is_active = true`,
    );

    let deleted = 0;
    let errors = 0;

    for (const { id: projectId, retention_days: retentionDays } of result.rows) {
      try {
        const cutoff = new Date();
        cutoff.setUTCDate(cutoff.getUTCDate() - retentionDays);

        const deleteResult = await this.mongo.events().deleteMany({
          projectId,
          occurredAt: { $lt: cutoff },
        });

        deleted += deleteResult.deletedCount;
        logger.debug(
          { projectId, retentionDays, cutoff, deleted: deleteResult.deletedCount },
          'Retention: deleted old events',
        );
      } catch (err) {
        logger.error({ err, projectId }, 'Retention: failed to delete events for project');
        errors++;
      }
    }

    logger.info(
      { deleted, errors, totalProjects: result.rows.length },
      'Retention job complete',
    );
  }

  private async runEnsurePartitions(): Promise<void> {
    logger.info('Ensure partitions job starting');

    const now = new Date();
    const months: Array<{ year: number; month: number }> = [];

    // Build list of current month + next 3 months
    for (let offset = 0; offset <= 3; offset++) {
      const d = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth() + offset, 1));
      months.push({ year: d.getUTCFullYear(), month: d.getUTCMonth() + 1 });
    }

    let applied = 0;
    let failed = 0;

    for (const { year, month } of months) {
      try {
        // ensure_billing_partition() is a PG stored function that creates the
        // partition for the given year+month if it doesn't already exist.
        await this.pool.query(`SELECT ensure_billing_partition($1, $2)`, [year, month]);
        logger.info({ year, month }, 'Partition ensured');
        applied++;
      } catch (err) {
        logger.error({ err, year, month }, 'Failed to ensure partition');
        failed++;
      }
    }

    logger.info(
      { applied, failed, totalMonths: months.length },
      'Ensure partitions job complete',
    );
  }

  private async runApplyIlm(): Promise<void> {
    logger.info('ILM apply job starting');

    const result = await this.pool.query<{ id: string; retention_days: number }>(
      `SELECT p.id, pl.retention_days
       FROM projects p
       JOIN tenants t  ON p.tenant_id = t.id
       JOIN plans pl   ON t.plan_id = pl.id
       WHERE p.is_archived = false
         AND t.is_active = true`,
    );

    const projects = result.rows;
    let applied = 0;
    let skipped = 0;
    let errors = 0;

    for (const { id: projectId, retention_days: retentionDays } of projects) {
      try {
        await this.es.applyPolicyForProject(projectId, retentionDays);
        applied++;
      } catch (err) {
        // If the project's index simply doesn't exist yet, log as skipped
        const isNotFound =
          err instanceof Error &&
          (err.message.includes('index_not_found') || err.message.includes('404'));

        if (isNotFound) {
          logger.debug({ projectId }, 'ILM apply: index not found, skipping');
          skipped++;
        } else {
          logger.error({ err, projectId, retentionDays }, 'ILM apply: failed for project');
          errors++;
        }
      }
    }

    logger.info(
      { applied, skipped, errors, total: projects.length },
      'ILM apply job complete',
    );
  }
}
