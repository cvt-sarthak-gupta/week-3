/**
 * PulseBoard scheduled jobs runner.
 *
 * Schedule:
 *   - PartitionJob:  daily at 00:05 UTC
 *   - IlmApplyJob:   daily at 01:00 UTC
 *   - RetentionJob:  daily at 02:00 UTC
 *
 * Uses hand-rolled setInterval / setTimeout scheduling (node-cron is available
 * in deps but hand-rolled avoids an extra dependency layer for this simple case).
 * Graceful shutdown clears all timers.
 */

import { config } from '../src/config.js';
import { AppContainer } from '../src/container.js';
import { PartitionJob } from '../src/jobs/pg-partitions.js';
import { IlmApplyJob } from '../src/jobs/ilm-apply.js';
import { RetentionJob } from '../src/jobs/retention.js';
import { logger } from '../src/logger.js';

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main(): Promise<void> {
  logger.info('Jobs runner booting');

  const container = new AppContainer(config);
  await container.initialize();
  logger.info('All DBs ready — scheduling jobs');

  const timers: Array<ReturnType<typeof setInterval> | ReturnType<typeof setTimeout>> = [];

  // -------------------------------------------------------------------------
  // 1. Partition job — daily at 00:05 UTC ('5 0 * * *')
  // -------------------------------------------------------------------------
  const DAY_MS = 24 * 60 * 60 * 1000;
  const partitionsRunner = safeRun('PartitionJob', () => new PartitionJob(container.pg).run());

  // Run once immediately so partitions exist from the start
  partitionsRunner();

  const msUntil0005 = msUntilNextUtc(0, 5);
  logger.info(
    { nextRunMs: msUntil0005, nextRunAt: new Date(Date.now() + msUntil0005).toISOString() },
    'PartitionJob scheduled: daily at 00:05 UTC',
  );
  timers.push(
    setTimeout(() => {
      partitionsRunner();
      timers.push(setInterval(partitionsRunner, DAY_MS));
    }, msUntil0005),
  );

  // -------------------------------------------------------------------------
  // 2. ILM apply job — daily at 01:00 UTC ('0 1 * * *')
  // -------------------------------------------------------------------------
  const ilmRunner = safeRun('IlmApplyJob', () => new IlmApplyJob(container.pg, container.es).run());

  // Run once immediately to reconcile on startup
  ilmRunner();

  const msUntil1am = msUntilNextUtc(1, 0);
  logger.info(
    { nextRunMs: msUntil1am, nextRunAt: new Date(Date.now() + msUntil1am).toISOString() },
    'IlmApplyJob scheduled: daily at 01:00 UTC',
  );
  timers.push(
    setTimeout(() => {
      ilmRunner();
      timers.push(setInterval(ilmRunner, DAY_MS));
    }, msUntil1am),
  );

  // -------------------------------------------------------------------------
  // 3. Retention job — daily at 02:00 UTC ('0 2 * * *')
  // -------------------------------------------------------------------------
  const retentionRunner = safeRun('RetentionJob', () => new RetentionJob(container.retention).run());

  // Run once immediately on startup
  retentionRunner();

  const msUntil2am = msUntilNextUtc(2, 0);
  logger.info(
    { nextRunMs: msUntil2am, nextRunAt: new Date(Date.now() + msUntil2am).toISOString() },
    'RetentionJob scheduled: daily at 02:00 UTC',
  );
  timers.push(
    setTimeout(() => {
      retentionRunner();
      timers.push(setInterval(retentionRunner, DAY_MS));
    }, msUntil2am),
  );

  // -------------------------------------------------------------------------
  // Graceful shutdown
  // -------------------------------------------------------------------------
  const shutdown = async (): Promise<void> => {
    logger.info('SIGTERM received — stopping jobs runner');
    for (const timer of timers) {
      clearInterval(timer as ReturnType<typeof setInterval>);
      clearTimeout(timer as ReturnType<typeof setTimeout>);
    }
    await container.close();
    logger.info('Jobs runner shutdown complete');
    process.exit(0);
  };

  process.once('SIGTERM', () => {
    shutdown().catch((err: unknown) => {
      logger.error({ err }, 'Error during jobs runner shutdown');
      process.exit(1);
    });
  });

  logger.info('Jobs runner ready');
}

main().catch((err: unknown) => {
  logger.error({ err }, 'Fatal error in jobs runner');
  process.exit(1);
});
