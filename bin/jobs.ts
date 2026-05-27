/**
 * PulseBoard scheduled jobs runner.
 *
 * Schedule:
 *   - retentionJob:        every hour
 *   - ensurePartitionsJob: daily at midnight (00:00 UTC)
 *   - applyIlmJob:         daily at 01:00 UTC
 *
 * Uses hand-rolled setInterval / setTimeout scheduling (node-cron is available
 * in deps but hand-rolled avoids an extra dependency layer for this simple case).
 * Graceful shutdown clears all timers.
 */

import * as pgDb from '../src/db/postgres.js';
import * as mongoDb from '../src/db/mongo.js';
import * as redisDb from '../src/db/redis.js';
import * as esDb from '../src/db/elastic.js';
import { retentionJob } from '../src/jobs/retention.js';
import { ensurePartitionsJob } from '../src/jobs/pg-partitions.js';
import { applyIlmJob } from '../src/jobs/ilm-apply.js';
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

  // Connect all data stores
  await redisDb.connect();
  await mongoDb.connect();
  await esDb.ensureIlmPolicies();

  const pgHealth = await pgDb.healthCheck();
  if (!pgHealth.ok) {
    logger.error({ pgHealth }, 'PostgreSQL health check failed at startup');
    process.exit(1);
  }

  logger.info('All DBs ready — scheduling jobs');

  const timers: Array<ReturnType<typeof setInterval> | ReturnType<typeof setTimeout>> = [];

  // -------------------------------------------------------------------------
  // 1. Retention job — every hour
  // -------------------------------------------------------------------------
  const HOUR_MS = 60 * 60 * 1000;
  const retentionRunner = safeRun('retentionJob', retentionJob);

  // Run immediately on startup, then every hour
  retentionRunner();
  timers.push(setInterval(retentionRunner, HOUR_MS));
  logger.info('retentionJob scheduled: every 1 hour');

  // -------------------------------------------------------------------------
  // 2. Ensure partitions job — daily at 00:00 UTC
  // -------------------------------------------------------------------------
  const DAY_MS = 24 * 60 * 60 * 1000;
  const partitionsRunner = safeRun('ensurePartitionsJob', ensurePartitionsJob);

  // Run once immediately so partitions exist from the start
  partitionsRunner();

  const msUntilMidnight = msUntilNextUtc(0, 0);
  logger.info(
    { nextRunMs: msUntilMidnight, nextRunAt: new Date(Date.now() + msUntilMidnight).toISOString() },
    'ensurePartitionsJob scheduled: daily at 00:00 UTC',
  );
  timers.push(
    setTimeout(() => {
      partitionsRunner();
      timers.push(setInterval(partitionsRunner, DAY_MS));
    }, msUntilMidnight),
  );

  // -------------------------------------------------------------------------
  // 3. ILM apply job — daily at 01:00 UTC
  // -------------------------------------------------------------------------
  const ilmRunner = safeRun('applyIlmJob', applyIlmJob);

  // Run once immediately to reconcile on startup
  ilmRunner();

  const msUntil1am = msUntilNextUtc(1, 0);
  logger.info(
    { nextRunMs: msUntil1am, nextRunAt: new Date(Date.now() + msUntil1am).toISOString() },
    'applyIlmJob scheduled: daily at 01:00 UTC',
  );
  timers.push(
    setTimeout(() => {
      ilmRunner();
      timers.push(setInterval(ilmRunner, DAY_MS));
    }, msUntil1am),
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
    await redisDb.close();
    await mongoDb.close();
    await esDb.close();
    await pgDb.close();
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
