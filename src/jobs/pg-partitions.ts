import { pool } from '../db/postgres.js';
import { logger } from '../logger.js';

// ---------------------------------------------------------------------------
// ensurePartitionsJob — called daily at midnight
// Ensures billing_events partitions exist for current + next 3 months.
// ---------------------------------------------------------------------------

export async function ensurePartitionsJob(): Promise<void> {
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
      await pool.query(`SELECT ensure_billing_partition($1, $2)`, [year, month]);
      logger.info({ year, month }, 'Partition ensured');
      applied++;
    } catch (err) {
      logger.error({ err, year, month }, 'Failed to ensure partition');
      failed++;
    }
  }

  logger.info({ applied, failed, totalMonths: months.length }, 'Ensure partitions job complete');
}
