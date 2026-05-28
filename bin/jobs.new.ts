/**
 * PulseBoard scheduled jobs runner entry point (new modular version).
 *
 * Boots the dependency container, then starts the JobScheduler which schedules:
 *   - Retention job:            daily at 02:00 UTC
 *   - PG partition creation:    daily at 00:05 UTC
 *   - ILM apply:                daily at 01:00 UTC
 *
 * All jobs run immediately on startup as well as on their scheduled intervals.
 * Handles graceful shutdown on SIGTERM / SIGINT.
 */

import { createContainer, closeContainer } from '../src/container.js';
import { logger } from '../src/utils/logger.js';

async function main(): Promise<void> {
  logger.info('Jobs runner booting (new modular entry point)');

  const container = await createContainer();

  // Graceful shutdown — stop the scheduler first, then close all connections
  for (const signal of ['SIGTERM', 'SIGINT'] as const) {
    process.once(signal, () => {
      void (async () => {
        logger.info({ signal }, 'Jobs runner shutting down');
        try {
          container.scheduler.stop();
        } catch (err) {
          logger.error({ err }, 'Error stopping JobScheduler');
        }
        await closeContainer(container);
        logger.info('Jobs runner shutdown complete');
        process.exit(0);
      })();
    });
  }

  container.scheduler.start();
  logger.info('Job scheduler started — process will stay alive until signalled');
}

main().catch((err: unknown) => {
  logger.error({ err }, 'Fatal error in jobs runner');
  process.exit(1);
});
