/**
 * PulseBoard ingest worker entry point (new modular version).
 *
 * Boots the dependency container, then starts the IngestWorker which runs the
 * XREADGROUP consumer loop. Handles graceful shutdown on SIGTERM / SIGINT.
 */

import { createContainer, closeContainer } from '../src/container.js';
import { logger } from '../src/utils/logger.js';

async function main(): Promise<void> {
  const workerIndex = process.env['WORKER_INDEX'] ?? '0';
  logger.info({ workerIndex }, 'Ingest worker booting (new modular entry point)');

  const container = await createContainer();

  // Graceful shutdown — stop the worker first, then close all connections
  for (const signal of ['SIGTERM', 'SIGINT'] as const) {
    process.once(signal, () => {
      void (async () => {
        logger.info({ signal }, 'Ingest worker shutting down');
        try {
          await container.ingestWorker.stop();
        } catch (err) {
          logger.error({ err }, 'Error stopping IngestWorker');
        }
        await closeContainer(container);
        logger.info('Ingest worker shutdown complete');
        process.exit(0);
      })();
    });
  }

  logger.info({ workerIndex }, 'All DBs ready — starting ingest consumer');

  // start() blocks until running = false (set by SIGTERM handler or stop())
  await container.ingestWorker.start();

  // If start() returns without signal (e.g. in test), shut down cleanly
  await closeContainer(container);
  logger.info('Ingest worker exited');
}

main().catch((err: unknown) => {
  logger.error({ err }, 'Fatal error in ingest worker');
  process.exit(1);
});
