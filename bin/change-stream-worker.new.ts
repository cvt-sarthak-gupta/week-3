/**
 * PulseBoard change stream worker entry point (new modular version).
 *
 * Boots the dependency container, then starts the ChangeStreamWorker which
 * runs leader election and the MongoDB change stream → Redis pub/sub pipeline.
 * Handles graceful shutdown on SIGTERM / SIGINT.
 */

import { createContainer, closeContainer } from '../src/container.js';
import { logger } from '../src/utils/logger.js';

async function main(): Promise<void> {
  logger.info('Change stream worker booting (new modular entry point)');

  const container = await createContainer();

  // Graceful shutdown — stop the worker first, then close all connections
  for (const signal of ['SIGTERM', 'SIGINT'] as const) {
    process.once(signal, () => {
      void (async () => {
        logger.info({ signal }, 'Change stream worker shutting down');
        try {
          await container.changeStreamWorker.stop();
        } catch (err) {
          logger.error({ err }, 'Error stopping ChangeStreamWorker');
        }
        await closeContainer(container);
        logger.info('Change stream worker shutdown complete');
        process.exit(0);
      })();
    });
  }

  logger.info('All DBs ready — starting change stream worker');

  // start() blocks until running = false (set by SIGTERM handler or stop())
  await container.changeStreamWorker.start();

  // If start() returns without signal (e.g. in test), shut down cleanly
  await closeContainer(container);
  logger.info('Change stream worker exited');
}

main().catch((err: unknown) => {
  logger.error({ err }, 'Fatal error in change stream worker');
  process.exit(1);
});
