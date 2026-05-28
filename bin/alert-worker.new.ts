/**
 * PulseBoard alert worker entry point (new modular version).
 *
 * Boots the dependency container, then starts the AlertWorker which creates a
 * dedicated ioredis pub/sub subscriber, PSUBSCRIBEs to `alerts:fatal:*`, and
 * fans out to fireDedupAlert for each matching alert rule.
 * Handles graceful shutdown on SIGTERM / SIGINT.
 */

import { createContainer, closeContainer } from '../src/container.js';
import { logger } from '../src/utils/logger.js';

async function main(): Promise<void> {
  logger.info('Alert worker booting (new modular entry point)');

  const container = await createContainer();

  // Graceful shutdown — stop the subscriber first, then close all connections
  for (const signal of ['SIGTERM', 'SIGINT'] as const) {
    process.once(signal, () => {
      void (async () => {
        logger.info({ signal }, 'Alert worker shutting down');
        try {
          await container.alertWorker.stop();
        } catch (err) {
          logger.error({ err }, 'Error stopping AlertWorker');
        }
        await closeContainer(container);
        logger.info('Alert worker shutdown complete');
        process.exit(0);
      })();
    });
  }

  logger.info('All DBs ready — starting alert subscriber');

  // start() sets up the pub/sub listeners and returns — the ioredis subscriber
  // keeps the event loop alive until stop() calls subscriber.quit().
  await container.alertWorker.start();

  logger.info('Alert worker ready, listening for pub/sub messages');
  // Keep the process alive — the ioredis subscriber holds the event loop open
}

main().catch((err: unknown) => {
  logger.error({ err }, 'Fatal error in alert worker');
  process.exit(1);
});
