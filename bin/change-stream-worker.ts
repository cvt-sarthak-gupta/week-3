/**
 * PulseBoard change stream worker entry point.
 *
 * Connects all DBs and starts the Mongo change stream → Redis pub/sub worker
 * with built-in leader election.
 */

import { config } from '../src/config.js';
import { AppContainer } from '../src/container.js';
import { ChangeStreamWorker } from '../src/workers/change-stream.js';
import { logger } from '../src/logger.js';

async function main(): Promise<void> {
  logger.info('Change stream worker booting');

  const container = new AppContainer(config);
  await container.initialize();
  logger.info('All DBs ready — starting change stream worker');

  const worker = new ChangeStreamWorker(container.mongo, container.redis);

  // Graceful shutdown
  const shutdown = async (): Promise<void> => {
    logger.info('Shutting down change stream worker');
    await worker.stop();
    await container.close();
    logger.info('Change stream worker shutdown complete');
    process.exit(0);
  };

  process.once('SIGTERM', () => {
    shutdown().catch((err: unknown) => {
      logger.error({ err }, 'Error during graceful shutdown');
      process.exit(1);
    });
  });

  // Run the change stream worker (blocks until stopped)
  await worker.start();
}

main().catch((err: unknown) => {
  logger.error({ err }, 'Fatal error in change stream worker');
  process.exit(1);
});
