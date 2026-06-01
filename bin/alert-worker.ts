/**
 * PulseBoard alert worker entry point.
 *
 * Connects all DBs and starts the Redis pub/sub subscriber for alert fan-out.
 */

import { config } from '../src/config.js';
import { AppContainer } from '../src/container.js';
import { AlertSubscriber } from '../src/workers/alert-subscriber.js';
import { logger } from '../src/logger.js';

async function main(): Promise<void> {
  logger.info('Alert worker booting');

  const container = new AppContainer(config);
  await container.initialize();
  logger.info('All DBs ready — starting alert subscriber');

  const worker = new AlertSubscriber(container.pg, container.alerts);

  // Graceful shutdown
  const shutdown = async (): Promise<void> => {
    logger.info('Shutting down alert worker');
    await worker.stop();
    await container.close();
    logger.info('Alert worker shutdown complete');
    process.exit(0);
  };

  process.once('SIGTERM', () => {
    shutdown().catch((err: unknown) => {
      logger.error({ err }, 'Error during graceful shutdown');
      process.exit(1);
    });
  });

  // Start the alert subscriber (sets up pub/sub listeners)
  await worker.start();

  logger.info('Alert worker ready, listening for pub/sub messages');
  // Keep the process alive — the ioredis subscriber holds the event loop open
}

main().catch((err: unknown) => {
  logger.error({ err }, 'Fatal error in alert worker');
  process.exit(1);
});
