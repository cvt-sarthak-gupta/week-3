/**
 * PulseBoard alert worker entry point.
 *
 * Connects all DBs and starts the Redis pub/sub subscriber for alert fan-out.
 */

import * as pgDb from '../src/db/postgres.js';
import * as mongoDb from '../src/db/mongo.js';
import * as redisDb from '../src/db/redis.js';
import { runAlertSubscriber } from '../src/workers/alert-subscriber.js';
import { logger } from '../src/logger.js';

async function main(): Promise<void> {
  logger.info('Alert worker booting');

  // Connect all data stores
  await redisDb.connect();
  await mongoDb.connect();

  // PG pool connects lazily — verify with a health check
  const pgHealth = await pgDb.healthCheck();
  if (!pgHealth.ok) {
    logger.error({ pgHealth }, 'PostgreSQL health check failed at startup');
    process.exit(1);
  }

  logger.info('All DBs ready — starting alert subscriber');

  // Graceful shutdown
  const shutdown = async (): Promise<void> => {
    logger.info('Shutting down alert worker');
    await new Promise<void>((resolve) => setTimeout(resolve, 1_000));
    await redisDb.close();
    await mongoDb.close();
    await pgDb.close();
    logger.info('Alert worker shutdown complete');
    process.exit(0);
  };

  process.once('SIGTERM', () => {
    shutdown().catch((err: unknown) => {
      logger.error({ err }, 'Error during graceful shutdown');
      process.exit(1);
    });
  });

  // Run the alert subscriber (sets up pub/sub listeners and returns — stays alive via event loop)
  await runAlertSubscriber();

  logger.info('Alert worker ready, listening for pub/sub messages');
  // Keep the process alive — the ioredis subscriber holds the event loop open
}

main().catch((err: unknown) => {
  logger.error({ err }, 'Fatal error in alert worker');
  process.exit(1);
});
