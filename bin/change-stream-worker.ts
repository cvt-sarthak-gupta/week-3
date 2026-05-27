/**
 * PulseBoard change stream worker entry point.
 *
 * Connects all DBs and starts the Mongo change stream → Redis pub/sub worker
 * with built-in leader election.
 */

import * as pgDb from '../src/db/postgres.js';
import * as mongoDb from '../src/db/mongo.js';
import * as redisDb from '../src/db/redis.js';
import { runChangeStreamWorker } from '../src/workers/change-stream.js';
import { logger } from '../src/logger.js';

async function main(): Promise<void> {
  logger.info('Change stream worker booting');

  // Connect all data stores
  await redisDb.connect();
  await mongoDb.connect();

  // PG pool connects lazily — verify with a health check
  const pgHealth = await pgDb.healthCheck();
  if (!pgHealth.ok) {
    logger.error({ pgHealth }, 'PostgreSQL health check failed at startup');
    process.exit(1);
  }

  logger.info('All DBs ready — starting change stream worker');

  // Graceful shutdown
  const shutdown = async (): Promise<void> => {
    logger.info('Shutting down change stream worker');
    await new Promise<void>((resolve) => setTimeout(resolve, 1_000));
    await redisDb.close();
    await mongoDb.close();
    await pgDb.close();
    logger.info('Change stream worker shutdown complete');
    process.exit(0);
  };

  process.once('SIGTERM', () => {
    shutdown().catch((err: unknown) => {
      logger.error({ err }, 'Error during graceful shutdown');
      process.exit(1);
    });
  });

  // Run the change stream worker (blocks until SIGTERM)
  await runChangeStreamWorker();

  await shutdown();
}

main().catch((err: unknown) => {
  logger.error({ err }, 'Fatal error in change stream worker');
  process.exit(1);
});
