/**
 * PulseBoard ingest worker entry point.
 *
 * Reads WORKER_INDEX env var (0, 1, 2) to name the consumer, connects all DBs,
 * then starts the Redis Stream consumer loop.
 */

import * as pgDb from '../src/db/postgres.js';
import * as mongoDb from '../src/db/mongo.js';
import * as redisDb from '../src/db/redis.js';
import * as esDb from '../src/db/elastic.js';
import { runIngestConsumer } from '../src/workers/ingest-consumer.js';
import { logger } from '../src/logger.js';

const workerIndex = process.env['WORKER_INDEX'] ?? '0';
const workerName = `worker-${workerIndex}`;

async function main(): Promise<void> {
  logger.info({ workerName }, 'Ingest worker booting');

  // Connect all data stores
  await redisDb.connect();
  await mongoDb.connect();
  // ES client connects lazily; ensure ILM policies exist
  await esDb.ensureIlmPolicies();

  // PG pool connects lazily — verify with a health check
  const pgHealth = await pgDb.healthCheck();
  if (!pgHealth.ok) {
    logger.error({ pgHealth }, 'PostgreSQL health check failed at startup');
    process.exit(1);
  }

  logger.info({ workerName }, 'All DBs ready — starting ingest consumer');

  // Graceful shutdown: close DBs after consumer exits
  const shutdown = async (): Promise<void> => {
    logger.info({ workerName }, 'Shutting down ingest worker');
    // Give the consumer loop a moment to finish in-flight messages
    await new Promise<void>((resolve) => setTimeout(resolve, 2_000));
    await redisDb.close();
    await mongoDb.close();
    await esDb.close();
    await pgDb.close();
    logger.info({ workerName }, 'Ingest worker shutdown complete');
    process.exit(0);
  };

  process.once('SIGTERM', () => {
    shutdown().catch((err: unknown) => {
      logger.error({ err }, 'Error during graceful shutdown');
      process.exit(1);
    });
  });

  // Run the consumer (blocks until SIGTERM)
  await runIngestConsumer(workerName);

  await shutdown();
}

main().catch((err: unknown) => {
  logger.error({ err }, 'Fatal error in ingest worker');
  process.exit(1);
});
