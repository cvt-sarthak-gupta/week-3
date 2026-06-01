/**
 * PulseBoard ingest worker entry point.
 *
 * Reads WORKER_INDEX env var (0, 1, 2) to name the consumer, connects all DBs,
 * then starts the Redis Stream consumer loop.
 */

import { config } from '../src/config.js';
import { AppContainer } from '../src/container.js';
import { IngestConsumer } from '../src/workers/ingest-consumer.js';
import { logger } from '../src/logger.js';

const workerIndex = process.env['WORKER_INDEX'] ?? '0';
const workerName = `worker-${workerIndex}`;

async function main(): Promise<void> {
  logger.info({ workerName }, 'Ingest worker booting');

  const container = new AppContainer(config);
  await container.initialize();
  logger.info({ workerName }, 'All DBs ready — starting ingest consumer');

  const worker = new IngestConsumer(container.redis, container.ingestion);

  // Graceful shutdown
  const shutdown = async (): Promise<void> => {
    logger.info({ workerName }, 'Shutting down ingest worker');
    await worker.stop();
    await container.close();
    logger.info({ workerName }, 'Ingest worker shutdown complete');
    process.exit(0);
  };

  process.once('SIGTERM', () => {
    shutdown().catch((err: unknown) => {
      logger.error({ err }, 'Error during graceful shutdown');
      process.exit(1);
    });
  });

  // Run the consumer (blocks until stopped)
  await worker.start();
}

main().catch((err: unknown) => {
  logger.error({ err }, 'Fatal error in ingest worker');
  process.exit(1);
});
