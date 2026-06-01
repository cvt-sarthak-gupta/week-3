/**
 * PulseBoard ingest worker entry point.
 *
 * Reads WORKER_INDEX (0, 1, …) to name the consumer, then starts
 * INGEST_WORKER_CONCURRENCY IngestConsumer instances so that multiple
 * goroutines are reading from the same Redis Stream consumer group
 * concurrently — matching Task R3's "3 worker goroutines" requirement.
 */

import { config } from '../src/config.js';
import { AppContainer } from '../src/container.js';
import { IngestConsumer } from '../src/workers/ingest-consumer.js';
import { logger } from '../src/logger.js';

const workerIndex = process.env['WORKER_INDEX'] ?? '0';
const workerName = `worker-${workerIndex}`;
const concurrency = config.ingest.workerConcurrency; // default 3

async function main(): Promise<void> {
  logger.info({ workerName, concurrency }, 'Ingest worker booting');

  const container = new AppContainer(config);
  await container.initialize();
  logger.info({ workerName, concurrency }, 'All DBs ready — starting ingest consumers');

  // Spawn `concurrency` IngestConsumer instances, each running independently
  // in the same process.  They all join the same consumer group but use
  // distinct consumer names so Redis distributes messages between them.
  const consumers = Array.from({ length: concurrency }, (_, i) => {
    // Suffix the consumer name with the slot index so Redis can track each
    // independently via XPENDING and XCLAIM.
    const consumer = new IngestConsumer(
      container.redis,
      container.ingestion,
      `${workerName}-slot-${i}`,
    );
    return consumer;
  });

  // Graceful shutdown — stop all consumers then close DB connections.
  const shutdown = async (): Promise<void> => {
    logger.info({ workerName }, 'Shutting down ingest worker');
    consumers.forEach((c) => c.stop());
    // Wait a tick so in-flight loops can exit cleanly.
    await new Promise<void>((resolve) => setTimeout(resolve, 200));
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

  // Run all consumers concurrently (each blocks in its own loop).
  await Promise.all(consumers.map((c) => c.start()));
}

main().catch((err: unknown) => {
  logger.error({ err }, 'Fatal error in ingest worker');
  process.exit(1);
});
