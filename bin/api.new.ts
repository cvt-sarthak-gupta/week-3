/**
 * PulseBoard API server entry point (new modular version).
 *
 * Uses the dependency container + app factory. Run this instead of bin/api.ts
 * once all modules are wired up via createContainer().
 */

import { createContainer, closeContainer } from '../src/container.js';
import { buildApp } from '../src/app.js';
import { config } from '../src/config.js';
import { logger } from '../src/utils/logger.js';

async function main(): Promise<void> {
  logger.info('PulseBoard API server booting (new modular entry point)');

  const container = await createContainer();
  const app = await buildApp(container);

  // Graceful shutdown
  const shutdown = async (signal: string): Promise<void> => {
    logger.info({ signal }, 'Graceful shutdown initiated');
    try {
      await app.close();
      logger.info('Fastify server closed');
    } catch (err) {
      logger.error({ err }, 'Error closing Fastify');
    }
    await closeContainer(container);
    logger.info('All connections closed. Exiting.');
    process.exit(0);
  };

  for (const signal of ['SIGTERM', 'SIGINT'] as const) {
    process.once(signal, () => {
      shutdown(signal).catch((err: unknown) => {
        logger.error({ err }, 'Error during graceful shutdown');
        process.exit(1);
      });
    });
  }

  await app.listen({ port: config.api.port, host: '0.0.0.0' });
  logger.info(
    { port: config.api.port, env: config.node },
    `PulseBoard API listening on port ${config.api.port}`,
  );
}

main().catch((err: unknown) => {
  logger.error({ err }, 'Fatal startup error');
  process.exit(1);
});
