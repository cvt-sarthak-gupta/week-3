import Fastify from 'fastify';
import cors from '@fastify/cors';
import jwt from '@fastify/jwt';
import swagger from '@fastify/swagger';
import swaggerUi from '@fastify/swagger-ui';
import { config } from '../src/config.js';
import { logger } from '../src/logger.js';
import { fastifyErrorHandler } from '../src/errors.js';
import { correlationPlugin } from '../src/lib/correlation.js';
import { AppContainer } from '../src/container.js';

// Route plugins
import { ingestRoutes } from '../src/routes/ingest.js';
import { authRoutes } from '../src/routes/auth.js';
import { tenantRoutes } from '../src/routes/tenants.js';
import { projectRoutes } from '../src/routes/projects.js';
import { alertRoutes } from '../src/routes/alerts.js';
import { searchRoutes } from '../src/routes/search.js';
import { reportRoutes } from '../src/routes/reports.js';
import { leaderboardRoutes } from '../src/routes/leaderboard.js';
import { healthRoutes } from '../src/routes/health.js';
import { metricsRoutes } from '../src/routes/metrics.js';
import { consistencyRoutes } from '../src/routes/consistency.js';
import { dashboardRoutes } from '../src/routes/dashboards.js';

// ---------------------------------------------------------------------------
// Build
// ---------------------------------------------------------------------------

async function build(container: AppContainer): Promise<ReturnType<typeof Fastify>> {
  const fastify = Fastify({
    logger: false, // We use our own pino instance
    trustProxy: true,
    // Explicit body limit — Fastify default is 1 MB but we set it clearly so
    // any future increase requires a deliberate decision.
    bodyLimit: 1_048_576, // 1 MiB
  });

  // -------------------------------------------------------------------------
  // Plugins
  // -------------------------------------------------------------------------

  await fastify.register(cors, {
    origin: config.node === 'production' ? false : true,
    credentials: true,
  });

  await fastify.register(jwt, {
    secret: config.jwt.secret,
    sign: { expiresIn: config.jwt.expiry },
  });

  await fastify.register(swagger, {
    openapi: {
      openapi: '3.0.0',
      info: {
        title: 'PulseBoard API',
        description: 'Multi-tenant SaaS monitoring and observability platform',
        version: '1.0.0',
      },
      servers: [
        { url: `http://localhost:${config.api.port}`, description: 'Local development' },
      ],
      components: {
        securitySchemes: {
          bearerAuth: {
            type: 'http',
            scheme: 'bearer',
            bearerFormat: 'JWT',
          },
          apiKey: {
            type: 'apiKey',
            in: 'header',
            name: 'X-PulseBoard-Key',
          },
        },
      },
    },
  });

  await fastify.register(swaggerUi, {
    routePrefix: '/docs',
    uiConfig: {
      docExpansion: 'list',
      deepLinking: true,
    },
  });

  await fastify.register(correlationPlugin);

  // -------------------------------------------------------------------------
  // Error handler
  // -------------------------------------------------------------------------

  fastify.setErrorHandler(fastifyErrorHandler);

  // -------------------------------------------------------------------------
  // Health routes at root (no /v1 prefix) — registered first so they
  // aren't shadowed by the v1 prefix block below
  // -------------------------------------------------------------------------

  await fastify.register(healthRoutes(container));

  // Metrics at root — internal, no auth, firewalled at infra level
  await fastify.register(metricsRoutes(container));

  await fastify.register(async (v1) => {
    await v1.register(authRoutes(container));
    await v1.register(tenantRoutes(container));
    await v1.register(projectRoutes(container));
    await v1.register(alertRoutes(container));
    await v1.register(searchRoutes(container));
    await v1.register(reportRoutes(container));
    await v1.register(leaderboardRoutes(container));
    await v1.register(ingestRoutes(container));
    await v1.register(consistencyRoutes(container));
    await v1.register(dashboardRoutes(container));
  }, { prefix: '/v1' });

  return fastify;
}

// ---------------------------------------------------------------------------
// Start
// ---------------------------------------------------------------------------

async function start(): Promise<void> {
  const container = new AppContainer(config);

  logger.info('Initializing AppContainer…');
  await container.initialize();
  logger.info('AppContainer ready');

  const fastify = await build(container);

  // -----------------------------------------------------------------------
  // Graceful shutdown
  // -----------------------------------------------------------------------

  const shutdown = async (signal: string): Promise<void> => {
    logger.info({ signal }, 'Graceful shutdown initiated');

    try {
      await fastify.close();
      logger.info('Fastify server closed');
    } catch (err) {
      logger.error({ err }, 'Error closing Fastify');
    }

    await container.close();

    logger.info('All connections closed. Exiting.');
    process.exit(0);
  };

  process.once('SIGTERM', () => { void shutdown('SIGTERM'); });
  process.once('SIGINT',  () => { void shutdown('SIGINT'); });

  // -----------------------------------------------------------------------
  // Listen
  // -----------------------------------------------------------------------

  await fastify.listen({ port: config.api.port, host: '0.0.0.0' });

  logger.info(
    { port: config.api.port, env: config.node },
    `PulseBoard API listening on port ${config.api.port}`,
  );
}

start().catch((err) => {
  logger.error({ err }, 'Fatal startup error');
  process.exit(1);
});
