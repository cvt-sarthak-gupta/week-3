import Fastify from 'fastify';
import cors from '@fastify/cors';
import jwt from '@fastify/jwt';
import swagger from '@fastify/swagger';
import swaggerUi from '@fastify/swagger-ui';
import { config } from '../src/config.js';
import { logger } from '../src/logger.js';
import { fastifyErrorHandler } from '../src/errors.js';
import { correlationPlugin } from '../src/lib/correlation.js';
import { connect as connectMongo, close as closeMongo } from '../src/db/mongo.js';
import { pool, close as closePg } from '../src/db/postgres.js';
import { redis, connect as connectRedis, ensureConsumerGroup } from '../src/db/redis.js';
import { esClient, ensureIlmPolicies, ensureIndexTemplate, ensurePercolatorIndex } from '../src/db/elastic.js';

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

// ---------------------------------------------------------------------------
// Build
// ---------------------------------------------------------------------------

async function build(): Promise<ReturnType<typeof Fastify>> {
  const fastify = Fastify({
    logger: false, // We use our own pino instance
    trustProxy: true,
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

  await fastify.register(healthRoutes);

  // Metrics at root — internal, no auth, firewalled at infra level
  await fastify.register(metricsRoutes);

  // -------------------------------------------------------------------------
  // All v1 routes under /v1 prefix
  // -------------------------------------------------------------------------

  await fastify.register(async (v1) => {
    // Auth
    await v1.register(authRoutes);

    // Tenants
    await v1.register(tenantRoutes);

    // Projects (nested under tenants)
    await v1.register(projectRoutes);

    // Alerts (nested under projects)
    await v1.register(alertRoutes);

    // Search
    await v1.register(searchRoutes);

    // Reports (tenant-scoped + admin)
    await v1.register(reportRoutes);

    // Leaderboard
    await v1.register(leaderboardRoutes);

    // Ingest hot path
    await v1.register(ingestRoutes);

    // Admin: consistency audit
    await v1.register(consistencyRoutes);
  }, { prefix: '/v1' });

  return fastify;
}

// ---------------------------------------------------------------------------
// Start
// ---------------------------------------------------------------------------

async function start(): Promise<void> {
  const fastify = await build();

  // -----------------------------------------------------------------------
  // Startup: connect to all datastores
  // -----------------------------------------------------------------------

  logger.info('Connecting to MongoDB…');
  await connectMongo();

  logger.info('Connecting to Redis…');
  await connectRedis();

  logger.info('Ensuring Redis consumer group…');
  await ensureConsumerGroup();

  logger.info('Ensuring Elasticsearch ILM policies…');
  await ensureIlmPolicies();

  logger.info('Ensuring Elasticsearch index template…');
  await ensureIndexTemplate();

  logger.info('Ensuring Elasticsearch percolator index…');
  await ensurePercolatorIndex();

  // Touch PG pool to verify connectivity before serving traffic
  await pool.query('SELECT 1');
  logger.info('PostgreSQL pool ready');

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

    await Promise.allSettled([
      closePg().catch((err) => logger.error({ err }, 'Error closing PG pool')),
      closeMongo().catch((err) => logger.error({ err }, 'Error closing MongoDB')),
      esClient.close().catch((err) => logger.error({ err }, 'Error closing Elasticsearch')),
      redis.quit().catch((err) => logger.error({ err }, 'Error closing Redis')),
    ]);

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
