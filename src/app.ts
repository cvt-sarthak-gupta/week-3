import Fastify, { type FastifyInstance } from 'fastify';
import cors from '@fastify/cors';
import jwt from '@fastify/jwt';
import swagger from '@fastify/swagger';
import swaggerUi from '@fastify/swagger-ui';
import { config } from './config.js';
import { correlationPlugin } from './utils/correlation.js';
import { fastifyErrorHandler } from './utils/errors.js';
import { createAuthRoutes } from './modules/auth/index.js';
import { createTenantRoutes } from './modules/tenants/index.js';
import { createProjectRoutes } from './modules/projects/index.js';
import { createIngestRoutes } from './modules/ingest/index.js';
import { createAlertRoutes } from './modules/alerts/index.js';
import { createSearchRoutes } from './modules/search/index.js';
import { createReportRoutes } from './modules/reports/index.js';
import { createLeaderboardRoutes } from './modules/leaderboard/index.js';
import { createHealthRoutes } from './modules/health/index.js';
import { createMetricsRoutes } from './modules/metrics/index.js';
import { createConsistencyRoutes } from './modules/consistency/index.js';
import type { AppContainer } from './container.js';

export async function buildApp(container: AppContainer): Promise<FastifyInstance> {
  const app = Fastify({
    logger: false, // We use our own pino instance
    trustProxy: true,
  });

  await app.register(cors, {
    origin: config.node === 'production' ? false : true,
    credentials: true,
  });

  await app.register(jwt, {
    secret: config.jwt.secret,
    sign: { expiresIn: config.jwt.expiry },
  });

  await app.register(swagger, {
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

  await app.register(swaggerUi, {
    routePrefix: '/docs',
    uiConfig: {
      docExpansion: 'list',
      deepLinking: true,
    },
  });

  await app.register(correlationPlugin);

  app.setErrorHandler(fastifyErrorHandler);

  // Ingest at root (no /v1 prefix) — API-key authenticated hot path

  await app.register(createIngestRoutes(container.ingest, container.apiKey));

  // Health and metrics at root (no auth, no /v1 prefix)

  await app.register(createHealthRoutes(container.health));
  await app.register(createMetricsRoutes(container.metrics));

  // All v1 routes under /v1 prefix

  await app.register(
    async (v1) => {
      // Auth
      await v1.register(createAuthRoutes(container.auth));

      // Tenants
      await v1.register(createTenantRoutes(container.tenants));

      // Projects (nested under tenants)
      await v1.register(createProjectRoutes(container.projects));

      await v1.register(createAlertRoutes(container.alerts, container.pool));
      await v1.register(createSearchRoutes(container.search, container.pool));
      await v1.register(createReportRoutes(container.reports, container.pool));
      await v1.register(createLeaderboardRoutes(container.leaderboard, container.pool));
      await v1.register(createConsistencyRoutes(container.consistency, container.pool));
    },
    { prefix: '/v1' },
  );

  await app.ready();
  return app;
}
