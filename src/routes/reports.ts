import type { FastifyPluginAsync, FastifyReply } from 'fastify';
import fp from 'fastify-plugin';
import { AppContainer } from '../container.js';
import { ForbiddenError } from '../errors.js';

interface ProjectParams {
  tenantId: string;
  projectId: string;
}

interface ErrorIntelligenceQuerystring {
  days?: number;
}

interface MemberRow {
  user_id: string;
  role: string;
}

export function reportRoutes(container: AppContainer): FastifyPluginAsync {
  return fp(async (fastify) => {
    const userPreHandler = container.auth.userPreHandler();

    async function assertMember(userId: string, tenantId: string): Promise<MemberRow> {
      const result = await container.pg.query<MemberRow>(
        'SELECT user_id, role FROM tenant_members WHERE tenant_id = $1 AND user_id = $2 LIMIT 1',
        [tenantId, userId],
      );
      const member = result.rows[0];
      if (member === undefined) {
        throw new ForbiddenError('Not a member of this tenant');
      }
      return member;
    }

    async function assertAdmin(userId: string): Promise<void> {
      const result = await container.pg.query<{ role: string }>(
        `SELECT role FROM tenant_members WHERE user_id = $1 AND role IN ('owner','admin') LIMIT 1`,
        [userId],
      );
      if (result.rows[0] === undefined) {
        throw new ForbiddenError('Admin access required');
      }
    }

    fastify.get<{ Params: ProjectParams; Querystring: ErrorIntelligenceQuerystring }>(
      '/tenants/:tenantId/projects/:projectId/reports/error-intelligence',
      {
        preHandler: [userPreHandler],
        schema: {
          tags: ['reports'],
          params: {
            type: 'object',
            properties: {
              tenantId: { type: 'string' },
              projectId: { type: 'string' },
            },
          },
          querystring: {
            type: 'object',
            properties: {
              days: { type: 'integer', minimum: 1, maximum: 90, default: 7 },
            },
          },
        },
      },
      async (request, reply: FastifyReply): Promise<void> => {
        const { tenantId, projectId } = request.params;
        const userId = request.user.userId;
        const days = request.query.days ?? 7;

        await assertMember(userId, tenantId);

        // ReportService.getErrorIntelligenceReport already caches with the key
        // 'report:error-intelligence:{projectId}:{days}' and handles invalidation
        // via invalidateProjectReports(). A second getOrFill here with a different
        // key pattern would create stale entries that are never evicted.
        const report = await container.reports.getErrorIntelligenceReport(projectId, days);

        void reply.status(200).send(report);
      },
    );

    fastify.get<{ Params: ProjectParams }>(
      '/tenants/:tenantId/projects/:projectId/reports/dashboard',
      {
        preHandler: [userPreHandler],
        schema: {
          tags: ['reports'],
          params: {
            type: 'object',
            properties: {
              tenantId: { type: 'string' },
              projectId: { type: 'string' },
            },
          },
        },
      },
      async (request, reply: FastifyReply): Promise<void> => {
        const { tenantId, projectId } = request.params;
        const userId = request.user.userId;

        await assertMember(userId, tenantId);

        // ReportService.getDashboardReport caches with key 'report:dashboard:{projectId}'.
        // Wrapping again with a different key would bypass invalidation.
        const report = await container.reports.getDashboardReport(projectId);

        void reply.status(200).send(report);
      },
    );

    // Admin-only: full multi-tenant quota report
    fastify.get(
      '/admin/reports/quota',
      {
        preHandler: [userPreHandler],
        schema: {
          tags: ['admin', 'reports'],
        },
      },
      async (request, reply: FastifyReply): Promise<void> => {
        const userId = request.user.userId;

        await assertAdmin(userId);

        const report = await container.tenants.getAllTenantsQuotaReport();
        void reply.status(200).send({ report });
      },
    );
  }, { name: 'report-routes', fastify: '4.x' });
}
