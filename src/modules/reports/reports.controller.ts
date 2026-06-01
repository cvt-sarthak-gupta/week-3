import fp from 'fastify-plugin';
import type { FastifyInstance, FastifyPluginAsync, FastifyReply } from 'fastify';
import { authenticateUser } from '../../lib/auth.js';
import { ForbiddenError } from '../../utils/errors.js';
import type { PostgresPool } from '../../db/postgres/index.js';
import type { ReportsService } from './reports.service.js';

// Route param/query interfaces

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

export function createReportRoutes(
  reportsService: ReportsService,
  pool: PostgresPool,
): FastifyPluginAsync {
  return fp(
    async (fastify: FastifyInstance) => {
      async function assertMember(userId: string, tenantId: string): Promise<MemberRow> {
        const result = await pool.query<MemberRow>(
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
        const result = await pool.query<{ role: string }>(
          `SELECT role FROM users WHERE id = $1 AND role = 'admin' LIMIT 1`,
          [userId],
        );
        if (result.rows[0] === undefined) {
          throw new ForbiddenError('Admin access required');
        }
      }

      // GET /tenants/:tenantId/projects/:projectId/reports/error-intelligence

      fastify.get<{ Params: ProjectParams; Querystring: ErrorIntelligenceQuerystring }>(
        '/tenants/:tenantId/projects/:projectId/reports/error-intelligence',
        {
          preHandler: [authenticateUser],
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

          const report = await reportsService.getErrorIntelligence(projectId, days);
          void reply.status(200).send(report);
        },
      );

      // GET /admin/reports/quota

      fastify.get(
        '/admin/reports/quota',
        {
          preHandler: [authenticateUser],
          schema: {
            tags: ['admin', 'reports'],
          },
        },
        async (request, reply: FastifyReply): Promise<void> => {
          const userId = request.user.userId;

          await assertAdmin(userId);

          const report = await reportsService.getQuotaReport();
          void reply.status(200).send({ report });
        },
      );
    },
    { name: 'report-routes', fastify: '4.x' },
  );
}
