import type { FastifyPluginAsync, FastifyReply } from 'fastify';
import fp from 'fastify-plugin';
import { AppContainer } from '../container.js';
import { ForbiddenError, ValidationError } from '../errors.js';

interface DashboardParams {
  tenantId: string;
  projectId: string;
}

interface CreateDashboardBody {
  name: string;
  layout?: unknown[];
}

export function dashboardRoutes(container: AppContainer): FastifyPluginAsync {
  return fp(async (fastify) => {
    const userPreHandler = container.auth.userPreHandler();

    async function assertMember(userId: string, tenantId: string): Promise<void> {
      const cacheKey = `member:${tenantId}:${userId}`;
      const cached = await container.redis.client.get(cacheKey).catch(() => null);
      if (cached !== null) return;

      const result = await container.pg.query<{ user_id: string }>(
        'SELECT user_id FROM tenant_members WHERE tenant_id = $1 AND user_id = $2 LIMIT 1',
        [tenantId, userId],
      );
      if (result.rows[0] === undefined) {
        throw new ForbiddenError('Not a member of this tenant');
      }

      void container.redis.client
        .set(cacheKey, '1', 'EX', 300)
        .catch(() => {});
    }

    async function assertProjectBelongsToTenant(projectId: string, tenantId: string): Promise<void> {
      const result = await container.pg.query<{ id: string }>(
        'SELECT id FROM projects WHERE id = $1 AND tenant_id = $2 LIMIT 1',
        [projectId, tenantId],
      );
      if (result.rows[0] === undefined) {
        throw new ForbiddenError('Project not found in this tenant');
      }
    }

    // POST /tenants/:tenantId/projects/:projectId/dashboards
    fastify.post<{ Params: DashboardParams; Body: CreateDashboardBody }>(
      '/tenants/:tenantId/projects/:projectId/dashboards',
      {
        preHandler: [userPreHandler],
        schema: {
          tags: ['dashboards'],
          params: {
            type: 'object',
            required: ['tenantId', 'projectId'],
            properties: {
              tenantId: { type: 'string' },
              projectId: { type: 'string' },
            },
          },
          body: {
            type: 'object',
            required: ['name'],
            properties: {
              name: { type: 'string', minLength: 1, maxLength: 200 },
              layout: { type: 'array' },
            },
          },
        },
      },
      async (request, reply: FastifyReply): Promise<void> => {
        const { tenantId, projectId } = request.params;
        const userId = request.user.userId;

        await assertMember(userId, tenantId);
        await assertProjectBelongsToTenant(projectId, tenantId);

        if (!request.body.name) {
          throw new ValidationError('name is required');
        }

        const result = await container.dashboards.createDashboard({
          projectId,
          tenantId,
          userId,
          name: request.body.name,
          layout: request.body.layout ?? [],
        });

        void reply.status(201).send(result);
      },
    );

    // GET /tenants/:tenantId/projects/:projectId/dashboards
    fastify.get<{ Params: DashboardParams }>(
      '/tenants/:tenantId/projects/:projectId/dashboards',
      {
        preHandler: [userPreHandler],
        schema: {
          tags: ['dashboards'],
          params: {
            type: 'object',
            required: ['tenantId', 'projectId'],
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
        await assertProjectBelongsToTenant(projectId, tenantId);

        const dashboards = await container.dashboards.listDashboards(projectId);
        void reply.status(200).send({ dashboards });
      },
    );
  }, { name: 'dashboard-routes', fastify: '4.x' });
}
