import type { FastifyPluginAsync, FastifyReply } from 'fastify';
import fp from 'fastify-plugin';
import { AppContainer } from '../container.js';
import { ForbiddenError, NotFoundError } from '../errors.js';

interface TenantRow {
  id: string;
  name: string;
  slug: string;
  plan_id: string | null;
  is_active: boolean;
  created_at: Date;
}

interface MemberRow {
  user_id: string;
  role: string;
}

// Request body / param types

interface CreateTenantBody {
  tenantName: string;
  tenantSlug: string;
  planId?: string;
  projectName: string;
  projectSlug?: string;
}

interface TenantParams {
  tenantId: string;
}

interface PatchTenantBody {
  name?: string;
  isActive?: boolean;
}

export function tenantRoutes(container: AppContainer): FastifyPluginAsync {
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

    fastify.post<{ Body: CreateTenantBody }>(
      '/tenants',
      {
        preHandler: [userPreHandler],
        schema: {
          tags: ['tenants'],
          body: {
            type: 'object',
            required: ['tenantName', 'tenantSlug', 'projectName'],
            properties: {
              tenantName: { type: 'string', minLength: 1 },
              tenantSlug: { type: 'string', minLength: 1, pattern: '^[a-z0-9-]+$' },
              planId: { type: 'string' },
              projectName: { type: 'string', minLength: 1 },
              projectSlug: { type: 'string', minLength: 1, pattern: '^[a-z0-9-]+$' },
            },
          },
        },
      },
      async (request, reply: FastifyReply): Promise<void> => {
        const { tenantName, tenantSlug, planId, projectName, projectSlug } = request.body;
        const userId = request.user.userId;

        const result = await container.tenants.onboardTenant({
          tenantName,
          tenantSlug,
          planId: planId ?? '',
          projectName,
          projectSlug: projectSlug ?? tenantSlug,
          userId,
        });

        void reply.status(201).send(result);
      },
    );

    fastify.get<{ Params: TenantParams }>(
      '/tenants/:tenantId',
      {
        preHandler: [userPreHandler],
        schema: {
          tags: ['tenants'],
          params: {
            type: 'object',
            properties: { tenantId: { type: 'string' } },
          },
        },
      },
      async (request, reply: FastifyReply): Promise<void> => {
        const { tenantId } = request.params;
        const userId = request.user.userId;

        await assertMember(userId, tenantId);

        const [tenantResult, countResult] = await Promise.all([
          container.pg.query<TenantRow & { plan_name: string | null }>(
            `SELECT t.id, t.name, t.slug, t.plan_id, t.is_active, t.created_at,
                    p.name AS plan_name
             FROM tenants t
             LEFT JOIN plans p ON p.id = t.plan_id
             WHERE t.id = $1
             LIMIT 1`,
            [tenantId],
          ),
          container.pg.query<{ count: string }>(
            'SELECT COUNT(*)::text AS count FROM tenant_members WHERE tenant_id = $1',
            [tenantId],
          ),
        ]);

        const tenant = tenantResult.rows[0];
        if (tenant === undefined) {
          throw new NotFoundError('Tenant not found');
        }

        const memberCount = parseInt(countResult.rows[0]?.count ?? '0', 10);

        void reply.status(200).send({
          id: tenant.id,
          name: tenant.name,
          slug: tenant.slug,
          planId: tenant.plan_id,
          planName: tenant.plan_name,
          isActive: tenant.is_active,
          createdAt: tenant.created_at,
          memberCount,
        });
      },
    );

    fastify.patch<{ Params: TenantParams; Body: PatchTenantBody }>(
      '/tenants/:tenantId',
      {
        preHandler: [userPreHandler],
        schema: {
          tags: ['tenants'],
          params: {
            type: 'object',
            properties: { tenantId: { type: 'string' } },
          },
          body: {
            type: 'object',
            properties: {
              name: { type: 'string', minLength: 1 },
              isActive: { type: 'boolean' },
            },
          },
        },
      },
      async (request, reply: FastifyReply): Promise<void> => {
        const { tenantId } = request.params;
        const userId = request.user.userId;

        const member = await assertMember(userId, tenantId);
        if (member.role !== 'owner' && member.role !== 'admin') {
          throw new ForbiddenError('Only owners or admins can update tenant settings');
        }

        const { name, isActive } = request.body;

        await container.pg.withTenant(tenantId, async (client) => {
          await client.query(
            `UPDATE tenants
             SET name      = COALESCE($1, name),
                 is_active = COALESCE($2, is_active),
                 updated_at = NOW()
             WHERE id = $3`,
            [name ?? null, isActive ?? null, tenantId],
          );
        });

        void reply.status(200).send({ updated: true });
      },
    );

    fastify.get<{ Params: TenantParams }>(
      '/tenants/:tenantId/quota',
      {
        preHandler: [userPreHandler],
        schema: {
          tags: ['tenants'],
          params: {
            type: 'object',
            properties: { tenantId: { type: 'string' } },
          },
        },
      },
      async (request, reply: FastifyReply): Promise<void> => {
        const { tenantId } = request.params;
        const userId = request.user.userId;

        await assertMember(userId, tenantId);

        const quota = await container.tenants.getTenantQuotaReport(tenantId);
        void reply.status(200).send(quota);
      },
    );
  }, { name: 'tenant-routes', fastify: '4.x' });
}
