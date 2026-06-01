import fp from 'fastify-plugin';
import type { FastifyPluginAsync, FastifyReply } from 'fastify';
import { authenticateUser } from '../../lib/auth.js';
import type { TenantService } from './tenants.service.js';
import type { PatchTenantInput } from './tenants.types.js';

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

export function createTenantRoutes(tenantService: TenantService): FastifyPluginAsync {
  return fp(async (fastify) => {
    fastify.post<{ Body: CreateTenantBody }>(
      '/tenants',
      {
        preHandler: [authenticateUser],
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

        const result = await tenantService.onboard({
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
        preHandler: [authenticateUser],
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

        const detail = await tenantService.getById(tenantId, userId);
        void reply.status(200).send(detail);
      },
    );

    fastify.patch<{ Params: TenantParams; Body: PatchTenantBody }>(
      '/tenants/:tenantId',
      {
        preHandler: [authenticateUser],
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

        const data: PatchTenantInput = {
          ...(request.body.name !== undefined ? { name: request.body.name } : {}),
          ...(request.body.isActive !== undefined ? { isActive: request.body.isActive } : {}),
        };

        await tenantService.update(tenantId, userId, data);
        void reply.status(200).send({ updated: true });
      },
    );

    fastify.get<{ Params: TenantParams }>(
      '/tenants/:tenantId/quota',
      {
        preHandler: [authenticateUser],
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

        const quota = await tenantService.getQuota(tenantId, userId);
        void reply.status(200).send(quota);
      },
    );
  }, { name: 'tenant-routes', fastify: '4.x' });
}
