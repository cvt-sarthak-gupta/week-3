import fp from 'fastify-plugin';
import type { FastifyPluginAsync, FastifyReply } from 'fastify';
import { authenticateUser } from '../../lib/auth.js';
import type { ProjectService } from './projects.service.js';

interface TenantParams {
  tenantId: string;
}

interface ProjectParams {
  tenantId: string;
  projectId: string;
}

interface CreateProjectBody {
  name: string;
  slug: string;
}

export function createProjectRoutes(projectService: ProjectService): FastifyPluginAsync {
  return fp(async (fastify) => {
    fastify.get<{ Params: TenantParams }>(
      '/tenants/:tenantId/projects',
      {
        preHandler: [authenticateUser],
        schema: {
          tags: ['projects'],
          params: {
            type: 'object',
            properties: { tenantId: { type: 'string' } },
          },
        },
      },
      async (request, reply: FastifyReply): Promise<void> => {
        const { tenantId } = request.params;
        const userId = request.user.userId;

        const projects = await projectService.list(tenantId, userId);
        void reply.status(200).send({ projects });
      },
    );

    fastify.post<{ Params: TenantParams; Body: CreateProjectBody }>(
      '/tenants/:tenantId/projects',
      {
        preHandler: [authenticateUser],
        schema: {
          tags: ['projects'],
          params: {
            type: 'object',
            properties: { tenantId: { type: 'string' } },
          },
          body: {
            type: 'object',
            required: ['name', 'slug'],
            properties: {
              name: { type: 'string', minLength: 1 },
              slug: { type: 'string', minLength: 1, pattern: '^[a-z0-9-]+$' },
            },
          },
        },
      },
      async (request, reply: FastifyReply): Promise<void> => {
        const { tenantId } = request.params;
        const userId = request.user.userId;
        const { name, slug } = request.body;

        const result = await projectService.create(tenantId, userId, { name, slug });
        void reply.status(201).send(result);
      },
    );

    fastify.get<{ Params: ProjectParams }>(
      '/tenants/:tenantId/projects/:projectId',
      {
        preHandler: [authenticateUser],
        schema: {
          tags: ['projects'],
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

        const project = await projectService.getById(tenantId, projectId, userId);
        void reply.status(200).send(project);
      },
    );

    fastify.delete<{ Params: ProjectParams }>(
      '/tenants/:tenantId/projects/:projectId',
      {
        preHandler: [authenticateUser],
        schema: {
          tags: ['projects'],
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

        await projectService.archive(tenantId, projectId, userId);
        void reply.status(204).send();
      },
    );

    fastify.post<{ Params: ProjectParams }>(
      '/tenants/:tenantId/projects/:projectId/roll-key',
      {
        preHandler: [authenticateUser],
        schema: {
          tags: ['projects'],
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

        const result = await projectService.rollKey(tenantId, projectId, userId);
        void reply.status(200).send(result);
      },
    );
  }, { name: 'project-routes', fastify: '4.x' });
}
