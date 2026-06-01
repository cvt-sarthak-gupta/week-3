import type { FastifyInstance, FastifyPluginAsync, FastifyReply, FastifyRequest } from 'fastify';
import fp from 'fastify-plugin';
import { pool, withTenant } from '../db/postgres.js';
import { authenticateUser } from '../lib/auth.js';
import { applyPolicyForProject } from '../db/elastic.js';
import { redis } from '../db/redis.js';
import { ForbiddenError, NotFoundError } from '../errors.js';
import { logger } from '../logger.js';

interface ProjectRow {
  id: string;
  tenant_id: string;
  name: string;
  slug: string;
  api_key: string;
  is_archived: boolean;
  created_at: Date;
  updated_at: Date;
}

interface MemberRow {
  user_id: string;
  role: string;
}

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

const projectPluginHandler: FastifyPluginAsync = async (fastify: FastifyInstance) => {
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

      await assertMember(userId, tenantId);

      const result = await pool.query<ProjectRow>(
        `SELECT id, tenant_id, name, slug, api_key, is_archived, created_at, updated_at
         FROM projects
         WHERE tenant_id = $1 AND is_archived = false
         ORDER BY created_at DESC`,
        [tenantId],
      );

      void reply.status(200).send({ projects: result.rows });
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

      const member = await assertMember(userId, tenantId);
      if (member.role !== 'owner' && member.role !== 'admin') {
        throw new ForbiddenError('Only owners or admins can create projects');
      }

      const result = await withTenant(tenantId, async (client) => {
        return client.query<{ id: string; api_key: string }>(
          `INSERT INTO projects (tenant_id, name, slug, api_key)
           VALUES ($1, $2, $3, gen_random_uuid())
           RETURNING id, api_key`,
          [tenantId, name, slug],
        );
      });

      const project = result.rows[0];
      if (project === undefined) {
        throw new Error('Failed to create project');
      }

      await applyPolicyForProject(project.id, 30).catch((err) => {
        logger.warn({ err, projectId: project.id }, 'Failed to create ES index for new project');
      });

      void reply.status(201).send({ id: project.id, apiKey: project.api_key });
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

      await assertMember(userId, tenantId);

      const result = await pool.query<ProjectRow>(
        `SELECT id, tenant_id, name, slug, api_key, is_archived, created_at, updated_at
         FROM projects
         WHERE id = $1 AND tenant_id = $2
         LIMIT 1`,
        [projectId, tenantId],
      );

      const project = result.rows[0];
      if (project === undefined) {
        throw new NotFoundError('Project not found');
      }

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

      const member = await assertMember(userId, tenantId);
      if (member.role !== 'owner' && member.role !== 'admin') {
        throw new ForbiddenError('Only owners or admins can delete projects');
      }

      const check = await pool.query<{ id: string }>(
        'SELECT id FROM projects WHERE id = $1 AND tenant_id = $2 LIMIT 1',
        [projectId, tenantId],
      );
      if (check.rows[0] === undefined) {
        throw new NotFoundError('Project not found');
      }

      await withTenant(tenantId, async (client) => {
        await client.query(
          'UPDATE projects SET is_archived = true, updated_at = NOW() WHERE id = $1',
          [projectId],
        );
      });

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

      const member = await assertMember(userId, tenantId);
      if (member.role !== 'owner' && member.role !== 'admin') {
        throw new ForbiddenError('Only owners or admins can roll the API key');
      }

      const oldResult = await pool.query<{ api_key: string }>(
        'SELECT api_key FROM projects WHERE id = $1 AND tenant_id = $2 LIMIT 1',
        [projectId, tenantId],
      );
      const oldProject = oldResult.rows[0];
      if (oldProject === undefined) {
        throw new NotFoundError('Project not found');
      }

      const oldKey = oldProject.api_key;

      const newResult = await withTenant(tenantId, async (client) => {
        return client.query<{ api_key: string }>(
          `UPDATE projects
           SET api_key = gen_random_uuid(), updated_at = NOW()
           WHERE id = $1
           RETURNING api_key`,
          [projectId],
        );
      });

      const newKey = newResult.rows[0]?.api_key;
      if (newKey === undefined) {
        throw new Error('Failed to roll API key');
      }

      // Invalidate old key in Redis cache
      await redis.del(`apikey:${oldKey}`).catch((err) => {
        logger.warn({ err, projectId }, 'Failed to invalidate old API key in Redis');
      });

      void reply.status(200).send({ apiKey: newKey });
    },
  );
};

export const projectRoutes = fp(projectPluginHandler, {
  name: 'project-routes',
  fastify: '4.x',
});
