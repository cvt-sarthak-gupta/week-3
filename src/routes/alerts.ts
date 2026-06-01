import type { FastifyPluginAsync, FastifyReply } from 'fastify';
import fp from 'fastify-plugin';
import { AppContainer } from '../container.js';
import { percolatorIndex } from '../db/elastic.js';
import { ForbiddenError, NotFoundError } from '../errors.js';

interface AlertRuleRow {
  id: string;
  project_id: string;
  name: string;
  condition: Record<string, unknown>;
  severity: string;
  is_active: boolean;
  created_at: Date;
  updated_at: Date;
}

interface ProjectParams {
  tenantId: string;
  projectId: string;
}

interface AlertParams {
  tenantId: string;
  projectId: string;
  alertId: string;
}

interface AlertRuleBody {
  name: string;
  condition: Record<string, unknown>;
  severity: string;
  isActive?: boolean;
}

interface MemberRow {
  user_id: string;
  role: string;
}

export function alertRoutes(container: AppContainer): FastifyPluginAsync {
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

    async function assertProjectBelongsToTenant(projectId: string, tenantId: string): Promise<void> {
      const result = await container.pg.query<{ id: string }>(
        'SELECT id FROM projects WHERE id = $1 AND tenant_id = $2 AND is_archived = false LIMIT 1',
        [projectId, tenantId],
      );
      if (result.rows[0] === undefined) {
        throw new NotFoundError('Project not found');
      }
    }

    fastify.get<{ Params: ProjectParams }>(
      '/tenants/:tenantId/projects/:projectId/alerts',
      {
        preHandler: [userPreHandler],
        schema: {
          tags: ['alerts'],
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
        await assertProjectBelongsToTenant(projectId, tenantId);

        const result = await container.pg.query<AlertRuleRow>(
          `SELECT id, project_id, name, condition, severity, is_active, created_at, updated_at
           FROM alert_rules
           WHERE project_id = $1
           ORDER BY created_at DESC`,
          [projectId],
        );

        void reply.status(200).send({ alerts: result.rows });
      },
    );

    fastify.post<{ Params: ProjectParams; Body: AlertRuleBody }>(
      '/tenants/:tenantId/projects/:projectId/alerts',
      {
        preHandler: [userPreHandler],
        schema: {
          tags: ['alerts'],
          params: {
            type: 'object',
            properties: {
              tenantId: { type: 'string' },
              projectId: { type: 'string' },
            },
          },
          body: {
            type: 'object',
            required: ['name', 'condition', 'severity'],
            properties: {
              name: { type: 'string', minLength: 1 },
              condition: { type: 'object' },
              severity: { type: 'string', enum: ['debug', 'info', 'warn', 'error', 'fatal'] },
              isActive: { type: 'boolean' },
            },
          },
        },
      },
      async (request, reply: FastifyReply): Promise<void> => {
        const { tenantId, projectId } = request.params;
        const userId = request.user.userId;
        const { name, condition, severity, isActive = true } = request.body;

        await assertMember(userId, tenantId);
        await assertProjectBelongsToTenant(projectId, tenantId);

        const result = await container.pg.withTenant(tenantId, async (client) => {
          return client.query<{ id: string }>(
            `INSERT INTO alert_rules (project_id, name, condition, severity, is_active)
             VALUES ($1, $2, $3, $4, $5)
             RETURNING id`,
            [projectId, name, JSON.stringify(condition), severity, isActive],
          );
        });

        const alertRuleId = result.rows[0]?.id;
        if (alertRuleId === undefined) {
          throw new Error('Failed to create alert rule');
        }

        await container.es.client.index({
          index: percolatorIndex,
          id: alertRuleId,
          document: {
            query: condition,
            projectId,
            severity,
            name,
          },
          refresh: 'wait_for',
        });

        void reply.status(201).send({ alertRuleId });
      },
    );

    fastify.put<{ Params: AlertParams; Body: Partial<AlertRuleBody> }>(
      '/tenants/:tenantId/projects/:projectId/alerts/:alertId',
      {
        preHandler: [userPreHandler],
        schema: {
          tags: ['alerts'],
          params: {
            type: 'object',
            properties: {
              tenantId: { type: 'string' },
              projectId: { type: 'string' },
              alertId: { type: 'string' },
            },
          },
          body: {
            type: 'object',
            properties: {
              name: { type: 'string', minLength: 1 },
              condition: { type: 'object' },
              severity: { type: 'string', enum: ['debug', 'info', 'warn', 'error', 'fatal'] },
              isActive: { type: 'boolean' },
            },
          },
        },
      },
      async (request, reply: FastifyReply): Promise<void> => {
        const { tenantId, projectId, alertId } = request.params;
        const userId = request.user.userId;
        const { name, condition, severity, isActive } = request.body;

        await assertMember(userId, tenantId);
        await assertProjectBelongsToTenant(projectId, tenantId);

        const existing = await container.pg.query<AlertRuleRow>(
          'SELECT id FROM alert_rules WHERE id = $1 AND project_id = $2 LIMIT 1',
          [alertId, projectId],
        );
        if (existing.rows[0] === undefined) {
          throw new NotFoundError('Alert rule not found');
        }

        await container.pg.withTenant(tenantId, async (client) => {
          await client.query(
            `UPDATE alert_rules
             SET name      = COALESCE($1, name),
                 condition = COALESCE($2, condition),
                 severity  = COALESCE($3, severity),
                 is_active = COALESCE($4, is_active),
                 updated_at = NOW()
             WHERE id = $5`,
            [
              name ?? null,
              condition !== undefined ? JSON.stringify(condition) : null,
              severity ?? null,
              isActive ?? null,
              alertId,
            ],
          );
        });

        if (condition !== undefined || severity !== undefined || name !== undefined) {
          await container.es.client.update({
            index: percolatorIndex,
            id: alertId,
            doc: {
              ...(condition !== undefined ? { query: condition } : {}),
              ...(severity !== undefined ? { severity } : {}),
              ...(name !== undefined ? { name } : {}),
            },
          });
        }

        void reply.status(200).send({ updated: true });
      },
    );

    fastify.delete<{ Params: AlertParams }>(
      '/tenants/:tenantId/projects/:projectId/alerts/:alertId',
      {
        preHandler: [userPreHandler],
        schema: {
          tags: ['alerts'],
          params: {
            type: 'object',
            properties: {
              tenantId: { type: 'string' },
              projectId: { type: 'string' },
              alertId: { type: 'string' },
            },
          },
        },
      },
      async (request, reply: FastifyReply): Promise<void> => {
        const { tenantId, projectId, alertId } = request.params;
        const userId = request.user.userId;

        await assertMember(userId, tenantId);
        await assertProjectBelongsToTenant(projectId, tenantId);

        const existing = await container.pg.query<{ id: string }>(
          'SELECT id FROM alert_rules WHERE id = $1 AND project_id = $2 LIMIT 1',
          [alertId, projectId],
        );
        if (existing.rows[0] === undefined) {
          throw new NotFoundError('Alert rule not found');
        }

        await container.pg.withTenant(tenantId, async (client) => {
          await client.query('DELETE FROM alert_rules WHERE id = $1', [alertId]);
        });

        await container.es.client.delete({ index: percolatorIndex, id: alertId }).catch(() => {
          // Ignore: document may have already been removed
        });

        void reply.status(204).send();
      },
    );
  }, { name: 'alert-routes', fastify: '4.x' });
}
