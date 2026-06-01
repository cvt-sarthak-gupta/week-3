import type { FastifyPluginAsync, FastifyReply } from 'fastify';
import fp from 'fastify-plugin';
import { AppContainer } from '../container.js';
import { ForbiddenError, NotFoundError } from '../errors.js';

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
  conditionType: 'threshold' | 'anomaly' | 'keyword';
  threshold?: number;
  windowSeconds?: number;
  notificationChannel: string;
  isEnabled?: boolean;
  esQuery?: Record<string, unknown>;
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

        const alerts = await container.alerts.listAlertRules(projectId, tenantId);

        void reply.status(200).send({ alerts });
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
            required: ['name', 'conditionType', 'notificationChannel'],
            properties: {
              name: { type: 'string', minLength: 1 },
              conditionType: { type: 'string', enum: ['threshold', 'anomaly', 'keyword'] },
              threshold: { type: 'number' },
              windowSeconds: { type: 'integer', minimum: 60, maximum: 86400 },
              notificationChannel: { type: 'string', format: 'uri' },
              isEnabled: { type: 'boolean' },
              esQuery: { type: 'object' },
            },
          },
        },
      },
      async (request, reply: FastifyReply): Promise<void> => {
        const { tenantId, projectId } = request.params;
        const userId = request.user.userId;

        await assertMember(userId, tenantId);
        await assertProjectBelongsToTenant(projectId, tenantId);

        const created = await container.alerts.createAlertRule({
          projectId,
          tenantId,
          rule: {
            name: request.body.name,
            conditionType: request.body.conditionType,
            threshold: request.body.threshold,
            windowSeconds: request.body.windowSeconds ?? 300,
            notificationChannel: request.body.notificationChannel,
            isEnabled: request.body.isEnabled ?? true,
            esQuery: request.body.esQuery ?? {},
          },
        });

        void reply.status(201).send({ alertRuleId: created.id });
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
              conditionType: { type: 'string', enum: ['threshold', 'anomaly', 'keyword'] },
              threshold: { type: 'number' },
              windowSeconds: { type: 'integer', minimum: 60, maximum: 86400 },
              notificationChannel: { type: 'string', format: 'uri' },
              isEnabled: { type: 'boolean' },
              esQuery: { type: 'object' },
            },
          },
        },
      },
      async (request, reply: FastifyReply): Promise<void> => {
        const { tenantId, projectId, alertId } = request.params;
        const userId = request.user.userId;

        await assertMember(userId, tenantId);
        await assertProjectBelongsToTenant(projectId, tenantId);

        const { name, conditionType, threshold, windowSeconds, notificationChannel, isEnabled, esQuery } = request.body;
        const updates: Parameters<typeof container.alerts.updateAlertRule>[2]['updates'] = {};
        if (name !== undefined) updates.name = name;
        if (conditionType !== undefined) updates.conditionType = conditionType;
        if (threshold !== undefined) updates.threshold = threshold;
        if (windowSeconds !== undefined) updates.windowSeconds = windowSeconds;
        if (notificationChannel !== undefined) updates.notificationChannel = notificationChannel;
        if (isEnabled !== undefined) updates.isEnabled = isEnabled;
        if (esQuery !== undefined) updates.esQuery = esQuery;

        const updated = await container.alerts.updateAlertRule(alertId, tenantId, {
          projectId,
          updates,
        });

        void reply.status(200).send({ updated });
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

        // Verify ownership before delegating (listAlertRules scopes by tenantId via RLS)
        const rules = await container.alerts.listAlertRules(projectId, tenantId);
        if (!rules.some((r) => r.id === alertId)) {
          throw new NotFoundError('Alert rule not found');
        }

        await container.alerts.deleteAlertRule(alertId, tenantId);

        void reply.status(204).send();
      },
    );
  }, { name: 'alert-routes', fastify: '4.x' });
}
