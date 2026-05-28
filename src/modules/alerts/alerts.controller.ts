import fp from 'fastify-plugin';
import type { FastifyInstance, FastifyPluginAsync, FastifyReply } from 'fastify';
import { authenticateUser } from '../../lib/auth.js';
import { ForbiddenError, NotFoundError } from '../../utils/errors.js';
import type { AlertsService } from './alerts.service.js';
import type { CreateAlertRuleInput, UpdateAlertRuleInput } from './alerts.types.js';
import type { PostgresPool } from '../../db/postgres/index.js';

// ---------------------------------------------------------------------------
// Route param/body interfaces
// ---------------------------------------------------------------------------

interface ProjectParams {
  tenantId: string;
  projectId: string;
}

interface AlertParams {
  tenantId: string;
  projectId: string;
  alertRuleId: string;
}

interface MemberRow {
  user_id: string;
  role: string;
}

// ---------------------------------------------------------------------------
// Factory
// ---------------------------------------------------------------------------

export function createAlertRoutes(
  alertsService: AlertsService,
  pool: PostgresPool,
): FastifyPluginAsync {
  return fp(
    async (fastify: FastifyInstance) => {
      // -----------------------------------------------------------------------
      // Membership guard (inline — no Redis cache needed for alert CRUD)
      // -----------------------------------------------------------------------

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

      async function assertProjectBelongsToTenant(
        projectId: string,
        tenantId: string,
      ): Promise<void> {
        const result = await pool.query<{ id: string }>(
          'SELECT id FROM projects WHERE id = $1 AND tenant_id = $2 AND is_archived = false LIMIT 1',
          [projectId, tenantId],
        );
        if (result.rows[0] === undefined) {
          throw new NotFoundError('Project not found');
        }
      }

      // -----------------------------------------------------------------------
      // GET /tenants/:tenantId/projects/:projectId/alert-rules
      // -----------------------------------------------------------------------

      fastify.get<{ Params: ProjectParams }>(
        '/tenants/:tenantId/projects/:projectId/alert-rules',
        {
          preHandler: [authenticateUser],
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

          const rules = await alertsService.listRules(projectId, tenantId);
          void reply.status(200).send({ alertRules: rules });
        },
      );

      // -----------------------------------------------------------------------
      // POST /tenants/:tenantId/projects/:projectId/alert-rules
      // -----------------------------------------------------------------------

      fastify.post<{ Params: ProjectParams; Body: CreateAlertRuleInput }>(
        '/tenants/:tenantId/projects/:projectId/alert-rules',
        {
          preHandler: [authenticateUser],
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
              required: ['name', 'conditionType', 'windowSeconds', 'notificationChannel', 'esQuery'],
              properties: {
                name: { type: 'string', minLength: 1 },
                conditionType: { type: 'string' },
                threshold: { type: 'number' },
                windowSeconds: { type: 'integer', minimum: 1 },
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

          const alertRuleId = await alertsService.createRule(
            projectId,
            tenantId,
            request.body,
          );

          void reply.status(201).send({ alertRuleId });
        },
      );

      // -----------------------------------------------------------------------
      // PUT /tenants/:tenantId/projects/:projectId/alert-rules/:alertRuleId
      // -----------------------------------------------------------------------

      fastify.put<{ Params: AlertParams; Body: UpdateAlertRuleInput }>(
        '/tenants/:tenantId/projects/:projectId/alert-rules/:alertRuleId',
        {
          preHandler: [authenticateUser],
          schema: {
            tags: ['alerts'],
            params: {
              type: 'object',
              properties: {
                tenantId: { type: 'string' },
                projectId: { type: 'string' },
                alertRuleId: { type: 'string' },
              },
            },
            body: {
              type: 'object',
              properties: {
                name: { type: 'string', minLength: 1 },
                conditionType: { type: 'string' },
                threshold: { type: 'number' },
                windowSeconds: { type: 'integer', minimum: 1 },
                notificationChannel: { type: 'string', format: 'uri' },
                isEnabled: { type: 'boolean' },
                esQuery: { type: 'object' },
              },
            },
          },
        },
        async (request, reply: FastifyReply): Promise<void> => {
          const { tenantId, projectId, alertRuleId } = request.params;
          const userId = request.user.userId;

          await assertMember(userId, tenantId);
          await assertProjectBelongsToTenant(projectId, tenantId);

          // Verify rule exists
          const existing = await pool.query<{ id: string }>(
            'SELECT id FROM alert_rules WHERE id = $1 AND project_id = $2 LIMIT 1',
            [alertRuleId, projectId],
          );
          if (existing.rows[0] === undefined) {
            throw new NotFoundError('Alert rule not found');
          }

          await alertsService.updateRule(alertRuleId, projectId, tenantId, request.body);
          void reply.status(200).send({ updated: true });
        },
      );

      // -----------------------------------------------------------------------
      // DELETE /tenants/:tenantId/projects/:projectId/alert-rules/:alertRuleId
      // -----------------------------------------------------------------------

      fastify.delete<{ Params: AlertParams }>(
        '/tenants/:tenantId/projects/:projectId/alert-rules/:alertRuleId',
        {
          preHandler: [authenticateUser],
          schema: {
            tags: ['alerts'],
            params: {
              type: 'object',
              properties: {
                tenantId: { type: 'string' },
                projectId: { type: 'string' },
                alertRuleId: { type: 'string' },
              },
            },
          },
        },
        async (request, reply: FastifyReply): Promise<void> => {
          const { tenantId, projectId, alertRuleId } = request.params;
          const userId = request.user.userId;

          await assertMember(userId, tenantId);
          await assertProjectBelongsToTenant(projectId, tenantId);

          // Verify rule exists
          const existing = await pool.query<{ id: string }>(
            'SELECT id FROM alert_rules WHERE id = $1 AND project_id = $2 LIMIT 1',
            [alertRuleId, projectId],
          );
          if (existing.rows[0] === undefined) {
            throw new NotFoundError('Alert rule not found');
          }

          await alertsService.deleteRule(alertRuleId, projectId, tenantId);
          void reply.status(204).send();
        },
      );
    },
    { name: 'alert-routes', fastify: '4.x' },
  );
}
