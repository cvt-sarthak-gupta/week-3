import fp from 'fastify-plugin';
import type { FastifyInstance, FastifyPluginAsync, FastifyReply } from 'fastify';
import { authenticateUser } from '../../lib/auth.js';
import { ForbiddenError } from '../../utils/errors.js';
import type { PostgresPool } from '../../db/postgres/index.js';
import type { SearchService } from './search.service.js';
import type { SearchQuerystring } from './search.types.js';

// ---------------------------------------------------------------------------
// Route param interfaces
// ---------------------------------------------------------------------------

interface ProjectSearchParams {
  tenantId: string;
  projectId: string;
}

interface MemberRow {
  user_id: string;
}

const MEMBER_CACHE_TTL_SECONDS = 300;

// ---------------------------------------------------------------------------
// Factory
// ---------------------------------------------------------------------------

export function createSearchRoutes(
  searchService: SearchService,
  pool: PostgresPool,
): FastifyPluginAsync {
  return fp(
    async (fastify: FastifyInstance) => {
      // -----------------------------------------------------------------------
      // Membership guard with inline Redis cache (via searchService's redis)
      // We accept pool here and do a plain PG check (no Redis dependency at
      // controller level — service owns its redis).
      // -----------------------------------------------------------------------

      async function assertMember(userId: string, tenantId: string): Promise<void> {
        const result = await pool.query<MemberRow>(
          'SELECT user_id FROM tenant_members WHERE tenant_id = $1 AND user_id = $2 LIMIT 1',
          [tenantId, userId],
        );
        if (result.rows[0] === undefined) {
          throw new ForbiddenError('Not a member of this tenant');
        }
      }

      // -----------------------------------------------------------------------
      // GET /tenants/:tenantId/projects/:projectId/logs/search
      // -----------------------------------------------------------------------

      fastify.get<{ Params: ProjectSearchParams; Querystring: SearchQuerystring }>(
        '/tenants/:tenantId/projects/:projectId/logs/search',
        {
          preHandler: [authenticateUser],
          schema: {
            tags: ['search'],
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
                q: { type: 'string' },
                severity: {
                  type: 'string',
                  enum: ['debug', 'info', 'warn', 'error', 'fatal'],
                },
                from: { type: 'string' },
                to: { type: 'string' },
                cursor: { type: 'string' },
                limit: { type: 'integer', minimum: 1, maximum: 100, default: 20 },
              },
            },
          },
        },
        async (request, reply: FastifyReply): Promise<void> => {
          const { tenantId, projectId } = request.params;
          const userId = request.user.userId;

          await assertMember(userId, tenantId);

          const { result, status, headers } = await searchService.search(
            projectId,
            request.query,
          );

          for (const [key, value] of Object.entries(headers)) {
            void reply.header(key, value);
          }

          if (status === 503) {
            void reply.status(503).send({
              error: 'Search service temporarily unavailable',
              retryAfter: 60,
            });
            return;
          }

          void reply.status(status).send(result);
        },
      );
    },
    { name: 'search-routes', fastify: '4.x' },
  );
}
