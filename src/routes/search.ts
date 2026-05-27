import type { FastifyInstance, FastifyPluginAsync, FastifyReply } from 'fastify';
import fp from 'fastify-plugin';
import { pool } from '../db/postgres.js';
import { esClient, aliasName } from '../db/elastic.js';
import { redis } from '../db/redis.js';
import { authenticateUser } from '../lib/auth.js';
import { ForbiddenError, ValidationError } from '../errors.js';
import { logger } from '../logger.js';
import { breakers } from '../lib/circuit-breaker.js';

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

interface ProjectSearchParams {
  tenantId: string;
  projectId: string;
}

interface SearchQuerystring {
  q?: string;
  severity?: string;
  from?: string;
  to?: string;
  cursor?: string;
  limit?: number;
}

interface MemberRow {
  user_id: string;
}

interface EsHit {
  _id: string;
  _source: Record<string, unknown>;
  highlight?: Record<string, string[]>;
  sort?: unknown[];
}

// ---------------------------------------------------------------------------
// Membership guard
// ---------------------------------------------------------------------------

async function assertMember(userId: string, tenantId: string): Promise<void> {
  const result = await pool.query<MemberRow>(
    'SELECT user_id FROM tenant_members WHERE tenant_id = $1 AND user_id = $2 LIMIT 1',
    [tenantId, userId],
  );
  if (result.rows[0] === undefined) {
    throw new ForbiddenError('Not a member of this tenant');
  }
}

// ---------------------------------------------------------------------------
// Cursor encode / decode (base64 JSON)
// ---------------------------------------------------------------------------

function encodeCursor(sortValues: unknown[]): string {
  return Buffer.from(JSON.stringify(sortValues)).toString('base64');
}

function decodeCursor(cursor: string): unknown[] {
  try {
    return JSON.parse(Buffer.from(cursor, 'base64').toString('utf8')) as unknown[];
  } catch {
    throw new ValidationError('Invalid cursor token');
  }
}

// ---------------------------------------------------------------------------
// Plugin
// ---------------------------------------------------------------------------

const searchPluginHandler: FastifyPluginAsync = async (fastify: FastifyInstance) => {
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
            severity: { type: 'string', enum: ['debug', 'info', 'warn', 'error', 'fatal'] },
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

      const { q, severity, from, to, cursor, limit = 20 } = request.query;
      const pageSize = Math.min(limit, 100);

      // -----------------------------------------------------------------------
      // Build the ES bool query
      // -----------------------------------------------------------------------

      type EsQuery = Record<string, unknown>;

      const mustClauses: EsQuery[] = [];
      const filterClauses: EsQuery[] = [];
      const shouldClauses: EsQuery[] = [];
      const mustNotClauses: EsQuery[] = [
        { term: { severity: 'debug' } },
      ];

      // Full-text search on message + stackTrace
      if (q !== undefined && q.length > 0) {
        mustClauses.push({
          multi_match: {
            query: q,
            fields: ['message', 'stackTrace'],
            type: 'best_fields',
            tie_breaker: 0.3,
          },
        });
      }

      // Filter: always scope to this project
      filterClauses.push({ term: { projectId } });

      // Filter: date range on occurredAt
      if (from !== undefined || to !== undefined) {
        const rangeFilter: Record<string, string> = {};
        if (from !== undefined) rangeFilter['gte'] = from;
        if (to !== undefined) rangeFilter['lte'] = to;
        filterClauses.push({ range: { occurredAt: rangeFilter } });
      }

      // Filter: optional severity
      if (severity !== undefined) {
        filterClauses.push({ term: { severity } });
      }

      // Should: nested boost for production environment tags
      shouldClauses.push({
        nested: {
          path: 'tags',
          query: {
            bool: {
              must: [
                { term: { 'tags.key': 'env' } },
                { term: { 'tags.value': 'production' } },
              ],
            },
          },
          boost: 1.5,
        },
      });

      const esQuery: EsQuery = {
        bool: {
          ...(mustClauses.length > 0 ? { must: mustClauses } : {}),
          filter: filterClauses,
          should: shouldClauses,
          must_not: mustNotClauses,
        },
      };

      // -----------------------------------------------------------------------
      // Search-after pagination
      // -----------------------------------------------------------------------

      const searchAfter = cursor !== undefined ? decodeCursor(cursor) : undefined;

      const alias = aliasName(projectId);
      const searchCacheKey = `search:${projectId}:${Buffer.from(JSON.stringify({ q, severity, from, to, cursor, pageSize })).toString('base64')}`;

      // X5: If ES circuit is open, try returning the last cached result (up to 1 hour stale)
      const esCircuitOpen = breakers.elasticsearch.getState() === 'open';
      if (esCircuitOpen) {
        const staleResult = await redis.get(searchCacheKey).catch(() => null);
        if (staleResult !== null) {
          logger.warn({ projectId }, 'ES down — serving stale cached search result (X5)');
          void reply.status(206).header('X-Cache', 'STALE').send(JSON.parse(staleResult));
          return;
        }
        // No cached result available
        void reply
          .status(503)
          .header('Retry-After', '60')
          .send({ error: 'Search service temporarily unavailable', retryAfter: 60 });
        return;
      }

      let response: Awaited<ReturnType<typeof esClient.search<Record<string, unknown>>>>;
      try {
        response = await breakers.elasticsearch.run(() =>
          esClient.search<Record<string, unknown>>({
            index: alias,
            size: pageSize,
            query: esQuery,
            highlight: {
              fields: {
                message: {
                  pre_tags: ['<em>'],
                  post_tags: ['</em>'],
                  number_of_fragments: 3,
                  fragment_size: 150,
                },
              },
            },
            sort: [
              { occurredAt: { order: 'desc' } },
              { _id: { order: 'asc' } },
            ],
            ...(searchAfter !== undefined ? { search_after: searchAfter } : {}),
            track_total_hits: true,
          }),
        );
      } catch (err) {
        // ES went down mid-request — check stale cache then 503
        logger.warn({ err, projectId }, 'ES search failed (X5 degradation)');
        const staleResult = await redis.get(searchCacheKey).catch(() => null);
        if (staleResult !== null) {
          void reply.status(206).header('X-Cache', 'STALE').send(JSON.parse(staleResult));
          return;
        }
        void reply
          .status(503)
          .header('Retry-After', '60')
          .send({ error: 'Search service temporarily unavailable', retryAfter: 60 });
        return;
      }

      const hitsArray = response.hits.hits as EsHit[];
      const total =
        typeof response.hits.total === 'object'
          ? response.hits.total.value
          : (response.hits.total ?? 0);

      const lastHit = hitsArray[hitsArray.length - 1];
      const nextCursor =
        lastHit?.sort !== undefined && hitsArray.length === pageSize
          ? encodeCursor(lastHit.sort as unknown[])
          : null;

      const hits = hitsArray.map((hit) => ({
        id: hit._id,
        ...hit._source,
        highlight: hit.highlight ?? {},
      }));

      const responseBody = { hits, total, nextCursor, took: response.took };

      // Cache successful search result for up to 1 hour (X5 stale serving)
      void redis
        .set(searchCacheKey, JSON.stringify(responseBody), 'EX', 3600)
        .catch(() => {});

      void reply.status(200).header('X-Cache', 'MISS').send(responseBody);
    },
  );
};

export const searchRoutes = fp(searchPluginHandler, {
  name: 'search-routes',
  fastify: '4.x',
});
