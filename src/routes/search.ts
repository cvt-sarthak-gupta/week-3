import type { FastifyPluginAsync, FastifyReply } from 'fastify';
import fp from 'fastify-plugin';
import { AppContainer } from '../container.js';
import { ForbiddenError, ValidationError } from '../errors.js';
import { logger } from '../logger.js';

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

const MEMBER_CACHE_TTL_SECONDS = 300;

// Cursor encode / decode (base64 JSON)

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

export function searchRoutes(container: AppContainer): FastifyPluginAsync {
  return fp(async (fastify) => {
    const userPreHandler = container.auth.userPreHandler();

    async function assertMember(userId: string, tenantId: string): Promise<void> {
      const cacheKey = `member:${tenantId}:${userId}`;
      const cached = await container.redis.client.get(cacheKey).catch(() => null);
      if (cached !== null) return;

      const result = await container.pg.query<MemberRow>(
        'SELECT user_id FROM tenant_members WHERE tenant_id = $1 AND user_id = $2 LIMIT 1',
        [tenantId, userId],
      );
      if (result.rows[0] === undefined) {
        throw new ForbiddenError('Not a member of this tenant');
      }

      void container.redis.client.set(cacheKey, '1', 'EX', MEMBER_CACHE_TTL_SECONDS).catch(() => {});
    }

    fastify.get<{ Params: ProjectSearchParams; Querystring: SearchQuerystring }>(
      '/tenants/:tenantId/projects/:projectId/logs/search',
      {
        preHandler: [userPreHandler],
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

        const searchAfter = cursor !== undefined ? decodeCursor(cursor) : undefined;

        const alias = container.es.aliasName(projectId);
        const searchCacheKey = `search:${projectId}:${Buffer.from(JSON.stringify({ q, severity, from, to, cursor, pageSize })).toString('base64')}`;

        // Check cache first — serves fresh hits on the normal path and stale hits when ES is down
        const cachedResult = await container.redis.client.get(searchCacheKey).catch(() => null);
        const esCircuitOpen = container.breakers.elasticsearch.getState() === 'open';

        if (cachedResult !== null) {
          const cached = JSON.parse(cachedResult) as Record<string, unknown>;
          if (esCircuitOpen) {
            // ES is down — serve the last cached result (stale) as required by X5.
            // "Cache the last successful search result in Redis for up to 1 hour."
            logger.warn({ projectId }, 'ES down — serving stale cache result (X5 degradation)');
            void reply
              .status(200)
              .header('X-Cache', 'STALE')
              .header('Retry-After', '60')
              .send({ ...cached, cacheHit: true, stale: true });
          } else {
            void reply
              .status(200)
              .header('X-Cache', 'HIT')
              .send({ ...cached, cacheHit: true });
          }
          return;
        }

        if (esCircuitOpen) {
          // No cached result available and ES is down — return 503.
          void reply
            .status(503)
            .header('Retry-After', '60')
            .send({ error: 'Search service temporarily unavailable', retryAfter: 60 });
          return;
        }

        let response: Awaited<ReturnType<typeof container.es.client.search<Record<string, unknown>>>>;
        try {
          response = await container.breakers.elasticsearch.run(() =>
            container.es.client.search<Record<string, unknown>>({
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
                // _shard_doc is a tie-breaker that is stable across shards
                // for search_after pagination (preferred over _doc in ES 7.12+).
                { _shard_doc: { order: 'asc' } },
              ],
              ...(searchAfter !== undefined ? { search_after: searchAfter } : {}),
              track_total_hits: true,
            }),
          );
        } catch (err) {
          // ES went down mid-request — cache was already checked above (miss), return 503
          logger.warn({ err, projectId }, 'ES search failed (X5 degradation)');
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
        void container.redis.client
          .set(searchCacheKey, JSON.stringify(responseBody), 'EX', 3600)
          .catch(() => {});

        void reply
          .status(200)
          .header('X-Cache', 'MISS')
          .send({ ...responseBody, cacheHit: false });
      },
    );
  }, { name: 'search-routes', fastify: '4.x' });
}
