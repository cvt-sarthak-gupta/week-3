import type { ElasticClient } from '../../db/elastic/index.js';
import type { RedisClient } from '../../db/redis/index.js';
import type { CircuitBreakers } from '../../lib/circuit-breaker/index.js';
import { logger } from '../../utils/logger.js';
import { ValidationError } from '../../utils/errors.js';
import type { SearchQuerystring, SearchHit, SearchResult } from './search.types.js';

// Cache TTL: 1 hour for search results (stale-on-open pattern)
const SEARCH_CACHE_TTL_SECONDS = 3600;

interface EsHit {
  _id: string;
  _source: Record<string, unknown>;
  highlight?: Record<string, string[]>;
  sort?: unknown[];
}

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

export class SearchService {
  constructor(
    private readonly es: ElasticClient,
    private readonly redis: RedisClient,
    private readonly breakers: CircuitBreakers,
  ) {}

  async search(
    projectId: string,
    query: SearchQuerystring,
  ): Promise<{ result: SearchResult; status: number; headers: Record<string, string> }> {
    const { q, severity, from, to, cursor, limit = 20 } = query;
    const pageSize = Math.min(limit, 100);

    // -------------------------------------------------------------------------
    // Build the ES bool query
    // -------------------------------------------------------------------------

    type EsQuery = Record<string, unknown>;

    const mustClauses: EsQuery[] = [];
    const filterClauses: EsQuery[] = [];
    const shouldClauses: EsQuery[] = [];
    const mustNotClauses: EsQuery[] = [{ term: { severity: 'debug' } }];

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

    // -------------------------------------------------------------------------
    // Search-after pagination
    // -------------------------------------------------------------------------

    const searchAfter = cursor !== undefined ? decodeCursor(cursor) : undefined;
    const alias = this.es.aliasName(projectId);
    const cacheKey = `search:${projectId}:${Buffer.from(
      JSON.stringify({ q, severity, from, to, cursor, pageSize }),
    ).toString('base64')}`;

    // Check cache first — serves fresh hits on normal path and stale hits when ES is down
    const cachedResult = await this.redis.client.get(cacheKey).catch(() => null);
    const esCircuitOpen = this.breakers.elasticsearch.getState() === 'open';

    if (cachedResult !== null) {
      const cached = JSON.parse(cachedResult) as unknown as SearchResult;
      if (esCircuitOpen) {
        logger.warn({ projectId }, 'ES down — serving stale cached search result (X5)');
        return {
          result: { ...cached, cacheHit: true, stale: true },
          status: 206,
          headers: { 'X-Cache': 'STALE' },
        };
      }
      return {
        result: { ...cached, cacheHit: true },
        status: 200,
        headers: { 'X-Cache': 'HIT' },
      };
    }

    if (esCircuitOpen) {
      return {
        result: {
          hits: [],
          total: 0,
          nextCursor: null,
          took: 0,
          cacheHit: false,
        },
        status: 503,
        headers: { 'Retry-After': '60' },
      };
    }

    let response: Awaited<ReturnType<typeof this.es.client.search<Record<string, unknown>>>>;
    try {
      response = await this.breakers.elasticsearch.run(() =>
        this.es.client.search<Record<string, unknown>>({
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
      // ES went down mid-request — cache was already checked above (miss), return 503
      logger.warn({ err, projectId }, 'ES search failed (X5 degradation)');
      return {
        result: {
          hits: [],
          total: 0,
          nextCursor: null,
          took: 0,
          cacheHit: false,
        },
        status: 503,
        headers: { 'Retry-After': '60' },
      };
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

    const hits: SearchHit[] = hitsArray.map((hit) => ({
      id: hit._id,
      ...hit._source,
      highlight: hit.highlight ?? {},
    }));

    const responseBody: SearchResult = {
      hits,
      total,
      nextCursor,
      took: response.took,
      cacheHit: false,
    };

    // Cache successful search result for up to 1 hour (X5 stale serving)
    void this.redis.client
      .set(cacheKey, JSON.stringify(responseBody), 'EX', SEARCH_CACHE_TTL_SECONDS)
      .catch(() => {});

    return {
      result: responseBody,
      status: 200,
      headers: { 'X-Cache': 'MISS' },
    };
  }
}
