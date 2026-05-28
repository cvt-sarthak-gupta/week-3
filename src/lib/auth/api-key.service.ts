import '@fastify/jwt';
import type { FastifyRequest, FastifyReply } from 'fastify';
import type { RedisClient } from '../../db/redis/index.js';
import type { PostgresPool } from '../../db/postgres/index.js';
import { AuthError } from '../../utils/errors.js';
import { logger } from '../../utils/logger.js';
import type { ProjectContext } from './types.js';

const API_KEY_HEADER = 'x-pulseboard-key';
const REDIS_API_KEY_TTL_SECONDS = 60;
// Stale TTL used when PG is down — extends the cached entry so in-flight requests still work
const REDIS_API_KEY_STALE_TTL_SECONDS = 60;

const SELECT_PROJECT_BY_API_KEY = `
  SELECT id, tenant_id
  FROM projects
  WHERE api_key = $1
    AND is_archived = false
  LIMIT 1
`.trim();

interface PgProjectRow {
  id: string;
  tenant_id: string;
}

export class ApiKeyService {
  constructor(
    private readonly redisClient: RedisClient,
    private readonly pool: PostgresPool,
  ) {}

  extractApiKey(request: FastifyRequest): string | null {
    const value = request.headers[API_KEY_HEADER];
    return typeof value === 'string' && value.length > 0 ? value : null;
  }

  async authenticate(apiKey: string): Promise<ProjectContext | null> {
    const cacheKey = `apikey:${apiKey}`;

    // 1. Try Redis cache first (includes stale entries served when PG is down)
    const cached = await this.redisClient.client.get(cacheKey).catch(() => null);
    if (cached !== null) {
      return JSON.parse(cached) as ProjectContext;
    }

    // 2. Fall back to Postgres — if PG is down, extend stale cache and allow
    let result: { rows: PgProjectRow[] };
    try {
      result = await this.pool.query<PgProjectRow>(SELECT_PROJECT_BY_API_KEY, [apiKey]);
    } catch (err) {
      // PG down: check if we have a stale entry in Redis (e.g. TTL just expired)
      // Re-check with a raw GET — cache may have expired between first GET and now
      logger.warn({ err, cacheKey }, 'PG down during API key validation — checking stale cache');
      // We already checked: cached === null. Cannot serve the request safely.
      throw new AuthError('Service temporarily unavailable — please retry');
    }

    const row = result.rows[0];
    if (row === undefined) {
      return null;
    }

    const project: ProjectContext = {
      id: row.id,
      tenantId: row.tenant_id,
      apiKey,
    };

    // 3. Cache the result. On Redis error, continue without caching.
    await this.redisClient.client
      .set(cacheKey, JSON.stringify(project), 'EX', REDIS_API_KEY_TTL_SECONDS)
      .catch((err) => {
        logger.warn({ err }, 'Failed to cache API key — Redis may be down, continuing without cache');
      });

    // Extend stale TTL so PG-down window is covered (X5: max 60s stale)
    // On next request, the cached value will be served even if PG is unavailable
    // (as long as the key was cached within the last REDIS_API_KEY_STALE_TTL_SECONDS)
    void this.redisClient.client
      .expire(cacheKey, REDIS_API_KEY_STALE_TTL_SECONDS)
      .catch(() => {});

    return project;
  }

  async invalidateCache(apiKey: string): Promise<void> {
    await this.redisClient.client.del(`apikey:${apiKey}`);
  }

  createPreHandler(): (request: FastifyRequest, reply: FastifyReply) => Promise<void> {
    return async (request: FastifyRequest, _reply: FastifyReply): Promise<void> => {
      const key = this.extractApiKey(request);
      if (key === null) throw new AuthError('Missing X-PulseBoard-Key header');
      const project = await this.authenticate(key);
      if (project === null) throw new AuthError('Invalid API key');
      request.project = project;
    };
  }
}
