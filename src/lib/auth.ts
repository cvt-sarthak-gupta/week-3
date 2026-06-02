import '@fastify/jwt';
import type { FastifyInstance, FastifyReply, FastifyRequest } from 'fastify';
import type { PostgresDatabase } from '../db/postgres.js';
import type { RedisDatabase } from '../db/redis.js';
import { config } from '../config.js';
import { AuthError } from '../errors.js';
import { logger } from '../logger.js';
import type { UserTokenPayload, ProjectContext, SignedTokens } from '../types/auth.js';

export type { UserTokenPayload, ProjectContext, SignedTokens };

// Augment @fastify/jwt so that request.user resolves to UserTokenPayload
// throughout the entire application. This is the official pattern for typed JWT users.
declare module '@fastify/jwt' {
  interface FastifyJWT {
    payload: UserTokenPayload;
    user: UserTokenPayload;
  }
}

// Augment FastifyRequest with the project field (user is handled by @fastify/jwt above)
declare module 'fastify' {
  interface FastifyRequest {
    project?: ProjectContext;
  }
}

interface PgProjectRow {
  id: string;
  tenant_id: string;
}

const SELECT_PROJECT_BY_API_KEY = `
  SELECT id, tenant_id
  FROM projects
  WHERE api_key = $1
    AND is_archived = false
  LIMIT 1
`.trim();

// Stale TTL used when PG is down — extends the cached entry so in-flight requests still work
const REDIS_API_KEY_STALE_TTL_SECONDS = 60;

export class AuthService {
  private readonly pg: PostgresDatabase;
  private readonly redis: RedisDatabase;
  private readonly API_KEY_HEADER = 'x-pulseboard-key';
  private readonly REDIS_API_KEY_TTL_SECONDS = 60;

  constructor(pg: PostgresDatabase, redis: RedisDatabase) {
    this.pg = pg;
    this.redis = redis;
  }

  async signTokens(
    payload: UserTokenPayload,
    jwtInstance: FastifyInstance,
  ): Promise<SignedTokens> {
    const accessToken = await jwtInstance.jwt.sign(
      { ...payload },
      { expiresIn: config.jwt.expiry },
    );

    const refreshToken = await jwtInstance.jwt.sign(
      { ...payload },
      { expiresIn: config.jwt.refreshExpiry },
    );

    return { accessToken, refreshToken };
  }

  extractApiKey(request: FastifyRequest): string | null {
    const value = request.headers[this.API_KEY_HEADER];
    if (typeof value === 'string' && value.length > 0) {
      return value;
    }
    return null;
  }

  userPreHandler(): (request: FastifyRequest, reply: FastifyReply) => Promise<void> {
    return async function authenticateUser(
      request: FastifyRequest,
      _reply: FastifyReply,
    ): Promise<void> {
      try {
        await request.jwtVerify<UserTokenPayload>();
      } catch {
        throw new AuthError('Invalid or expired access token');
      }

      // Guard: ensure the decoded payload has the fields we require.
      // jwtVerify<UserTokenPayload>() sets request.user; validate the shape.
      const u = request.user;
      if (
        typeof u.userId !== 'string' ||
        typeof u.tenantId !== 'string' ||
        typeof u.email !== 'string' ||
        typeof u.role !== 'string'
      ) {
        throw new AuthError('Malformed token payload');
      }
    };
  }

  apiKeyPreHandler(): (request: FastifyRequest, reply: FastifyReply) => Promise<void> {
    const redisClient = this.redis.client;
    const pg = this.pg;
    const extractApiKey = this.extractApiKey.bind(this);
    const TTL = this.REDIS_API_KEY_TTL_SECONDS;

    return async function apiKeyPreHandler(
      request: FastifyRequest,
      _reply: FastifyReply,
    ): Promise<void> {
      const key = extractApiKey(request);
      if (key === null) {
        throw new AuthError('Missing X-PulseBoard-Key header');
      }

      const cacheKey = `apikey:${key}`;

      // 1. Try Redis cache first (includes stale entries served when PG is down)
      const cached = await redisClient.get(cacheKey).catch(() => null);
      if (cached !== null) {
        request.project = JSON.parse(cached) as ProjectContext;
        return;
      }

      // 2. Fall back to Postgres — if PG is down, extend stale cache and allow
      let result: { rows: PgProjectRow[] };
      try {
        result = await pg.query<PgProjectRow>(SELECT_PROJECT_BY_API_KEY, [key]);
      } catch (err) {
        // PG down: check if we have a stale entry in Redis (e.g. TTL just expired)
        // Re-check with a raw GET — cache may have expired between first GET and now
        logger.warn({ err, cacheKey }, 'PG down during API key validation — checking stale cache');
        // We already checked: cached === null. Cannot serve the request safely.
        throw new AuthError('Service temporarily unavailable — please retry');
      }

      const row = result.rows[0];
      if (row === undefined) {
        throw new AuthError('Invalid API key');
      }

      const project: ProjectContext = {
        id: row.id,
        tenantId: row.tenant_id,
        apiKey: key,
      };

      // 3. Cache the result. On Redis error, continue without caching.
      await redisClient
        .set(cacheKey, JSON.stringify(project), 'EX', TTL)
        .catch((err) => {
          logger.warn({ err }, 'Failed to cache API key — Redis may be down, continuing without cache');
        });

      // Extend stale TTL so PG-down window is covered (X5: max 60s stale)
      // On next request, the cached value will be served even if PG is unavailable
      // (as long as the key was cached within the last REDIS_API_KEY_STALE_TTL_SECONDS)
      void redisClient
        .expire(cacheKey, REDIS_API_KEY_STALE_TTL_SECONDS)
        .catch(() => {});

      request.project = project;
    };
  }
}
