import { randomBytes, scrypt, timingSafeEqual } from 'node:crypto';
import { promisify } from 'node:util';
import type { FastifyPluginAsync, FastifyReply, FastifyRequest } from 'fastify';
import fp from 'fastify-plugin';
import { AppContainer } from '../container.js';
import { AuthError, ConflictError, ValidationError, RateLimitError } from '../errors.js';

const scryptAsync = promisify(scrypt);
const SCRYPT_KEY_LEN = 64;

async function hashPassword(password: string): Promise<string> {
  const salt = randomBytes(16).toString('hex');
  const derivedKey = await scryptAsync(password, salt, SCRYPT_KEY_LEN) as Buffer;
  return `${salt}:${derivedKey.toString('hex')}`;
}

async function verifyPassword(password: string, stored: string): Promise<boolean> {
  const [salt, storedHash] = stored.split(':');
  if (salt === undefined || storedHash === undefined) return false;
  const derivedKey = await scryptAsync(password, salt, SCRYPT_KEY_LEN) as Buffer;
  const storedBuffer = Buffer.from(storedHash, 'hex');
  if (derivedKey.length !== storedBuffer.length) return false;
  return timingSafeEqual(derivedKey, storedBuffer);
}

interface UserRow {
  id: string;
  email: string;
  full_name: string;
  password_hash: string;
  tenant_id: string | null;
  role: string;
}

interface RegisterBody {
  email: string;
  password: string;
  fullName: string;
}

interface LoginBody {
  email: string;
  password: string;
}

interface RefreshBody {
  refreshToken: string;
}

// Auth endpoints are not behind apiKeyPreHandler but still need brute-force protection.
// Login: 10 attempts per IP per minute. Register: 5 per IP per minute.
// Uses the same Redis sliding-window Lua script as ingest rate limiting.
const LOGIN_RL    = { windowMs: 60_000, maxRequests: 10 };
const REGISTER_RL = { windowMs: 60_000, maxRequests: 5  };

export function authRoutes(container: AppContainer): FastifyPluginAsync {
  return fp(async (fastify) => {
    fastify.post(
      '/auth/register',
      {
        schema: {
          tags: ['auth'],
          body: {
            type: 'object',
            required: ['email', 'password', 'fullName'],
            properties: {
              email: { type: 'string', format: 'email' },
              password: { type: 'string', minLength: 8 },
              fullName: { type: 'string', minLength: 1 },
            },
          },
        },
      },
      async (request: FastifyRequest<{ Body: RegisterBody }>, reply: FastifyReply): Promise<void> => {
        const rl = await container.rateLimit.checkRateLimit(`register:${request.ip}`, REGISTER_RL);
        if (!rl.allowed) {
          const retryAfter = Math.ceil((rl.resetAt - Date.now()) / 1000);
          void reply.header('Retry-After', String(retryAfter));
          throw new RateLimitError('Too many registration attempts — try again later', retryAfter);
        }

        const { email, password, fullName } = request.body;

        const existing = await container.pg.query<{ id: string }>(
          'SELECT id FROM users WHERE email = $1 LIMIT 1',
          [email],
        );
        if ((existing.rowCount ?? 0) > 0) {
          throw new ConflictError('Email already registered');
        }

        if (password.length < 8) {
          throw new ValidationError('Password must be at least 8 characters');
        }

        const passwordHash = await hashPassword(password);

        const result = await container.pg.query<{ id: string }>(
          `INSERT INTO users (email, password_hash, full_name)
           VALUES ($1, $2, $3)
           RETURNING id`,
          [email, passwordHash, fullName],
        );

        const userId = result.rows[0]?.id;
        if (userId === undefined) {
          throw new Error('Failed to create user');
        }

        void reply.status(201).send({ userId, email });
      },
    );

    fastify.post(
      '/auth/login',
      {
        schema: {
          tags: ['auth'],
          body: {
            type: 'object',
            required: ['email', 'password'],
            properties: {
              email: { type: 'string', format: 'email' },
              password: { type: 'string', minLength: 1 },
            },
          },
        },
      },
      async (request: FastifyRequest<{ Body: LoginBody }>, reply: FastifyReply): Promise<void> => {
        const rl = await container.rateLimit.checkRateLimit(`login:${request.ip}`, LOGIN_RL);
        if (!rl.allowed) {
          const retryAfter = Math.ceil((rl.resetAt - Date.now()) / 1000);
          void reply.header('Retry-After', String(retryAfter));
          throw new RateLimitError('Too many login attempts — try again later', retryAfter);
        }

        const { email, password } = request.body;

        const result = await container.pg.query<UserRow>(
          `SELECT u.id, u.email, u.full_name, u.password_hash,
                  tm.tenant_id, tm.role
           FROM users u
           LEFT JOIN tenant_members tm ON tm.user_id = u.id
           WHERE u.email = $1
           LIMIT 1`,
          [email],
        );

        const user = result.rows[0];
        if (user === undefined) {
          throw new AuthError('Invalid email or password');
        }

        const valid = await verifyPassword(password, user.password_hash);
        if (!valid) {
          throw new AuthError('Invalid email or password');
        }

        await container.pg.query(
          'UPDATE users SET last_login_at = NOW() WHERE id = $1',
          [user.id],
        );

        const payload = {
          userId: user.id,
          tenantId: user.tenant_id ?? '',
          email: user.email,
          role: user.role,
        };

        const { accessToken, refreshToken } = await container.auth.signTokens(payload, fastify);

        void reply.status(200).send({
          accessToken,
          refreshToken,
          user: {
            id: user.id,
            email: user.email,
            fullName: user.full_name,
          },
        });
      },
    );

    fastify.post(
      '/auth/refresh',
      {
        schema: {
          tags: ['auth'],
          body: {
            type: 'object',
            required: ['refreshToken'],
            properties: {
              refreshToken: { type: 'string', minLength: 1 },
            },
          },
        },
      },
      async (request: FastifyRequest<{ Body: RefreshBody }>, reply: FastifyReply): Promise<void> => {
        const { refreshToken } = request.body;

        let payload;
        try {
          payload = await fastify.jwt.verify<{
            userId: string;
            tenantId: string;
            email: string;
            role: string;
          }>(refreshToken);
        } catch {
          throw new AuthError('Invalid or expired refresh token');
        }

        const accessToken = await fastify.jwt.sign(
          {
            userId: payload.userId,
            tenantId: payload.tenantId,
            email: payload.email,
            role: payload.role,
          },
          // config.jwt.expiry is accessed via the registered plugin at startup
        );

        void reply.status(200).send({ accessToken });
      },
    );
  }, { name: 'auth-routes', fastify: '4.x' });
}
