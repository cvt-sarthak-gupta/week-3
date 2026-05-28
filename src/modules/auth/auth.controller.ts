import fp from 'fastify-plugin';
import type { FastifyInstance, FastifyPluginAsync } from 'fastify';
import { config } from '../../config.js';
import type { AuthService } from './auth.service.js';
import type { RegisterBody, LoginBody, RefreshBody } from './auth.types.js';
import { AuthError } from '../../utils/errors.js';

export function createAuthRoutes(authService: AuthService): FastifyPluginAsync {
  return fp(async (fastify: FastifyInstance) => {
    fastify.post<{ Body: RegisterBody }>(
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
      async (request, reply) => {
        const result = await authService.register(
          request.body.email,
          request.body.password,
          request.body.fullName,
        );
        void reply.status(201).send(result);
      },
    );

    fastify.post<{ Body: LoginBody }>(
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
      async (request, reply) => {
        const user = await authService.verifyCredentials(request.body.email, request.body.password);
        await authService.updateLastLogin(user.id);
        const accessToken = await fastify.jwt.sign(
          { userId: user.id, tenantId: user.tenant_id ?? '', email: user.email, role: user.role },
          { expiresIn: config.jwt.expiry },
        );
        const refreshToken = await fastify.jwt.sign(
          { userId: user.id, tenantId: user.tenant_id ?? '', email: user.email, role: user.role },
          { expiresIn: config.jwt.refreshExpiry },
        );
        void reply.status(200).send({
          accessToken,
          refreshToken,
          user: { id: user.id, email: user.email, fullName: user.full_name },
        });
      },
    );

    fastify.post<{ Body: RefreshBody }>(
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
      async (request, reply) => {
        let payload: { userId: string; tenantId: string; email: string; role: string };
        try {
          payload = await fastify.jwt.verify<{
            userId: string;
            tenantId: string;
            email: string;
            role: string;
          }>(request.body.refreshToken);
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
          { expiresIn: config.jwt.expiry },
        );
        void reply.status(200).send({ accessToken });
      },
    );
  }, { name: 'auth-routes', fastify: '4.x' });
}
