import '@fastify/jwt';
import type { FastifyInstance } from 'fastify';
import { config } from '../../config.js';
import type { UserTokenPayload, SignedTokens } from './types.js';
import { AuthError } from '../../utils/errors.js';

export class TokenService {
  constructor(private readonly fastify: FastifyInstance) {}

  async sign(payload: UserTokenPayload): Promise<SignedTokens> {
    const accessToken = await this.fastify.jwt.sign(
      { ...payload },
      { expiresIn: config.jwt.expiry },
    );
    const refreshToken = await this.fastify.jwt.sign(
      { ...payload },
      { expiresIn: config.jwt.refreshExpiry },
    );
    return { accessToken, refreshToken };
  }

  async verifyAccess(token: string): Promise<UserTokenPayload> {
    try {
      return await this.fastify.jwt.verify<UserTokenPayload>(token);
    } catch {
      throw new AuthError('Invalid or expired access token');
    }
  }

  async verifyRefresh(token: string): Promise<UserTokenPayload> {
    try {
      return await this.fastify.jwt.verify<UserTokenPayload>(token);
    } catch {
      throw new AuthError('Invalid or expired refresh token');
    }
  }
}
