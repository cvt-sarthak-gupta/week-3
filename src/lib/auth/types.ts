export interface UserTokenPayload {
  userId: string;
  tenantId: string;
  email: string;
  role: string;
}

export interface ProjectContext {
  id: string;
  tenantId: string;
  apiKey: string;
}

export interface SignedTokens {
  accessToken: string;
  refreshToken: string;
}

// Augment @fastify/jwt
declare module '@fastify/jwt' {
  interface FastifyJWT {
    payload: UserTokenPayload;
    user: UserTokenPayload;
  }
}

// Augment FastifyRequest
declare module 'fastify' {
  interface FastifyRequest {
    project?: ProjectContext;
  }
}
