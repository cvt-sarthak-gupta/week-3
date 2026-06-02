export interface UserTokenPayload {
  userId: string;
  tenantId: string;
  email: string;
  role: string;
}

/** Minimal project context attached to requests authenticated via API key. */
export interface ProjectContext {
  id: string;
  tenantId: string;
  apiKey: string;
}

export interface SignedTokens {
  accessToken: string;
  refreshToken: string;
}
