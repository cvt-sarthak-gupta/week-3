export interface RateLimitResult {
  allowed: boolean;
  remaining: number;
  resetAt: number; // Unix timestamp ms
}

export interface RateLimitConfig {
  windowMs: number;
  maxRequests: number;
}
