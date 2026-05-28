export interface RateLimitResult { allowed: boolean; remaining: number; resetAt: number }
export interface RateLimitConfig { windowMs: number; maxRequests: number }
