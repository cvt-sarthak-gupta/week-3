export interface LuaScript {
  evalsha(keys: string[], args: (string | number)[]): Promise<unknown>;
}
export interface HealthCheckResult { ok: boolean; latencyMs: number }
