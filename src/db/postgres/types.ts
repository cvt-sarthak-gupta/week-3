import type pg from 'pg';
export type { QueryResultRow, PoolClient, QueryResult } from 'pg';
export interface HealthCheckResult { ok: boolean; latencyMs: number }
