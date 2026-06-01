import pg from 'pg';
import { config } from '../config.js';
import { logger } from '../logger.js';

const poolConfig: pg.PoolConfig = {
  host: config.pg.host,
  port: config.pg.port,
  database: config.pg.database,
  user: config.pg.user,
  password: config.pg.password,
  min: config.pg.poolMin,
  max: config.pg.poolMax,
  idleTimeoutMillis: 30_000,
  connectionTimeoutMillis: 5_000,
};

/**
 * Application pool — connects as the configured user (app_user in production).
 * All tenant-scoped queries run through this pool via `withTenant`.
 */
export const pool = new pg.Pool(poolConfig);

pool.on('connect', () => {
  logger.info({ host: config.pg.host, database: config.pg.database }, 'pg pool: new client connected');
});

pool.on('error', (err: Error) => {
  logger.error({ err }, 'pg pool: idle client error');
});

/**
 * Checks out a client, begins a transaction, sets the RLS tenant context via
 * `app.current_tenant_id`, runs `fn`, then commits or rolls back.
 */
export async function withTenant<T>(
  tenantId: string,
  fn: (client: pg.PoolClient) => Promise<T>,
): Promise<T> {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    // SET LOCAL does not accept parameterized values in PostgreSQL; validate UUID format
    // before interpolating to prevent SQL injection.
    const uuidRe = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
    if (!uuidRe.test(tenantId)) throw new Error(`withTenant: invalid tenantId: ${tenantId}`);
    await client.query(`SET LOCAL "app.current_tenant_id" = '${tenantId}'`);
    const result = await fn(client);
    await client.query('COMMIT');
    return result;
  } catch (err) {
    await client.query('ROLLBACK');
    throw err;
  } finally {
    client.release();
  }
}

// Admin / migration helper (no RLS context)

/**
 * Checks out a client and runs `fn` inside a transaction without setting any
 * tenant context. Intended for migrations and admin operations.
 */
export async function withClient<T>(
  fn: (client: pg.PoolClient) => Promise<T>,
): Promise<T> {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    const result = await fn(client);
    await client.query('COMMIT');
    return result;
  } catch (err) {
    await client.query('ROLLBACK');
    throw err;
  } finally {
    client.release();
  }
}

/**
 * Runs a single query on a transient pooled client (no explicit transaction).
 */
export async function query<T extends pg.QueryResultRow = pg.QueryResultRow>(
  sql: string,
  params?: unknown[],
): Promise<pg.QueryResult<T>> {
  return pool.query<T>(sql, params);
}

export async function close(): Promise<void> {
  await pool.end();
  logger.info('pg pool: closed');
}

export async function healthCheck(): Promise<{ ok: boolean; latencyMs: number }> {
  const start = Date.now();
  try {
    await pool.query('SELECT 1');
    return { ok: true, latencyMs: Date.now() - start };
  } catch (err) {
    logger.error({ err }, 'pg healthCheck failed');
    return { ok: false, latencyMs: Date.now() - start };
  }
}
