import pg from 'pg';
import { logger } from '../logger.js';

// pg.QueryResult<T> constrains T extends QueryResultRow (an index-signature type)
// which conflicts with noPropertyAccessFromIndexSignature. We expose our own
// result type so callers get plain-typed rows without the index signature.
export interface PgQueryResult<T = any> {
  rows: T[];
  rowCount: number | null;
  command: string;
  fields: pg.FieldDef[];
}

/**
 * Class-based PostgreSQL database module.
 *
 * Wraps a `pg.Pool` and exposes tenant-scoped transactions (RLS), bare
 * client transactions, single-shot queries, a health-check, and pool
 * lifecycle management.
 */
export class PostgresDatabase {
  private readonly pool: pg.Pool;

  constructor(poolConfig: pg.PoolConfig) {
    this.pool = new pg.Pool(poolConfig);

    this.pool.on('connect', () => {
      logger.info(
        { host: poolConfig.host, database: poolConfig.database },
        'pg pool: new client connected',
      );
    });

    this.pool.on('error', (err: Error) => {
      logger.error({ err }, 'pg pool: idle client error');
    });
  }

  /**
   * Checks out a client, begins a transaction, sets the RLS tenant context via
   * `app.current_tenant_id`, runs `fn`, then commits or rolls back.
   */
  async withTenant<T>(
    tenantId: string,
    fn: (client: pg.PoolClient) => Promise<T>,
  ): Promise<T> {
    const client = await this.pool.connect();
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

  /**
   * Runs a single query on a transient pooled client (no explicit transaction).
   */
  async query<T = any>(sql: string, values?: unknown[]): Promise<PgQueryResult<T>> {
    return this.pool.query(sql, values) as unknown as PgQueryResult<T>;
  }

  /**
   * Checks out a raw client from the pool. Caller is responsible for
   * releasing it via `client.release()`.
   */
  async connect(): Promise<pg.PoolClient> {
    return this.pool.connect();
  }

  /**
   * Pings the database with `SELECT 1` and returns liveness + round-trip
   * latency in milliseconds.
   */
  async healthCheck(): Promise<{ ok: boolean; latencyMs: number }> {
    const start = Date.now();
    try {
      await this.pool.query('SELECT 1');
      return { ok: true, latencyMs: Date.now() - start };
    } catch (err) {
      logger.error({ err }, 'pg healthCheck failed');
      return { ok: false, latencyMs: Date.now() - start };
    }
  }

  /**
   * Drains the pool and closes all connections.
   */
  async close(): Promise<void> {
    await this.pool.end();
    logger.info('pg pool: closed');
  }
}

/**
 * Convenience factory — equivalent to `new PostgresDatabase(config)`.
 */
export function createPostgresDatabase(config: pg.PoolConfig): PostgresDatabase {
  return new PostgresDatabase(config);
}
