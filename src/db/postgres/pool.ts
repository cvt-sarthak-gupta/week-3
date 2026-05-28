import pg from 'pg';
import { config } from '../../config.js';
import { logger } from '../../utils/logger.js';

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

export class PostgresPool {
  readonly pool: pg.Pool;

  constructor() {
    this.pool = new pg.Pool({
      host: config.pg.host,
      port: config.pg.port,
      database: config.pg.database,
      user: config.pg.user,
      password: config.pg.password,
      min: config.pg.poolMin,
      max: config.pg.poolMax,
      idleTimeoutMillis: 30_000,
      connectionTimeoutMillis: 5_000,
    });
    this.pool.on('connect', () => {
      logger.info({ host: config.pg.host, database: config.pg.database }, 'pg pool: new client connected');
    });
    this.pool.on('error', (err: Error) => {
      logger.error({ err }, 'pg pool: idle client error');
    });
  }

  async query<T extends pg.QueryResultRow = pg.QueryResultRow>(
    sql: string, params?: unknown[]
  ): Promise<pg.QueryResult<T>> {
    return this.pool.query<T>(sql, params);
  }

  async withTenant<T>(tenantId: string, fn: (client: pg.PoolClient) => Promise<T>): Promise<T> {
    if (!UUID_RE.test(tenantId)) throw new Error(`withTenant: invalid tenantId: ${tenantId}`);
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
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

  async withClient<T>(fn: (client: pg.PoolClient) => Promise<T>): Promise<T> {
    const client = await this.pool.connect();
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

  async close(): Promise<void> {
    await this.pool.end();
    logger.info('pg pool: closed');
  }
}
