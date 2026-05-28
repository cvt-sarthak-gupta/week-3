import { MongoClient, ServerApiVersion, type Db, type Collection } from 'mongodb';
import { config } from '../../config.js';
import { logger } from '../../utils/logger.js';
import type {
  EventDocument,
  LogDocument,
  DashboardDocument,
  ProjectConfigDocument,
  PipelineMetricsDocument,
} from './types.js';

export class MongoDatabase {
  private _client: MongoClient | null = null;
  private static readonly RETRY_ATTEMPTS = 3;
  private static readonly RETRY_DELAY_MS = 2_000;

  private static async sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }

  async connect(): Promise<void> {
    if (this._client !== null) return; // already connected

    let lastErr: unknown;

    for (let attempt = 1; attempt <= MongoDatabase.RETRY_ATTEMPTS; attempt++) {
      try {
        const client = new MongoClient(config.mongo.url, {
          serverApi: {
            version: ServerApiVersion.v1,
            strict: true,
            deprecationErrors: true,
          },
          // Replica set awareness — MongoClient discovers topology automatically.
          // These timeouts keep startup fast in dev while allowing replica sets to
          // be found in production.
          serverSelectionTimeoutMS: 10_000,
          connectTimeoutMS: 10_000,
          socketTimeoutMS: 45_000,
        });

        await client.connect();
        // Ping to confirm connection is live.
        await client.db('admin').command({ ping: 1 });

        this._client = client;
        logger.info(
          { url: config.mongo.url, dbName: config.mongo.dbName },
          'MongoDB connected',
        );
        return;
      } catch (err) {
        lastErr = err;
        logger.warn(
          { err, attempt, maxAttempts: MongoDatabase.RETRY_ATTEMPTS },
          'MongoDB connection attempt failed, retrying…',
        );
        if (attempt < MongoDatabase.RETRY_ATTEMPTS) {
          await MongoDatabase.sleep(MongoDatabase.RETRY_DELAY_MS * attempt);
        }
      }
    }

    throw new Error(
      `MongoDB failed to connect after ${MongoDatabase.RETRY_ATTEMPTS} attempts: ${String(lastErr)}`,
    );
  }

  private getClient(): MongoClient {
    if (this._client === null) {
      throw new Error('MongoDB client is not connected. Call connect() first.');
    }
    return this._client;
  }

  private getDb(): Db {
    return this.getClient().db(config.mongo.dbName);
  }

  /** Raw ingested events (errors, logs, metrics, custom). */
  events(): Collection<EventDocument> {
    return this.getDb().collection<EventDocument>('events');
  }

  /**
   * Log documents — semantically distinct from events even though they share the
   * same physical MongoDB collection in this schema. Use this accessor wherever
   * code is reasoning about log entries specifically.
   */
  logs(): Collection<EventDocument> {
    return this.getDb().collection<EventDocument>('events');
  }

  dashboards(): Collection<DashboardDocument> {
    return this.getDb().collection<DashboardDocument>('dashboards');
  }

  projectConfigs(): Collection<ProjectConfigDocument> {
    return this.getDb().collection<ProjectConfigDocument>('project_configs');
  }

  pipelineMetrics(): Collection<PipelineMetricsDocument> {
    return this.getDb().collection<PipelineMetricsDocument>('pipeline_metrics');
  }

  async healthCheck(): Promise<{ ok: boolean; latencyMs: number }> {
    const start = Date.now();
    try {
      await this.getClient().db('admin').command({ ping: 1 });
      return { ok: true, latencyMs: Date.now() - start };
    } catch (err) {
      logger.error({ err }, 'MongoDB healthCheck failed');
      return { ok: false, latencyMs: Date.now() - start };
    }
  }

  async close(): Promise<void> {
    if (this._client !== null) {
      await this._client.close();
      this._client = null;
      logger.info('MongoDB client closed');
    }
  }
}
