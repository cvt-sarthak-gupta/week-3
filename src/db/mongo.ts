import { MongoClient, ServerApiVersion, type Db, type Collection } from 'mongodb';
import { logger } from '../logger.js';

export interface StackFrame {
  file?: string;
  line?: number;
  column?: number;
  function?: string;
  source?: string;
}

export interface EventDocument {
  _id: string; // eventId (uuidv7)
  projectId: string;
  type: 'error' | 'log' | 'metric' | 'custom';
  severity: 'debug' | 'info' | 'warn' | 'error' | 'fatal';
  message: string;
  stackTrace?: StackFrame[];
  tags?: Record<string, string>;
  userContext?: { userId?: string; email?: string; ip?: string };
  deviceContext?: { os?: string; browser?: string; version?: string };
  payload?: Record<string, unknown>;
  occurredAt: Date;
  ingestedAt: Date;
  fingerprint: string;
  traceId?: string;
}

export interface LogDocument {
  _id: string;
  projectId: string;
  level: string;
  message: string;
  timestamp: Date;
  meta?: Record<string, unknown>;
}

export interface DashboardDocument {
  _id: string;
  tenantId: string;
  name: string;
  layout: unknown[];
  createdAt: Date;
  updatedAt: Date;
}

export interface ProjectConfigDocument {
  _id: string; // projectId
  tenantId: string;
  name: string;
  retentionDays: number;
  alertsEnabled: boolean;
  createdAt: Date;
  updatedAt: Date;
  settings?: Record<string, unknown>;
}

export interface PipelineMetricsDocument {
  _id: string;
  projectId: string;
  pipelineId: string;
  stage: string;
  durationMs: number;
  status: 'success' | 'failure' | 'skipped';
  recordedAt: Date;
  meta?: Record<string, unknown>;
}

const RETRY_ATTEMPTS = 3;
const RETRY_DELAY_MS = 2_000;

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

export class MongoDatabase {
  private readonly url: string;
  private readonly dbName: string;
  private _client: MongoClient | null = null;

  constructor(url: string, dbName: string) {
    this.url = url;
    this.dbName = dbName;
  }

  /**
   * Connects to MongoDB with up to 3 retry attempts on failure.
   * Uses the Stable API (serverApi v1) and replica-set-aware topology.
   */
  async connect(): Promise<void> {
    if (this._client !== null) return; // already connected

    let lastErr: unknown;

    for (let attempt = 1; attempt <= RETRY_ATTEMPTS; attempt++) {
      try {
        const client = new MongoClient(this.url, {
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
          { url: this.url, dbName: this.dbName },
          'MongoDB connected',
        );
        return;
      } catch (err) {
        lastErr = err;
        logger.warn(
          { err, attempt, maxAttempts: RETRY_ATTEMPTS },
          'MongoDB connection attempt failed, retrying…',
        );
        if (attempt < RETRY_ATTEMPTS) {
          await sleep(RETRY_DELAY_MS * attempt);
        }
      }
    }

    throw new Error(
      `MongoDB failed to connect after ${RETRY_ATTEMPTS} attempts: ${String(lastErr)}`,
    );
  }

  /**
   * Returns the live MongoClient. Throws if connect() has not been called.
   */
  private getClient(): MongoClient {
    if (this._client === null) {
      throw new Error('MongoDB client is not connected. Call connect() first.');
    }
    return this._client;
  }

  /** Returns the internal Db instance. */
  db(): Db {
    return this.getClient().db(this.dbName);
  }

  /** Raw ingested events (errors, logs, metrics, custom). */
  events(): Collection<EventDocument> {
    return this.db().collection<EventDocument>('events');
  }

  dashboards(): Collection<DashboardDocument> {
    return this.db().collection<DashboardDocument>('dashboards');
  }

  projectConfigs(): Collection<ProjectConfigDocument> {
    return this.db().collection<ProjectConfigDocument>('project_configs');
  }

  pipelineMetrics(): Collection<PipelineMetricsDocument> {
    return this.db().collection<PipelineMetricsDocument>('pipeline_metrics');
  }

  rateLimitViolations(): Collection<{
    apiKeyTail: string;
    projectId: string;
    tenantId: string;
    violatedAt: Date;
    resetAt: Date;
  }> {
    return this.db().collection('rate_limit_violations');
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
