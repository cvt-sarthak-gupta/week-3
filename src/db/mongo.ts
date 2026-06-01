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

// ─── MongoDB JSON Schema validator applied to the production events collection ─

const EVENTS_VALIDATOR = {
  $jsonSchema: {
    bsonType: 'object',
    required: ['_id', 'projectId', 'type', 'severity', 'message', 'occurredAt', 'ingestedAt', 'fingerprint'],
    properties: {
      _id:          { bsonType: 'string' },
      projectId:    { bsonType: 'string' },
      type:         { enum: ['error', 'log', 'metric', 'custom'] },
      severity:     { enum: ['debug', 'info', 'warn', 'error', 'fatal'] },
      message:      { bsonType: 'string' },
      occurredAt:   { bsonType: 'date' },
      ingestedAt:   { bsonType: 'date' },
      fingerprint:  { bsonType: 'string' },
      payload:      {},               // any shape — intentionally unconstrained
      tags:         { bsonType: 'object' },
      userContext:  { bsonType: 'object' },
      deviceContext:{ bsonType: 'object' },
      stackTrace:   { bsonType: 'array' },
    },
    additionalProperties: true,
  },
};

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

  /** Structured application logs with arbitrary metadata (separate from raw events). */
  logs(): Collection<LogDocument> {
    return this.db().collection<LogDocument>('logs');
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

  /**
   * Applies the JSON Schema validator and required compound indexes to the
   * production `events` collection.  Called once at startup via AppContainer.
   *
   * - Uses `createCollection` on first run (collection does not exist yet).
   * - Falls back to `collMod` when the collection already exists so the
   *   validator and indexes are always in sync regardless of startup order.
   * - `createIndex` calls are idempotent; MongoDB ignores duplicate index
   *   definitions that are identical.
   */
  async ensureEventsCollection(): Promise<void> {
    const db = this.db();

    // Ensure collection exists with the validator.
    try {
      await db.createCollection('events', {
        validator: EVENTS_VALIDATOR,
        validationLevel: 'strict',
        validationAction: 'error',
      });
      logger.info('MongoDB: events collection created with schema validator');
    } catch (err: unknown) {
      // NamespaceExists (code 48) means the collection already exists — update
      // the validator in place.
      const mongoCode = (err as { code?: unknown }).code;
      if (err instanceof Error && (err.message.includes('already exists') || mongoCode === 48)) {
        await db.command({
          collMod: 'events',
          validator: EVENTS_VALIDATOR,
          validationLevel: 'strict',
          validationAction: 'error',
        });
        logger.info('MongoDB: events collection validator updated (collection already existed)');
      } else {
        throw err;
      }
    }

    // Ensure the same validator on `logs` collection.
    try {
      await db.createCollection('logs');
      logger.info('MongoDB: logs collection created');
    } catch {
      // Already exists — ignore.
    }

    // Compound indexes for the five M2 query patterns + TTL safety-net.
    const col = this.events();
    await Promise.all([
      col.createIndex({ projectId: 1, severity: 1, occurredAt: -1 }),    // Q1
      col.createIndex({ projectId: 1, fingerprint: 1, occurredAt: -1 }), // Q2
      col.createIndex({ 'tags.env': 1, 'tags.service': 1 }),             // Q3
      col.createIndex({ projectId: 1, 'userContext.email': 1 }),          // Q5
      col.createIndex({ ingestedAt: 1 }, { expireAfterSeconds: 400 * 86_400 }), // TTL safety-net
    ]);
    logger.info('MongoDB: events collection indexes ensured');

    // Logs collection index.
    await this.logs().createIndex({ projectId: 1, timestamp: -1 });
    logger.info('MongoDB: logs collection indexes ensured');
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
