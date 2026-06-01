import { MongoClient, ServerApiVersion, type Db, type Collection } from 'mongodb';
import { config } from '../config.js';
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

let _client: MongoClient | null = null;

/**
 * Returns the live MongoClient singleton. Throws if `connect()` has not been
 * called or if the client is not yet connected.
 */
export function getClient(): MongoClient {
  if (_client === null) {
    throw new Error('MongoDB client is not connected. Call connect() first.');
  }
  return _client;
}

const RETRY_ATTEMPTS = 3;
const RETRY_DELAY_MS = 2_000;

async function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Connects to MongoDB with up to 3 retry attempts on failure.
 * Uses the Stable API (serverApi v1) and replica-set-aware topology.
 */
export async function connect(): Promise<void> {
  if (_client !== null) return; // already connected

  let lastErr: unknown;

  for (let attempt = 1; attempt <= RETRY_ATTEMPTS; attempt++) {
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

      _client = client;
      logger.info(
        { url: config.mongo.url, dbName: config.mongo.dbName },
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

export function getDb(): Db {
  return getClient().db(config.mongo.dbName);
}

/** Raw ingested events (errors, logs, metrics, custom). */
export function eventsCollection(): Collection<EventDocument> {
  return getDb().collection<EventDocument>('events');
}

/**
 * Log documents — semantically distinct from events even though they share the
 * same physical MongoDB collection in this schema. Use this accessor wherever
 * code is reasoning about log entries specifically.
 */
export function logsCollection(): Collection<EventDocument> {
  return getDb().collection<EventDocument>('events');
}

export function dashboardsCollection(): Collection<DashboardDocument> {
  return getDb().collection<DashboardDocument>('dashboards');
}

export function projectConfigsCollection(): Collection<ProjectConfigDocument> {
  return getDb().collection<ProjectConfigDocument>('project_configs');
}

export function pipelineMetricsCollection(): Collection<PipelineMetricsDocument> {
  return getDb().collection<PipelineMetricsDocument>('pipeline_metrics');
}

export function rateLimitViolationsCollection(): Collection<{
  _id?: unknown;
  apiKeyTail: string;
  projectId: string;
  tenantId: string;
  violatedAt: Date;
  resetAt: Date;
}> {
  return getDb().collection('rate_limit_violations');
}

export async function healthCheck(): Promise<{ ok: boolean; latencyMs: number }> {
  const start = Date.now();
  try {
    await getClient().db('admin').command({ ping: 1 });
    return { ok: true, latencyMs: Date.now() - start };
  } catch (err) {
    logger.error({ err }, 'MongoDB healthCheck failed');
    return { ok: false, latencyMs: Date.now() - start };
  }
}

export async function close(): Promise<void> {
  if (_client !== null) {
    await _client.close();
    _client = null;
    logger.info('MongoDB client closed');
  }
}
