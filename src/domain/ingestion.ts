// This module is called by the ingest-worker for each event from the stream.
// It validates, enriches, writes to all stores, and runs percolation.

import { eventsCollection, pipelineMetricsCollection, type EventDocument } from '../db/mongo.js';
import { pool } from '../db/postgres.js';
import { redis } from '../db/redis.js';
import { esClient, indexName, percolatorIndex, ensureTemplateOnce } from '../db/elastic.js';
import { EventIngestSchema, generateFingerprint, type EventIngest } from '../schemas/event.js';
import { breakers } from '../lib/circuit-breaker.js';
import { logger } from '../logger.js';

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export interface IngestPayload {
  eventId: string;  // uuidv7, generated at HTTP edge
  traceId: string;  // generated at HTTP edge
  projectId: string;
  tenantId: string;
  planId: string;
  raw: EventIngest;
}

interface PipelineMetricEntry {
  stage: string;
  eventId: string;
  traceId: string;
  durationMs: number;
  success: boolean;
  error?: string;
}

// ---------------------------------------------------------------------------
// traceStage — timing + recording helper
// ---------------------------------------------------------------------------

export async function traceStage<T>(
  name: string,
  eventId: string,
  traceId: string,
  fn: () => Promise<T>,
): Promise<T> {
  const start = Date.now();
  let success = false;
  let errorMsg: string | undefined;

  try {
    const result = await fn();
    success = true;
    return result;
  } catch (err) {
    errorMsg = err instanceof Error ? err.message : String(err);
    throw err;
  } finally {
    const entry: PipelineMetricEntry = {
      stage: name,
      eventId,
      traceId,
      durationMs: Date.now() - start,
      success,
      ...(errorMsg !== undefined ? { error: errorMsg } : {}),
    };

    // Fire-and-forget — don't let metric recording block or fail the pipeline
    pipelineMetricsCollection()
      .insertOne({
        _id: `${eventId}:${name}`,
        projectId: '',
        pipelineId: traceId,
        stage: name,
        durationMs: entry.durationMs,
        status: success ? 'success' : 'failure',
        recordedAt: new Date(),
        meta: { eventId, traceId, error: errorMsg },
      })
      .catch((err) => {
        logger.warn({ err, stage: name, eventId }, 'failed to record pipeline metric');
      });
  }
}

// ---------------------------------------------------------------------------
// upsertMonthlyUsage — advisory-lock + dedup gate
// ---------------------------------------------------------------------------

const MAX_USAGE_RETRIES = 5;

async function upsertMonthlyUsage(
  tenantId: string,
  eventId: string,
  year: number,
  month: number,
): Promise<void> {
  // 1. Idempotency gate: INSERT INTO usage_dedup ON CONFLICT DO NOTHING
  const dedupResult = await pool.query<{ event_id: string }>(
    `
    INSERT INTO usage_dedup (event_id, tenant_id)
    VALUES ($1, $2)
    ON CONFLICT (event_id) DO NOTHING
    RETURNING event_id
    `,
    [eventId, tenantId],
  );

  // If 0 rows returned, this event was already counted — skip
  if (dedupResult.rowCount === 0) {
    logger.debug({ eventId, tenantId }, 'usage_dedup: duplicate event, skipping');
    return;
  }

  // 2. Acquire advisory lock on the monthly_usage slot
  // hashtext is a PG function returning int4 from a string
  const lockKey = `monthly_usage:${tenantId}:${year}:${month}`;

  for (let attempt = 1; attempt <= MAX_USAGE_RETRIES; attempt++) {
    const client = await pool.connect();
    try {
      await client.query('BEGIN');

      const lockResult = await client.query<{ acquired: boolean }>(
        `SELECT pg_try_advisory_xact_lock(hashtext($1)) AS acquired`,
        [lockKey],
      );

      const acquired = lockResult.rows[0]?.acquired ?? false;

      if (!acquired) {
        await client.query('ROLLBACK');
        // Exponential backoff + jitter
        const delay = Math.min(50 * Math.pow(2, attempt - 1), 500);
        const jitter = Math.floor(Math.random() * 50);
        await new Promise<void>((resolve) => setTimeout(resolve, delay + jitter));
        continue;
      }

      // 3. Upsert the monthly_usage counter
      await client.query(
        `
        INSERT INTO monthly_usage (tenant_id, year, month, event_count)
        VALUES ($1, $2, $3, 1)
        ON CONFLICT (tenant_id, year, month)
        DO UPDATE SET event_count = monthly_usage.event_count + 1
        `,
        [tenantId, year, month],
      );

      await client.query('COMMIT');
      return;
    } catch (err) {
      await client.query('ROLLBACK').catch(() => undefined);
      throw err;
    } finally {
      client.release();
    }
  }

  logger.error(
    { tenantId, eventId, year, month, maxRetries: MAX_USAGE_RETRIES },
    'upsertMonthlyUsage: failed to acquire advisory lock after max retries',
  );
}

// ---------------------------------------------------------------------------
// processEvent — main pipeline
// ---------------------------------------------------------------------------

export async function processEvent(payload: IngestPayload): Promise<void> {
  const { eventId, traceId, projectId, tenantId, raw } = payload;

  // Stage 1: validate
  let validated!: EventIngest;
  await traceStage('validate', eventId, traceId, async () => {
    validated = EventIngestSchema.parse(raw);
  });

  // Stage 2: enrich — build the full EventDocument
  let doc!: EventDocument;
  await traceStage('enrich', eventId, traceId, async () => {
    const now = new Date();
    const occurredAt = validated.occurredAt != null
      ? new Date(validated.occurredAt)
      : now;

    const fingerprint =
      validated.fingerprint ?? generateFingerprint(validated.type, validated.message);

    doc = {
      _id: eventId,
      projectId,
      type: validated.type,
      severity: validated.severity,
      message: validated.message,
      occurredAt,
      ingestedAt: now,
      fingerprint,
      traceId,
      ...(validated.stackTrace !== undefined ? {
        stackTrace: validated.stackTrace.map((f) => {
          const frame: import('../db/mongo.js').StackFrame = {};
          if (f.filename !== undefined) frame.file = f.filename;
          if (f.line !== undefined) frame.line = f.line;
          if (f.column !== undefined) frame.column = f.column;
          if (f.function !== undefined) frame.function = f.function;
          if (f.context !== undefined) frame.source = f.context;
          return frame;
        }),
      } : {}),
      ...(validated.tags !== undefined ? { tags: validated.tags } : {}),
      ...(validated.userContext !== undefined ? {
        userContext: (() => {
          const uc: EventDocument['userContext'] = {};
          if (validated.userContext?.userId !== undefined) uc!.userId = validated.userContext.userId;
          if (validated.userContext?.email !== undefined) uc!.email = validated.userContext.email;
          if (validated.userContext?.ip !== undefined) uc!.ip = validated.userContext.ip;
          return uc;
        })(),
      } : {}),
      ...(validated.deviceContext !== undefined ? {
        deviceContext: (() => {
          const dc: EventDocument['deviceContext'] = {};
          if (validated.deviceContext?.os !== undefined) dc!.os = validated.deviceContext.os;
          if (validated.deviceContext?.browser !== undefined) dc!.browser = validated.deviceContext.browser;
          if (validated.deviceContext?.version !== undefined) dc!.version = validated.deviceContext.version;
          return dc;
        })(),
      } : {}),
      ...(validated.payload !== undefined ? { payload: validated.payload } : {}),
    };
  });

  // Stage 3: insert to MongoDB — canonical write; failure causes event to stay in stream for retry
  await traceStage('mongo', eventId, traceId, async () => {
    await breakers.mongo.run(() =>
      eventsCollection().replaceOne(
        { _id: eventId },
        doc,
        { upsert: true },
      ),
    );
  });

  // Stage 4: index to Elasticsearch — best-effort; if ES is down skip and continue
  // (ES is a projection of Mongo; it can be rebuilt by replay)
  await traceStage('elasticsearch', eventId, traceId, async () => {
    try {
      await breakers.elasticsearch.run(async () => {
        await ensureTemplateOnce(); // registers our template to override built-in data-stream template
        const idx = indexName(projectId, doc.occurredAt);
        // Exclude _id: it's a MongoDB metadata field that ES rejects in document body.
        const { _id: _mongoId, ...docForEs } = doc;
        await esClient.index({
          index: idx,
          id: eventId,
          document: {
            ...docForEs,
            occurredAt: doc.occurredAt.toISOString(),
            ingestedAt: doc.ingestedAt.toISOString(),
          },
          op_type: 'index', // idempotent upsert-by-id
        });
      });
    } catch (err) {
      // ES down: log and continue — Mongo is canonical, ES can be replayed
      logger.warn({ eventId, err }, 'ES indexing skipped (circuit open or ES unavailable)');
    }
  });

  // Stage 5: PG usage accounting — best-effort; if PG is down push to retry queue
  await traceStage('pg-usage', eventId, traceId, async () => {
    try {
      await breakers.postgres.run(async () => {
        const now = new Date();
        await upsertMonthlyUsage(
          tenantId,
          eventId,
          now.getUTCFullYear(),
          now.getUTCMonth() + 1,
        );
      });
    } catch (err) {
      // PG down: queue the increment for the jobs process to drain later
      await redis.rpush(`usage:retry:${tenantId}`, eventId).catch(() => {});
      logger.warn({ eventId, tenantId, err }, 'PG usage deferred to retry queue (PG down)');
    }
  });

  // Stage 6: ES percolator check — best-effort; skip if ES unavailable
  await traceStage('percolate', eventId, traceId, async () => {
    try {
      const percolateDoc = {
        message: doc.message,
        severity: doc.severity,
        fingerprint: doc.fingerprint,
        projectId,
        occurredAt: doc.occurredAt.toISOString(),
        ...(doc.tags !== undefined ? { tags: doc.tags } : {}),
        ...(doc.payload !== undefined ? { payload: doc.payload } : {}),
      };

      await breakers.elasticsearch.run(async () => {
        const response = await esClient.search({
          index: percolatorIndex,
          query: {
            percolate: {
              field: 'query',
              document: percolateDoc,
            },
          },
          _source: true,
        });

        const hits = response.hits.hits;
        if (hits.length > 0) {
          const pipeline = redis.pipeline();
          for (const hit of hits) {
            pipeline.xadd(
              'alert-matches',
              '*',
              'alertRuleId', hit._id ?? '',
              'eventId', eventId,
              'projectId', projectId,
              'tenantId', tenantId,
              'fingerprint', doc.fingerprint,
              'severity', doc.severity,
              'message', doc.message.slice(0, 500),
            );
          }
          await pipeline.exec();
          logger.info(
            { eventId, matchCount: hits.length, projectId },
            'percolator matches published to alert-matches stream',
          );
        }
      });
    } catch (err) {
      logger.warn({ eventId, err }, 'percolation skipped (ES unavailable)');
    }
  });

  // Stage 7: leaderboard ZINCRBY for per-project daily event counts
  await traceStage('leaderboard', eventId, traceId, async () => {
    const d = doc.occurredAt;
    const dateKey = `${d.getUTCFullYear()}-${String(d.getUTCMonth() + 1).padStart(2, '0')}-${String(d.getUTCDate()).padStart(2, '0')}`;
    await redis.zincrby(`leaderboard:${dateKey}`, 1, projectId);
  });

  logger.debug({ eventId, projectId, traceId }, 'event processed successfully');
}
