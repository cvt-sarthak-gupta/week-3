import type { PostgresDatabase } from '../db/postgres.js';
import type { MongoDatabase } from '../db/mongo.js';
import type { RedisDatabase } from '../db/redis.js';
import { ElasticsearchDatabase, percolatorIndex } from '../db/elastic.js';
import type { CircuitBreakerRegistry } from '../lib/circuit-breaker.js';
import { EventIngestSchema, generateFingerprint, type EventIngest } from '../schemas/event.js';
import type { EventDocument, StackFrame } from '../db/mongo.js';
import { logger } from '../logger.js';
import type { IngestPayload, PipelineMetricEntry } from '../types/ingestion.js';

export type { IngestPayload };

const MAX_USAGE_RETRIES = 5;

export class IngestionService {
  private readonly pg: PostgresDatabase;
  private readonly mongo: MongoDatabase;
  private readonly redis: RedisDatabase;
  private readonly es: ElasticsearchDatabase;
  private readonly breakers: CircuitBreakerRegistry;

  constructor(
    pg: PostgresDatabase,
    mongo: MongoDatabase,
    redis: RedisDatabase,
    es: ElasticsearchDatabase,
    breakers: CircuitBreakerRegistry,
  ) {
    this.pg = pg;
    this.mongo = mongo;
    this.redis = redis;
    this.es = es;
    this.breakers = breakers;
  }

  async traceStage<T>(
    name: string,
    eventId: string,
    traceId: string,
    fn: () => Promise<T>,
    projectId = '',
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

      // Fire-and-forget — don't let metric recording block or fail the pipeline.
      // replaceOne+upsert is idempotent: replaying the same eventId (e.g. stream retry)
      // overwrites the existing metric instead of throwing a duplicate key error.
      const metricDoc = {
        _id: `${eventId}:${name}`,
        projectId,
        pipelineId: traceId,
        stage: name,
        durationMs: entry.durationMs,
        status: (success ? 'success' : 'failure') as 'success' | 'failure',
        recordedAt: new Date(),
        meta: { eventId, traceId, error: errorMsg },
      };
      this.mongo
        .pipelineMetrics()
        .replaceOne({ _id: metricDoc._id }, metricDoc, { upsert: true })
        .catch((err) => {
          logger.warn({ err, stage: name, eventId }, 'failed to record pipeline metric');
        });
    }
  }

  private async upsertMonthlyUsage(
    tenantId: string,
    eventId: string,
    year: number,
    month: number,
  ): Promise<void> {
    // The idempotency gate (usage_dedup) and the counter increment (monthly_usage)
    // must be inside the same transaction as the advisory lock.  Moving dedup inside
    // the lock prevents the bug where dedup commits but the counter never increments:
    // if we exhaust retries and throw, dedup was never written so the retry queue
    // job can succeed on the next attempt.
    const lockKey = `monthly_usage:${tenantId}:${year}:${month}`;

    for (let attempt = 1; attempt <= MAX_USAGE_RETRIES; attempt++) {
      const client = await this.pg.connect();
      try {
        await client.query('BEGIN');

        // 1. Try to acquire advisory lock on the monthly_usage slot.
        const lockResult = await client.query<{ acquired: boolean }>(
          `SELECT pg_try_advisory_xact_lock(hashtext($1)) AS acquired`,
          [lockKey],
        );

        const acquired = lockResult.rows[0]?.acquired ?? false;

        if (!acquired) {
          await client.query('ROLLBACK');
          // Exponential backoff + jitter before next attempt.
          const delay = Math.min(50 * Math.pow(2, attempt - 1), 500);
          const jitter = Math.floor(Math.random() * 50);
          await new Promise<void>((resolve) => setTimeout(resolve, delay + jitter));
          continue;
        }

        // 2. Idempotency gate: only write if this event hasn't been counted yet.
        //    Runs inside the lock so that dedup and the counter update are atomic.
        const dedupResult = await client.query<{ event_id: string }>(
          `INSERT INTO usage_dedup (event_id, tenant_id)
           VALUES ($1, $2)
           ON CONFLICT (event_id) DO NOTHING
           RETURNING event_id`,
          [eventId, tenantId],
        );

        if ((dedupResult.rowCount ?? 0) === 0) {
          // Already counted — release the lock and exit cleanly.
          await client.query('ROLLBACK');
          logger.debug({ eventId, tenantId }, 'usage_dedup: duplicate event, skipping');
          return;
        }

        // 3. Upsert the monthly_usage counter.
        await client.query(
          `INSERT INTO monthly_usage (tenant_id, year, month, event_count)
           VALUES ($1, $2, $3, 1)
           ON CONFLICT (tenant_id, year, month)
           DO UPDATE SET event_count = monthly_usage.event_count + 1`,
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

    // All retries exhausted — throw so the pg-usage stage in processEvent pushes
    // the eventId to the retry queue.  Because dedup was never committed, a future
    // retry will be able to count this event correctly.
    throw new Error(
      `upsertMonthlyUsage: failed to acquire advisory lock after ${MAX_USAGE_RETRIES} retries ` +
      `(tenantId=${tenantId}, eventId=${eventId}, year=${year}, month=${month})`,
    );
  }

  async processEvent(payload: IngestPayload): Promise<void> {
    const { eventId, traceId, projectId, tenantId, raw } = payload;

    // Stage 1: validate
    let validated!: EventIngest;
    await this.traceStage('validate', eventId, traceId, async () => {
      validated = EventIngestSchema.parse(raw);
    }, projectId);

    // Stage 2: enrich — build the full EventDocument
    let doc!: EventDocument;
    await this.traceStage('enrich', eventId, traceId, async () => {
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
            const frame: StackFrame = {};
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
    }, projectId);

    // Stage 3: insert to MongoDB — canonical write; failure causes event to stay in stream for retry
    await this.traceStage('mongo', eventId, traceId, async () => {
      await this.breakers.mongo.run(() =>
        this.mongo.events().replaceOne(
          { _id: eventId },
          doc,
          { upsert: true },
        ),
      );
    }, projectId);

    // Stage 4: index to Elasticsearch — best-effort; if ES is down skip and continue
    // (ES is a projection of Mongo; it can be rebuilt by replay)
    await this.traceStage('elasticsearch', eventId, traceId, async () => {
      try {
        await this.breakers.elasticsearch.run(async () => {
          await this.es.ensureSetup(); // registers our template to override built-in data-stream template
          const idx = this.es.indexName(projectId, doc.occurredAt);
          // Exclude _id (Mongo metadata) and tags (needs format conversion for ES).
          const { _id: _mongoId, tags: _mongoTags, ...docForEsBase } = doc;
          // ES mapping defines tags as nested[{key,value}] but EventDocument stores
          // tags as Record<string,string>. Convert before indexing so nested queries work.
          const tagsForEs = _mongoTags !== undefined
            ? Object.entries(_mongoTags).map(([key, value]) => ({ key, value }))
            : undefined;
          // ES maps stackTrace as `text` (a searchable string), but EventDocument
          // stores it as StackFrame[]. Serialize to a readable format before indexing
          // so ES can parse the document. Without this, any event with a stackTrace
          // throws document_parsing_exception and is silently dropped from ES.
          const { stackTrace: stackFrames, ...docWithoutStack } = docForEsBase;
          const stackTraceText = stackFrames !== undefined
            ? (stackFrames as StackFrame[]).map((f) =>
                `  at ${f.function ?? '<anonymous>'} (${f.file ?? '<unknown>'}:${f.line ?? 0}:${f.column ?? 0})`
              ).join('\n')
            : undefined;

          await this.es.client.index({
            index: idx,
            id: eventId,
            document: {
              ...docWithoutStack,
              ...(stackTraceText !== undefined ? { stackTrace: stackTraceText } : {}),
              ...(tagsForEs !== undefined ? { tags: tagsForEs } : {}),
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
    }, projectId);

    // Stage 5: PG usage accounting — best-effort; if PG is down push to retry queue
    await this.traceStage('pg-usage', eventId, traceId, async () => {
      try {
        await this.breakers.postgres.run(async () => {
          const now = new Date();
          await this.upsertMonthlyUsage(
            tenantId,
            eventId,
            now.getUTCFullYear(),
            now.getUTCMonth() + 1,
          );
        });
      } catch (err) {
        // PG down: queue the increment for the jobs process to drain later
        await this.redis.client.rpush(`usage:retry:${tenantId}`, eventId).catch(() => {});
        logger.warn({ eventId, tenantId, err }, 'PG usage deferred to retry queue (PG down)');
      }
    }, projectId);

    // Stage 6: ES percolator check — async fire-and-forget.
    // Percolation runs against all events (not just fatal) to catch rules on any severity.
    // Results are published to the same Redis Pub/Sub channel the AlertSubscriber already
    // listens on (alerts:fatal:<projectId>).  The dedup lock in fireDedupAlert() prevents
    // double-firing for fatal events that also come through the MongoDB change stream path.
    // Previously this wrote to an unconsumed `alert-matches` Redis stream, causing unbounded
    // memory growth and silent alert drops for non-fatal matched events.
    void this.traceStage('percolate', eventId, traceId, async () => {
      try {
        // Convert tags from Record<string,string> to nested [{key,value}] format
        // that matches the ES percolator index mapping (type: nested).
        const tagsForPercolate = doc.tags !== undefined
          ? Object.entries(doc.tags).map(([key, value]) => ({ key, value }))
          : undefined;

        const percolateDoc = {
          message: doc.message,
          severity: doc.severity,
          fingerprint: doc.fingerprint,
          projectId,
          occurredAt: doc.occurredAt.toISOString(),
          ...(tagsForPercolate !== undefined ? { tags: tagsForPercolate } : {}),
          ...(doc.payload !== undefined ? { payload: doc.payload } : {}),
        };

        await this.breakers.elasticsearch.run(async () => {
          const response = await this.es.client.search({
            index: percolatorIndex,
            query: {
              bool: {
                must: [
                  {
                    percolate: {
                      field: 'query',
                      document: percolateDoc,
                    },
                  },
                  // Scope to this project so rules from other tenants never match
                  { term: { projectId } },
                ],
              },
            },
            _source: true,
          });

          const hits = response.hits.hits;
          if (hits.length > 0) {
            // Publish the event doc to the alerts channel so the AlertSubscriber
            // fires the matched rules via its dedup-fire lock mechanism.
            // Use PUBLISH (Pub/Sub) not XADD (stream) so messages are not accumulated
            // without a consumer.
            // Include _matchedRuleIds so the subscriber can skip a redundant ES
            // percolation call (fatal events otherwise trigger 3 total calls:
            // here + change-stream publish + subscriber re-percolation).
            const channel = `alerts:fatal:${projectId}`;
            const matchedRuleIds = hits.map((h) => h._id).filter(Boolean);
            await this.redis.client.publish(channel, JSON.stringify({
              ...doc,
              // occurredAt/ingestedAt must be strings so JSON.parse in the subscriber works
              occurredAt: doc.occurredAt.toISOString(),
              ingestedAt: doc.ingestedAt.toISOString(),
              _matchedRuleIds: matchedRuleIds,
            }));
            logger.info(
              { eventId, matchCount: hits.length, projectId, channel },
              'percolator matches published to alert channel',
            );
          }
        });
      } catch (err) {
        logger.warn({ eventId, err }, 'percolation skipped (ES unavailable)');
      }
    }, projectId).catch((err) => {
      logger.warn({ eventId, projectId, err }, 'percolation stage threw unexpectedly');
    });

    // Stage 7: leaderboard ZINCRBY for per-project daily event counts.
    // EXPIREAT to next midnight UTC ensures each daily key is cleaned up automatically.
    await this.traceStage('leaderboard', eventId, traceId, async () => {
      const d = doc.occurredAt;
      const dateKey = `${d.getUTCFullYear()}-${String(d.getUTCMonth() + 1).padStart(2, '0')}-${String(d.getUTCDate()).padStart(2, '0')}`;
      const leaderboardKey = `leaderboard:${dateKey}`;

      const nextMidnightUtc = new Date(d);
      nextMidnightUtc.setUTCHours(24, 0, 0, 0); // rolls to next day at 00:00:00.000 UTC
      const midnightUnixSeconds = Math.floor(nextMidnightUtc.getTime() / 1000);

      const pipeline = this.redis.client.pipeline();
      pipeline.zincrby(leaderboardKey, 1, projectId);
      pipeline.expireat(leaderboardKey, midnightUnixSeconds);
      await pipeline.exec();
    }, projectId);

    logger.debug({ eventId, projectId, traceId }, 'event processed successfully');
  }
}
