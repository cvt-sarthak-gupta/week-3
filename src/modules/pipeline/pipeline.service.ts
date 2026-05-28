import type { PostgresPool } from '../../db/postgres/index.js';
import type { MongoDatabase, EventDocument, StackFrame } from '../../db/mongo/index.js';
import type { ElasticClient } from '../../db/elastic/index.js';
import type { RedisClient } from '../../db/redis/index.js';
import type { CircuitBreakers } from '../../lib/circuit-breaker/index.js';
import { logger } from '../../utils/logger.js';
import { generateFingerprint } from '../../utils/fingerprint.js';
import { EventIngestSchema } from '../../schemas/event.js';
import type { IngestPayload } from './pipeline.types.js';

interface PipelineMetricEntry {
  stage: string;
  eventId: string;
  traceId: string;
  durationMs: number;
  success: boolean;
  error?: string;
}

const MAX_USAGE_RETRIES = 5;

export class PipelineService {
  constructor(
    private readonly pool: PostgresPool,
    private readonly mongo: MongoDatabase,
    private readonly es: ElasticClient,
    private readonly redis: RedisClient,
    private readonly breakers: CircuitBreakers,
  ) {}

  async processEvent(payload: IngestPayload): Promise<void> {
    const { eventId, traceId, projectId, tenantId, raw } = payload;

    // Stage 1: validate
    let validated!: ReturnType<typeof EventIngestSchema.parse>;
    await this.traceStage('validate', eventId, traceId, async () => {
      validated = EventIngestSchema.parse(raw);
    });

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
          stackTrace: validated.stackTrace.map((f): StackFrame => {
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
    });

    // Stage 3: insert to MongoDB — canonical write; failure causes event to stay in stream for retry
    await this.traceStage('mongo', eventId, traceId, async () => {
      await this.breakers.mongo.run(() =>
        this.mongo.events().replaceOne(
          { _id: eventId },
          doc,
          { upsert: true },
        ),
      );
    });

    // Stage 4: index to Elasticsearch — best-effort; if ES is down skip and continue
    // (ES is a projection of Mongo; it can be rebuilt by replay)
    await this.traceStage('elasticsearch', eventId, traceId, async () => {
      try {
        await this.breakers.elasticsearch.run(async () => {
          await this.es.ensureTemplateOnce(); // registers our template to override built-in data-stream template
          const idx = this.es.indexName(projectId, doc.occurredAt);
          // Exclude _id: it's a MongoDB metadata field that ES rejects in document body.
          const { _id: _mongoId, ...docForEs } = doc;
          await this.es.client.index({
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
    });

    // Stage 6: ES percolator check — best-effort; skip if ES unavailable
    await this.traceStage('percolate', eventId, traceId, async () => {
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

        await this.breakers.elasticsearch.run(async () => {
          const response = await this.es.client.search({
            index: this.es.percolatorIndex,
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
            const pipeline = this.redis.client.pipeline();
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
    await this.traceStage('leaderboard', eventId, traceId, async () => {
      const d = doc.occurredAt;
      const dateKey = `${d.getUTCFullYear()}-${String(d.getUTCMonth() + 1).padStart(2, '0')}-${String(d.getUTCDate()).padStart(2, '0')}`;
      await this.redis.client.zincrby(`leaderboard:${dateKey}`, 1, projectId);
    });

    logger.debug({ eventId, projectId, traceId }, 'event processed successfully');
  }

  private async traceStage<T>(
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
      this.mongo.pipelineMetrics()
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

  private async upsertMonthlyUsage(
    tenantId: string,
    eventId: string,
    year: number,
    month: number,
  ): Promise<void> {
    // 1. Idempotency gate: INSERT INTO usage_dedup ON CONFLICT DO NOTHING
    const dedupResult = await this.pool.query<{ event_id: string }>(
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
      const client = await this.pool.pool.connect();
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
}
