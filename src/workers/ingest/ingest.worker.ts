import type { RedisClient } from '../../db/redis/index.js';
import type { MongoDatabase } from '../../db/mongo/index.js';
import type { ElasticClient } from '../../db/elastic/index.js';
import type { PostgresPool } from '../../db/postgres/index.js';
import type { PipelineService } from '../../modules/pipeline/index.js';
import type { IngestPayload } from '../../modules/pipeline/pipeline.types.js';
import { createWorkerLogger } from '../../utils/logger.js';
import type { Logger } from 'pino';

const BATCH_SIZE = 64;
const BLOCK_MS = 5000;
const MAX_RETRIES = 3;
const RECLAIM_IDLE_MS = 30_000;
const RECLAIM_INTERVAL_MS = 30_000;

/**
 * Convert a flat Redis stream field array [ 'key', 'value', ... ] into a
 * plain object. Redis returns alternating key/value string pairs.
 */
function fieldsToObject(fields: string[]): Record<string, string> {
  const obj: Record<string, string> = {};
  for (let i = 0; i < fields.length - 1; i += 2) {
    const key = fields[i];
    const val = fields[i + 1];
    if (key !== undefined && val !== undefined) {
      obj[key] = val;
    }
  }
  return obj;
}

export class IngestWorker {
  private readonly log: Logger;
  private running = false;
  private readonly workerName: string;

  constructor(
    private readonly redis: RedisClient,
    private readonly mongo: MongoDatabase,
    private readonly es: ElasticClient,
    private readonly pool: PostgresPool,
    private readonly pipeline: PipelineService,
  ) {
    // Use WORKER_INDEX env var if set, otherwise default to worker-0
    const workerIndex = process.env['WORKER_INDEX'] ?? '0';
    this.workerName = `worker-${workerIndex}`;
    this.log = createWorkerLogger(this.workerName);
  }

  async start(): Promise<void> {
    this.log.info(
      { stream: this.redis.STREAM_KEY, group: this.redis.CONSUMER_GROUP },
      'Ingest consumer starting',
    );

    await this.redis.ensureConsumerGroup();

    this.running = true;
    let lastReclaimAt = Date.now();

    // Graceful shutdown
    process.once('SIGTERM', () => {
      this.log.info('SIGTERM received — stopping ingest consumer after current batch');
      this.running = false;
    });

    await this.runLoop(lastReclaimAt, (t) => { lastReclaimAt = t; });

    this.log.info('Ingest consumer stopped');
  }

  async stop(): Promise<void> {
    this.running = false;
    this.log.info('IngestWorker stop requested — draining current batch');
  }

  private async runLoop(
    initialLastReclaimAt: number,
    setLastReclaimAt: (t: number) => void,
  ): Promise<void> {
    let lastReclaimAt = initialLastReclaimAt;

    while (this.running) {
      // Periodic reclaim of idle/orphaned PEL entries
      if (Date.now() - lastReclaimAt >= RECLAIM_INTERVAL_MS) {
        lastReclaimAt = Date.now();
        setLastReclaimAt(lastReclaimAt);
        try {
          // XPENDING: returns array of [msgId, consumerName, idleMs, deliveryCount]
          const pendingEntries = await this.redis.client.xpending(
            this.redis.STREAM_KEY,
            this.redis.CONSUMER_GROUP,
            '-',
            '+',
            10,
          ) as Array<[string, string, number, number]>;

          for (const entry of pendingEntries) {
            const [msgId, , idleMs] = entry;
            if (msgId === undefined) continue;
            if ((idleMs ?? 0) >= RECLAIM_IDLE_MS) {
              try {
                await this.redis.client.xclaim(
                  this.redis.STREAM_KEY,
                  this.redis.CONSUMER_GROUP,
                  this.workerName,
                  RECLAIM_IDLE_MS,
                  msgId,
                );
                await this.redis.incrCounter('ingest.reclaim.count');
                this.log.debug({ msgId }, 'Reclaimed idle stream message');
              } catch (claimErr) {
                this.log.warn({ err: claimErr, msgId }, 'Failed to XCLAIM idle message');
              }
            }
          }
        } catch (pendingErr) {
          this.log.warn({ err: pendingErr }, 'XPENDING scan failed');
        }
      }

      let streamResults: Array<[string, Array<[string, string[]]>]> | null = null;
      try {
        // XREADGROUP returns: [[streamName, [[msgId, [f,v,...]], ...]]] or null
        streamResults = await this.redis.client.xreadgroup(
          'GROUP',
          this.redis.CONSUMER_GROUP,
          this.workerName,
          'COUNT',
          BATCH_SIZE,
          'BLOCK',
          BLOCK_MS,
          'STREAMS',
          this.redis.STREAM_KEY,
          '>',
        ) as Array<[string, Array<[string, string[]]>]> | null;
      } catch (readErr) {
        if (!this.running) break;
        this.log.error({ err: readErr }, 'XREADGROUP failed');
        // Brief pause before retry to avoid tight error loops
        await new Promise<void>((resolve) => setTimeout(resolve, 1_000));
        continue;
      }

      if (streamResults === null || streamResults.length === 0) {
        // Blocked and timed out — loop again
        continue;
      }

      const [, messages] = streamResults[0] ?? ['', []];
      if (messages === undefined) continue;

      for (const [msgId, fields] of messages) {
        if (!this.running) break;
        if (msgId === undefined || fields === undefined) continue;

        const data = fieldsToObject(fields);
        const retryKey = `retry:${msgId}`;

        try {
          // Parse and dispatch the event payload.
          // The stream encodes individual event fields; reconstruct the raw shape
          // that PipelineService.processEvent() expects.
          const rawType = data['type'] as IngestPayload['raw']['type'] | undefined;
          const rawSeverity = data['severity'] as IngestPayload['raw']['severity'] | undefined;

          const rawPayload: IngestPayload['raw'] = {
            type: rawType ?? 'custom',
            severity: rawSeverity ?? 'info',
            message: data['message'] ?? '',
          };

          if (data['occurredAt'] !== undefined && data['occurredAt'] !== '') {
            rawPayload.occurredAt = data['occurredAt'];
          }
          if (data['fingerprint'] !== undefined && data['fingerprint'] !== '') {
            rawPayload.fingerprint = data['fingerprint'];
          }
          if (data['stackTrace'] !== undefined && data['stackTrace'] !== '') {
            const parsedStackTrace = JSON.parse(data['stackTrace']) as Array<{
              filename?: string;
              function?: string;
              line?: number;
              column?: number;
              context?: string;
            }>;
            rawPayload.stackTrace = parsedStackTrace;
          }
          if (data['tags'] !== undefined && data['tags'] !== '') {
            rawPayload.tags = JSON.parse(data['tags']) as Record<string, string>;
          }
          if (data['userContext'] !== undefined && data['userContext'] !== '') {
            const parsedUserContext = JSON.parse(data['userContext']) as {
              userId?: string;
              email?: string;
              ip?: string;
            };
            rawPayload.userContext = parsedUserContext;
          }
          if (data['deviceContext'] !== undefined && data['deviceContext'] !== '') {
            const parsedDeviceContext = JSON.parse(data['deviceContext']) as {
              os?: string;
              browser?: string;
              version?: string;
            };
            rawPayload.deviceContext = parsedDeviceContext;
          }
          if (data['payload'] !== undefined && data['payload'] !== '') {
            rawPayload.payload = JSON.parse(data['payload']) as Record<string, unknown>;
          }

          const payload: IngestPayload = {
            eventId: data['eventId'] ?? msgId,
            traceId: data['traceId'] ?? '',
            projectId: data['projectId'] ?? '',
            tenantId: data['tenantId'] ?? '',
            planId: data['planId'] ?? '',
            raw: rawPayload,
          };

          await this.pipeline.processEvent(payload);

          // ACK on success and clean up retry counter
          await this.redis.client.xack(this.redis.STREAM_KEY, this.redis.CONSUMER_GROUP, msgId);
          await this.redis.client.del(retryKey);

          this.log.debug({ msgId, eventId: payload.eventId }, 'Message processed and ACKed');
        } catch (procErr) {
          this.log.error({ err: procErr, msgId }, 'Failed to process stream message');

          // Increment retry counter
          const retryCount = await this.redis.client.hincrby(retryKey, 'count', 1);
          await this.redis.client.expire(retryKey, 3600); // expire retry keys after 1h

          if (retryCount > MAX_RETRIES) {
            // Move to DLQ and ACK
            this.log.warn({ msgId, retryCount }, 'Max retries exceeded — moving to DLQ');
            const dlqFields: string[] = [];
            for (const [k, v] of Object.entries(data)) {
              dlqFields.push(k, v);
            }
            dlqFields.push('original_msg_id', msgId);
            dlqFields.push('failed_at', new Date().toISOString());
            dlqFields.push('error', procErr instanceof Error ? procErr.message : String(procErr));

            await this.redis.client.xadd(this.redis.STREAM_DLQ, '*', ...dlqFields);
            await this.redis.client.xack(this.redis.STREAM_KEY, this.redis.CONSUMER_GROUP, msgId);
            await this.redis.client.del(retryKey);
            await this.redis.incrCounter('ingest.dlq.count');
          }
          // If under retry limit, do not ACK — message stays in PEL for reclaim
        }
      }
    }
  }
}
