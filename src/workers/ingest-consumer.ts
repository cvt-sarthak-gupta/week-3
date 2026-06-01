import { STREAM_KEY, DLQ_KEY, CONSUMER_GROUP } from '../db/redis.js';
import type { RedisDatabase } from '../db/redis.js';
import type { IngestionService, IngestPayload } from '../domain/ingestion.js';
import type { EventIngest } from '../schemas/event.js';
import { createWorkerLogger } from '../logger.js';

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

export class IngestConsumer {
  private _running = false;
  private readonly BATCH_SIZE = 64;
  private readonly BLOCK_MS = 5000;
  private readonly MAX_RETRIES = 3;
  private readonly RECLAIM_IDLE_MS = 30_000;

  constructor(
    private readonly redis: RedisDatabase,
    private readonly ingestion: IngestionService,
    /** Unique consumer name within the group.  Defaults to pid for single-worker compat. */
    private readonly consumerName: string = `ingest-consumer-${process.pid}`,
  ) {}

  async start(): Promise<void> {
    const workerName = this.consumerName;
    const log = createWorkerLogger(workerName);
    log.info({ stream: STREAM_KEY, group: CONSUMER_GROUP }, 'Ingest consumer starting');

    await this.redis.ensureConsumerGroup(STREAM_KEY, CONSUMER_GROUP);

    this._running = true;
    let lastReclaimAt = Date.now();

    while (this._running) {
      // Periodic reclaim of idle/orphaned PEL entries
      if (Date.now() - lastReclaimAt >= this.RECLAIM_IDLE_MS) {
        lastReclaimAt = Date.now();
        try {
          // XPENDING: returns array of [msgId, consumerName, idleMs, deliveryCount]
          const pendingEntries = await this.redis.client.xpending(
            STREAM_KEY,
            CONSUMER_GROUP,
            '-',
            '+',
            10,
          ) as Array<[string, string, number, number]>;

          for (const entry of pendingEntries) {
            const [msgId, , idleMs] = entry;
            if (msgId === undefined) continue;
            if ((idleMs ?? 0) >= this.RECLAIM_IDLE_MS) {
              try {
                await this.redis.client.xclaim(
                  STREAM_KEY,
                  CONSUMER_GROUP,
                  workerName,
                  this.RECLAIM_IDLE_MS,
                  msgId,
                );
                log.debug({ msgId }, 'Reclaimed idle stream message');
              } catch (claimErr) {
                log.warn({ err: claimErr, msgId }, 'Failed to XCLAIM idle message');
              }
            }
          }
        } catch (pendingErr) {
          log.warn({ err: pendingErr }, 'XPENDING scan failed');
        }
      }

      let streamResults: Array<[string, Array<[string, string[]]>]> | null = null;
      try {
        // XREADGROUP returns: [[streamName, [[msgId, [f,v,...]], ...]]] or null
        streamResults = await this.redis.client.xreadgroup(
          'GROUP',
          CONSUMER_GROUP,
          workerName,
          'COUNT',
          this.BATCH_SIZE,
          'BLOCK',
          this.BLOCK_MS,
          'STREAMS',
          STREAM_KEY,
          '>',
        ) as Array<[string, Array<[string, string[]]>]> | null;
      } catch (readErr) {
        if (!this._running) break;
        log.error({ err: readErr }, 'XREADGROUP failed');
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
        if (!this._running) break;
        if (msgId === undefined || fields === undefined) continue;

        const data = fieldsToObject(fields);
        const retryKey = `retry:${msgId}`;

        try {
          // Reconstruct raw EventIngest from the individual flat fields the ingest
          // route writes to the stream (it never writes a single 'raw' JSON blob).
          const safeJsonParse = <T>(s: string | undefined): T | undefined => {
            if (!s) return undefined;
            try { return JSON.parse(s) as T; } catch { return undefined; }
          };

          const payload: IngestPayload = {
            eventId:   data['eventId']  ?? msgId,
            traceId:   data['traceId']  ?? '',
            projectId: data['projectId'] ?? '',
            tenantId:  data['tenantId'] ?? '',
            planId:    data['planId']   ?? '',
            raw: {
              type:          (data['type']     ?? 'custom') as EventIngest['type'],
              severity:      (data['severity'] ?? 'info')   as EventIngest['severity'],
              message:        data['message']  ?? '',
              ...(data['occurredAt']    ? { occurredAt:    data['occurredAt'] }                                                  : {}),
              ...(data['fingerprint']   ? { fingerprint:   data['fingerprint'] }                                                 : {}),
              ...(data['stackTrace']    ? { stackTrace:    safeJsonParse<EventIngest['stackTrace']>(data['stackTrace']) }        : {}),
              ...(data['tags']          ? { tags:          safeJsonParse<EventIngest['tags']>(data['tags']) }                    : {}),
              ...(data['userContext']   ? { userContext:   safeJsonParse<EventIngest['userContext']>(data['userContext']) }       : {}),
              ...(data['deviceContext'] ? { deviceContext: safeJsonParse<EventIngest['deviceContext']>(data['deviceContext']) }  : {}),
              ...(data['payload']       ? { payload:       safeJsonParse<EventIngest['payload']>(data['payload']) }              : {}),
            },
          };

          await this.ingestion.processEvent(payload);

          // ACK on success and clean up retry counter
          await this.redis.client.xack(STREAM_KEY, CONSUMER_GROUP, msgId);
          await this.redis.client.del(retryKey);

          log.debug({ msgId, eventId: payload.eventId }, 'Message processed and ACKed');
        } catch (procErr) {
          log.error({ err: procErr, msgId }, 'Failed to process stream message');

          // Increment retry counter
          const retryCount = await this.redis.client.hincrby(retryKey, 'count', 1);
          await this.redis.client.expire(retryKey, 3600); // expire retry keys after 1h

          if (retryCount > this.MAX_RETRIES) {
            // Move to DLQ and ACK
            log.warn({ msgId, retryCount }, 'Max retries exceeded — moving to DLQ');
            const dlqFields: string[] = [];
            for (const [k, v] of Object.entries(data)) {
              dlqFields.push(k, v);
            }
            dlqFields.push('original_msg_id', msgId);
            dlqFields.push('failed_at', new Date().toISOString());
            dlqFields.push('error', procErr instanceof Error ? procErr.message : String(procErr));

            // Write to DLQ first; only ACK if the DLQ write succeeded.
            // Using a pipeline reduces the crash window but xadd result is checked
            // explicitly so a Redis OOM doesn't silently lose the message.
            const dlqPipeline = this.redis.client.pipeline();
            dlqPipeline.xadd(DLQ_KEY, '*', ...dlqFields);
            const dlqResults = await dlqPipeline.exec();
            const dlqErr = dlqResults?.[0]?.[0];
            if (dlqErr instanceof Error) {
              log.error({ err: dlqErr, msgId }, 'DLQ write failed — retaining in PEL for reclaim');
            } else {
              await this.redis.client.xack(STREAM_KEY, CONSUMER_GROUP, msgId);
              await this.redis.client.del(retryKey);
            }
          }
          // If under retry limit, do not ACK — message stays in PEL for reclaim
        }
      }
    }

    log.info('Ingest consumer stopped');
  }

  stop(): void {
    this._running = false;
  }
}
