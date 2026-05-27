import { redis, STREAM_KEY, STREAM_DLQ, CONSUMER_GROUP, ensureConsumerGroup } from '../db/redis.js';
import { processEvent, type IngestPayload } from '../domain/ingestion.js';
import { incrCounter } from '../db/redis.js';
import { createWorkerLogger } from '../logger.js';

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const BATCH_SIZE = 64;
const BLOCK_MS = 5000;
const MAX_RETRIES = 3;
const RECLAIM_IDLE_MS = 30_000;
const RECLAIM_INTERVAL_MS = 30_000;

// ---------------------------------------------------------------------------
// Parsing helpers
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// runIngestConsumer
// ---------------------------------------------------------------------------

export async function runIngestConsumer(workerName: string): Promise<void> {
  const log = createWorkerLogger(workerName);
  log.info({ stream: STREAM_KEY, group: CONSUMER_GROUP }, 'Ingest consumer starting');

  await ensureConsumerGroup();

  let running = true;
  let lastReclaimAt = Date.now();

  // Graceful shutdown
  process.once('SIGTERM', () => {
    log.info('SIGTERM received — stopping ingest consumer after current batch');
    running = false;
  });

  while (running) {
    // -----------------------------------------------------------------------
    // Periodic reclaim of idle/orphaned PEL entries
    // -----------------------------------------------------------------------
    if (Date.now() - lastReclaimAt >= RECLAIM_INTERVAL_MS) {
      lastReclaimAt = Date.now();
      try {
        // XPENDING: returns array of [msgId, consumerName, idleMs, deliveryCount]
        const pendingEntries = await redis.xpending(
          STREAM_KEY,
          CONSUMER_GROUP,
          '-',
          '+',
          10,
        ) as Array<[string, string, number, number]>;

        for (const entry of pendingEntries) {
          const [msgId, , idleMs] = entry;
          if (msgId === undefined) continue;
          if ((idleMs ?? 0) >= RECLAIM_IDLE_MS) {
            try {
              await redis.xclaim(
                STREAM_KEY,
                CONSUMER_GROUP,
                workerName,
                RECLAIM_IDLE_MS,
                msgId,
              );
              await incrCounter('ingest.reclaim.count');
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

    // -----------------------------------------------------------------------
    // Read new messages from the stream
    // -----------------------------------------------------------------------
    let streamResults: Array<[string, Array<[string, string[]]>]> | null = null;
    try {
      // XREADGROUP returns: [[streamName, [[msgId, [f,v,...]], ...]]] or null
      streamResults = await redis.xreadgroup(
        'GROUP',
        CONSUMER_GROUP,
        workerName,
        'COUNT',
        BATCH_SIZE,
        'BLOCK',
        BLOCK_MS,
        'STREAMS',
        STREAM_KEY,
        '>',
      ) as Array<[string, Array<[string, string[]]>]> | null;
    } catch (readErr) {
      if (!running) break;
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
      if (!running) break;
      if (msgId === undefined || fields === undefined) continue;

      const data = fieldsToObject(fields);
      const retryKey = `retry:${msgId}`;

      try {
        // Parse and dispatch the event payload
        const payload: IngestPayload = {
          eventId: data['eventId'] ?? msgId,
          traceId: data['traceId'] ?? '',
          projectId: data['projectId'] ?? '',
          tenantId: data['tenantId'] ?? '',
          planId: data['planId'] ?? '',
          raw: JSON.parse(data['raw'] ?? '{}') as IngestPayload['raw'],
        };

        await processEvent(payload);

        // ACK on success and clean up retry counter
        await redis.xack(STREAM_KEY, CONSUMER_GROUP, msgId);
        await redis.del(retryKey);

        log.debug({ msgId, eventId: payload.eventId }, 'Message processed and ACKed');
      } catch (procErr) {
        log.error({ err: procErr, msgId }, 'Failed to process stream message');

        // Increment retry counter
        const retryCount = await redis.hincrby(retryKey, 'count', 1);
        await redis.expire(retryKey, 3600); // expire retry keys after 1h

        if (retryCount > MAX_RETRIES) {
          // Move to DLQ and ACK
          log.warn({ msgId, retryCount }, 'Max retries exceeded — moving to DLQ');
          const dlqFields: string[] = [];
          for (const [k, v] of Object.entries(data)) {
            dlqFields.push(k, v);
          }
          dlqFields.push('original_msg_id', msgId);
          dlqFields.push('failed_at', new Date().toISOString());
          dlqFields.push('error', procErr instanceof Error ? procErr.message : String(procErr));

          await redis.xadd(STREAM_DLQ, '*', ...dlqFields);
          await redis.xack(STREAM_KEY, CONSUMER_GROUP, msgId);
          await redis.del(retryKey);
          await incrCounter('ingest.dlq.count');
        }
        // If under retry limit, do not ACK — message stays in PEL for reclaim
      }
    }
  }

  log.info('Ingest consumer stopped');
}
