import { nanoid } from 'nanoid';
import { redis } from '../db/redis.js';
import { eventsCollection } from '../db/mongo.js';
import { createWorkerLogger } from '../logger.js';

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const LEADER_KEY = 'change-stream:fatal:leader';
const RESUME_HASH_KEY = 'change-stream:fatal:resume';
const LEADER_TTL_MS = 30_000;
const LEADER_POLL_MS = 5_000;
const LEADER_RENEW_MS = 10_000;

const log = createWorkerLogger('change-stream');

// ---------------------------------------------------------------------------
// runChangeStreamWorker
// ---------------------------------------------------------------------------

export async function runChangeStreamWorker(): Promise<void> {
  const nodeId = process.env['HOSTNAME'] ?? nanoid(8);
  log.info({ nodeId }, 'Change stream worker starting');

  let running = true;
  let changeStreamOpen: ReturnType<typeof eventsCollection.prototype.watch> | null = null;
  let holdsLock = false;

  // Graceful shutdown handler
  process.once('SIGTERM', () => {
    log.info('SIGTERM received — stopping change stream worker');
    running = false;
    if (changeStreamOpen !== null) {
      changeStreamOpen.close().catch((err: unknown) => {
        log.warn({ err }, 'Error closing change stream on SIGTERM');
      });
    }
    if (holdsLock) {
      redis.del(LEADER_KEY).catch((err: unknown) => {
        log.warn({ err }, 'Error releasing leader lock on SIGTERM');
      });
    }
  });

  // -----------------------------------------------------------------------
  // Leader election + watch loop
  // -----------------------------------------------------------------------
  while (running) {
    // Try to acquire the leader lock
    const acquired = await redis
      .set(LEADER_KEY, nodeId, 'PX', LEADER_TTL_MS, 'NX')
      .catch((err: unknown) => {
        log.warn({ err }, 'Redis SET NX failed during leader election');
        return null;
      });

    if (acquired === null || acquired !== 'OK') {
      // Not the leader — poll until lock expires
      log.debug({ nodeId }, 'Not leader, polling...');
      await new Promise<void>((resolve) => setTimeout(resolve, LEADER_POLL_MS));
      continue;
    }

    holdsLock = true;
    log.info({ nodeId }, 'Acquired change stream leader lock');

    // Start renewal timer
    let renewalTimer: ReturnType<typeof setInterval> | null = setInterval(() => {
      if (!holdsLock) return;
      redis
        .set(LEADER_KEY, nodeId, 'PX', LEADER_TTL_MS, 'XX')
        .then((result) => {
          if (result === null) {
            log.warn({ nodeId }, 'Lock renewal failed (key expired?) — will re-enter election');
            holdsLock = false;
          }
        })
        .catch((err: unknown) => {
          log.warn({ err }, 'Lock renewal error');
          holdsLock = false;
        });
    }, LEADER_RENEW_MS);

    try {
      await watchFatalEvents(nodeId, () => holdsLock);
    } catch (err) {
      log.error({ err }, 'Change stream watch loop error');
    } finally {
      holdsLock = false;
      changeStreamOpen = null;
      if (renewalTimer !== null) {
        clearInterval(renewalTimer);
        renewalTimer = null;
      }
      // Release lock if we still hold it
      await redis.del(LEADER_KEY).catch((err: unknown) => {
        log.warn({ err }, 'Error releasing leader lock after watch loop exit');
      });
    }

    // Brief pause before re-entering election loop
    if (running) {
      await new Promise<void>((resolve) => setTimeout(resolve, LEADER_POLL_MS));
    }
  }

  log.info('Change stream worker stopped');
}

// ---------------------------------------------------------------------------
// watchFatalEvents — opens and drains the change stream
// ---------------------------------------------------------------------------

async function watchFatalEvents(
  nodeId: string,
  holdsLock: () => boolean,
): Promise<void> {
  // Read resume token from Redis
  const resumeTokenStr = await redis.hget(RESUME_HASH_KEY, 'token').catch(() => null);
  let resumeToken: Record<string, unknown> | undefined;
  if (resumeTokenStr !== null && resumeTokenStr !== undefined) {
    try {
      resumeToken = JSON.parse(resumeTokenStr) as Record<string, unknown>;
    } catch {
      resumeToken = undefined;
    }
  }

  log.info({ nodeId, hasResumeToken: resumeToken !== undefined }, 'Opening change stream');

  const pipeline = [
    {
      $match: {
        'fullDocument.severity': 'fatal',
        operationType: 'insert',
      },
    },
  ];

  const changeStream = eventsCollection().watch(pipeline, {
    fullDocument: 'updateLookup',
    ...(resumeToken !== undefined ? { resumeAfter: resumeToken } : {}),
  });

  try {
    for await (const event of changeStream) {
      if (!holdsLock()) {
        log.warn({ nodeId }, 'Lost leader lock mid-stream — closing change stream');
        break;
      }

      // Only insert events with a fullDocument (insert operations)
      if ('fullDocument' in event && event.fullDocument !== null && event.fullDocument !== undefined) {
        const fullDoc = event.fullDocument as unknown as { projectId?: string; _id?: unknown; [key: string]: unknown };
        const projectId = fullDoc['projectId'];
        if (typeof projectId === 'string') {
          const channel = `alerts:fatal:${projectId}`;
          await redis.publish(channel, JSON.stringify(fullDoc)).catch((err: unknown) => {
            log.warn({ err, channel }, 'Redis PUBLISH failed');
          });
          log.debug({ projectId, eventId: fullDoc['_id'] }, 'Fatal event published');
        }
      }

      // Persist resume token
      const resumeId = event._id;
      if (resumeId !== null && resumeId !== undefined) {
        await redis
          .hset(RESUME_HASH_KEY, 'token', JSON.stringify(resumeId))
          .catch((err: unknown) => {
            log.warn({ err }, 'Failed to persist resume token');
          });
      }
    }
  } catch (err) {
    // ChangeStreamInvalidateDocument error or topology change — clear stale token and re-enter election
    const isInvalidated =
      err instanceof Error &&
      (err.name === 'ChangeStreamInvalidatedError' ||
        err.message.includes('invalidated') ||
        err.message.includes('ChangeStreamHistoryLost'));

    if (isInvalidated) {
      log.warn({ nodeId }, 'Change stream invalidated — will re-open with no resume token');
      await redis.hdel(RESUME_HASH_KEY, 'token').catch(() => undefined);
    } else {
      throw err;
    }
  } finally {
    await changeStream.close().catch((err: unknown) => {
      log.warn({ err }, 'Error closing change stream');
    });
  }
}
