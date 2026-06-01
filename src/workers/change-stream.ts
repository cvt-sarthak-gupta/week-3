import { nanoid } from 'nanoid';
import { createWorkerLogger } from '../logger.js';
import type { MongoDatabase } from '../db/mongo.js';
import type { RedisDatabase } from '../db/redis.js';

const LEADER_KEY = 'change-stream:fatal:leader';
const RESUME_HASH_KEY = 'change-stream:fatal:resume';
const LEADER_TTL_MS = 30_000;
const LEADER_POLL_MS = 5_000;
const LEADER_RENEW_MS = 10_000;

const log = createWorkerLogger('change-stream');

export class ChangeStreamWorker {
  private readonly mongo: MongoDatabase;
  private readonly redis: RedisDatabase;
  private readonly nodeId: string;
  private readonly onFatalEvent: ((projectId: string) => Promise<void>) | undefined;

  private _running = false;
  private _holdsLock = false;
  private _renewalTimer: ReturnType<typeof setInterval> | null = null;
  private _changeStream: Awaited<ReturnType<ReturnType<MongoDatabase['events']>['watch']>> | null = null;

  constructor(
    mongo: MongoDatabase,
    redis: RedisDatabase,
    onFatalEvent?: (projectId: string) => Promise<void>,
  ) {
    this.mongo = mongo;
    this.redis = redis;
    this.onFatalEvent = onFatalEvent;
    this.nodeId = process.env['HOSTNAME'] ?? nanoid(8);
  }

  async start(): Promise<void> {
    this._running = true;
    log.info({ nodeId: this.nodeId }, 'Change stream worker starting');

    while (this._running) {
      // Try to acquire the leader lock via SETNX (PX = milliseconds, NX = only set if not exists)
      const acquired = await this.redis.client
        .set(LEADER_KEY, this.nodeId, 'PX', LEADER_TTL_MS, 'NX')
        .catch((err: unknown) => {
          log.warn({ err }, 'Redis SET NX failed during leader election');
          return null;
        });

      if (acquired === null || acquired !== 'OK') {
        // Not the leader — poll until lock expires
        log.debug({ nodeId: this.nodeId }, 'Not leader, polling...');
        await new Promise<void>((resolve) => setTimeout(resolve, LEADER_POLL_MS));
        continue;
      }

      this._holdsLock = true;
      log.info({ nodeId: this.nodeId }, 'Acquired change stream leader lock');

      // Start 10-second renewal timer (XX = only set if exists)
      this._renewalTimer = setInterval(() => {
        if (!this._holdsLock) return;
        this.redis.client
          .set(LEADER_KEY, this.nodeId, 'PX', LEADER_TTL_MS, 'XX')
          .then((result) => {
            if (result === null) {
              log.warn({ nodeId: this.nodeId }, 'Lock renewal failed (key expired?) — will re-enter election');
              this._holdsLock = false;
            }
          })
          .catch((err: unknown) => {
            log.warn({ err }, 'Lock renewal error');
            this._holdsLock = false;
          });
      }, LEADER_RENEW_MS);

      try {
        await this._watchFatalEvents();
      } catch (err) {
        log.error({ err }, 'Change stream watch loop error');
      } finally {
        this._holdsLock = false;
        this._changeStream = null;
        if (this._renewalTimer !== null) {
          clearInterval(this._renewalTimer);
          this._renewalTimer = null;
        }
        // Release lock if we still hold it
        await this.redis.client.del(LEADER_KEY).catch((err: unknown) => {
          log.warn({ err }, 'Error releasing leader lock after watch loop exit');
        });
      }

      // Brief pause before re-entering election loop
      if (this._running) {
        await new Promise<void>((resolve) => setTimeout(resolve, LEADER_POLL_MS));
      }
    }

    log.info('Change stream worker stopped');
  }

  async stop(): Promise<void> {
    log.info('Stopping change stream worker');
    this._running = false;

    if (this._changeStream !== null) {
      await this._changeStream.close().catch((err: unknown) => {
        log.warn({ err }, 'Error closing change stream on stop');
      });
      this._changeStream = null;
    }

    if (this._renewalTimer !== null) {
      clearInterval(this._renewalTimer);
      this._renewalTimer = null;
    }

    if (this._holdsLock) {
      await this.redis.client.del(LEADER_KEY).catch((err: unknown) => {
        log.warn({ err }, 'Error releasing leader lock on stop');
      });
      this._holdsLock = false;
    }
  }

  private async _watchFatalEvents(): Promise<void> {
    // Read resume token from Redis HASH
    const resumeTokenStr = await this.redis.client.hget(RESUME_HASH_KEY, 'token').catch(() => null);
    let resumeToken: Record<string, unknown> | undefined;
    if (resumeTokenStr !== null && resumeTokenStr !== undefined) {
      try {
        resumeToken = JSON.parse(resumeTokenStr) as Record<string, unknown>;
      } catch {
        resumeToken = undefined;
      }
    }

    log.info({ nodeId: this.nodeId, hasResumeToken: resumeToken !== undefined }, 'Opening change stream');

    const pipeline = [
      {
        $match: {
          'fullDocument.severity': 'fatal',
          operationType: 'insert',
        },
      },
    ];

    // fullDocument is always present for insert events; updateLookup adds a
    // round-trip for updates but this stream is filtered to inserts only, so
    // the default ('default') is sufficient.
    const changeStream = this.mongo.events().watch(pipeline, {
      ...(resumeToken !== undefined ? { resumeAfter: resumeToken } : {}),
    });

    this._changeStream = changeStream;

    try {
      for await (const event of changeStream) {
        if (!this._holdsLock) {
          log.warn({ nodeId: this.nodeId }, 'Lost leader lock mid-stream — closing change stream');
          break;
        }

        // At-least-once delivery: publish FIRST, then checkpoint the resume
        // token.  If the process crashes between publish and token-save the
        // stream reopens at the same position and we republish — the dedup-fire
        // lock in AlertService.fireDedupAlert() prevents duplicate webhook
        // calls within the 60-second cooldown window.
        if ('fullDocument' in event && event.fullDocument !== null && event.fullDocument !== undefined) {
          const fullDoc = event.fullDocument as unknown as { projectId?: string; _id?: unknown; [key: string]: unknown };
          const projectId = fullDoc['projectId'];
          if (typeof projectId === 'string') {
            const channel = `alerts:fatal:${projectId}`;
            await this.redis.client.publish(channel, JSON.stringify(fullDoc)).catch((err: unknown) => {
              log.warn({ err, channel }, 'Redis PUBLISH failed');
            });
            log.debug({ projectId, eventId: fullDoc['_id'] }, 'Fatal event published');

            // Invalidate cached reports for this project so next read reflects the new event
            void this.onFatalEvent?.(projectId).catch((err: unknown) => {
              log.warn({ err, projectId }, 'onFatalEvent cache-invalidation callback failed (non-fatal)');
            });
          }
        }

        // Checkpoint the resume token AFTER publishing (at-least-once semantics).
        const resumeId = event._id;
        if (resumeId !== null && resumeId !== undefined) {
          await this.redis.client
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
        log.warn({ nodeId: this.nodeId }, 'Change stream invalidated — will re-open with no resume token');
        await this.redis.client.hdel(RESUME_HASH_KEY, 'token').catch(() => undefined);
      } else {
        throw err;
      }
    } finally {
      await changeStream.close().catch((err: unknown) => {
        log.warn({ err }, 'Error closing change stream');
      });
      if (this._changeStream === changeStream) {
        this._changeStream = null;
      }
    }
  }
}
