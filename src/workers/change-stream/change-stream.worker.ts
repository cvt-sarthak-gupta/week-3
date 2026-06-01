import { nanoid } from 'nanoid';
import type { MongoDatabase } from '../../db/mongo/index.js';
import type { RedisClient } from '../../db/redis/index.js';
import { createWorkerLogger } from '../../utils/logger.js';
import type { Logger } from 'pino';

const LEADER_KEY = 'change-stream:fatal:leader';
const RESUME_HASH_KEY = 'change-stream:fatal:resume';
const LEADER_TTL_MS = 30_000;
const LEADER_POLL_MS = 5_000;
const LEADER_RENEW_MS = 10_000;

export class ChangeStreamWorker {
  private readonly log: Logger;
  private running = false;
  private holdsLock = false;
  private readonly nodeId: string;

  constructor(
    private readonly mongo: MongoDatabase,
    private readonly redis: RedisClient,
  ) {
    this.log = createWorkerLogger('change-stream');
    this.nodeId = process.env['HOSTNAME'] ?? nanoid(8);
  }

  async start(): Promise<void> {
    this.log.info({ nodeId: this.nodeId }, 'Change stream worker starting');
    this.running = true;

    // Graceful shutdown handler
    process.once('SIGTERM', () => {
      this.log.info('SIGTERM received — stopping change stream worker');
      this.running = false;
      if (this.holdsLock) {
        this.redis.client.del(LEADER_KEY).catch((err: unknown) => {
          this.log.warn({ err }, 'Error releasing leader lock on SIGTERM');
        });
      }
    });

    await this.leaderElectionLoop();

    this.log.info('Change stream worker stopped');
  }

  async stop(): Promise<void> {
    this.running = false;
    if (this.holdsLock) {
      await this.redis.client.del(LEADER_KEY).catch((err: unknown) => {
        this.log.warn({ err }, 'Error releasing leader lock on stop()');
      });
      this.holdsLock = false;
    }
    this.log.info('ChangeStreamWorker stop requested');
  }

  private async leaderElectionLoop(): Promise<void> {
    while (this.running) {
      // Try to acquire the leader lock
      const acquired = await this.redis.client
        .set(LEADER_KEY, this.nodeId, 'PX', LEADER_TTL_MS, 'NX')
        .catch((err: unknown) => {
          this.log.warn({ err }, 'Redis SET NX failed during leader election');
          return null;
        });

      if (acquired === null || acquired !== 'OK') {
        // Not the leader — poll until lock expires
        this.log.debug({ nodeId: this.nodeId }, 'Not leader, polling...');
        await new Promise<void>((resolve) => setTimeout(resolve, LEADER_POLL_MS));
        continue;
      }

      this.holdsLock = true;
      this.log.info({ nodeId: this.nodeId }, 'Acquired change stream leader lock');

      // Start renewal timer
      let renewalTimer: ReturnType<typeof setInterval> | null = setInterval(() => {
        if (!this.holdsLock) return;
        this.redis.client
          .set(LEADER_KEY, this.nodeId, 'PX', LEADER_TTL_MS, 'XX')
          .then((result) => {
            if (result === null) {
              this.log.warn(
                { nodeId: this.nodeId },
                'Lock renewal failed (key expired?) — will re-enter election',
              );
              this.holdsLock = false;
            }
          })
          .catch((err: unknown) => {
            this.log.warn({ err }, 'Lock renewal error');
            this.holdsLock = false;
          });
      }, LEADER_RENEW_MS);

      try {
        await this.watchFatalEvents();
      } catch (err) {
        this.log.error({ err }, 'Change stream watch loop error');
      } finally {
        this.holdsLock = false;
        if (renewalTimer !== null) {
          clearInterval(renewalTimer);
          renewalTimer = null;
        }
        // Release lock if we still hold it
        await this.redis.client.del(LEADER_KEY).catch((err: unknown) => {
          this.log.warn({ err }, 'Error releasing leader lock after watch loop exit');
        });
      }

      // Brief pause before re-entering election loop
      if (this.running) {
        await new Promise<void>((resolve) => setTimeout(resolve, LEADER_POLL_MS));
      }
    }
  }

  private async watchFatalEvents(): Promise<void> {
    // Read resume token from Redis
    const resumeTokenStr = await this.redis.client
      .hget(RESUME_HASH_KEY, 'token')
      .catch(() => null);
    let resumeToken: Record<string, unknown> | undefined;
    if (resumeTokenStr !== null && resumeTokenStr !== undefined) {
      try {
        resumeToken = JSON.parse(resumeTokenStr) as Record<string, unknown>;
      } catch {
        resumeToken = undefined;
      }
    }

    this.log.info(
      { nodeId: this.nodeId, hasResumeToken: resumeToken !== undefined },
      'Opening change stream',
    );

    const pipeline = [
      {
        $match: {
          'fullDocument.severity': 'fatal',
          operationType: 'insert',
        },
      },
    ];

    const changeStream = this.mongo.events().watch(pipeline, {
      fullDocument: 'updateLookup',
      ...(resumeToken !== undefined ? { resumeAfter: resumeToken } : {}),
    });

    try {
      for await (const event of changeStream) {
        if (!this.holdsLock) {
          this.log.warn({ nodeId: this.nodeId }, 'Lost leader lock mid-stream — closing change stream');
          break;
        }

        if (!this.running) {
          break;
        }

        // Only insert events with a fullDocument (insert operations)
        if (
          'fullDocument' in event &&
          event.fullDocument !== null &&
          event.fullDocument !== undefined
        ) {
          const fullDoc = event.fullDocument as unknown as {
            projectId?: string;
            _id?: unknown;
            [key: string]: unknown;
          };
          const projectId = fullDoc['projectId'];
          if (typeof projectId === 'string') {
            const channel = `alerts:fatal:${projectId}`;
            await this.redis.client.publish(channel, JSON.stringify(fullDoc)).catch((err: unknown) => {
              this.log.warn({ err, channel }, 'Redis PUBLISH failed');
            });
            this.log.debug(
              { projectId, eventId: fullDoc['_id'] },
              'Fatal event published',
            );
          }
        }

        // Persist resume token
        const resumeId = event._id;
        if (resumeId !== null && resumeId !== undefined) {
          await this.redis.client
            .hset(RESUME_HASH_KEY, 'token', JSON.stringify(resumeId))
            .catch((err: unknown) => {
              this.log.warn({ err }, 'Failed to persist resume token');
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
        this.log.warn(
          { nodeId: this.nodeId },
          'Change stream invalidated — will re-open with no resume token',
        );
        await this.redis.client.hdel(RESUME_HASH_KEY, 'token').catch(() => undefined);
      } else {
        throw err;
      }
    } finally {
      await changeStream.close().catch((err: unknown) => {
        this.log.warn({ err }, 'Error closing change stream');
      });
    }
  }
}
