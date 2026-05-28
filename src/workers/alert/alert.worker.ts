import { nanoid } from 'nanoid';
import { Redis } from 'ioredis';
import { config } from '../../config.js';
import type { RedisClient } from '../../db/redis/index.js';
import type { PostgresPool } from '../../db/postgres/index.js';
import type { ElasticClient } from '../../db/elastic/index.js';
import type { LuaScript } from '../../db/redis/index.js';
import { createWorkerLogger } from '../../utils/logger.js';
import type { Logger } from 'pino';

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const WEBHOOK_TIMEOUT_MS = 5_000;

// ---------------------------------------------------------------------------
// AlertWorker
// ---------------------------------------------------------------------------

export class AlertWorker {
  private readonly log: Logger;
  private subscriber: Redis | null = null;
  private dedupScript: LuaScript | null = null;

  constructor(
    private readonly redis: RedisClient,
    private readonly pool: PostgresPool,
    private readonly es: ElasticClient,
  ) {
    this.log = createWorkerLogger('alert-subscriber');
  }

  async start(): Promise<void> {
    this.log.info('Alert subscriber starting');

    // Create a dedicated ioredis client for pub/sub (pub/sub requires its own connection)
    const sub = new Redis({
      host: config.redis.host,
      port: config.redis.port,
      lazyConnect: true,
      enableReadyCheck: true,
      maxRetriesPerRequest: 3,
      retryStrategy(times: number): number | null {
        if (times > 10) return null;
        return Math.min(times * 100, 3_000);
      },
    });

    sub.on('error', (err: Error) => {
      this.log.error({ err }, 'Alert subscriber Redis error');
    });

    sub.on('reconnecting', () => {
      this.log.warn('Alert subscriber Redis reconnecting...');
    });

    await sub.connect();
    this.subscriber = sub;
    this.log.info('Alert subscriber Redis connected');

    // Pre-load the dedup-fire Lua script via the main redis client
    this.dedupScript = await this.redis.loadLua('dedup-fire');

    // Pattern subscribe to all fatal alert channels across all projects
    await sub.psubscribe('alerts:fatal:*');
    this.log.info('Subscribed to pattern alerts:fatal:*');

    // Handle incoming pattern messages
    sub.on('pmessage', (pattern: string, channel: string, message: string) => {
      void this.handleMessage(pattern, channel, message);
    });

    this.log.info('Alert subscriber ready');
  }

  async stop(): Promise<void> {
    this.log.info('AlertWorker stop requested — disconnecting subscriber');
    if (this.subscriber !== null) {
      await this.subscriber.quit().catch((err: unknown) => {
        this.log.warn({ err }, 'Error quitting alert subscriber Redis client');
      });
      this.subscriber = null;
    }
  }

  // ---------------------------------------------------------------------------
  // Message handler
  // ---------------------------------------------------------------------------

  private async handleMessage(
    _pattern: string,
    channel: string,
    message: string,
  ): Promise<void> {
    // Extract projectId from channel: 'alerts:fatal:<projectId>'
    const parts = channel.split(':');
    const projectId = parts[2];

    if (projectId === undefined || projectId.length === 0) {
      this.log.warn({ channel }, 'Could not parse projectId from alert channel');
      return;
    }

    let eventDoc: Record<string, unknown>;
    try {
      eventDoc = JSON.parse(message) as Record<string, unknown>;
    } catch (parseErr) {
      this.log.warn({ err: parseErr, channel }, 'Failed to parse alert message JSON');
      return;
    }

    const eventId =
      typeof eventDoc['_id'] === 'string'
        ? eventDoc['_id']
        : String(eventDoc['_id'] ?? '');

    // Fan out to all enabled alert rules for this project (no RLS needed for worker)
    try {
      const result = await this.pool.query<{ id: string; notification_channel: string }>(
        `SELECT id, notification_channel
         FROM alert_rules
         WHERE project_id = $1
           AND is_enabled = true`,
        [projectId],
      );

      for (const rule of result.rows) {
        try {
          const fired = await this.fireDedupAlert(
            rule.id,
            eventId,
            rule.notification_channel,
            eventDoc,
          );
          if (fired) {
            this.log.info(
              { alertRuleId: rule.id, projectId, eventId },
              'Alert fired',
            );
          } else {
            this.log.debug(
              { alertRuleId: rule.id, projectId, eventId },
              'Alert deduplicated (already fired by another instance)',
            );
          }
        } catch (alertErr) {
          this.log.error(
            { err: alertErr, alertRuleId: rule.id, projectId, eventId },
            'Error firing dedup alert',
          );
        }
      }
    } catch (pgErr: unknown) {
      this.log.error({ err: pgErr, projectId }, 'Failed to query alert rules for project');
    }
  }

  // ---------------------------------------------------------------------------
  // fireDedupAlert — inlined from domain/alerts.ts using injected dependencies
  // ---------------------------------------------------------------------------

  private async fireDedupAlert(
    alertRuleId: string,
    eventId: string,
    notificationChannel: string,
    eventDoc: Record<string, unknown>,
  ): Promise<boolean> {
    // 1. Ensure the dedup-fire Lua script is loaded
    if (this.dedupScript === null) {
      this.dedupScript = await this.redis.loadLua('dedup-fire');
    }

    // 2. Attempt to acquire the dedup lock
    const lockKey = `fire-lock:${alertRuleId}:${eventId}`;
    const nodeId = process.env['HOSTNAME'] ?? nanoid(8);
    const TTL = 60;

    const result = await this.dedupScript.evalsha([lockKey], [nodeId, TTL]);

    if (result !== 1) {
      // Another instance already fired this alert
      return false;
    }

    // 3. Won the lock — fire the webhook
    let webhookSucceeded = false;
    try {
      this.log.info(
        { alertRuleId, eventId, notificationChannel },
        'fireDedupAlert: firing webhook',
      );

      const controller = new AbortController();
      const timeoutHandle = setTimeout(() => controller.abort(), WEBHOOK_TIMEOUT_MS);

      try {
        const response = await fetch(notificationChannel, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ alertRuleId, eventId, event: eventDoc }),
          signal: controller.signal,
        });
        webhookSucceeded = response.ok;
        if (!response.ok) {
          this.log.warn(
            { alertRuleId, eventId, status: response.status },
            'fireDedupAlert: webhook returned non-2xx status',
          );
        }
      } finally {
        clearTimeout(timeoutHandle);
      }
    } catch (webhookErr) {
      this.log.error(
        { err: webhookErr, alertRuleId, eventId },
        'fireDedupAlert: webhook call failed',
      );
      webhookSucceeded = false;
    }

    if (!webhookSucceeded) {
      await this.redis.incrCounter('alert.fanout.failures').catch((err: unknown) => {
        this.log.warn({ err }, 'fireDedupAlert: failed to increment failure counter');
      });
    }

    // 4. Update last_triggered_at in PG
    try {
      await this.pool.query(
        `UPDATE alert_rules SET last_triggered_at = NOW() WHERE id = $1`,
        [alertRuleId],
      );
    } catch (pgErr) {
      this.log.warn({ err: pgErr, alertRuleId }, 'fireDedupAlert: failed to update last_triggered_at');
    }

    return true;
  }
}
