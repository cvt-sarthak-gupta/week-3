import { Redis } from 'ioredis';
import { config } from '../config.js';
import type { PostgresDatabase } from '../db/postgres.js';
import type { AlertService } from '../domain/alerts.js';
import { createWorkerLogger } from '../logger.js';

const log = createWorkerLogger('alert-subscriber');

const PG_POLL_INTERVAL_MS = 10_000; // X5: Redis-down fallback polls PG every 10 seconds
const PG_POLL_LOOKBACK_MS = 15_000; // slightly longer than interval to avoid gaps

export class AlertSubscriber {
  private pgPollInterval: ReturnType<typeof setInterval> | null = null;
  private redisAvailable = true;
  private subscriber: Redis | undefined = undefined;

  constructor(
    private readonly pg: PostgresDatabase,
    private readonly alerts: AlertService,
  ) {}

  // -------------------------------------------------------------------------
  // Public lifecycle
  // -------------------------------------------------------------------------

  async start(): Promise<void> {
    log.info('Alert subscriber starting');

    // Create a dedicated ioredis client for pub/sub (pub/sub requires its own connection)
    this.subscriber = new Redis({
      host: config.redis.host,
      port: config.redis.port,
      lazyConnect: true,
      enableReadyCheck: true,
      maxRetriesPerRequest: 3,
      retryStrategy: (times: number): number | null => {
        if (times > 10) {
          this.startPgFallback();
          return null; // stop retrying; pgFallback takes over
        }
        return Math.min(times * 100, 3_000);
      },
    });

    this.subscriber.on('error', (err: Error) => {
      log.error({ err }, 'Alert subscriber Redis error');
      if (this.redisAvailable) this.startPgFallback();
    });

    this.subscriber.on('reconnecting', () => {
      log.warn('Alert subscriber Redis reconnecting...');
    });

    this.subscriber.on('ready', () => {
      this.stopPgFallback();
    });

    // Handle incoming pattern messages
    this.subscriber.on('pmessage', (_pattern: string, channel: string, message: string) => {
      this.handleAlertMessage(channel, message);
    });

    await this.subscriber.connect();
    log.info('Alert subscriber Redis connected');

    // Pattern subscribe to all fatal alert channels across all projects
    await this.subscriber.psubscribe('alerts:fatal:*');
    log.info('Subscribed to pattern alerts:fatal:*');

    log.info('Alert subscriber ready');
  }

  async stop(): Promise<void> {
    log.info('Alert subscriber stopping');

    if (this.pgPollInterval !== null) {
      clearInterval(this.pgPollInterval);
      this.pgPollInterval = null;
    }

    if (this.subscriber !== undefined) {
      try {
        await this.subscriber.quit();
      } catch (err: unknown) {
        log.warn({ err }, 'Error quitting alert subscriber Redis client');
      }
      this.subscriber = undefined;
    }

    log.info('Alert subscriber stopped');
  }

  // -------------------------------------------------------------------------
  // Private helpers
  // -------------------------------------------------------------------------

  private startPgFallback(): void {
    if (this.pgPollInterval !== null) return; // already polling
    log.warn('Redis unavailable — starting PG polling fallback every 10s');
    this.redisAvailable = false;
    this.pgPollInterval = setInterval(() => {
      void this.pollPgForRecentFatalEvents();
    }, PG_POLL_INTERVAL_MS);
  }

  private stopPgFallback(): void {
    if (this.pgPollInterval === null) return;
    clearInterval(this.pgPollInterval);
    this.pgPollInterval = null;
    this.redisAvailable = true;
    log.info('Redis reconnected — stopping PG polling fallback');
  }

  private async pollPgForRecentFatalEvents(): Promise<void> {
    const since = new Date(Date.now() - PG_POLL_LOOKBACK_MS).toISOString();
    try {
      // Find projects that have had fatal events recently via monthly_usage + alert_rules
      // We can't access Mongo directly here, so we check alert_rules for enabled rules
      // and log a warning — a more complete fallback would query Mongo directly.
      const result = await this.pg.query<{ project_id: string; id: string; notification_channel: string }>(
        `SELECT ar.project_id, ar.id, ar.notification_channel
           FROM alert_rules ar
           JOIN projects p ON p.id = ar.project_id
          WHERE ar.is_enabled = true
            AND ar.last_triggered_at IS NOT NULL
            AND ar.last_triggered_at >= $1`,
        [since],
      );
      if (result.rows.length > 0) {
        log.warn(
          { ruleCount: result.rows.length, since },
          'X5 PG fallback: found recently-triggered alert rules while Redis is down — notifications may have been missed',
        );
      }
    } catch (pgErr) {
      log.error({ err: pgErr }, 'X5 PG fallback poll failed');
    }
  }

  private handleAlertMessage(channel: string, message: string): void {
    const parts = channel.split(':');
    const projectId = parts[2];

    if (projectId === undefined || projectId.length === 0) {
      log.warn({ channel }, 'Could not parse projectId from alert channel');
      return;
    }

    let eventDoc: Record<string, unknown>;
    try {
      eventDoc = JSON.parse(message) as Record<string, unknown>;
    } catch (parseErr) {
      log.warn({ err: parseErr, channel }, 'Failed to parse alert message JSON');
      return;
    }

    const eventId =
      typeof eventDoc['_id'] === 'string'
        ? eventDoc['_id']
        : String(eventDoc['_id'] ?? '');

    // Fan out to all enabled alert rules for this project (no RLS needed for worker)
    this.pg
      .query<{ id: string; notification_channel: string }>(
        `SELECT id, notification_channel
           FROM alert_rules
          WHERE project_id = $1
            AND is_enabled = true`,
        [projectId],
      )
      .then(async (result) => {
        for (const rule of result.rows) {
          try {
            const fired = await this.alerts.fireDedupAlert(
              rule.id,
              eventId,
              rule.notification_channel,
              eventDoc,
            );
            if (fired) {
              log.info({ alertRuleId: rule.id, projectId, eventId }, 'Alert fired');
            } else {
              log.debug({ alertRuleId: rule.id, projectId, eventId }, 'Alert deduplicated (already fired by another instance)');
            }
          } catch (alertErr) {
            log.error({ err: alertErr, alertRuleId: rule.id, projectId, eventId }, 'Error firing dedup alert');
          }
        }
      })
      .catch((pgErr: unknown) => {
        log.error({ err: pgErr, projectId }, 'Failed to query alert rules for project');
      });
  }
}
