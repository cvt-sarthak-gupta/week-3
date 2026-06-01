import { Redis } from 'ioredis';
import { config } from '../config.js';
import type { PostgresDatabase } from '../db/postgres.js';
import type { AlertService } from '../domain/alerts.js';
import { createWorkerLogger } from '../logger.js';

const log = createWorkerLogger('alert-subscriber');

const PG_POLL_INTERVAL_MS = 10_000; // X5: Redis-down fallback polls PG every 10 seconds
const PG_POLL_LOOKBACK_MS = 15_000; // slightly longer than interval to avoid gaps
const WEBHOOK_TIMEOUT_MS  = 5_000;

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
      void this.handleAlertMessage(channel, message);
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

  /**
   * X5 Redis-down fallback: poll PG for alert rules that fired recently and
   * re-fire their webhooks directly (bypassing the Redis dedup lock which is
   * unavailable).  Duplicate notifications are acceptable in degraded mode.
   */
  private async pollPgForRecentFatalEvents(): Promise<void> {
    const since = new Date(Date.now() - PG_POLL_LOOKBACK_MS).toISOString();
    try {
      const result = await this.pg.query<{
        id: string;
        notification_channel: string;
        project_id: string;
      }>(
        `SELECT ar.id, ar.notification_channel, ar.project_id
           FROM alert_rules ar
           JOIN projects p ON p.id = ar.project_id
          WHERE ar.is_enabled = true
            AND ar.last_triggered_at IS NOT NULL
            AND ar.last_triggered_at >= $1`,
        [since],
      );

      if (result.rows.length === 0) return;

      log.warn(
        { ruleCount: result.rows.length, since },
        'X5 PG fallback: re-firing recently-triggered alert rules while Redis is down',
      );

      for (const rule of result.rows) {
        try {
          const controller = new AbortController();
          const timeoutId = setTimeout(() => controller.abort(), WEBHOOK_TIMEOUT_MS);
          try {
            const response = await fetch(rule.notification_channel, {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({
                alertRuleId: rule.id,
                projectId: rule.project_id,
                source: 'pg-fallback',
                note: 'Redis unavailable — re-firing from PG poll',
              }),
              signal: controller.signal,
            });
            log.info(
              { alertRuleId: rule.id, status: response.status },
              'X5 PG fallback: webhook fired',
            );
          } finally {
            clearTimeout(timeoutId);
          }
        } catch (webhookErr: unknown) {
          log.error({ err: webhookErr, alertRuleId: rule.id }, 'X5 PG fallback: webhook failed');
        }
      }
    } catch (pgErr) {
      log.error({ err: pgErr }, 'X5 PG fallback poll failed');
    }
  }

  /**
   * Handles a fatal event message received from `alerts:fatal:<projectId>`.
   *
   * Runs ES percolation to find which alert rules actually match the incoming
   * event, then fires only the matched rules via the dedup lock.  This prevents
   * false positives that would occur if every enabled rule were fired for every
   * fatal event regardless of its conditions.
   */
  private async handleAlertMessage(channel: string, message: string): Promise<void> {
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

    // Convert tags from MongoDB format (Record<string,string>) to ES nested
    // format ([{key, value}]) so the percolation query evaluates correctly.
    const rawTags = eventDoc['tags'];
    const tagsForEs =
      rawTags !== null &&
      rawTags !== undefined &&
      typeof rawTags === 'object' &&
      !Array.isArray(rawTags)
        ? Object.entries(rawTags as Record<string, string>).map(([key, value]) => ({ key, value }))
        : undefined;

    const esEventDoc: Record<string, unknown> = {
      ...eventDoc,
      ...(tagsForEs !== undefined ? { tags: tagsForEs } : {}),
    };

    // Run percolation to find which rules actually match this event.
    let matchedRuleIds: string[];
    try {
      matchedRuleIds = await this.alerts.runPercolation(projectId, esEventDoc);
    } catch (esErr) {
      log.warn({ err: esErr, projectId, eventId }, 'Percolation failed — skipping alert fan-out');
      return;
    }

    if (matchedRuleIds.length === 0) {
      log.debug({ projectId, eventId }, 'No alert rules matched via percolation');
      return;
    }

    // Fetch notification channels for matched rules only (not all enabled rules).
    let ruleRows: Array<{ id: string; notification_channel: string }>;
    try {
      const placeholders = matchedRuleIds.map((_, i) => `$${i + 1}`).join(',');
      const pgResult = await this.pg.query<{ id: string; notification_channel: string }>(
        `SELECT id, notification_channel
           FROM alert_rules
          WHERE id IN (${placeholders})
            AND is_enabled = true`,
        matchedRuleIds,
      );
      ruleRows = pgResult.rows;
    } catch (pgErr: unknown) {
      log.error({ err: pgErr, projectId }, 'Failed to query matched alert rules');
      return;
    }

    for (const rule of ruleRows) {
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
  }
}
