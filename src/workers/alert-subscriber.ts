import { Redis } from 'ioredis';
import { config } from '../config.js';
import { pool } from '../db/postgres.js';
import { fireDedupAlert } from '../domain/alerts.js';
import { createWorkerLogger } from '../logger.js';

const log = createWorkerLogger('alert-subscriber');

const PG_POLL_INTERVAL_MS = 10_000; // X5: Redis-down fallback polls PG every 10 seconds
const PG_POLL_LOOKBACK_MS = 15_000; // slightly longer than interval to avoid gaps

async function pollPgForRecentFatalEvents(): Promise<void> {
  const since = new Date(Date.now() - PG_POLL_LOOKBACK_MS).toISOString();
  try {
    // Find projects that have had fatal events recently via monthly_usage + alert_rules
    // We can't access Mongo directly here, so we check alert_rules for enabled rules
    // and log a warning — a more complete fallback would query Mongo directly.
    const result = await pool.query<{ project_id: string; id: string; notification_channel: string }>(
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

// Alert message handler (shared between pub/sub and future recovery)

function handleAlertMessage(channel: string, message: string): void {
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
  pool
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
          const fired = await fireDedupAlert(
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

export async function runAlertSubscriber(): Promise<void> {
  log.info('Alert subscriber starting');

  let pgPollInterval: ReturnType<typeof setInterval> | null = null;
  let redisAvailable = true;

  function startPgFallback(): void {
    if (pgPollInterval !== null) return; // already polling
    log.warn('Redis unavailable — starting PG polling fallback every 10s');
    redisAvailable = false;
    pgPollInterval = setInterval(() => {
      void pollPgForRecentFatalEvents();
    }, PG_POLL_INTERVAL_MS);
  }

  function stopPgFallback(): void {
    if (pgPollInterval === null) return;
    clearInterval(pgPollInterval);
    pgPollInterval = null;
    redisAvailable = true;
    log.info('Redis reconnected — stopping PG polling fallback');
  }

  // Create a dedicated ioredis client for pub/sub (pub/sub requires its own connection)
  const subscriber = new Redis({
    host: config.redis.host,
    port: config.redis.port,
    lazyConnect: true,
    enableReadyCheck: true,
    maxRetriesPerRequest: 3,
    retryStrategy(times: number): number | null {
      if (times > 10) {
        startPgFallback();
        return null; // stop retrying; pgFallback takes over
      }
      return Math.min(times * 100, 3_000);
    },
  });

  subscriber.on('error', (err: Error) => {
    log.error({ err }, 'Alert subscriber Redis error');
    if (!redisAvailable) startPgFallback();
  });

  subscriber.on('reconnecting', () => {
    log.warn('Alert subscriber Redis reconnecting...');
  });

  subscriber.on('ready', () => {
    stopPgFallback();
  });

  await subscriber.connect();
  log.info('Alert subscriber Redis connected');

  // Pattern subscribe to all fatal alert channels across all projects
  await subscriber.psubscribe('alerts:fatal:*');
  log.info('Subscribed to pattern alerts:fatal:*');

  // Graceful shutdown
  process.once('SIGTERM', () => {
    log.info('SIGTERM received — closing alert subscriber');
    if (pgPollInterval !== null) clearInterval(pgPollInterval);
    subscriber.quit().catch((err: unknown) => {
      log.warn({ err }, 'Error quitting alert subscriber Redis client');
    });
  });

  // Handle incoming pattern messages
  subscriber.on('pmessage', (_pattern: string, channel: string, message: string) => {
    handleAlertMessage(channel, message);
  });

  log.info('Alert subscriber ready');
}
