import { Redis } from 'ioredis';
import { config } from '../config.js';
import { pool } from '../db/postgres.js';
import { fireDedupAlert } from '../domain/alerts.js';
import { createWorkerLogger } from '../logger.js';

// ---------------------------------------------------------------------------
// runAlertSubscriber
// ---------------------------------------------------------------------------

const log = createWorkerLogger('alert-subscriber');

export async function runAlertSubscriber(): Promise<void> {
  log.info('Alert subscriber starting');

  // Create a dedicated ioredis client for pub/sub (pub/sub requires its own connection)
  const subscriber = new Redis({
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

  subscriber.on('error', (err: Error) => {
    log.error({ err }, 'Alert subscriber Redis error');
  });

  subscriber.on('reconnecting', () => {
    log.warn('Alert subscriber Redis reconnecting...');
  });

  await subscriber.connect();
  log.info('Alert subscriber Redis connected');

  // Pattern subscribe to all fatal alert channels across all projects
  await subscriber.psubscribe('alerts:fatal:*');
  log.info('Subscribed to pattern alerts:fatal:*');

  // Graceful shutdown
  process.once('SIGTERM', () => {
    log.info('SIGTERM received — closing alert subscriber');
    subscriber.quit().catch((err: unknown) => {
      log.warn({ err }, 'Error quitting alert subscriber Redis client');
    });
  });

  // Handle incoming pattern messages
  subscriber.on(
    'pmessage',
    (pattern: string, channel: string, message: string) => {
      // Extract projectId from channel: 'alerts:fatal:<projectId>'
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
                log.info(
                  { alertRuleId: rule.id, projectId, eventId },
                  'Alert fired',
                );
              } else {
                log.debug(
                  { alertRuleId: rule.id, projectId, eventId },
                  'Alert deduplicated (already fired by another instance)',
                );
              }
            } catch (alertErr) {
              log.error(
                { err: alertErr, alertRuleId: rule.id, projectId, eventId },
                'Error firing dedup alert',
              );
            }
          }
        })
        .catch((pgErr: unknown) => {
          log.error({ err: pgErr, projectId }, 'Failed to query alert rules for project');
        });
    },
  );

  log.info('Alert subscriber ready');
}
