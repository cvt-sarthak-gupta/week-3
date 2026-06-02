import { randomUUID } from 'node:crypto';
import { nanoid } from 'nanoid';
import { logger } from '../logger.js';
import type { PostgresDatabase } from '../db/postgres.js';
import type { ElasticsearchDatabase } from '../db/elastic.js';
import { percolatorIndex } from '../db/elastic.js';
import type { RedisDatabase } from '../db/redis.js';
import type { AlertRule } from '../schemas/alert.js';

export type { AlertRule };

export interface CreateAlertRuleInput {
  projectId: string;
  tenantId: string;
  rule: AlertRule;
}

export interface UpdateAlertRuleInput {
  projectId: string;
  updates: Partial<AlertRule>;
}

const WEBHOOK_TIMEOUT_MS = 5_000;

export class AlertService {
  private readonly pg: PostgresDatabase;
  private readonly es: ElasticsearchDatabase;
  private readonly redis: RedisDatabase;

  constructor(pg: PostgresDatabase, es: ElasticsearchDatabase, redis: RedisDatabase) {
    this.pg = pg;
    this.es = es;
    this.redis = redis;
  }

  async createAlertRule(input: CreateAlertRuleInput): Promise<AlertRule & { id: string }> {
    const { projectId, tenantId, rule } = input;
    const alertRuleId = randomUUID();

    // 1. INSERT into alert_rules in PG (tenant-scoped)
    await this.pg.withTenant(tenantId, async (client) => {
      await client.query(
        `INSERT INTO alert_rules (id, project_id, tenant_id, name, condition_type, threshold,
           window_seconds, notification_channel, is_enabled, es_query, created_at, updated_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, NOW(), NOW())`,
        [
          alertRuleId,
          projectId,
          tenantId,
          rule.name,
          rule.conditionType,
          rule.threshold ?? null,
          rule.windowSeconds,
          rule.notificationChannel,
          rule.isEnabled,
          JSON.stringify(rule.esQuery),
        ],
      );
    });

    // 2. Index percolator document in ES
    await this.es.client.index({
      index: percolatorIndex,
      id: alertRuleId,
      document: {
        query: rule.esQuery,
        projectId,
        alertRuleId,
        notificationChannel: rule.notificationChannel,
      },
      refresh: 'wait_for',
    });

    logger.info({ alertRuleId, projectId, tenantId }, 'Alert rule created');
    return { ...rule, id: alertRuleId };
  }

  async updateAlertRule(
    id: string,
    tenantId: string,
    input: UpdateAlertRuleInput,
  ): Promise<AlertRule & { id: string }> {
    const { projectId, updates } = input;

    // Build SET clause dynamically from provided fields
    const setClauses: string[] = [];
    const params: unknown[] = [];
    let paramIndex = 1;

    if (updates.name !== undefined) {
      setClauses.push(`name = $${paramIndex++}`);
      params.push(updates.name);
    }
    if (updates.conditionType !== undefined) {
      setClauses.push(`condition_type = $${paramIndex++}`);
      params.push(updates.conditionType);
    }
    if (updates.threshold !== undefined) {
      setClauses.push(`threshold = $${paramIndex++}`);
      params.push(updates.threshold);
    }
    if (updates.windowSeconds !== undefined) {
      setClauses.push(`window_seconds = $${paramIndex++}`);
      params.push(updates.windowSeconds);
    }
    if (updates.notificationChannel !== undefined) {
      setClauses.push(`notification_channel = $${paramIndex++}`);
      params.push(updates.notificationChannel);
    }
    if (updates.isEnabled !== undefined) {
      setClauses.push(`is_enabled = $${paramIndex++}`);
      params.push(updates.isEnabled);
    }
    if (updates.esQuery !== undefined) {
      setClauses.push(`es_query = $${paramIndex++}`);
      params.push(JSON.stringify(updates.esQuery));
    }

    if (setClauses.length > 0) {
      setClauses.push(`updated_at = NOW()`);
      params.push(id, projectId);

      await this.pg.withTenant(tenantId, async (client) => {
        await client.query(
          `UPDATE alert_rules SET ${setClauses.join(', ')}
           WHERE id = $${paramIndex} AND project_id = $${paramIndex + 1}`,
          params,
        );
      });
    }

    logger.info({ alertRuleId: id, projectId, tenantId }, 'Alert rule updated');

    // Fetch the current rule state inside RLS context (withTenant) so the
    // superuser pool does not bypass FORCE ROW LEVEL SECURITY on alert_rules.
    // This also returns the merged es_query / notificationChannel for the
    // percolator upsert below.
    type AlertRuleRow = {
      id: string;
      name: string;
      condition_type: string;
      threshold: number | null;
      window_seconds: number;
      notification_channel: string;
      is_enabled: boolean;
      es_query: string;
    };

    const fetchResult = await this.pg.withTenant(tenantId, async (client) => {
      return client.query<AlertRuleRow>(
        `SELECT id, name, condition_type, threshold, window_seconds,
                notification_channel, is_enabled, es_query::text AS es_query
         FROM alert_rules WHERE id = $1`,
        [id],
      );
    });

    const row = fetchResult.rows[0];
    if (row === undefined) {
      throw new Error(`updateAlertRule: alert rule ${id} not found after update`);
    }

    // Update percolator via a single index (upsert-by-id) — no delete+create gap
    // where incoming events would silently miss the rule.
    if (updates.esQuery !== undefined || updates.notificationChannel !== undefined) {
      await this.es.client.index({
        index: percolatorIndex,
        id,
        document: {
          query: JSON.parse(row.es_query) as Record<string, unknown>,
          projectId,
          alertRuleId: id,
          notificationChannel: row.notification_channel,
        },
        refresh: 'wait_for',
      });
    }

    return {
      id: row.id,
      name: row.name,
      conditionType: row.condition_type as AlertRule['conditionType'],
      threshold: row.threshold ?? undefined,
      windowSeconds: row.window_seconds,
      notificationChannel: row.notification_channel,
      isEnabled: row.is_enabled,
      esQuery: JSON.parse(row.es_query) as Record<string, unknown>,
    };
  }

  async deleteAlertRule(id: string, tenantId: string): Promise<void> {
    // Delete from PG — projectId not required; tenantId scopes the RLS context
    await this.pg.withTenant(tenantId, async (client) => {
      await client.query(
        `DELETE FROM alert_rules WHERE id = $1`,
        [id],
      );
    });

    // Delete percolator document from ES
    await this.es.client.delete({ index: percolatorIndex, id }).catch((err: unknown) => {
      logger.warn({ err, alertRuleId: id }, 'deleteAlertRule: ES percolator doc delete failed (non-fatal)');
    });

    logger.info({ alertRuleId: id, tenantId }, 'Alert rule deleted');
  }

  async listAlertRules(
    projectId: string,
    tenantId: string,
  ): Promise<Array<AlertRule & { id: string }>> {
    const result = await this.pg.withTenant(tenantId, async (client) => {
      return client.query<{
        id: string;
        name: string;
        condition_type: string;
        threshold: number | null;
        window_seconds: number;
        notification_channel: string;
        is_enabled: boolean;
        es_query: string;
      }>(
        `SELECT id, name, condition_type, threshold, window_seconds, notification_channel, is_enabled, es_query::text AS es_query
         FROM alert_rules WHERE project_id = $1 ORDER BY created_at DESC`,
        [projectId],
      );
    });

    return result.rows.map((row) => ({
      id: row.id,
      name: row.name,
      conditionType: row.condition_type as AlertRule['conditionType'],
      threshold: row.threshold ?? undefined,
      windowSeconds: row.window_seconds,
      notificationChannel: row.notification_channel,
      isEnabled: row.is_enabled,
      esQuery: JSON.parse(row.es_query) as Record<string, unknown>,
    }));
  }

  async runPercolation(
    projectId: string,
    eventDoc: Record<string, unknown>,
  ): Promise<string[]> {
    const response = await this.es.client.search<{ alertRuleId: string }>({
      index: percolatorIndex,
      query: {
        bool: {
          must: [
            {
              percolate: {
                field: 'query',
                document: eventDoc,
              },
            },
            {
              term: { projectId },
            },
          ],
        },
      },
      _source: ['alertRuleId'],
    });

    return response.hits.hits
      .map((hit) => hit._source?.alertRuleId ?? hit._id)
      .filter((id): id is string => typeof id === 'string' && id.length > 0);
  }

  async fireDedupAlert(
    alertRuleId: string,
    eventId: string,
    channel: string,
    event: Record<string, unknown>,
  ): Promise<boolean> {
    // 1. Load dedup-fire Lua script
    const script = await this.redis.loadLua('dedup-fire');

    // 2. Attempt to acquire the dedup lock.
    // Key is per-alertRuleId only — not per-eventId — so that a burst of events
    // matching the same rule within the TTL window fires exactly one webhook
    // instead of one per event. The TTL (60 s) acts as the cooldown period.
    const lockKey = `fire-lock:${alertRuleId}`;
    const nodeId = process.env['HOSTNAME'] ?? nanoid(8);
    const TTL = 60;

    const result = await this.redis.client.evalsha(script.sha, 1, lockKey, nodeId, String(TTL));

    if (result !== 1) {
      // Another instance already fired this alert
      return false;
    }

    // 3. Won the lock — fire the webhook
    let webhookSucceeded = false;
    try {
      logger.info(
        { alertRuleId, eventId, channel },
        'fireDedupAlert: firing webhook',
      );

      const controller = new AbortController();
      const timeoutHandle = setTimeout(() => controller.abort(), WEBHOOK_TIMEOUT_MS);

      try {
        const response = await fetch(channel, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ alertRuleId, eventId, event }),
          signal: controller.signal,
        });
        webhookSucceeded = response.ok;
        if (!response.ok) {
          logger.warn(
            { alertRuleId, eventId, status: response.status },
            'fireDedupAlert: webhook returned non-2xx status',
          );
        }
      } finally {
        clearTimeout(timeoutHandle);
      }
    } catch (webhookErr) {
      logger.error({ err: webhookErr, alertRuleId, eventId }, 'fireDedupAlert: webhook call failed');
      webhookSucceeded = false;
    }

    if (!webhookSucceeded) {
      await this.redis.client.incr('counter:alert.fanout.failures').catch((err: unknown) => {
        logger.warn({ err }, 'fireDedupAlert: failed to increment failure counter');
      });
    }

    // 4. Update last_triggered_at in PG
    try {
      await this.pg.query(
        `UPDATE alert_rules SET last_triggered_at = NOW() WHERE id = $1`,
        [alertRuleId],
      );
    } catch (pgErr) {
      logger.warn({ err: pgErr, alertRuleId }, 'fireDedupAlert: failed to update last_triggered_at');
    }

    return true;
  }
}
