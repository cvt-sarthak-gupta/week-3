import { randomUUID } from 'node:crypto';
import { nanoid } from 'nanoid';
import { pool, withTenant } from '../db/postgres.js';
import { esClient, percolatorIndex } from '../db/elastic.js';
import { loadLua } from '../db/redis.js';
import { incrCounter } from '../db/redis.js';
import { logger } from '../logger.js';
import type { AlertRule } from '../schemas/alert.js';

// ---------------------------------------------------------------------------
// createAlertRule
// ---------------------------------------------------------------------------

export async function createAlertRule(
  projectId: string,
  tenantId: string,
  rule: AlertRule,
): Promise<string> {
  const alertRuleId = randomUUID();

  // 1. INSERT into alert_rules in PG (tenant-scoped)
  await withTenant(tenantId, async (client) => {
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
  await esClient.index({
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
  return alertRuleId;
}

// ---------------------------------------------------------------------------
// updateAlertRule
// ---------------------------------------------------------------------------

export async function updateAlertRule(
  alertRuleId: string,
  projectId: string,
  tenantId: string,
  updates: Partial<AlertRule>,
): Promise<void> {
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
    params.push(alertRuleId, projectId);

    await withTenant(tenantId, async (client) => {
      await client.query(
        `UPDATE alert_rules SET ${setClauses.join(', ')}
         WHERE id = $${paramIndex} AND project_id = $${paramIndex + 1}`,
        params,
      );
    });
  }

  // Update percolator: DELETE + re-index with merged query
  if (updates.esQuery !== undefined || updates.notificationChannel !== undefined) {
    // Fetch current state to merge
    const pgResult = await pool.query<{
      es_query: string;
      notification_channel: string;
    }>(
      `SELECT es_query, notification_channel FROM alert_rules WHERE id = $1 AND project_id = $2`,
      [alertRuleId, projectId],
    );
    const row = pgResult.rows[0];
    if (row !== undefined) {
      await esClient.delete({ index: percolatorIndex, id: alertRuleId }).catch(() => {
        // Ignore if document doesn't exist
      });
      await esClient.index({
        index: percolatorIndex,
        id: alertRuleId,
        document: {
          query: JSON.parse(row.es_query) as Record<string, unknown>,
          projectId,
          alertRuleId,
          notificationChannel: row.notification_channel,
        },
        refresh: 'wait_for',
      });
    }
  }

  logger.info({ alertRuleId, projectId, tenantId }, 'Alert rule updated');
}

// ---------------------------------------------------------------------------
// deleteAlertRule
// ---------------------------------------------------------------------------

export async function deleteAlertRule(
  alertRuleId: string,
  projectId: string,
  tenantId: string,
): Promise<void> {
  // Delete from PG
  await withTenant(tenantId, async (client) => {
    await client.query(
      `DELETE FROM alert_rules WHERE id = $1 AND project_id = $2`,
      [alertRuleId, projectId],
    );
  });

  // Delete percolator document from ES
  await esClient.delete({ index: percolatorIndex, id: alertRuleId }).catch((err: unknown) => {
    logger.warn({ err, alertRuleId }, 'deleteAlertRule: ES percolator doc delete failed (non-fatal)');
  });

  logger.info({ alertRuleId, projectId, tenantId }, 'Alert rule deleted');
}

// ---------------------------------------------------------------------------
// runPercolation
// ---------------------------------------------------------------------------

export async function runPercolation(
  projectId: string,
  eventDoc: Record<string, unknown>,
): Promise<string[]> {
  const response = await esClient.search<{ alertRuleId: string }>({
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

// ---------------------------------------------------------------------------
// fireDedupAlert
// ---------------------------------------------------------------------------

const WEBHOOK_TIMEOUT_MS = 5_000;

export async function fireDedupAlert(
  alertRuleId: string,
  eventId: string,
  notificationChannel: string,
  eventDoc: Record<string, unknown>,
): Promise<boolean> {
  // 1. Load dedup-fire Lua script
  const script = await loadLua('dedup-fire');

  // 2. Attempt to acquire the dedup lock
  const lockKey = `fire-lock:${alertRuleId}:${eventId}`;
  const nodeId = process.env['HOSTNAME'] ?? nanoid(8);
  const TTL = 60;

  const result = await script.evalsha([lockKey], [nodeId, TTL]);

  if (result !== 1) {
    // Another instance already fired this alert
    return false;
  }

  // 3. Won the lock — fire the webhook
  let webhookSucceeded = false;
  try {
    logger.info(
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
    await incrCounter('alert.fanout.failures').catch((err: unknown) => {
      logger.warn({ err }, 'fireDedupAlert: failed to increment failure counter');
    });
  }

  // 4. Update last_triggered_at in PG
  try {
    await pool.query(
      `UPDATE alert_rules SET last_triggered_at = NOW() WHERE id = $1`,
      [alertRuleId],
    );
  } catch (pgErr) {
    logger.warn({ err: pgErr, alertRuleId }, 'fireDedupAlert: failed to update last_triggered_at');
  }

  return true;
}
