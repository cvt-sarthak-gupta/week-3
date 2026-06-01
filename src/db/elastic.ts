import { Client } from '@elastic/elasticsearch';
import { config } from '../config.js';
import { logger } from '../logger.js';

export const esClient = new Client({
  node: config.es.url,
  requestTimeout: 30_000,
  maxRetries: 3,
});

// Index / alias naming helpers

/**
 * Returns the time-bucketed index name for a project and month.
 * e.g. `logs-my-project-2024.03`
 */
export function indexName(projectId: string, date?: Date): string {
  const d = date ?? new Date();
  const yyyy = d.getUTCFullYear().toString();
  const mm = (d.getUTCMonth() + 1).toString().padStart(2, '0');
  return `logs-${projectId}-${yyyy}.${mm}`;
}

/**
 * Returns the always-current alias for a project.
 * e.g. `logs-my-project-active`
 */
export function aliasName(projectId: string): string {
  return `logs-${projectId}-active`;
}

interface TierPolicy {
  name: string;
  deleteAfterRolloverDays: number;
}

const TIER_POLICIES: TierPolicy[] = [
  { name: 'logs-tier-30d', deleteAfterRolloverDays: 30 },
  { name: 'logs-tier-90d', deleteAfterRolloverDays: 90 },
  { name: 'logs-tier-365d', deleteAfterRolloverDays: 365 },
];

/**
 * Creates or updates all three ILM lifecycle policies at startup.
 *
 * Phases:
 *   hot   → rollover at 7d / 5 GB, priority 100
 *   warm  → enter 7d after rollover, forcemerge (1 segment), shrink to 1 shard
 *   cold  → enter 30d after rollover, searchable-snapshot / freeze
 *   delete → enter `deleteAfterRolloverDays` after rollover
 */
export async function ensureIlmPolicies(): Promise<void> {
  for (const policy of TIER_POLICIES) {
    const body: Parameters<typeof esClient.ilm.putLifecycle>[0] = {
      name: policy.name,
      policy: {
        phases: {
          hot: {
            min_age: '0ms',
            actions: {
              rollover: {
                max_age: '7d',
                max_primary_shard_size: '5gb',
              },
              set_priority: { priority: 100 },
            },
          },
          warm: {
            min_age: '7d',
            actions: {
              set_priority: { priority: 50 },
              forcemerge: { max_num_segments: 1 },
              shrink: { number_of_shards: 1 },
            },
          },
          cold: {
            min_age: '30d',
            actions: {
              set_priority: { priority: 0 },
              freeze: {},
            },
          },
          delete: {
            min_age: `${policy.deleteAfterRolloverDays}d`,
            actions: {
              delete: {},
            },
          },
        },
      },
    };

    await esClient.ilm.putLifecycle(body);
    logger.info({ policy: policy.name }, 'Elasticsearch ILM policy ensured');
  }
}

/**
 * Maps a retention value (days) to one of the three named ILM policies.
 */
export function resolveTierPolicy(retentionDays: number): string {
  if (retentionDays <= 30) return 'logs-tier-30d';
  if (retentionDays <= 90) return 'logs-tier-90d';
  return 'logs-tier-365d';
}

/**
 * Creates / updates the composable index template `logs-template` that covers
 * all `logs-*` indices. Sets up:
 *   - A custom analyser with lowercase + English stop words + synonym filter
 *     (exception/error/err) + edge_ngram (min=2, max=20)
 *   - Field mappings for all core log fields
 */
export async function ensureIndexTemplate(): Promise<void> {
  const body: Parameters<typeof esClient.indices.putIndexTemplate>[0] = {
    name: 'logs-template',
    index_patterns: ['logs-*'],
    priority: 500,
    template: {
      settings: {
        analysis: {
          filter: {
            english_stop: {
              type: 'stop',
              stopwords: '_english_',
            },
            error_synonyms: {
              type: 'synonym',
              synonyms: ['exception, error, err'],
            },
            edge_ngram_filter: {
              type: 'edge_ngram',
              min_gram: 2,
              max_gram: 20,
            },
          },
          analyzer: {
            logs_analyzer: {
              type: 'custom',
              tokenizer: 'standard',
              filter: [
                'lowercase',
                'english_stop',
                'error_synonyms',
                'edge_ngram_filter',
              ],
            },
          },
        },
        'index.lifecycle.rollover_alias': 'placeholder', // overridden per-index
      },
      mappings: {
        dynamic: false,
        properties: {
          message: {
            type: 'text',
            analyzer: 'logs_analyzer',
          },
          severity: {
            type: 'keyword',
          },
          stackTrace: {
            type: 'text',
            analyzer: 'standard',
          },
          tags: {
            type: 'nested',
            properties: {
              key: { type: 'keyword' },
              value: { type: 'keyword' },
            },
          },
          occurredAt: {
            type: 'date',
          },
          projectId: {
            type: 'keyword',
          },
          fingerprint: {
            type: 'keyword',
          },
          payload: {
            properties: {
              responseTimeMs: {
                type: 'float',
                doc_values: true,
              },
            },
          },
        },
      },
    },
  };

  await esClient.indices.putIndexTemplate(body);
  logger.info('Elasticsearch index template "logs-template" ensured');
}

// One-time guard: ensure our index template is registered before creating any
// per-project index. This overrides the built-in ES8 `logs` data-stream template.
let _templateEnsured = false;
export async function ensureTemplateOnce(): Promise<void> {
  if (_templateEnsured) return;
  await ensureIlmPolicies();
  await ensureIndexTemplate();
  await ensurePercolatorIndex();
  _templateEnsured = true;
}

/**
 * Ensures the monthly index for `projectId` exists and the project alias
 * points to it, then applies the appropriate ILM policy.
 */
export async function applyPolicyForProject(
  projectId: string,
  retentionDays: number,
): Promise<void> {
  await ensureTemplateOnce();
  const policyName = resolveTierPolicy(retentionDays);
  const idx = indexName(projectId);
  const alias = aliasName(projectId);

  // Create index if it doesn't already exist.
  const exists = await esClient.indices.exists({ index: idx });
  if (!exists) {
    await esClient.indices.create({
      index: idx,
      settings: {
        'index.lifecycle.name': policyName,
        'index.lifecycle.rollover_alias': alias,
      },
      aliases: {
        [alias]: { is_write_index: true },
      },
    });
    logger.info({ index: idx, alias, policy: policyName }, 'Elasticsearch index created');
  } else {
    // Update ILM policy on existing index.
    await esClient.indices.putSettings({
      index: idx,
      settings: {
        'index.lifecycle.name': policyName,
      },
    });
    logger.info({ index: idx, policy: policyName }, 'Elasticsearch ILM policy updated on existing index');
  }
}

/**
 * Bulk-indexes an array of log/event documents into the project's active alias.
 * Uses `eventId` (field `_id`) as the ES document `_id`.
 */
export async function bulkIndex(
  projectId: string,
  docs: Array<Record<string, unknown>>,
): Promise<{ indexed: number; errors: number }> {
  if (docs.length === 0) return { indexed: 0, errors: 0 };

  const alias = aliasName(projectId);
  const operations = docs.flatMap((doc) => [
    { index: { _index: alias, _id: doc['_id'] as string | undefined } },
    doc,
  ]);

  const response = await esClient.bulk({ operations, refresh: false });

  let indexed = 0;
  let errors = 0;

  for (const item of response.items) {
    const op = item['index'];
    if (op?.error != null) {
      errors++;
      logger.warn({ error: op.error, id: op._id }, 'Elasticsearch bulk index error');
    } else {
      indexed++;
    }
  }

  return { indexed, errors };
}

export const percolatorIndex = 'alert_percolator';

/**
 * Creates the alert percolator index with:
 *   - `query` field mapped as `percolator`
 *   - Mirrored field mappings from the logs-* template so percolation works
 *     correctly against live documents.
 */
export async function ensurePercolatorIndex(): Promise<void> {
  const exists = await esClient.indices.exists({ index: percolatorIndex });
  if (exists) {
    logger.info({ index: percolatorIndex }, 'Percolator index already exists, skipping creation');
    return;
  }

  await esClient.indices.create({
    index: percolatorIndex,
    mappings: {
      properties: {
        // The percolator query field.
        query: { type: 'percolator' },
        // Mirrored fields from logs-* template — percolation requires the same
        // field mappings as the documents being matched.
        message: { type: 'text', analyzer: 'standard' },
        severity: { type: 'keyword' },
        stackTrace: { type: 'text', analyzer: 'standard' },
        tags: {
          type: 'nested',
          properties: {
            key: { type: 'keyword' },
            value: { type: 'keyword' },
          },
        },
        occurredAt: { type: 'date' },
        projectId: { type: 'keyword' },
        fingerprint: { type: 'keyword' },
        payload: {
          properties: {
            responseTimeMs: { type: 'float', doc_values: true },
          },
        },
      },
    },
  });

  logger.info({ index: percolatorIndex }, 'Elasticsearch percolator index created');
}

export async function healthCheck(): Promise<{ ok: boolean; latencyMs: number }> {
  const start = Date.now();
  try {
    await esClient.ping();
    return { ok: true, latencyMs: Date.now() - start };
  } catch (err) {
    logger.error({ err }, 'Elasticsearch healthCheck failed');
    return { ok: false, latencyMs: Date.now() - start };
  }
}

export async function close(): Promise<void> {
  await esClient.close();
  logger.info('Elasticsearch client closed');
}
