/**
 * seed-elastic.ts
 * Reads events from MongoDB and bulk-indexes them into Elasticsearch.
 * Indexes into per-project monthly indices: logs-{projectId}-{YYYY.MM}
 *
 * Run with: tsx seed/seed-elastic.ts
 */

import { MongoClient } from 'mongodb';
import { Client as EsClient } from '@elastic/elasticsearch';

// ---------------------------------------------------------------------------
// Connections (standalone)
// ---------------------------------------------------------------------------

const MONGO_URL = process.env['MONGO_URL'] ?? 'mongodb://localhost:27017/?replicaSet=rs0';
const MONGO_DB_NAME = process.env['MONGO_DB_NAME'] ?? 'pulseboard';
const ES_URL = process.env['ES_URL'] ?? 'http://localhost:9200';

// Keep batches small to avoid OOM on the 512MB dev/test ES container.
const MONGO_BATCH_SIZE = 500;
const ES_BULK_MAX_BYTES = 512 * 1024; // 512 KB soft cap per bulk request
const PROGRESS_INTERVAL = 10_000;
// Sample only this many projects to cap shard count on a single-node dev ES.
const MAX_SEED_PROJECTS = 200;

// ---------------------------------------------------------------------------
// ILM policies
// ---------------------------------------------------------------------------

const ILM_POLICIES = [
  { name: 'logs-tier-30d',  deleteAfterDays: 30 },
  { name: 'logs-tier-90d',  deleteAfterDays: 90 },
  { name: 'logs-tier-365d', deleteAfterDays: 365 },
];

async function ensureIlmPolicies(es: EsClient): Promise<void> {
  for (const policy of ILM_POLICIES) {
    await es.ilm.putLifecycle({
      name: policy.name,
      policy: {
        phases: {
          hot: {
            min_age: '0ms',
            actions: {
              rollover: { max_age: '7d', max_primary_shard_size: '5gb' },
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
            min_age: `${policy.deleteAfterDays}d`,
            actions: { delete: {} },
          },
        },
      },
    });
    console.log(`[elastic] ILM policy ensured: ${policy.name}`);
  }
}

// ---------------------------------------------------------------------------
// Index template
// ---------------------------------------------------------------------------

async function ensureIndexTemplate(es: EsClient): Promise<void> {
  await es.indices.putIndexTemplate({
    name: 'logs-template',
    index_patterns: ['logs-*'],
    priority: 500,
    template: {
      settings: {
        analysis: {
          filter: {
            english_stop: { type: 'stop', stopwords: '_english_' },
            error_synonyms: { type: 'synonym', synonyms: ['exception, error, err'] },
            edge_ngram_filter: { type: 'edge_ngram', min_gram: 2, max_gram: 20 },
          },
          analyzer: {
            logs_analyzer: {
              type: 'custom',
              tokenizer: 'standard',
              filter: ['lowercase', 'english_stop', 'error_synonyms', 'edge_ngram_filter'],
            },
          },
        },
      },
      mappings: {
        dynamic: false,
        properties: {
          projectId:   { type: 'keyword' },
          type:        { type: 'keyword' },
          severity:    { type: 'keyword' },
          fingerprint: { type: 'keyword' },
          occurredAt:  { type: 'date' },
          ingestedAt:  { type: 'date' },
          message:     { type: 'text', analyzer: 'logs_analyzer' },
          stackTrace:  { type: 'text', analyzer: 'standard' },
          tags: {
            type: 'nested',
            properties: {
              key:   { type: 'keyword' },
              value: { type: 'keyword' },
            },
          },
          userContext: {
            properties: {
              userId: { type: 'keyword' },
              email:  { type: 'keyword' },
              ip:     { type: 'keyword' },
            },
          },
          payload: {
            properties: {
              responseTimeMs: { type: 'float', doc_values: true },
            },
          },
        },
      },
    },
  });
  console.log('[elastic] Index template "logs-template" ensured.');
}

// ---------------------------------------------------------------------------
// Index name helper
// ---------------------------------------------------------------------------

function esIndexName(projectId: string, occurredAt: Date): string {
  const yyyy = occurredAt.getUTCFullYear().toString();
  const mm = (occurredAt.getUTCMonth() + 1).toString().padStart(2, '0');
  return `logs-${projectId}-${yyyy}.${mm}`;
}

// ---------------------------------------------------------------------------
// Bulk flush helper
// ---------------------------------------------------------------------------

interface BulkOp {
  indexName: string;
  docId: string;
  doc: Record<string, unknown>;
}

async function flushBulk(
  es: EsClient,
  ops: BulkOp[],
): Promise<{ indexed: number; errors: number }> {
  if (ops.length === 0) return { indexed: 0, errors: 0 };

  // Group by index to minimise cross-index bulk requests
  const grouped = new Map<string, BulkOp[]>();
  for (const op of ops) {
    if (!grouped.has(op.indexName)) grouped.set(op.indexName, []);
    grouped.get(op.indexName)!.push(op);
  }

  let indexed = 0;
  let errors = 0;

  for (const [, indexOps] of grouped) {
    const operations: unknown[] = [];
    for (const op of indexOps) {
      operations.push({ index: { _index: op.indexName, _id: op.docId } });
      operations.push(op.doc);
    }

    const resp = await es.bulk({ operations, refresh: false });
    let firstError: unknown = null;
    for (const item of resp.items) {
      const action = item.index;
      if (action?.error) {
        if (!firstError) firstError = action.error;
        errors++;
      } else {
        indexed++;
      }
    }
    if (firstError && errors === indexOps.length) {
      console.error('[elastic] bulk error sample:', JSON.stringify(firstError));
    }
  }

  return { indexed, errors };
}

// ---------------------------------------------------------------------------
// Main seeder
// ---------------------------------------------------------------------------

export async function seedElastic(): Promise<void> {
  const mongoClient = new MongoClient(MONGO_URL, {
    serverSelectionTimeoutMS: 15_000,
    connectTimeoutMS: 15_000,
  });
  await mongoClient.connect();
  console.log('[elastic] MongoDB connected.');

  const es = new EsClient({
    node: ES_URL,
    requestTimeout: 60_000,
    maxRetries: 3,
  });

  // Verify ES is reachable
  await es.ping();
  console.log('[elastic] Elasticsearch reachable.');

  // Raise shard limit so 30k-project seeding can create all necessary indices
  await es.cluster.putSettings({
    persistent: { 'cluster.max_shards_per_node': 100000 },
  });

  // Ensure ILM + template
  await ensureIlmPolicies(es);
  await ensureIndexTemplate(es);

  const db = mongoClient.db(MONGO_DB_NAME);
  const collection = db.collection<Record<string, unknown>>('events');

  // Sample the first MAX_SEED_PROJECTS distinct project IDs to keep shard count
  // manageable on the 512 MB single-node dev ES container.
  const sampledProjects = await collection.distinct('projectId', {});
  const projectSet = new Set<string>(
    (sampledProjects as string[]).slice(0, MAX_SEED_PROJECTS),
  );
  console.log(`[elastic] Sampling ${projectSet.size} of ${sampledProjects.length} projects.`);

  const totalDocs = await collection.countDocuments({ projectId: { $in: [...projectSet] } });
  console.log(`[elastic] Events to index: ${totalDocs.toLocaleString()}`);

  const cursor = collection.find(
    { projectId: { $in: [...projectSet] } },
    { batchSize: MONGO_BATCH_SIZE },
  );

  let totalIndexed = 0;
  let totalErrors = 0;
  let pendingOps: BulkOp[] = [];
  let pendingBytes = 0;

  for await (const doc of cursor) {
    const projectId = String(doc['projectId'] ?? '');
    const docId = String(doc['_id'] ?? '');
    const rawOccurredAt = doc['occurredAt'];
    const occurredAt = rawOccurredAt instanceof Date
      ? rawOccurredAt
      : new Date(rawOccurredAt as string);

    const indexName = esIndexName(projectId, occurredAt);

    // Build the ES document (dates as ISO strings, strip _id — it's the ES _id)
    const rawIngestedAt = doc['ingestedAt'];
    const esDoc: Record<string, unknown> = {
      projectId,
      type:        doc['type'],
      severity:    doc['severity'],
      message:     doc['message'],
      fingerprint: doc['fingerprint'],
      occurredAt:  occurredAt.toISOString(),
      ingestedAt:  rawIngestedAt instanceof Date
        ? rawIngestedAt.toISOString()
        : new Date(rawIngestedAt as string).toISOString(),
    };

    if (doc['tags'])        esDoc['tags'] = doc['tags'];
    if (doc['userContext']) esDoc['userContext'] = doc['userContext'];
    if (doc['payload'])     esDoc['payload'] = doc['payload'];
    if (doc['traceId'])     esDoc['traceId'] = doc['traceId'];

    const docBytes = JSON.stringify(esDoc).length;
    pendingOps.push({ indexName, docId, doc: esDoc });
    pendingBytes += docBytes;

    // Flush when we hit batch size or byte cap
    if (pendingOps.length >= MONGO_BATCH_SIZE || pendingBytes >= ES_BULK_MAX_BYTES) {
      const { indexed, errors } = await flushBulk(es, pendingOps);
      totalIndexed += indexed;
      totalErrors += errors;
      pendingOps = [];
      pendingBytes = 0;

      const processed = totalIndexed + totalErrors;
      if (processed % PROGRESS_INTERVAL < MONGO_BATCH_SIZE || processed >= totalDocs) {
        console.log(`[elastic]   indexed: ${totalIndexed.toLocaleString()} / ${totalDocs.toLocaleString()} (errors: ${totalErrors})`);
      }
    }
  }

  // Flush remaining
  if (pendingOps.length > 0) {
    const { indexed, errors } = await flushBulk(es, pendingOps);
    totalIndexed += indexed;
    totalErrors += errors;
  }

  console.log(`[elastic] Done. Indexed: ${totalIndexed.toLocaleString()}, Errors: ${totalErrors}`);

  await mongoClient.close();
  await es.close();
}

// ---------------------------------------------------------------------------
// Run directly
// ---------------------------------------------------------------------------

if (process.argv[1]?.endsWith('seed-elastic.ts') || process.argv[1]?.endsWith('seed-elastic.js')) {
  console.time('elastic');
  seedElastic()
    .then(() => { console.timeEnd('elastic'); process.exit(0); })
    .catch((err) => { console.error('[elastic] FATAL:', err); process.exit(1); });
}
