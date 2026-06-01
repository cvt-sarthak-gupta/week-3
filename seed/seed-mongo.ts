/**
 * seed-mongo.ts
 * Seeds MongoDB with 1,000,000 events across 30,000 projects.
 *
 * Run with: tsx seed/seed-mongo.ts
 */

import { MongoClient, type Db, type Collection } from 'mongodb';
import { randomUUID, createHash } from 'node:crypto';

// ---------------------------------------------------------------------------
// DB connection (standalone)
// ---------------------------------------------------------------------------

const MONGO_URL = process.env['MONGO_URL'] ?? 'mongodb://localhost:27017/?replicaSet=rs0';
const MONGO_DB_NAME = process.env['MONGO_DB_NAME'] ?? 'pulseboard';

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const TOTAL_EVENTS = 1_000_000;
const BATCH_SIZE = 10_000;
const PROJECT_COUNT = 30_000;
const FINGERPRINT_COUNT = 200;

// Severity distribution: 40% info, 40% warn, 15% error, 5% fatal
// (combined info+warn = 80%, error = 15%, fatal = 5%)
const SEVERITY_WEIGHTS = [
  { severity: 'info',  weight: 0.40 },
  { severity: 'warn',  weight: 0.40 },
  { severity: 'error', weight: 0.15 },
  { severity: 'fatal', weight: 0.05 },
] as const;

type Severity = 'debug' | 'info' | 'warn' | 'error' | 'fatal';
type EventType = 'error' | 'log' | 'metric' | 'custom';

const SERVICES = [
  'api-gateway', 'auth-service', 'user-service', 'order-service',
  'payment-service', 'notification-service', 'search-service',
  'analytics-service', 'storage-service', 'scheduler-service',
];

const ENVS = ['production', 'staging'] as const;

const SAMPLE_MESSAGES: Record<Severity, string[]> = {
  debug: [
    'Processing request',
    'Cache lookup performed',
    'Query executed in %dms',
  ],
  info: [
    'Request completed successfully',
    'User session started',
    'Payment processed for order #%d',
    'Email notification sent to %s',
    'Cache refreshed for key %s',
    'Background job completed',
    'Health check passed',
    'Rate limit checked: %d remaining',
    'Token validated for user %s',
    'Webhook delivered to %s',
  ],
  warn: [
    'High memory usage detected: %d%%',
    'Slow query detected: %dms',
    'Rate limit approaching for project %s',
    'Retry attempt %d of 3',
    'Deprecated API endpoint called: %s',
    'Cache miss rate elevated: %d%%',
    'Queue depth growing: %d messages',
    'Disk usage at %d%%',
    'Response time degraded to %dms',
    'Third-party API returned 429',
  ],
  error: [
    'Database connection failed: %s',
    'Unhandled exception in request handler',
    'Failed to deliver webhook after 3 retries',
    'Payment processing failed: card declined',
    'JWT validation failed: token expired',
    'File upload failed: storage quota exceeded',
    'External API call timeout after %dms',
    'Null pointer exception in %s.process()',
    'Redis pipeline failed: connection reset',
    'Schema validation failed for event %s',
  ],
  fatal: [
    'Out of memory: process killed',
    'Segmentation fault in native module',
    'Critical: database primary unreachable',
    'Fatal: message queue overflow',
    'Unrecoverable state: shutting down',
  ],
};

const FIRST_NAMES = ['alice', 'bob', 'carol', 'dave', 'eve', 'frank', 'grace', 'henry', 'iris', 'jake'];
const LAST_NAMES = ['smith', 'jones', 'brown', 'davis', 'miller', 'wilson', 'moore', 'taylor'];
const EMAIL_DOMAINS = ['gmail.com', 'yahoo.com', 'outlook.com', 'example.com'];

// ---------------------------------------------------------------------------
// Random helpers
// ---------------------------------------------------------------------------

function randInt(min: number, max: number): number {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

function pick<T>(arr: readonly T[]): T {
  return arr[Math.floor(Math.random() * arr.length)]!;
}

function pickWeighted(weights: typeof SEVERITY_WEIGHTS): Severity {
  const r = Math.random();
  let cumulative = 0;
  for (const w of weights) {
    cumulative += w.weight;
    if (r < cumulative) return w.severity;
  }
  return 'info';
}

function randomDate(daysBack: number): Date {
  const now = Date.now();
  const past = now - daysBack * 24 * 60 * 60 * 1000;
  return new Date(past + Math.random() * (now - past));
}

function interpolate(template: string): string {
  return template
    .replace(/%d/g, () => String(randInt(1, 10000)))
    .replace(/%s/g, () => `item-${randInt(1, 9999)}`);
}

function randomUserEmail(): string {
  return `${pick(FIRST_NAMES)}.${pick(LAST_NAMES)}@${pick(EMAIL_DOMAINS)}`;
}

function randomIp(): string {
  return `${randInt(1, 254)}.${randInt(0, 255)}.${randInt(0, 255)}.${randInt(1, 254)}`;
}

// ---------------------------------------------------------------------------
// Pregenerate project IDs and fingerprints
// ---------------------------------------------------------------------------

function generateProjectIds(count: number): string[] {
  const ids: string[] = [];
  for (let i = 0; i < count; i++) {
    ids.push(randomUUID());
  }
  return ids;
}

function generateFingerprints(count: number): string[] {
  const fps: string[] = [];
  const types: EventType[] = ['error', 'log', 'metric', 'custom'];
  for (let i = 0; i < count; i++) {
    const type = pick(types);
    const msg = `seed-fingerprint-${i}-${type}`;
    const hash = createHash('sha256').update(`${type}:${msg.slice(0, 100)}`).digest('hex').slice(0, 16);
    fps.push(hash);
  }
  return fps;
}

// ---------------------------------------------------------------------------
// Document factory
// ---------------------------------------------------------------------------

function makeEvent(
  projectId: string,
  fingerprint: string,
): Record<string, unknown> {
  const severity = pickWeighted(SEVERITY_WEIGHTS);
  const messageTemplate = pick(SAMPLE_MESSAGES[severity]);
  const message = interpolate(messageTemplate);

  // Type distribution: errors/fatals map to 'error' type, logs to 'log', rest split
  let type: EventType;
  if (severity === 'error' || severity === 'fatal') {
    type = 'error';
  } else if (severity === 'info') {
    type = Math.random() < 0.7 ? 'log' : 'metric';
  } else {
    type = Math.random() < 0.5 ? 'log' : 'custom';
  }

  const occurredAt = randomDate(90);
  const ingestedAt = new Date(occurredAt.getTime() + randInt(0, 1000)); // within 1s

  const doc: Record<string, unknown> = {
    _id: randomUUID(),
    projectId,
    type,
    severity,
    message,
    occurredAt,
    ingestedAt,
    fingerprint,
    tags: {
      env: pick(ENVS),
      service: pick(SERVICES),
    },
  };

  // 60% have userContext
  if (Math.random() < 0.60) {
    doc['userContext'] = {
      userId: randomUUID(),
      email: randomUserEmail(),
      ip: randomIp(),
    };
  }

  // 40% have payload with responseTimeMs
  if (Math.random() < 0.40) {
    doc['payload'] = {
      responseTimeMs: randInt(50, 5000),
    };
  }

  return doc;
}

// ---------------------------------------------------------------------------
// Setup indexes and validator
// ---------------------------------------------------------------------------

async function setupCollection(db: Db): Promise<Collection<Record<string, unknown>>> {
  // Create collection if needed
  const collections = await db.listCollections({ name: 'events' }).toArray();
  if (collections.length === 0) {
    await db.createCollection('events');
  }

  const collection = db.collection<Record<string, unknown>>('events');

  console.log('[mongo] Creating indexes…');

  await collection.createIndex({ projectId: 1, severity: 1, occurredAt: -1 });
  await collection.createIndex({ projectId: 1, fingerprint: 1, occurredAt: -1 });
  await collection.createIndex({ 'tags.env': 1, 'tags.service': 1 });
  await collection.createIndex({ projectId: 1, 'userContext.email': 1 });
  await collection.createIndex({ ingestedAt: 1 }, { expireAfterSeconds: 400 * 86400 }); // 400d TTL

  console.log('[mongo] Indexes created.');

  // Apply collection-level validator
  await db.command({
    collMod: 'events',
    validator: {
      $jsonSchema: {
        bsonType: 'object',
        required: ['_id', 'projectId', 'type', 'severity', 'message', 'occurredAt', 'ingestedAt', 'fingerprint'],
        properties: {
          _id: { bsonType: 'string' },
          projectId: { bsonType: 'string' },
          type: { enum: ['error', 'log', 'metric', 'custom'] },
          severity: { enum: ['debug', 'info', 'warn', 'error', 'fatal'] },
          message: { bsonType: 'string' },
          occurredAt: { bsonType: 'date' },
          ingestedAt: { bsonType: 'date' },
          fingerprint: { bsonType: 'string' },
          // payload is intentionally unconstrained — arbitrary JSON allowed
        },
      },
    },
    validationLevel: 'strict',
    validationAction: 'error',
  });

  console.log('[mongo] Collection validator applied.');

  return collection;
}

// ---------------------------------------------------------------------------
// Main seeder
// ---------------------------------------------------------------------------

export async function seedMongo(): Promise<{
  projectIds: string[];
  totalInserted: number;
}> {
  const client = new MongoClient(MONGO_URL, {
    serverSelectionTimeoutMS: 15_000,
    connectTimeoutMS: 15_000,
  });

  await client.connect();
  console.log('[mongo] Connected.');

  const db = client.db(MONGO_DB_NAME);

  // Drop existing events collection for a clean seed
  try {
    await db.collection('events').drop();
    console.log('[mongo] Dropped existing events collection.');
  } catch {
    // Collection may not exist yet — that's fine
  }

  const collection = await setupCollection(db);

  // Pregenerate project IDs and fingerprints
  console.log(`[mongo] Pregenerating ${PROJECT_COUNT.toLocaleString()} project IDs and ${FINGERPRINT_COUNT} fingerprints…`);
  const projectIds = generateProjectIds(PROJECT_COUNT);
  const fingerprints = generateFingerprints(FINGERPRINT_COUNT);

  // Assign a base weight to each project — some projects get more events
  // Use a power-law distribution: top 10% of projects get ~50% of events
  const projectWeights: number[] = projectIds.map((_, i) => {
    if (i < PROJECT_COUNT * 0.10) return 5; // top 10%: 5x weight
    if (i < PROJECT_COUNT * 0.30) return 2; // next 20%: 2x weight
    return 1;
  });

  const totalWeight = projectWeights.reduce((sum, w) => sum + w, 0);
  const projectCumulative: number[] = [];
  let cum = 0;
  for (const w of projectWeights) {
    cum += w / totalWeight;
    projectCumulative.push(cum);
  }

  function pickProject(): string {
    const r = Math.random();
    let lo = 0;
    let hi = projectCumulative.length - 1;
    while (lo < hi) {
      const mid = (lo + hi) >> 1;
      if (projectCumulative[mid]! < r) lo = mid + 1;
      else hi = mid;
    }
    return projectIds[lo]!;
  }

  console.log(`[mongo] Inserting ${TOTAL_EVENTS.toLocaleString()} events in batches of ${BATCH_SIZE.toLocaleString()}…`);

  let totalInserted = 0;
  let totalErrors = 0;

  for (let batch = 0; batch < TOTAL_EVENTS; batch += BATCH_SIZE) {
    const batchSize = Math.min(BATCH_SIZE, TOTAL_EVENTS - batch);
    const docs: Record<string, unknown>[] = [];

    for (let i = 0; i < batchSize; i++) {
      const projectId = pickProject();
      const fingerprint = pick(fingerprints);
      docs.push(makeEvent(projectId, fingerprint));
    }

    try {
      const result = await collection.insertMany(docs, { ordered: false });
      totalInserted += result.insertedCount;
    } catch (err: unknown) {
      // BulkWriteError: some inserts may succeed
      if (err && typeof err === 'object' && 'insertedCount' in err) {
        totalInserted += (err as { insertedCount: number }).insertedCount;
        totalErrors += batchSize - (err as { insertedCount: number }).insertedCount;
      } else {
        throw err;
      }
    }

    if ((batch + BATCH_SIZE) % 50_000 === 0 || totalInserted + totalErrors >= TOTAL_EVENTS) {
      console.log(`[mongo]   inserted: ${totalInserted.toLocaleString()} / ${TOTAL_EVENTS.toLocaleString()} (errors: ${totalErrors})`);
    }
  }

  console.log(`[mongo] Done. ${totalInserted.toLocaleString()} events inserted (${totalErrors} errors).`);

  await client.close();
  return { projectIds, totalInserted };
}

// ---------------------------------------------------------------------------
// Run directly
// ---------------------------------------------------------------------------

if (process.argv[1]?.endsWith('seed-mongo.ts') || process.argv[1]?.endsWith('seed-mongo.js')) {
  console.time('mongo');
  seedMongo()
    .then(() => { console.timeEnd('mongo'); process.exit(0); })
    .catch((err) => { console.error('[mongo] FATAL:', err); process.exit(1); });
}
