import { MongoClient } from 'mongodb';
import { randomUUID } from 'node:crypto';

const MONGO_URL = process.env['MONGO_URL'] ?? 'mongodb://localhost:27017/?directConnection=true';
const PROJECT_ID = '00000000-0000-0000-0000-000000000003';
const COUNT = 50_000;

const severities = ['error', 'fatal', 'warn', 'info', 'debug'];
const messages = [
  'Unhandled exception in payment processor',
  'Database connection timeout',
  'Rate limit exceeded for user session',
  'Authentication token invalid',
  'Null pointer dereference in handler',
  'Session expired unexpectedly',
  'Invalid request payload received',
  'Cache miss on critical path',
  'Circuit breaker open for downstream',
  'Memory allocation failed in worker',
];
const fingerprints = Array.from({ length: 20 }, () => randomUUID());

const client = new MongoClient(MONGO_URL);
await client.connect();
const db = client.db(process.env['MONGO_DB_NAME'] ?? 'pulseboard');
const col = db.collection('events');

const BATCH = 1000;
let inserted = 0;
const now = Date.now();

for (let i = 0; i < COUNT; i += BATCH) {
  const docs = [];
  for (let j = 0; j < BATCH && i + j < COUNT; j++) {
    const occurredAt = new Date(now - Math.random() * 30 * 24 * 3600 * 1000);
    docs.push({
      _id: randomUUID(),
      projectId: PROJECT_ID,
      type: 'error',
      severity: severities[Math.floor(Math.random() * severities.length)]!,
      message: messages[Math.floor(Math.random() * messages.length)]!,
      occurredAt,
      ingestedAt: new Date(),
      fingerprint: fingerprints[Math.floor(Math.random() * fingerprints.length)]!,
      tags: [{ key: 'env', value: 'production' }, { key: 'service', value: 'api' }],
      payload: { responseTimeMs: Math.floor(Math.random() * 2000) },
    });
  }
  await col.insertMany(docs as never[], { ordered: false }).catch(() => {});
  inserted += docs.length;
  process.stdout.write(`\r  inserted ${inserted.toLocaleString()}/${COUNT.toLocaleString()}`);
}
console.log('\nDone seeding Mongo events.');
await client.close();
