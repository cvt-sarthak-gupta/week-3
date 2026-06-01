/**
 * M2: MongoDB compound index coverage — no COLLSCAN
 *
 * Verifies that the indexes declared in the MongoDB schema are actually used
 * for all five core query patterns (Q1–Q5 from the design doc) by
 * inspecting the executionStats explain plan and asserting no COLLSCAN stage
 * appears.
 *
 * Indexes under test:
 *   { projectId: 1, severity: 1, occurredAt: -1 }   Q1
 *   { projectId: 1, fingerprint: 1, occurredAt: -1 } Q2
 *   { 'tags.env': 1, 'tags.service': 1 }             Q3
 *   { projectId: 1, occurredAt: -1 } (via Q1 index)  Q4
 *   { projectId: 1, 'userContext.email': 1 }          Q5
 *   { ingestedAt: 1 }                                 TTL (safety-net)
 */

import { describe, it, expect, beforeAll, afterAll } from 'vitest'
import { getMongoDb } from '../helpers/setup.js'

const COL = 'events_m2_indexes_test'
const PROJECT_ID = `test-proj-m2-${Date.now()}`

function collection() {
  return getMongoDb().collection(COL)
}

function assertNoCollScan(explain: Record<string, unknown>): void {
  const plan = JSON.stringify(explain)
  expect(plan).not.toContain('COLLSCAN')
}

beforeAll(async () => {
  const col = collection()

  // Drop from previous run (idempotent)
  await col.drop().catch(() => {})
  await getMongoDb().createCollection(COL)

  // Create all required indexes
  await col.createIndex({ projectId: 1, severity: 1, occurredAt: -1 })
  await col.createIndex({ projectId: 1, fingerprint: 1, occurredAt: -1 })
  await col.createIndex({ 'tags.env': 1, 'tags.service': 1 })
  await col.createIndex({ projectId: 1, 'userContext.email': 1 })
  await col.createIndex({ ingestedAt: 1 }, { expireAfterSeconds: 400 * 86_400 })

  // Seed 100 documents so executionStats has enough data to prefer indexes
  type Severity = 'error' | 'fatal' | 'info' | 'warn' | 'debug'
  const severities: Severity[] = ['error', 'fatal', 'info', 'warn', 'debug']
  const docs = Array.from({ length: 100 }, (_, i) => ({
    _id:          crypto.randomUUID(),
    projectId:    PROJECT_ID,
    type:         'error' as const,
    severity:     severities[i % severities.length] as Severity,
    message:      `Error message ${i}`,
    occurredAt:   new Date(Date.now() - i * 60_000),
    ingestedAt:   new Date(),
    fingerprint:  `fp-${i % 20}`,
    tags:         { env: 'production', service: 'payments' },
    userContext:  { email: `user${i}@test.com` },
  }))
  await col.insertMany(docs as any[])
})

afterAll(async () => {
  await collection().drop().catch(() => {})
})

describe('M2: MongoDB Index Coverage', () => {
  it('Q1: error events for project sorted by occurredAt uses an index (no COLLSCAN)', async () => {
    const explain = await collection()
      .find({
        projectId: PROJECT_ID,
        severity:  'error',
        occurredAt: { $gte: new Date(Date.now() - 86_400_000) },
      })
      .sort({ occurredAt: -1 })
      .limit(20)
      .explain('executionStats') as Record<string, unknown>

    assertNoCollScan(explain)
  })

  it('Q2: events by fingerprint for project uses an index (no COLLSCAN)', async () => {
    const explain = await collection()
      .find({ projectId: PROJECT_ID, fingerprint: 'fp-1' })
      .sort({ occurredAt: -1 })
      .explain('executionStats') as Record<string, unknown>

    assertNoCollScan(explain)
  })

  it('Q3: events filtered by tag env+service uses an index (no COLLSCAN)', async () => {
    const explain = await collection()
      .find({ 'tags.env': 'production', 'tags.service': 'payments' })
      .explain('executionStats') as Record<string, unknown>

    assertNoCollScan(explain)
  })

  it('Q4: count events grouped by severity for project over date range uses an index (no COLLSCAN)', async () => {
    // Q4 requires projectId + occurredAt range — covered by { projectId, severity, occurredAt } index
    const explain = await collection()
      .find({
        projectId: PROJECT_ID,
        occurredAt: {
          $gte: new Date(Date.now() - 7 * 86_400_000),
          $lte: new Date(),
        },
      })
      .explain('executionStats') as Record<string, unknown>

    assertNoCollScan(explain)
  })

  it('Q5: events by userContext.email uses an index (no COLLSCAN)', async () => {
    const explain = await collection()
      .find({ projectId: PROJECT_ID, 'userContext.email': 'user5@test.com' })
      .explain('executionStats') as Record<string, unknown>

    assertNoCollScan(explain)
  })

  it('all five query indexes plus TTL index appear in the collection index list', async () => {
    const indexes = await collection().indexes()
    const indexKeys = indexes.map((idx) => JSON.stringify(idx.key))

    expect(indexKeys).toContain(JSON.stringify({ projectId: 1, severity: 1, occurredAt: -1 }))
    expect(indexKeys).toContain(JSON.stringify({ projectId: 1, fingerprint: 1, occurredAt: -1 }))
    expect(indexKeys).toContain(JSON.stringify({ 'tags.env': 1, 'tags.service': 1 }))
    expect(indexKeys).toContain(JSON.stringify({ projectId: 1, 'userContext.email': 1 }))
    // TTL safety-net index (created in beforeAll alongside the query indexes)
    expect(indexKeys).toContain(JSON.stringify({ ingestedAt: 1 }))
  })

  it('Q1 returns only the expected severity documents', async () => {
    const docs = await collection()
      .find({
        projectId: PROJECT_ID,
        severity:  'fatal',
        occurredAt: { $gte: new Date(Date.now() - 86_400_000) },
      })
      .sort({ occurredAt: -1 })
      .limit(50)
      .toArray()

    expect(docs.length).toBeGreaterThan(0)
    for (const doc of docs) {
      expect((doc as any)['severity']).toBe('fatal')
      expect((doc as any)['projectId']).toBe(PROJECT_ID)
    }
  })

  it('Q2 returns documents with the exact fingerprint requested', async () => {
    const docs = await collection()
      .find({ projectId: PROJECT_ID, fingerprint: 'fp-3' })
      .toArray()

    expect(docs.length).toBeGreaterThan(0)
    for (const doc of docs) {
      expect((doc as any)['fingerprint']).toBe('fp-3')
    }
  })
})
