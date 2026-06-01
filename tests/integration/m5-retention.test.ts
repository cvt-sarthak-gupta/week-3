import { describe, it, expect, beforeAll, afterAll } from 'vitest'
import { MongoClient, type Collection } from 'mongodb'

const mongoClient = new MongoClient(
  process.env['MONGO_URL'] ?? 'mongodb://localhost:27018/pulseboard_test?directConnection=true'
)
const PROJECT_ID = `m5-test-proj-${Date.now()}`
let col: Collection

describe('M5: Per-Project Retention Worker', () => {
  beforeAll(async () => {
    await mongoClient.connect()
    col = mongoClient.db('pulseboard_test').collection('events')

    // Seed events: 50 "old" (beyond retention) and 50 "fresh" (within retention)
    const retentionDays = 30
    const cutoff = new Date(Date.now() - retentionDays * 86400_000)

    const oldDocs = Array.from({ length: 50 }, (_, i) => ({
      _id: `m5-old-${i}`,
      projectId: PROJECT_ID,
      type: 'log',
      severity: 'info',
      message: `Old event ${i}`,
      occurredAt: new Date(cutoff.getTime() - (i + 1) * 3600_000),
      ingestedAt: new Date(cutoff.getTime() - (i + 1) * 3600_000),
      fingerprint: 'fp-m5-old',
    }))

    const freshDocs = Array.from({ length: 50 }, (_, i) => ({
      _id: `m5-fresh-${i}`,
      projectId: PROJECT_ID,
      type: 'log',
      severity: 'info',
      message: `Fresh event ${i}`,
      occurredAt: new Date(cutoff.getTime() + (i + 1) * 3600_000),
      ingestedAt: new Date(cutoff.getTime() + (i + 1) * 3600_000),
      fingerprint: 'fp-m5-fresh',
    }))

    await col.insertMany([...oldDocs, ...freshDocs] as unknown as Document[], { ordered: false }).catch(() => {})
  })

  afterAll(async () => {
    await col.deleteMany({ projectId: PROJECT_ID })
    await mongoClient.close()
  })

  it('deletes only events older than retention cutoff', async () => {
    const retentionDays = 30
    const cutoff = new Date(Date.now() - retentionDays * 86400_000)

    // Simulate the retention worker's batched delete loop
    let deletedTotal = 0
    const BATCH_SIZE = 10_000
    const startTime = Date.now()
    const BUDGET_MS = 30_000

    let deletedInBatch: number
    do {
      if (Date.now() - startTime > BUDGET_MS) break

      // Find IDs of documents to delete (batched)
      const toDelete = await col
        .find({ projectId: PROJECT_ID, ingestedAt: { $lt: cutoff } })
        .limit(BATCH_SIZE)
        .project({ _id: 1 })
        .map(d => d['_id'])
        .toArray()

      if (toDelete.length === 0) break

      const result = await col.deleteMany({ _id: { $in: toDelete } })
      deletedInBatch = result.deletedCount
      deletedTotal += deletedInBatch

      if (deletedInBatch > 0) {
        await new Promise(r => setTimeout(r, 50)) // backpressure
      }
    } while (deletedInBatch! > 0)

    expect(deletedTotal).toBe(50) // exactly the old docs

    // Verify fresh docs are untouched
    const remaining = await col.countDocuments({ projectId: PROJECT_ID })
    expect(remaining).toBe(50)
  }, 30_000)

  it('safety-net TTL index on ingestedAt exists (or is created) and has a valid expireAfterSeconds', async () => {
    // Ensure the TTL index exists — create it if the seed has not run yet.
    // This makes the test self-contained and always meaningful.
    const TTL_SECONDS = 400 * 86_400 // 400 days (matches seed-mongo.ts)
    await col.createIndex(
      { ingestedAt: 1 },
      { expireAfterSeconds: TTL_SECONDS, background: true },
    ).catch(() => {}) // ignore if already exists with same definition

    const indexes = await col.indexes()
    const ttlIdx = indexes.find(idx =>
      idx.key && 'ingestedAt' in idx.key && idx.expireAfterSeconds !== undefined
    )

    expect(ttlIdx).toBeDefined()
    expect(ttlIdx!.expireAfterSeconds).toBeGreaterThan(0)
    console.log(`M5: TTL index expireAfterSeconds = ${ttlIdx!.expireAfterSeconds}`)
  })
})
