import { describe, it, expect, beforeAll, afterAll } from 'vitest'
import { MongoClient, type Collection } from 'mongodb'

const mongoClient = new MongoClient(
  process.env['MONGO_URL'] ?? 'mongodb://localhost:27018/pulseboard_test?directConnection=true'
)
const PROJECT_ID = `m3-test-proj-${Date.now()}`
let col: Collection

describe('M3: Error Intelligence Aggregation Pipeline', () => {
  beforeAll(async () => {
    await mongoClient.connect()
    const db = mongoClient.db('pulseboard_test')
    col = db.collection('events')

    // Seed 200 events with 10 fingerprints for this project
    const now = new Date()
    const sevenDaysAgo = new Date(now.getTime() - 7 * 86400_000)
    const oneDayAgo = new Date(now.getTime() - 86400_000)

    const fingerprints = Array.from({ length: 10 }, (_, i) => `fp-m3-${i}`)
    const browsers = ['chrome', 'firefox', 'safari', 'edge']
    const severities = ['error', 'fatal', 'warn', 'error', 'error'] // weighted

    const docs = Array.from({ length: 200 }, (_, i) => {
      const fp = fingerprints[i % 10]
      // Last 10 docs get occurredAt in last 24h (new fingerprints)
      const isNew = i >= 190
      const occurredAt = isNew
        ? new Date(oneDayAgo.getTime() + i * 3600_000)
        : new Date(sevenDaysAgo.getTime() + i * 3000_000)

      return {
        _id: `m3-event-${i}`,
        projectId: PROJECT_ID,
        type: 'error',
        severity: severities[i % severities.length] as 'error' | 'fatal' | 'warn',
        message: `Error message for fingerprint ${fp}`,
        occurredAt,
        ingestedAt: occurredAt,
        fingerprint: fp,
        userContext: i % 3 === 0 ? { userId: `user-${i % 30}` } : undefined,
        deviceContext: { browser: browsers[i % browsers.length] },
      }
    })

    await col.insertMany(docs as unknown as Document[], { ordered: false }).catch(() => {}) // ignore duplicate key
  })

  afterAll(async () => {
    await col.deleteMany({ projectId: PROJECT_ID })
    await mongoClient.close()
  })

  it('single $facet pipeline returns all 4 report sections', async () => {
    const sevenDaysAgo = new Date(Date.now() - 7 * 86400_000)
    const oneDayAgo = new Date(Date.now() - 86400_000)

    const start = performance.now()

    const rows = await col.aggregate([
      { $match: { projectId: PROJECT_ID, occurredAt: { $gte: sevenDaysAgo } } },
      {
        $facet: {
          topErrors: [
            {
              $group: {
                _id: '$fingerprint',
                message: { $first: '$message' },
                count: { $sum: 1 },
                firstSeen: { $min: '$occurredAt' },
                lastSeen: { $max: '$occurredAt' },
                affectedUsers: { $addToSet: '$userContext.userId' },
              },
            },
            { $sort: { count: -1 } },
            { $limit: 10 },
          ],
          hourlyHistogram: [
            {
              $group: {
                _id: {
                  $dateToString: { format: '%Y-%m-%dT%H:00:00Z', date: '$occurredAt' },
                },
                count: { $sum: 1 },
              },
            },
            { $sort: { _id: 1 } },
          ],
          severityBrowserBreakdown: [
            {
              $group: {
                _id: { severity: '$severity', browser: '$deviceContext.browser' },
                count: { $sum: 1 },
              },
            },
          ],
          newFingerprints: [
            { $match: { occurredAt: { $gte: oneDayAgo } } },
            { $group: { _id: '$fingerprint' } },
          ],
        },
      },
    ]).toArray()
    const result = rows[0] as Record<string, unknown[]> | undefined

    const elapsed = performance.now() - start
    console.log(`M3 pipeline elapsed: ${elapsed.toFixed(0)}ms`)

    expect(result?.['topErrors']).toBeDefined()
    expect(result?.['hourlyHistogram']).toBeDefined()
    expect(result?.['severityBrowserBreakdown']).toBeDefined()
    expect(result?.['newFingerprints']).toBeDefined()

    expect((result?.['topErrors'] as unknown[]).length).toBeGreaterThan(0)
    expect((result?.['topErrors'] as unknown[]).length).toBeLessThanOrEqual(10)

    // Verify the top error has required fields
    const top = (result?.['topErrors'] as Record<string, unknown>[])[0]
    expect(top).toHaveProperty('_id')
    expect(top).toHaveProperty('count')
    expect(top).toHaveProperty('firstSeen')
    expect(top).toHaveProperty('lastSeen')

    // New fingerprints should exist (last 10 events with distinct fingerprints in 24h)
    expect((result?.['newFingerprints'] as unknown[]).length).toBeGreaterThan(0)

    // Performance: on test dataset, well under 2s
    expect(elapsed).toBeLessThan(2000)
  }, 30_000)

  it('top errors are sorted by count descending', async () => {
    const sevenDaysAgo = new Date(Date.now() - 7 * 86400_000)
    const rows = await col.aggregate([
      { $match: { projectId: PROJECT_ID, occurredAt: { $gte: sevenDaysAgo } } },
      {
        $facet: {
          topErrors: [
            { $group: { _id: '$fingerprint', count: { $sum: 1 } } },
            { $sort: { count: -1 } },
            { $limit: 10 },
          ],
        },
      },
    ]).toArray()
    const result = rows[0] as Record<string, { count: number }[]> | undefined

    const counts = (result?.['topErrors'] ?? []).map(e => e.count)
    for (let i = 1; i < counts.length; i++) {
      expect(counts[i] ?? 0).toBeLessThanOrEqual(counts[i - 1] ?? 0)
    }
  })
})
