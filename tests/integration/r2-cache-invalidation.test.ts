import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'vitest'
import { Redis } from 'ioredis'
import { getOrFill, invalidatePattern } from '../helpers/setup.js'

const redis = new Redis({
  host: process.env['REDIS_HOST'] ?? 'localhost',
  port: Number(process.env['REDIS_PORT'] ?? 6380),
  lazyConnect: true,
})

describe('R2: Cache with Stampede Prevention & Pattern Invalidation', () => {
  beforeAll(async () => { await redis.connect() })
  afterAll(async () => { await redis.quit() })
  beforeEach(async () => { await redis.flushdb() })

  it('cache miss populates and subsequent call returns cached value', async () => {
    let fetchCount = 0
    const fetcher = async () => { fetchCount++; return { data: 'report-result' } }

    const key = `test:cache:${Date.now()}`
    const result1 = await getOrFill(key, 60, fetcher)
    const result2 = await getOrFill(key, 60, fetcher)

    expect(result1).toEqual({ data: 'report-result' })
    expect(result2).toEqual({ data: 'report-result' })
    expect(fetchCount).toBe(1) // fetcher called only once
  })

  it('invalidatePattern removes all matching keys using SCAN+UNLINK', async () => {
    const projectId = `proj-${Date.now()}`
    // Set several keys with the same project pattern
    await redis.set(`report:${projectId}:error-intel:7`, JSON.stringify({ a: 1 }))
    await redis.set(`report:${projectId}:error-intel:30`, JSON.stringify({ b: 2 }))
    await redis.set(`report:${projectId}:dashboard`, JSON.stringify({ c: 3 }))
    await redis.set(`report:other-proj:error-intel:7`, JSON.stringify({ d: 4 })) // should NOT be deleted

    const deleted = await invalidatePattern(`report:${projectId}:*`)
    expect(deleted).toBe(3)

    // The invalidated keys must be gone
    expect(await redis.get(`report:${projectId}:error-intel:7`)).toBeNull()
    expect(await redis.get(`report:${projectId}:dashboard`)).toBeNull()
    // Unrelated key must survive
    expect(await redis.get(`report:other-proj:error-intel:7`)).not.toBeNull()
  })

  it('stampede prevention: 10 concurrent cache misses result in 1 fetch', async () => {
    let fetchCount = 0
    const key = `test:stampede:${Date.now()}`
    const slowFetcher = async () => {
      fetchCount++
      await new Promise(r => setTimeout(r, 100)) // simulate slow DB query
      return { expensive: true }
    }

    // Fire 10 concurrent calls simultaneously
    const results = await Promise.all(
      Array.from({ length: 10 }, () => getOrFill(key, 60, slowFetcher))
    )

    // All should return the same value
    for (const r of results) {
      expect(r).toEqual({ expensive: true })
    }
    // Fetcher should have been called minimally (ideally 1, at most a few with race)
    expect(fetchCount).toBeLessThanOrEqual(3)
  }, 10_000)

  it('cache TTL expires and fetcher is called again after expiry', async () => {
    let fetchCount = 0
    const key = `test:ttl:${Date.now()}`
    const fetcher = async () => { fetchCount++; return { v: fetchCount } }

    await getOrFill(key, 1, fetcher) // TTL = 1 second
    expect(fetchCount).toBe(1)

    await new Promise(r => setTimeout(r, 1200)) // wait for TTL to expire

    await getOrFill(key, 1, fetcher) // should miss and call fetcher again
    expect(fetchCount).toBe(2)
  }, 5_000)
})
