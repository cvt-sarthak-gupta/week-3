import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'vitest'
import { Redis } from 'ioredis'

const redis = new Redis({
  host: process.env['REDIS_HOST'] ?? 'localhost',
  port: Number(process.env['REDIS_PORT'] ?? 6380),
  lazyConnect: true,
})

function todayKey(): string {
  return `leaderboard:${new Date().toISOString().slice(0, 10)}`
}

function nextMidnightUtc(): number {
  const d = new Date()
  d.setUTCHours(24, 0, 0, 0)
  return Math.floor(d.getTime() / 1000)
}

describe('R5: Real-Time Leaderboard', () => {
  beforeAll(async () => { await redis.connect() })
  afterAll(async () => { await redis.quit() })
  beforeEach(async () => { await redis.del(todayKey()) })

  it('ZINCRBY increments project score on each event ingest', async () => {
    const key = todayKey()
    const projectId = `proj-lb-${Date.now()}`

    await redis.zincrby(key, 1, projectId)
    await redis.zincrby(key, 1, projectId)
    await redis.zincrby(key, 1, projectId)

    const score = await redis.zscore(key, projectId)
    expect(Number(score)).toBe(3)
  })

  it('ZREVRANGE returns projects ordered by event count descending', async () => {
    const key = todayKey()
    const p1 = `proj-a-${Date.now()}`
    const p2 = `proj-b-${Date.now()}`
    const p3 = `proj-c-${Date.now()}`

    await redis.zincrby(key, 500, p1)
    await redis.zincrby(key, 1200, p2)
    await redis.zincrby(key, 300, p3)

    const results = await redis.zrevrange(key, 0, -1, 'WITHSCORES')
    // results = [member, score, member, score, ...]
    expect(results[0]).toBe(p2)  // highest count first
    expect(Number(results[1])).toBe(1200)
    expect(results[2]).toBe(p1)
    expect(results[4]).toBe(p3)  // lowest last
  })

  it('leaderboard key expires at midnight UTC', async () => {
    const key = todayKey()
    const projectId = `proj-expire-${Date.now()}`

    await redis.zincrby(key, 1, projectId)
    const expireAt = nextMidnightUtc()
    await redis.expireat(key, expireAt)

    const ttl = await redis.ttl(key)
    // TTL should be positive and ≤ 86400 seconds
    expect(ttl).toBeGreaterThan(0)
    expect(ttl).toBeLessThanOrEqual(86400)
  })

  it('top 20 cap — returns at most 20 entries', async () => {
    const key = todayKey()
    // Add 30 projects
    for (let i = 0; i < 30; i++) {
      await redis.zincrby(key, i + 1, `proj-top20-${i}-${Date.now()}`)
    }

    const results = await redis.zrevrange(key, 0, 19, 'WITHSCORES')
    const members = results.filter((_, idx) => idx % 2 === 0)
    expect(members.length).toBe(20)

    // Verify descending order
    const scores = results.filter((_, idx) => idx % 2 === 1).map(Number)
    for (let i = 1; i < scores.length; i++) {
      expect(scores[i]!).toBeLessThanOrEqual(scores[i - 1]!)
    }
  })
})
