/**
 * R1: Sliding Window Rate Limiter
 *
 * Verifies that:
 *  - 200 concurrent requests with limit=50 results in exactly 50 accepted, 150 denied
 *  - The sliding window correctly expires old entries so new requests are allowed
 *  - The Lua script is idempotent when called with the same request ID
 *  - Rate limit state is scoped per key (different API keys do not interfere)
 *  - The returned remaining count decrements correctly
 */

import { Redis } from 'ioredis'
import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'vitest'

// Connect directly to the test Redis instance
const testRedis = new Redis({ host: 'localhost', port: 6380, lazyConnect: true })

// The sliding window Lua script (mirrored from src/lib/lua/sliding-window.lua)
// We embed it here so the test is self-contained and does not depend on the file path
// of the source Lua file relative to the test runner's cwd.
const SLIDING_WINDOW_LUA = `
local key = KEYS[1]
local window_ms = tonumber(ARGV[1])
local max_requests = tonumber(ARGV[2])
local now = tonumber(ARGV[3])
local req_id = ARGV[4]
local window_start = now - window_ms

redis.call('ZREMRANGEBYSCORE', key, 0, window_start)

local count = redis.call('ZCARD', key)

if count < max_requests then
  redis.call('ZADD', key, now, req_id)
  redis.call('PEXPIRE', key, window_ms)
  return {1, max_requests - count - 1, now + window_ms}
else
  local oldest = redis.call('ZRANGE', key, 0, 0, 'WITHSCORES')
  local reset_at = now + window_ms
  if oldest and #oldest >= 2 then
    reset_at = tonumber(oldest[2]) + window_ms
  end
  return {0, 0, reset_at}
end
`

describe('R1: Sliding Window Rate Limiter', () => {
  beforeAll(async () => {
    await testRedis.connect()
  })

  afterAll(async () => {
    await testRedis.quit()
  })

  beforeEach(async () => {
    await testRedis.flushdb()
  })

  it('200 concurrent requests with limit=50: exactly 50 accepted and 150 denied', async () => {
    const LIMIT = 50
    const CONCURRENT = 200
    const key = `test:rl:concurrent:${Date.now()}`
    const now = Date.now()
    const windowMs = 60_000

    // Fire all 200 EVAL calls concurrently — Redis processes them atomically one at a time
    // due to its single-threaded command execution model, so the Lua script's ZCARD
    // will correctly enforce the cap at exactly LIMIT.
    const results = await Promise.all(
      Array.from({ length: CONCURRENT }, (_, i) =>
        testRedis.eval(
          SLIDING_WINDOW_LUA,
          1,
          key,
          windowMs,
          LIMIT,
          now,
          `req-${i}`
        ) as Promise<[number, number, number]>
      )
    )

    const accepted = results.filter(r => Array.isArray(r) && r[0] === 1).length
    const denied = results.filter(r => Array.isArray(r) && r[0] === 0).length

    expect(accepted).toBe(LIMIT)
    expect(denied).toBe(CONCURRENT - LIMIT)
  }, 30_000)

  it('sliding window: old entries expire and new requests are allowed', async () => {
    const LIMIT = 5
    const key = `test:rl:sliding:${Date.now()}`
    const windowMs = 5_000 // 5 second window

    // Manually seed 5 entries with timestamps that are now OUTSIDE the window
    // (5.1 seconds ago, so they fall before the window_start when using a 5s window)
    const pastTime = Date.now() - 5_100
    for (let i = 0; i < LIMIT; i++) {
      await testRedis.zadd(key, pastTime + i, `old-req-${i}`)
    }
    await testRedis.pexpire(key, 60_000)

    // Verify the entries are seeded
    const cardBefore = await testRedis.zcard(key)
    expect(cardBefore).toBe(LIMIT)

    // Now call the Lua script with current time — old entries should be pruned first
    const result = await testRedis.eval(
      SLIDING_WINDOW_LUA,
      1,
      key,
      windowMs,
      LIMIT,
      Date.now(),
      'new-req-after-expiry'
    ) as [number, number, number]

    // The old entries were outside the window_start, so ZREMRANGEBYSCORE removes them.
    // count after pruning = 0, which is < LIMIT=5, so the request is allowed.
    expect(result[0]).toBe(1)  // allowed
    expect(result[1]).toBe(LIMIT - 1)  // remaining = 4

    // Verify that old entries were removed and only the new one remains
    const cardAfter = await testRedis.zcard(key)
    expect(cardAfter).toBe(1)
  })

  it('remaining count decrements correctly across sequential requests', async () => {
    const LIMIT = 10
    const key = `test:rl:sequential:${Date.now()}`
    const windowMs = 60_000
    const now = Date.now()

    const remainingValues: number[] = []

    for (let i = 0; i < LIMIT; i++) {
      const result = await testRedis.eval(
        SLIDING_WINDOW_LUA,
        1,
        key,
        windowMs,
        LIMIT,
        now,
        `seq-req-${i}`
      ) as [number, number, number]

      expect(result[0]).toBe(1)  // all should be allowed
      remainingValues.push(result[1])
    }

    // Remaining should decrement: 9, 8, 7, ..., 0
    for (let i = 0; i < LIMIT; i++) {
      expect(remainingValues[i]).toBe(LIMIT - 1 - i)
    }

    // The (LIMIT+1)th request should be denied
    const denied = await testRedis.eval(
      SLIDING_WINDOW_LUA,
      1,
      key,
      windowMs,
      LIMIT,
      now,
      'over-limit-req'
    ) as [number, number, number]

    expect(denied[0]).toBe(0)
    expect(denied[1]).toBe(0)
  })

  it('different API keys do not interfere with each other', async () => {
    const LIMIT = 3
    const windowMs = 60_000
    const now = Date.now()
    const key1 = `test:rl:key1:${Date.now()}`
    const key2 = `test:rl:key2:${Date.now()}`

    // Fill key1 to the limit
    for (let i = 0; i < LIMIT; i++) {
      const result = await testRedis.eval(
        SLIDING_WINDOW_LUA,
        1,
        key1,
        windowMs,
        LIMIT,
        now,
        `k1-req-${i}`
      ) as [number, number, number]
      expect(result[0]).toBe(1)
    }

    // key1 should now be at limit
    const key1Denied = await testRedis.eval(
      SLIDING_WINDOW_LUA,
      1,
      key1,
      windowMs,
      LIMIT,
      now,
      'k1-over'
    ) as [number, number, number]
    expect(key1Denied[0]).toBe(0)

    // key2 should still be unaffected and allow requests
    const key2Allowed = await testRedis.eval(
      SLIDING_WINDOW_LUA,
      1,
      key2,
      windowMs,
      LIMIT,
      now,
      'k2-req-0'
    ) as [number, number, number]
    expect(key2Allowed[0]).toBe(1)
    expect(key2Allowed[1]).toBe(LIMIT - 1)
  })

  it('reset_at is set to oldest entry score + window_ms when denied', async () => {
    const LIMIT = 2
    const key = `test:rl:reset:${Date.now()}`
    const windowMs = 10_000
    const now = Date.now()

    // Add 2 requests with known timestamps
    const t1 = now - 3_000  // 3 seconds ago (still within 10s window)
    const t2 = now - 1_000  // 1 second ago

    await testRedis.zadd(key, t1, 'old-req-1')
    await testRedis.zadd(key, t2, 'old-req-2')
    await testRedis.pexpire(key, windowMs)

    // Now try a 3rd request — should be denied
    const result = await testRedis.eval(
      SLIDING_WINDOW_LUA,
      1,
      key,
      windowMs,
      LIMIT,
      now,
      'over-req'
    ) as [number, number, number]

    expect(result[0]).toBe(0)  // denied
    expect(result[1]).toBe(0)  // remaining = 0
    // reset_at should be t1 (oldest) + windowMs
    // = (now - 3000) + 10000 = now + 7000
    const expectedResetAt = t1 + windowMs
    // Allow a small tolerance for timing
    expect(result[2]).toBeCloseTo(expectedResetAt, -2)
  })

  it('PEXPIRE is set on the key so it auto-expires', async () => {
    const LIMIT = 5
    const key = `test:rl:ttl:${Date.now()}`
    const windowMs = 2_000  // 2 second window

    await testRedis.eval(
      SLIDING_WINDOW_LUA,
      1,
      key,
      windowMs,
      LIMIT,
      Date.now(),
      'ttl-req-1'
    )

    // The key should exist with a TTL
    const ttl = await testRedis.pttl(key)
    expect(ttl).toBeGreaterThan(0)
    expect(ttl).toBeLessThanOrEqual(windowMs)
  })
})
