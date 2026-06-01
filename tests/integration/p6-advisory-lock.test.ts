/**
 * P6: Advisory locks for idempotent monthly usage updates
 *
 * Verifies that:
 *  - pg_try_advisory_xact_lock() with retry logic serialises concurrent
 *    increments correctly — ALL 50 increments must eventually succeed.
 *  - Without advisory locks, concurrent UPSERT still produces a consistent
 *    result (demonstrates DB-level row locking is safe on its own).
 *  - Advisory lock key is stable and unique per (tenant, year, month).
 *  - pg_try_advisory_xact_lock releases automatically on transaction end.
 */

import { describe, it, expect, beforeEach } from 'vitest'
import { getPgPool, truncateAll, createTestTenant, getContainer } from '../helpers/setup.js'

describe('P6: Advisory Lock for Monthly Usage', () => {
  beforeEach(async () => {
    await truncateAll()
  })

  it('50 concurrent increments with retry all succeed (matches production upsertMonthlyUsage)', async () => {
    // This test exercises the same retry loop used by IngestionService.upsertMonthlyUsage():
    //   1. INSERT into usage_dedup (idempotency gate)
    //   2. pg_try_advisory_xact_lock — if denied, back off and retry up to MAX_RETRIES
    //   3. UPSERT monthly_usage counter
    //
    // With MAX_RETRIES=5 and exponential back-off the final count MUST equal
    // CONCURRENT (all increments succeed), not merely be > 0.
    const container = getContainer()
    const pool = getPgPool()
    const { tenantId } = await createTestTenant()
    const now = new Date()
    const year = now.getUTCFullYear()
    const month = now.getUTCMonth() + 1
    const CONCURRENT = 50
    const MAX_RETRIES = 5

    await pool.query(
      `INSERT INTO monthly_usage (tenant_id, year, month, event_count)
       VALUES ($1, $2, $3, 0)
       ON CONFLICT DO NOTHING`,
      [tenantId, year, month],
    )

    async function incrementWithRetry(eventId: string): Promise<void> {
      // Idempotency gate — mirrors upsertMonthlyUsage step 1.
      const dedup = await pool.query<{ event_id: string }>(
        `INSERT INTO usage_dedup (event_id, tenant_id)
         VALUES ($1, $2)
         ON CONFLICT (event_id) DO NOTHING
         RETURNING event_id`,
        [eventId, tenantId],
      )
      if ((dedup.rowCount ?? 0) === 0) return // already counted

      const lockKey = `monthly_usage:${tenantId}:${year}:${month}`

      for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
        const client = await pool.connect()
        try {
          await client.query('BEGIN')

          const lockResult = await client.query<{ acquired: boolean }>(
            `SELECT pg_try_advisory_xact_lock(hashtext($1)) AS acquired`,
            [lockKey],
          )
          const acquired = lockResult.rows[0]?.acquired ?? false

          if (!acquired) {
            await client.query('ROLLBACK')
            // Exponential back-off + jitter matching production.
            const delay = Math.min(50 * Math.pow(2, attempt - 1), 500)
            const jitter = Math.floor(Math.random() * 50)
            await new Promise<void>((resolve) => setTimeout(resolve, delay + jitter))
            continue
          }

          await client.query(
            `INSERT INTO monthly_usage (tenant_id, year, month, event_count)
             VALUES ($1, $2, $3, 1)
             ON CONFLICT (tenant_id, year, month)
             DO UPDATE SET event_count = monthly_usage.event_count + 1`,
            [tenantId, year, month],
          )
          await client.query('COMMIT')
          return // success
        } catch (err) {
          await client.query('ROLLBACK').catch(() => undefined)
          if (attempt === MAX_RETRIES) throw err
        } finally {
          client.release()
        }
      }
    }

    // Generate unique event IDs so the usage_dedup gate doesn't drop any.
    const eventIds = Array.from({ length: CONCURRENT }, (_, i) => `p6-event-${tenantId}-${i}`)
    await Promise.all(eventIds.map((id) => incrementWithRetry(id)))

    const result = await pool.query<{ event_count: string }>(
      `SELECT event_count FROM monthly_usage WHERE tenant_id = $1 AND year = $2 AND month = $3`,
      [tenantId, year, month],
    )
    const count = Number(result.rows[0]!.event_count)

    // All 50 increments must have been applied — the retry loop must not silently
    // drop any when contention is resolved through back-off.
    expect(count).toBe(CONCURRENT)
  }, 60_000)

  it('concurrent UPSERT without advisory lock converges to exact count', async () => {
    // Demonstrates the DB UPSERT alone is safe — no lost updates, no phantom rows.
    const pool = getPgPool()
    const { tenantId } = await createTestTenant()
    const now = new Date()
    const year = now.getUTCFullYear()
    const month = now.getUTCMonth() + 1
    const CONCURRENT = 20

    await pool.query(
      `INSERT INTO monthly_usage (tenant_id, year, month, event_count)
       VALUES ($1, $2, $3, 0)
       ON CONFLICT DO NOTHING`,
      [tenantId, year, month],
    )

    async function incrementNoLock(): Promise<void> {
      const client = await pool.connect()
      try {
        await client.query('BEGIN')
        await client.query(
          `INSERT INTO monthly_usage (tenant_id, year, month, event_count)
           VALUES ($1, $2, $3, 1)
           ON CONFLICT (tenant_id, year, month)
           DO UPDATE SET event_count = monthly_usage.event_count + 1`,
          [tenantId, year, month],
        )
        await client.query('COMMIT')
      } catch {
        await client.query('ROLLBACK').catch(() => undefined)
      } finally {
        client.release()
      }
    }

    await Promise.all(Array.from({ length: CONCURRENT }, incrementNoLock))

    const result = await pool.query<{ event_count: string }>(
      `SELECT event_count FROM monthly_usage WHERE tenant_id = $1 AND year = $2 AND month = $3`,
      [tenantId, year, month],
    )
    expect(Number(result.rows[0]!.event_count)).toBe(CONCURRENT)
  })

  it('advisory lock key is stable and unique per (tenant, year, month)', async () => {
    const pool = getPgPool()

    const key1 = await pool.query<{ h: string }>(`SELECT hashtext('monthly_usage:abc:2025:1') AS h`)
    const key2 = await pool.query<{ h: string }>(`SELECT hashtext('monthly_usage:abc:2025:1') AS h`)
    const key3 = await pool.query<{ h: string }>(`SELECT hashtext('monthly_usage:abc:2025:2') AS h`)

    expect(key1.rows[0]!.h).toBe(key2.rows[0]!.h)     // same input → same key
    expect(key1.rows[0]!.h).not.toBe(key3.rows[0]!.h) // different month → different key
  })

  it('pg_try_advisory_xact_lock releases automatically on transaction end', async () => {
    const pool = getPgPool()
    const lockKey = 999_999

    const clientA = await pool.connect()
    await clientA.query('BEGIN')
    const res = await clientA.query<{ acquired: boolean }>(
      `SELECT pg_try_advisory_xact_lock($1) AS acquired`,
      [lockKey],
    )
    expect(res.rows[0]!.acquired).toBe(true)
    await clientA.query('COMMIT')
    clientA.release()

    // After commit the lock is released — a second client must acquire it.
    const clientB = await pool.connect()
    await clientB.query('BEGIN')
    const res2 = await clientB.query<{ acquired: boolean }>(
      `SELECT pg_try_advisory_xact_lock($1) AS acquired`,
      [lockKey],
    )
    expect(res2.rows[0]!.acquired).toBe(true)
    await clientB.query('COMMIT')
    clientB.release()
  })

  it('usage_dedup gate prevents double-counting the same eventId', async () => {
    const pool = getPgPool()
    const { tenantId } = await createTestTenant()
    const now = new Date()
    const year = now.getUTCFullYear()
    const month = now.getUTCMonth() + 1
    const eventId = `p6-dedup-${Date.now()}`

    await pool.query(
      `INSERT INTO monthly_usage (tenant_id, year, month, event_count)
       VALUES ($1, $2, $3, 0) ON CONFLICT DO NOTHING`,
      [tenantId, year, month],
    )

    // Insert the same eventId three times — only the first should increment.
    for (let i = 0; i < 3; i++) {
      const dedup = await pool.query(
        `INSERT INTO usage_dedup (event_id, tenant_id) VALUES ($1, $2)
         ON CONFLICT (event_id) DO NOTHING RETURNING event_id`,
        [eventId, tenantId],
      )
      if ((dedup.rowCount ?? 0) === 0) continue // already counted — skip

      await pool.query(
        `INSERT INTO monthly_usage (tenant_id, year, month, event_count)
         VALUES ($1, $2, $3, 1)
         ON CONFLICT (tenant_id, year, month)
         DO UPDATE SET event_count = monthly_usage.event_count + 1`,
        [tenantId, year, month],
      )
    }

    const result = await pool.query<{ event_count: string }>(
      `SELECT event_count FROM monthly_usage WHERE tenant_id = $1 AND year = $2 AND month = $3`,
      [tenantId, year, month],
    )
    // Exactly 1, not 3.
    expect(Number(result.rows[0]!.event_count)).toBe(1)
  })
})
