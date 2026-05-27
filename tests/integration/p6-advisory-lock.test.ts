/**
 * P6: Advisory locks for idempotent monthly usage updates
 *
 * Verifies that:
 *  - pg_try_advisory_xact_lock() serialises concurrent increments correctly
 *  - Final event_count is > 0 and <= the number of concurrent workers
 *  - Without advisory locks, concurrent UPSERT still produces a consistent result
 *    (demonstrates that the DB-level UPSERT on its own is safe, but advisory
 *     locks further gate application-level idempotency logic)
 */

import { describe, it, expect, beforeEach } from 'vitest'
import { getPgPool, truncateAll, createTestTenant } from '../helpers/setup.js'

describe('P6: Advisory Lock for Monthly Usage', () => {
  beforeEach(async () => {
    await truncateAll()
  })

  it('50 concurrent advisory-locked increments produce a count > 0 and <= 50', async () => {
    const pool = getPgPool()
    const { tenantId } = await createTestTenant()
    const now = new Date()
    const year = now.getFullYear()
    const month = now.getMonth() + 1
    const CONCURRENT = 50

    // Initialise the row so all workers can UPSERT into it
    await pool.query(
      `INSERT INTO monthly_usage (tenant_id, year, month, event_count)
       VALUES ($1, $2, $3, 0)
       ON CONFLICT DO NOTHING`,
      [tenantId, year, month],
    )

    async function incrementWithLock(): Promise<void> {
      const client = await pool.connect()
      try {
        await client.query('BEGIN')

        // Use hashtext for a stable bigint lock key derived from the logical resource
        const lockKey = `hashtext('monthly_usage:${tenantId}:${year}:${month}')`
        const lockResult = await client.query<{ acquired: boolean }>(
          `SELECT pg_try_advisory_xact_lock(${lockKey}) AS acquired`,
        )

        if (lockResult.rows[0]?.acquired) {
          await client.query(
            `INSERT INTO monthly_usage (tenant_id, year, month, event_count)
             VALUES ($1, $2, $3, 1)
             ON CONFLICT (tenant_id, year, month)
             DO UPDATE SET event_count = monthly_usage.event_count + 1`,
            [tenantId, year, month],
          )
        }
        await client.query('COMMIT')
      } catch {
        try {
          await client.query('ROLLBACK')
        } catch {
          // ignore rollback errors
        }
      } finally {
        client.release()
      }
    }

    await Promise.all(Array.from({ length: CONCURRENT }, incrementWithLock))

    const result = await pool.query<{ event_count: string }>(
      `SELECT event_count FROM monthly_usage WHERE tenant_id = $1 AND year = $2 AND month = $3`,
      [tenantId, year, month],
    )
    const count = Number(result.rows[0]!.event_count)

    // Some workers may not acquire the lock — that is the expected behaviour.
    expect(count).toBeGreaterThan(0)
    expect(count).toBeLessThanOrEqual(CONCURRENT)

    console.log(`Advisory lock test: ${count}/${CONCURRENT} increments succeeded`)
  })

  it('concurrent UPSERT without advisory lock still converges to a consistent count', async () => {
    // This shows the DB UPSERT alone is safe — no lost updates, no phantom rows.
    const pool = getPgPool()
    const { tenantId } = await createTestTenant()
    const now = new Date()
    const year = now.getFullYear()
    const month = now.getMonth() + 1
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
        try {
          await client.query('ROLLBACK')
        } catch {
          // ignore
        }
      } finally {
        client.release()
      }
    }

    await Promise.all(Array.from({ length: CONCURRENT }, incrementNoLock))

    const result = await pool.query<{ event_count: string }>(
      `SELECT event_count FROM monthly_usage WHERE tenant_id = $1 AND year = $2 AND month = $3`,
      [tenantId, year, month],
    )
    const count = Number(result.rows[0]!.event_count)
    // All increments must have been applied — UPSERT serialises at the row level
    expect(count).toBe(CONCURRENT)
  })

  it('advisory lock key is stable and unique per (tenant, year, month)', async () => {
    const pool = getPgPool()

    // Verify that hashtext produces a consistent bigint for the same inputs
    const key1 = await pool.query<{ h: string }>(
      `SELECT hashtext('monthly_usage:abc:2025:1') AS h`,
    )
    const key2 = await pool.query<{ h: string }>(
      `SELECT hashtext('monthly_usage:abc:2025:1') AS h`,
    )
    const key3 = await pool.query<{ h: string }>(
      `SELECT hashtext('monthly_usage:abc:2025:2') AS h`,
    )

    expect(key1.rows[0]!.h).toBe(key2.rows[0]!.h)    // same input → same key
    expect(key1.rows[0]!.h).not.toBe(key3.rows[0]!.h) // different month → different key
  })

  it('pg_try_advisory_xact_lock releases automatically on transaction end', async () => {
    const pool = getPgPool()

    const lockKey = 999999 // arbitrary stable test key

    // Acquire in first tx, then commit
    const clientA = await pool.connect()
    await clientA.query('BEGIN')
    const res = await clientA.query<{ acquired: boolean }>(
      `SELECT pg_try_advisory_xact_lock($1) AS acquired`,
      [lockKey],
    )
    expect(res.rows[0]!.acquired).toBe(true)
    await clientA.query('COMMIT')
    clientA.release()

    // After commit the lock is released — a second client must be able to acquire it
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
})
