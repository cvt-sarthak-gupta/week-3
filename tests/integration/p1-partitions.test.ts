/**
 * P1: Partitioned billing_events table
 *
 * Verifies that:
 *  - ensure_billing_partition() is idempotent
 *  - Inserts route to the correct monthly partition table
 *  - Future-month partitions can be created on demand
 */

import { describe, it, expect, beforeEach } from 'vitest'
import { getPgPool, truncateAll, createTestTenant } from '../helpers/setup.js'

describe('P1: Billing Events Partitioning', () => {
  beforeEach(async () => {
    await truncateAll()
  })

  it('ensure_billing_partition creates partition idempotently', async () => {
    const pool = getPgPool()
    const now = new Date()
    const year = now.getFullYear()
    const month = now.getMonth() + 1

    // Call twice — must not throw (idempotent)
    await pool.query('SELECT ensure_billing_partition($1, $2)', [year, month])
    await pool.query('SELECT ensure_billing_partition($1, $2)', [year, month])

    const partitionName = `billing_events_${String(year).padStart(4, '0')}_${String(month).padStart(2, '0')}`
    const result = await pool.query(
      `SELECT tablename FROM pg_tables WHERE tablename = $1 AND schemaname = 'public'`,
      [partitionName],
    )
    expect(result.rows).toHaveLength(1)
  })

  it('inserts route to correct monthly partition', async () => {
    const pool = getPgPool()
    const { tenantId } = await createTestTenant()
    const now = new Date()
    const year = now.getFullYear()
    const month = now.getMonth() + 1

    await pool.query('SELECT ensure_billing_partition($1, $2)', [year, month])

    await pool.query(
      `INSERT INTO billing_events (tenant_id, event_type, amount_cents, metadata, occurred_at)
       VALUES ($1, 'charge', 9900, '{}', $2)`,
      [tenantId, now.toISOString()],
    )

    const partitionName = `billing_events_${String(year).padStart(4, '0')}_${String(month).padStart(2, '0')}`
    const result = await pool.query(
      `SELECT COUNT(*) FROM ${partitionName} WHERE tenant_id = $1`,
      [tenantId],
    )
    expect(Number(result.rows[0].count)).toBe(1)
  })

  it('insert into parent table goes into the right partition and not a sibling', async () => {
    const pool = getPgPool()
    const { tenantId } = await createTestTenant()
    const now = new Date()
    const year = now.getFullYear()
    const month = now.getMonth() + 1

    // Ensure both the current and next month partitions exist
    const nextDate = new Date(now)
    nextDate.setMonth(nextDate.getMonth() + 1)
    await pool.query('SELECT ensure_billing_partition($1, $2)', [year, month])
    await pool.query('SELECT ensure_billing_partition($1, $2)', [nextDate.getFullYear(), nextDate.getMonth() + 1])

    await pool.query(
      `INSERT INTO billing_events (tenant_id, event_type, amount_cents, metadata, occurred_at)
       VALUES ($1, 'charge', 1000, '{}', $2)`,
      [tenantId, now.toISOString()],
    )

    const currentPartition = `billing_events_${String(year).padStart(4, '0')}_${String(month).padStart(2, '0')}`
    const nextPartition = `billing_events_${String(nextDate.getFullYear()).padStart(4, '0')}_${String(nextDate.getMonth() + 1).padStart(2, '0')}`

    const inCurrent = await pool.query(
      `SELECT COUNT(*) FROM ${currentPartition} WHERE tenant_id = $1`,
      [tenantId],
    )
    const inNext = await pool.query(
      `SELECT COUNT(*) FROM ${nextPartition} WHERE tenant_id = $1`,
      [tenantId],
    )

    expect(Number(inCurrent.rows[0].count)).toBe(1)
    expect(Number(inNext.rows[0].count)).toBe(0)
  })

  it('creates partition for a future month', async () => {
    const pool = getPgPool()
    const future = new Date()
    future.setMonth(future.getMonth() + 2)
    const futureYear = future.getFullYear()
    const futureMonth = future.getMonth() + 1

    await pool.query('SELECT ensure_billing_partition($1, $2)', [futureYear, futureMonth])

    const partitionName = `billing_events_${String(futureYear).padStart(4, '0')}_${String(futureMonth).padStart(2, '0')}`
    const result = await pool.query(
      `SELECT tablename FROM pg_tables WHERE tablename = $1 AND schemaname = 'public'`,
      [partitionName],
    )
    expect(result.rows).toHaveLength(1)
  })

  it('partitioned table shows rows via the parent table as well', async () => {
    const pool = getPgPool()
    const { tenantId } = await createTestTenant()
    const now = new Date()
    await pool.query('SELECT ensure_billing_partition($1, $2)', [now.getFullYear(), now.getMonth() + 1])

    await pool.query(
      `INSERT INTO billing_events (tenant_id, event_type, amount_cents, metadata, occurred_at)
       VALUES ($1, 'refund', 500, '{"reason":"test"}', $2)`,
      [tenantId, now.toISOString()],
    )

    // Querying the parent table must also surface the row
    const result = await pool.query(
      `SELECT event_type, amount_cents FROM billing_events WHERE tenant_id = $1`,
      [tenantId],
    )
    expect(result.rows).toHaveLength(1)
    expect(result.rows[0].event_type).toBe('refund')
    expect(result.rows[0].amount_cents).toBe(500)
  })
})
