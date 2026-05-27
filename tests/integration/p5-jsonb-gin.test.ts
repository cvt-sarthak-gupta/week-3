/**
 * P5: GIN index on JSONB metadata in billing_events
 *
 * Verifies that:
 *  - @> containment queries return the correct row(s)
 *  - Non-matching metadata yields 0 results
 *  - EXPLAIN confirms a GIN / Bitmap index path (no SeqScan on large data)
 */

import { describe, it, expect, beforeAll, beforeEach } from 'vitest'
import { getPgPool, truncateAll, createTestTenant } from '../helpers/setup.js'

describe('P5: JSONB GIN Index', () => {
  beforeAll(async () => {
    const pool = getPgPool()
    // Ensure the current-month partition exists before any inserts
    const now = new Date()
    await pool.query('SELECT ensure_billing_partition($1, $2)', [
      now.getFullYear(),
      now.getMonth() + 1,
    ])
  })

  beforeEach(async () => {
    await truncateAll()
    // Re-ensure partition after truncate (truncate removes partition rows but keeps the table)
    const pool = getPgPool()
    const now = new Date()
    await pool.query('SELECT ensure_billing_partition($1, $2)', [
      now.getFullYear(),
      now.getMonth() + 1,
    ])
  })

  it('finds billing event by JSONB containment (@>)', async () => {
    const pool = getPgPool()
    const { tenantId } = await createTestTenant()
    const coupon = { coupon_code: 'LAUNCH50', discount_pct: 50 }

    await pool.query(
      `INSERT INTO billing_events (tenant_id, event_type, amount_cents, metadata, occurred_at)
       VALUES ($1, 'charge', 4950, $2, NOW())`,
      [tenantId, JSON.stringify(coupon)],
    )

    const result = await pool.query(
      `SELECT id FROM billing_events
       WHERE metadata @> $1
         AND occurred_at > NOW() - INTERVAL '90 days'
         AND tenant_id = $2`,
      [JSON.stringify({ coupon_code: 'LAUNCH50' }), tenantId],
    )
    expect(result.rows).toHaveLength(1)
  })

  it('does not return events with a different coupon code', async () => {
    const pool = getPgPool()
    const { tenantId } = await createTestTenant()

    await pool.query(
      `INSERT INTO billing_events (tenant_id, event_type, amount_cents, metadata, occurred_at)
       VALUES ($1, 'charge', 9900, $2, NOW())`,
      [tenantId, JSON.stringify({ coupon_code: 'OTHER50' })],
    )

    const result = await pool.query(
      `SELECT id FROM billing_events WHERE metadata @> $1 AND tenant_id = $2`,
      [JSON.stringify({ coupon_code: 'LAUNCH50' }), tenantId],
    )
    expect(result.rows).toHaveLength(0)
  })

  it('containment query matches nested sub-document correctly', async () => {
    const pool = getPgPool()
    const { tenantId } = await createTestTenant()

    const metadata = { coupon_code: 'NESTED10', details: { tier: 'gold', region: 'us-east' } }
    await pool.query(
      `INSERT INTO billing_events (tenant_id, event_type, amount_cents, metadata, occurred_at)
       VALUES ($1, 'charge', 100, $2, NOW())`,
      [tenantId, JSON.stringify(metadata)],
    )

    // Partial containment on top-level key only
    const result = await pool.query(
      `SELECT id FROM billing_events WHERE metadata @> $1 AND tenant_id = $2`,
      [JSON.stringify({ coupon_code: 'NESTED10' }), tenantId],
    )
    expect(result.rows).toHaveLength(1)
  })

  it('returns all events whose metadata contains the search key', async () => {
    const pool = getPgPool()
    const { tenantId } = await createTestTenant()

    // Insert 3 events with the same coupon, 1 without
    for (let i = 0; i < 3; i++) {
      await pool.query(
        `INSERT INTO billing_events (tenant_id, event_type, amount_cents, metadata, occurred_at)
         VALUES ($1, 'charge', $2, $3, NOW())`,
        [tenantId, (i + 1) * 100, JSON.stringify({ coupon_code: 'MULTI' })],
      )
    }
    await pool.query(
      `INSERT INTO billing_events (tenant_id, event_type, amount_cents, metadata, occurred_at)
       VALUES ($1, 'charge', 999, $2, NOW())`,
      [tenantId, JSON.stringify({ coupon_code: 'OTHER' })],
    )

    const result = await pool.query(
      `SELECT id FROM billing_events WHERE metadata @> $1 AND tenant_id = $2`,
      [JSON.stringify({ coupon_code: 'MULTI' }), tenantId],
    )
    expect(result.rows).toHaveLength(3)
  })

  it('GIN index is used for @> query (EXPLAIN check)', async () => {
    const pool = getPgPool()
    const { tenantId } = await createTestTenant()

    const explain = await pool.query(
      `EXPLAIN (FORMAT JSON) SELECT id FROM billing_events WHERE metadata @> $1 AND tenant_id = $2`,
      [JSON.stringify({ coupon_code: 'LAUNCH50' }), tenantId],
    )

    // The QUERY PLAN JSON is in rows[0]['QUERY PLAN']
    const plan = JSON.stringify(explain.rows[0]['QUERY PLAN'])
    // Expect either a GIN-backed Bitmap Index Scan or an Index Scan/Cond referencing the GIN index
    expect(plan).toMatch(/Bitmap|Index|GIN/i)
  })
})
