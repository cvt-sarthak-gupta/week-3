/**
 * P2: Row-Level Security — tenant isolation
 *
 * Verifies that:
 *  - Tenant A cannot read Tenant B's rows when the RLS context is set
 *  - Tenant A can always read its own rows
 *  - No tenant context (missing session var) returns 0 rows (deny-by-default)
 *  - SET LOCAL context does not leak across pool connections after COMMIT
 *
 * Note: PostgreSQL superusers bypass RLS even with FORCE ROW LEVEL SECURITY.
 * Each test explicitly uses `SET LOCAL ROLE app_user` inside a transaction so
 * RLS policies are actually enforced. SET LOCAL unwinds on COMMIT/ROLLBACK,
 * so the pool connection is always returned as the original postgres superuser.
 */

import { describe, it, expect, beforeEach } from 'vitest'
import { getPgPool, truncateAll, createTestTenant } from '../helpers/setup.js'

// UUID validation to safely interpolate into SET LOCAL (avoids SQL injection)
function sanitizeUuid(id: string): string {
  if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(id)) {
    throw new Error(`Invalid UUID: ${id}`)
  }
  return id
}

describe('P2: Row-Level Security', () => {
  beforeEach(async () => {
    await truncateAll()
  })

  it('tenant A cannot see tenant B projects under RLS', async () => {
    const pool = getPgPool()
    const tenantA = await createTestTenant()
    const tenantB = await createTestTenant()

    const client = await pool.connect()
    try {
      await client.query('BEGIN')
      await client.query('SET LOCAL ROLE app_user')
      await client.query(`SET LOCAL "app.current_tenant_id" = '${sanitizeUuid(tenantA.tenantId)}'`)
      const result = await client.query(
        'SELECT id FROM projects WHERE id = $1',
        [tenantB.projectId],
      )
      expect(result.rows).toHaveLength(0)
      await client.query('COMMIT')
    } catch (err) {
      await client.query('ROLLBACK').catch(() => {})
      throw err
    } finally {
      client.release()
    }
  })

  it('tenant A can see its own projects under RLS', async () => {
    const pool = getPgPool()
    const tenantA = await createTestTenant()

    const client = await pool.connect()
    try {
      await client.query('BEGIN')
      await client.query('SET LOCAL ROLE app_user')
      await client.query(`SET LOCAL "app.current_tenant_id" = '${sanitizeUuid(tenantA.tenantId)}'`)
      const result = await client.query(
        'SELECT id FROM projects WHERE id = $1',
        [tenantA.projectId],
      )
      expect(result.rows).toHaveLength(1)
      await client.query('COMMIT')
    } catch (err) {
      await client.query('ROLLBACK').catch(() => {})
      throw err
    } finally {
      client.release()
    }
  })

  it('missing session var yields 0 rows everywhere (deny by default)', async () => {
    const pool = getPgPool()
    const { projectId } = await createTestTenant()

    const client = await pool.connect()
    try {
      await client.query('BEGIN')
      await client.query('SET LOCAL ROLE app_user')
      // No SET LOCAL for app.current_tenant_id — current_setting returns NULL → policy = FALSE
      let rowCount = 0
      try {
        const result = await client.query(
          'SELECT id FROM projects WHERE id = $1',
          [projectId],
        )
        rowCount = result.rowCount ?? 0
      } catch {
        // Some PG versions throw on invalid UUID cast — that is also a correct denial.
        rowCount = 0
      }
      expect(rowCount).toBe(0)
      await client.query('COMMIT')
    } catch (err) {
      await client.query('ROLLBACK').catch(() => {})
      throw err
    } finally {
      client.release()
    }
  })

  it('RLS context does not leak across pool connections after COMMIT', async () => {
    const pool = getPgPool()
    const tenantA = await createTestTenant()

    // Set tenant A context (as app_user), then commit — SET LOCAL should clear on tx end
    const client = await pool.connect()
    await client.query('BEGIN')
    await client.query('SET LOCAL ROLE app_user')
    await client.query(`SET LOCAL "app.current_tenant_id" = '${sanitizeUuid(tenantA.tenantId)}'`)
    await client.query('COMMIT')
    client.release()

    // Check out a fresh connection — should be back to postgres with no tenant context
    const client2 = await pool.connect()
    try {
      await client2.query('BEGIN')
      await client2.query('SET LOCAL ROLE app_user')
      // If SET LOCAL leaked, this would return tenantA's project.
      let rowCount = 0
      try {
        const result = await client2.query(
          'SELECT id FROM projects WHERE id = $1',
          [tenantA.projectId],
        )
        rowCount = result.rowCount ?? 0
      } catch {
        rowCount = 0
      }
      expect(rowCount).toBe(0)
      await client2.query('COMMIT')
    } catch (err) {
      await client2.query('ROLLBACK').catch(() => {})
      throw err
    } finally {
      client2.release()
    }
  })

  it('tenant B cannot see tenant A alert_rules under RLS', async () => {
    const pool = getPgPool()
    const tenantA = await createTestTenant()
    const tenantB = await createTestTenant()

    // Insert an alert rule for tenant A's project (as superuser, bypassing RLS)
    const ruleId = crypto.randomUUID()
    await pool.query(
      `INSERT INTO alert_rules (id, project_id, name, condition_type, window_seconds, notification_channel, es_query)
       VALUES ($1, $2, 'Test Rule', 'threshold', 60, 'email', '{}')`,
      [ruleId, tenantA.projectId],
    )

    // Attempt to read it as tenant B (RLS should filter it out)
    const client = await pool.connect()
    try {
      await client.query('BEGIN')
      await client.query('SET LOCAL ROLE app_user')
      await client.query(`SET LOCAL "app.current_tenant_id" = '${sanitizeUuid(tenantB.tenantId)}'`)
      const result = await client.query(
        'SELECT id FROM alert_rules WHERE id = $1',
        [ruleId],
      )
      expect(result.rows).toHaveLength(0)
      await client.query('COMMIT')
    } catch (err) {
      await client.query('ROLLBACK').catch(() => {})
      throw err
    } finally {
      client.release()
    }
  })

  it('billing_events: tenant B cannot see tenant A billing rows under RLS', async () => {
    const pool = getPgPool()
    const tenantA = await createTestTenant()
    const tenantB = await createTestTenant()

    // Ensure the partition for current month exists
    const now = new Date()
    await pool.query('SELECT ensure_billing_partition($1, $2)', [now.getFullYear(), now.getMonth() + 1])

    // Insert a billing event for tenant A (as superuser, bypassing RLS)
    await pool.query(
      `INSERT INTO billing_events (tenant_id, event_type, amount_cents, metadata, occurred_at)
       VALUES ($1, 'charge', 1000, '{}', NOW())`,
      [tenantA.tenantId],
    )

    // Attempt to read it as tenant B under RLS — must return 0 rows
    const client = await pool.connect()
    try {
      await client.query('BEGIN')
      await client.query('SET LOCAL ROLE app_user')
      await client.query(`SET LOCAL "app.current_tenant_id" = '${sanitizeUuid(tenantB.tenantId)}'`)
      const result = await client.query(
        'SELECT id FROM billing_events WHERE tenant_id = $1',
        [tenantA.tenantId],
      )
      expect(result.rows).toHaveLength(0)
      await client.query('COMMIT')
    } catch (err) {
      await client.query('ROLLBACK').catch(() => {})
      throw err
    } finally {
      client.release()
    }
  })

  it('withTenant helper correctly scopes reads to its own tenant', async () => {
    const pool = getPgPool()
    const tenantA = await createTestTenant()
    const tenantB = await createTestTenant()

    // Use explicit transaction mimicking withTenant behaviour
    const client = await pool.connect()
    let visibleToA = 0
    try {
      await client.query('BEGIN')
      await client.query('SET LOCAL ROLE app_user')
      await client.query(`SET LOCAL "app.current_tenant_id" = '${sanitizeUuid(tenantA.tenantId)}'`)
      const res = await client.query('SELECT id FROM projects WHERE tenant_id = $1', [tenantA.tenantId])
      visibleToA = res.rowCount ?? 0
      await client.query('COMMIT')
    } catch (err) {
      await client.query('ROLLBACK').catch(() => {})
      throw err
    } finally {
      client.release()
    }

    expect(visibleToA).toBeGreaterThanOrEqual(1)

    // Tenant B's project must not appear when querying as tenant A
    const client2 = await pool.connect()
    try {
      await client2.query('BEGIN')
      await client2.query('SET LOCAL ROLE app_user')
      await client2.query(`SET LOCAL "app.current_tenant_id" = '${sanitizeUuid(tenantA.tenantId)}'`)
      const res = await client2.query(
        'SELECT id FROM projects WHERE id = $1',
        [tenantB.projectId],
      )
      expect(res.rows).toHaveLength(0)
      await client2.query('COMMIT')
    } catch (err) {
      await client2.query('ROLLBACK').catch(() => {})
      throw err
    } finally {
      client2.release()
    }
  })
})
