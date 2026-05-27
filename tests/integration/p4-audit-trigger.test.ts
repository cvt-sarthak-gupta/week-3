/**
 * P4: Audit trigger
 *
 * Verifies that:
 *  - UPDATE on tenants is recorded in audit_log with old/new row snapshots
 *  - DELETE on projects is recorded with old_row and null new_row
 *  - The tenant context captured via SET LOCAL ends up in audit_log.tenant_id
 *  - INSERT operations are not recorded (trigger is UPDATE/DELETE only)
 */

import { describe, it, expect, beforeEach } from 'vitest'
import { getPgPool, truncateAll, createTestTenant } from '../helpers/setup.js'

describe('P4: Audit Trigger', () => {
  beforeEach(async () => {
    await truncateAll()
  })

  it('records UPDATE to tenants in audit_log with old and new row data', async () => {
    const pool = getPgPool()
    const { tenantId } = await createTestTenant()

    await pool.query(`UPDATE tenants SET name = 'Updated Name' WHERE id = $1`, [tenantId])

    const result = await pool.query(
      `SELECT * FROM audit_log
       WHERE table_name = 'tenants' AND operation = 'UPDATE'
       ORDER BY occurred_at DESC
       LIMIT 1`,
    )
    expect(result.rows.length).toBeGreaterThan(0)

    const log = result.rows[0]!
    expect(log.table_name).toBe('tenants')
    expect(log.operation).toBe('UPDATE')
    // old_row should NOT have the new name
    expect(log.old_row).not.toBeNull()
    expect(log.old_row.name).not.toBe('Updated Name')
    // new_row should carry the updated name
    expect(log.new_row).not.toBeNull()
    expect(log.new_row.name).toBe('Updated Name')
  })

  it('records DELETE to projects in audit_log with old_row and null new_row', async () => {
    const pool = getPgPool()
    const { projectId } = await createTestTenant()

    await pool.query(`DELETE FROM projects WHERE id = $1`, [projectId])

    const result = await pool.query(
      `SELECT * FROM audit_log
       WHERE table_name = 'projects' AND operation = 'DELETE'
       ORDER BY occurred_at DESC
       LIMIT 1`,
    )
    expect(result.rows.length).toBeGreaterThan(0)

    const log = result.rows[0]!
    expect(log.table_name).toBe('projects')
    expect(log.operation).toBe('DELETE')
    expect(log.old_row).not.toBeNull()
    expect(log.old_row.id).toBe(projectId)
    expect(log.new_row).toBeNull()
  })

  it('captures tenant context in audit_log.tenant_id when SET LOCAL is used', async () => {
    const pool = getPgPool()
    const { tenantId, projectId } = await createTestTenant()

    const client = await pool.connect()
    try {
      await client.query('BEGIN')
      await client.query(`SET LOCAL "app.current_tenant_id" = '${tenantId}'`)
      await client.query(`UPDATE projects SET name = 'Audited Update' WHERE id = $1`, [projectId])
      await client.query('COMMIT')
    } catch (err) {
      await client.query('ROLLBACK').catch(() => {})
      throw err
    } finally {
      client.release()
    }

    const result = await pool.query(
      `SELECT tenant_id FROM audit_log
       WHERE table_name = 'projects' AND operation = 'UPDATE'
       ORDER BY occurred_at DESC
       LIMIT 1`,
    )
    expect(result.rows.length).toBeGreaterThan(0)
    expect(result.rows[0]!.tenant_id).toBe(tenantId)
  })

  it('audit_log.tenant_id is null when no session context is set', async () => {
    const pool = getPgPool()
    const { tenantId } = await createTestTenant()

    // Execute update without SET LOCAL — trigger falls back to NULL gracefully
    await pool.query(`UPDATE tenants SET name = 'No Context' WHERE id = $1`, [tenantId])

    const result = await pool.query(
      `SELECT tenant_id FROM audit_log
       WHERE table_name = 'tenants' AND operation = 'UPDATE'
       ORDER BY occurred_at DESC
       LIMIT 1`,
    )
    expect(result.rows.length).toBeGreaterThan(0)
    // tenant_id should be null since no session var was set
    expect(result.rows[0]!.tenant_id).toBeNull()
  })

  it('INSERT is not recorded by the audit trigger', async () => {
    const pool = getPgPool()
    const countBefore = await pool.query(
      `SELECT COUNT(*) FROM audit_log WHERE operation = 'INSERT'`,
    )
    // The audit trigger only fires on UPDATE and DELETE; INSERT must never appear
    expect(Number(countBefore.rows[0]!.count)).toBe(0)

    // Perform an insert (creating a tenant implicitly exercises INSERT on several tables)
    await createTestTenant()

    const countAfter = await pool.query(
      `SELECT COUNT(*) FROM audit_log WHERE operation = 'INSERT'`,
    )
    expect(Number(countAfter.rows[0]!.count)).toBe(0)
  })

  it('successive updates produce successive audit_log entries', async () => {
    const pool = getPgPool()
    const { tenantId } = await createTestTenant()

    await pool.query(`UPDATE tenants SET name = 'First'  WHERE id = $1`, [tenantId])
    await pool.query(`UPDATE tenants SET name = 'Second' WHERE id = $1`, [tenantId])

    const result = await pool.query(
      `SELECT new_row->>'name' AS name
       FROM audit_log
       WHERE table_name = 'tenants' AND operation = 'UPDATE'
       ORDER BY occurred_at ASC`,
    )
    const names = result.rows.map((r) => r.name)
    expect(names).toContain('First')
    expect(names).toContain('Second')
  })
})
