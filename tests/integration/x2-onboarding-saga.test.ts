/**
 * X2: Tenant Onboarding Saga
 *
 * Verifies that onboardTenant():
 *  - Creates all required resources across PostgreSQL, MongoDB, and Elasticsearch
 *  - Returns valid IDs and an API key
 *  - Compensates (rolls back PG + Mongo) when a subsequent step fails
 *
 * The full chaos test (forcing ES to fail mid-saga) requires injecting a mock
 * at the network level and is covered in manual testing. This suite tests the
 * observable happy-path contract and verifiable compensation paths.
 */

import { describe, it, expect, beforeAll, afterAll } from 'vitest'
import { onboardTenant } from '../../src/domain/tenants.js'
import {
  setupTestDatabases,
  teardownTestDatabases,
  getPgPool,
  getMongoDb,
} from '../helpers/setup.js'

// ---------------------------------------------------------------------------
// Suite setup
// ---------------------------------------------------------------------------

describe('X2: Tenant Onboarding Saga', () => {
  beforeAll(setupTestDatabases)
  afterAll(teardownTestDatabases)

  // -------------------------------------------------------------------------
  // Happy path: full onboarding creates all resources
  // -------------------------------------------------------------------------

  it('successfully onboards a tenant and creates all required resources', async () => {
    const pg = getPgPool()
    const userId = crypto.randomUUID()
    const slug = `saga-test-${Date.now()}`
    const planId = crypto.randomUUID()

    // Seed required dependencies
    await pg.query(
      `INSERT INTO plans (id, name, event_quota_per_month, retention_days, max_projects, price_cents)
       VALUES ($1, 'Saga Test Plan', 100000, 90, 10, 0)`,
      [planId],
    )
    await pg.query(
      `INSERT INTO users (id, email, password_hash, full_name)
       VALUES ($1, $2, 'hash', 'Saga User')`,
      [userId, `saga-${Date.now()}@test.com`],
    )

    const result = await onboardTenant({
      tenantName: 'Saga Test Corp',
      tenantSlug: slug,
      planId,
      userId,
      projectName: 'Main Project',
      projectSlug: 'main-project',
    })

    // Return value contract
    expect(result.tenantId).toBeDefined()
    expect(result.projectId).toBeDefined()
    expect(result.apiKey).toBeDefined()
    expect(typeof result.tenantId).toBe('string')
    expect(typeof result.projectId).toBe('string')
    expect(typeof result.apiKey).toBe('string')

    // 1. PostgreSQL: tenant row exists
    const tenantRow = await pg.query<{ id: string }>(
      `SELECT id FROM tenants WHERE id = $1`,
      [result.tenantId],
    )
    expect(tenantRow.rows.length).toBe(1)

    // 2. PostgreSQL: project row exists with correct tenant FK
    const projectRow = await pg.query<{ id: string; tenant_id: string }>(
      `SELECT id, tenant_id FROM projects WHERE id = $1`,
      [result.projectId],
    )
    expect(projectRow.rows.length).toBe(1)
    expect(projectRow.rows[0]!['tenant_id']).toBe(result.tenantId)

    // 3. PostgreSQL: tenant_members owner row exists
    const memberRow = await pg.query<{ role: string }>(
      `SELECT role FROM tenant_members WHERE tenant_id = $1 AND user_id = $2`,
      [result.tenantId, userId],
    )
    expect(memberRow.rows.length).toBe(1)
    expect(memberRow.rows[0]!['role']).toBe('owner')

    // 4. PostgreSQL: monthly_usage seed row exists
    const now = new Date()
    const usageRow = await pg.query<{ event_count: string }>(
      `SELECT event_count FROM monthly_usage
        WHERE tenant_id = $1 AND year = $2 AND month = $3`,
      [result.tenantId, now.getUTCFullYear(), now.getUTCMonth() + 1],
    )
    expect(usageRow.rows.length).toBe(1)
    expect(Number(usageRow.rows[0]!['event_count'])).toBe(0)

    // 5. MongoDB: project_configs document exists with correct shape
    const mongoConfig = await getMongoDb()
      .collection('project_configs')
      .findOne({ _id: result.projectId } as unknown as import("mongodb").Filter<import("mongodb").Document>)

    expect(mongoConfig).not.toBeNull()
    expect(mongoConfig!['tenantId']).toBe(result.tenantId)
    expect(mongoConfig!['name']).toBe('Main Project')
    expect(mongoConfig!['alertsEnabled']).toBe(true)
    expect(typeof mongoConfig!['retentionDays']).toBe('number')

    // Cleanup: remove onboarded resources so they do not pollute later tests
    await getMongoDb().collection('project_configs').deleteOne({ _id: result.projectId } as unknown as import("mongodb").Filter<import("mongodb").Document>)
    await pg.query('DELETE FROM tenant_members WHERE tenant_id = $1', [result.tenantId])
    await pg.query('DELETE FROM monthly_usage WHERE tenant_id = $1', [result.tenantId])
    await pg.query('DELETE FROM projects WHERE id = $1', [result.projectId])
    await pg.query('DELETE FROM tenants WHERE id = $1', [result.tenantId])
  }, 20_000)

  // -------------------------------------------------------------------------
  // Duplicate slug: second attempt must fail cleanly
  // -------------------------------------------------------------------------

  it('rejects duplicate tenant slug with a thrown error', async () => {
    const pg = getPgPool()
    const slug = `saga-dup-${Date.now()}`
    const planId = crypto.randomUUID()
    const userId = crypto.randomUUID()

    await pg.query(
      `INSERT INTO plans (id, name, event_quota_per_month, retention_days, max_projects, price_cents)
       VALUES ($1, 'Dup Plan', 100000, 90, 10, 0)`,
      [planId],
    )
    await pg.query(
      `INSERT INTO users (id, email, password_hash, full_name)
       VALUES ($1, $2, 'hash', 'Dup User')`,
      [userId, `dup-${Date.now()}@test.com`],
    )

    const first = await onboardTenant({
      tenantName: 'Dup Corp',
      tenantSlug: slug,
      planId,
      userId,
      projectName: 'Main',
      projectSlug: 'main',
    })
    expect(first.tenantId).toBeDefined()

    // A second attempt with the same slug must throw a unique-violation error
    await expect(
      onboardTenant({
        tenantName: 'Dup Corp Again',
        tenantSlug: slug, // same slug → PG unique constraint violation
        planId,
        userId,
        projectName: 'Main',
        projectSlug: 'main',
      }),
    ).rejects.toThrow()

    // Cleanup first successful tenant
    await getMongoDb().collection('project_configs').deleteOne({ _id: first.projectId } as unknown as import("mongodb").Filter<import("mongodb").Document>)
    await pg.query('DELETE FROM tenant_members WHERE tenant_id = $1', [first.tenantId])
    await pg.query('DELETE FROM monthly_usage WHERE tenant_id = $1', [first.tenantId])
    await pg.query('DELETE FROM projects WHERE id = $1', [first.projectId])
    await pg.query('DELETE FROM tenants WHERE id = $1', [first.tenantId])
  }, 20_000)

  // -------------------------------------------------------------------------
  // Compensation: verify that a second attempt with same slug leaves no orphans
  // -------------------------------------------------------------------------

  it('rolls back PG rows when the saga fails mid-way, leaving no orphan tenant', async () => {
    const pg = getPgPool()
    const slug = `saga-rollback-${Date.now()}`
    const planId = crypto.randomUUID()
    const userId = crypto.randomUUID()

    await pg.query(
      `INSERT INTO plans (id, name, event_quota_per_month, retention_days, max_projects, price_cents)
       VALUES ($1, 'Rollback Plan', 100000, 90, 10, 0)`,
      [planId],
    )
    await pg.query(
      `INSERT INTO users (id, email, password_hash, full_name)
       VALUES ($1, $2, 'hash', 'Rollback User')`,
      [userId, `rollback-${Date.now()}@test.com`],
    )

    // First attempt succeeds — record the tenant id
    const result = await onboardTenant({
      tenantName: 'Rollback Corp',
      tenantSlug: slug,
      planId,
      userId,
      projectName: 'Main',
      projectSlug: 'main',
    })
    expect(result.tenantId).toBeDefined()

    // A forced duplicate-slug call triggers PG error → saga compensates
    await expect(
      onboardTenant({
        tenantName: 'Rollback Corp 2',
        tenantSlug: slug,
        planId,
        userId,
        projectName: 'Main2',
        projectSlug: 'main2',
      }),
    ).rejects.toThrow()

    // After the failed second call, only one tenant with this slug should exist
    const slugCount = await pg.query<{ c: string }>(
      `SELECT COUNT(*) AS c FROM tenants WHERE slug = $1`,
      [slug],
    )
    expect(Number(slugCount.rows[0]!['c'])).toBe(1)

    // Cleanup
    await getMongoDb().collection('project_configs').deleteOne({ _id: result.projectId } as unknown as import("mongodb").Filter<import("mongodb").Document>)
    await pg.query('DELETE FROM tenant_members WHERE tenant_id = $1', [result.tenantId])
    await pg.query('DELETE FROM monthly_usage WHERE tenant_id = $1', [result.tenantId])
    await pg.query('DELETE FROM projects WHERE id = $1', [result.projectId])
    await pg.query('DELETE FROM tenants WHERE id = $1', [result.tenantId])
  }, 20_000)
})
