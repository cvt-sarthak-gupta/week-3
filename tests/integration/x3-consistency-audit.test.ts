/**
 * X3: Cross-DB Consistency Audit
 *
 * Verifies that runAudit():
 *  - Completes successfully and returns a structured result object
 *  - Detects a project that exists in PostgreSQL but has no corresponding
 *    project_configs document in MongoDB (missing_mongo_config)
 *  - Detects a project_configs document in MongoDB with no matching PG project
 *    (orphan_mongo_config) — validated via the normal audit flow
 *
 * Note: The AuditResult shape uses `duration_ms` (snake_case) as defined in
 * src/domain/consistency.ts. Tests assert this exact field name.
 */

import { describe, it, expect, beforeAll, afterAll } from 'vitest'
import {
  runAudit,
  setupTestDatabases,
  teardownTestDatabases,
  createTestTenant,
  getPgPool,
  getMongoDb,
} from '../helpers/setup.js'

// ---------------------------------------------------------------------------
// Suite setup
// ---------------------------------------------------------------------------

describe('X3: Consistency Audit', () => {
  beforeAll(async () => {
    await setupTestDatabases()
    // Create a baseline tenant so there is at least one project to audit
    await createTestTenant()
  })

  afterAll(teardownTestDatabases)

  // -------------------------------------------------------------------------
  // Structured result contract
  // -------------------------------------------------------------------------

  it('audit completes successfully and returns structured results', async () => {
    const result = await runAudit()

    // Shape assertions — all required top-level properties must exist
    expect(result).toHaveProperty('ranAt')
    expect(result).toHaveProperty('duration_ms')   // matches AuditResult type
    expect(result).toHaveProperty('checked')
    expect(result).toHaveProperty('inconsistencies')
    expect(result).toHaveProperty('summary')

    // Type assertions
    expect(result.ranAt).toBeInstanceOf(Date)
    expect(typeof result.duration_ms).toBe('number')
    expect(result.duration_ms).toBeGreaterThanOrEqual(0)
    expect(typeof result.checked).toBe('number')
    expect(result.checked).toBeGreaterThanOrEqual(0)
    expect(Array.isArray(result.inconsistencies)).toBe(true)

    // Summary shape
    expect(result.summary).toHaveProperty('total')
    expect(result.summary).toHaveProperty('byKind')
    expect(typeof result.summary.total).toBe('number')
    expect(typeof result.summary.byKind).toBe('object')

    // summary.total must equal the number of inconsistency entries
    expect(result.summary.total).toBe(result.inconsistencies.length)
  }, 30_000)

  // -------------------------------------------------------------------------
  // Inconsistency detection: missing MongoDB config
  // -------------------------------------------------------------------------

  it('detects a PG project without a Mongo project_config as missing_mongo_config', async () => {
    const pg = getPgPool()

    // Insert a PG tenant + project but deliberately skip the MongoDB document
    const planId = crypto.randomUUID()
    const tenantId = crypto.randomUUID()
    const projectId = crypto.randomUUID()
    const uniqueSuffix = Date.now()

    await pg.query(
      `INSERT INTO plans (id, name, event_quota_per_month, retention_days, max_projects, price_cents)
       VALUES ($1, 'Audit Plan', 100000, 90, 10, 0)`,
      [planId],
    )
    await pg.query(
      `INSERT INTO tenants (id, name, slug, plan_id)
       VALUES ($1, 'Audit Tenant', $2, $3)`,
      [tenantId, `audit-${uniqueSuffix}`, planId],
    )
    await pg.query(
      `INSERT INTO projects (id, tenant_id, name, slug)
       VALUES ($1, $2, 'Orphan Project', $3)`,
      [projectId, tenantId, `orphan-${uniqueSuffix}`],
    )
    // No MongoDB project_configs doc is inserted for this project

    const result = await runAudit()

    const mongoMissing = result.inconsistencies.filter(
      (i) => i.kind === 'missing_mongo_config' && i.projectId === projectId,
    )

    // Must surface at least one inconsistency for this project
    expect(mongoMissing.length).toBeGreaterThan(0)

    // Every inconsistency entry must have a human-readable suggestion
    expect(mongoMissing[0]!.suggestion).toBeDefined()
    expect(typeof mongoMissing[0]!.suggestion).toBe('string')
    expect(mongoMissing[0]!.suggestion.length).toBeGreaterThan(0)

    // The details field must reference the project
    expect(mongoMissing[0]!.details).toContain(projectId)

    // Cleanup
    await pg.query('DELETE FROM projects WHERE id = $1', [projectId])
    await pg.query('DELETE FROM tenants WHERE id = $1', [tenantId])
  }, 30_000)

  // -------------------------------------------------------------------------
  // Inconsistency detection: orphaned MongoDB config
  // -------------------------------------------------------------------------

  it('detects a Mongo project_config without a PG project as orphan_mongo_config', async () => {
    // Insert a project_configs document for a projectId that has no PG project row.
    // The audit will scan all project_configs docs and flag those without a match.
    const phantomProjectId = crypto.randomUUID()

    await getMongoDb().collection('project_configs').insertOne({
      _id: phantomProjectId,
      tenantId: crypto.randomUUID(),
      name: 'Ghost Project',
      retentionDays: 30,
      alertsEnabled: false,
      createdAt: new Date(),
      updatedAt: new Date(),
    } as any)

    const result = await runAudit()

    const orphans = result.inconsistencies.filter(
      (i) => i.kind === 'orphan_mongo_config' && i.projectId === phantomProjectId,
    )

    expect(orphans.length).toBeGreaterThan(0)
    expect(orphans[0]!.suggestion).toBeDefined()
    expect(orphans[0]!.details).toContain(phantomProjectId)

    // Cleanup
    await getMongoDb()
      .collection('project_configs')
      .deleteOne({ _id: phantomProjectId } as unknown as import('mongodb').Filter<import('mongodb').Document>)
  }, 30_000)

  // -------------------------------------------------------------------------
  // Audit performance budget
  // -------------------------------------------------------------------------

  it('audit completes within 30 seconds on a small dataset', async () => {
    const start = Date.now()
    await runAudit()
    const elapsed = Date.now() - start

    // Generous budget: tests run on a single local Docker stack
    expect(elapsed).toBeLessThan(30_000)
  }, 35_000)
})
