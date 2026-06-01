import { describe, it, expect, beforeAll, afterAll } from 'vitest'
import { MongoClient, type Db, type Filter, type Document } from 'mongodb'

const mongoClient = new MongoClient(
  process.env['MONGO_URL'] ?? 'mongodb://localhost:27018/pulseboard_test?directConnection=true'
)
let db: Db

// Helper to find by string _id without ObjectId type conflict
function byId(id: string): Filter<Document> {
  return { _id: id } as unknown as Filter<Document>
}

describe('M6: MongoDB Multi-Document Transactions', () => {
  beforeAll(async () => {
    await mongoClient.connect()
    db = mongoClient.db('pulseboard_test')
  })

  afterAll(async () => {
    await db.collection('dashboards_m6').drop().catch(() => {})
    await db.collection('project_configs_m6').drop().catch(() => {})
    await db.collection('events_m6').drop().catch(() => {})
    await mongoClient.close()
  })

  it('all 3 writes commit atomically on success', async () => {
    const dashboardId = `dash-${Date.now()}`
    const projectId = `proj-m6-${Date.now()}`
    const userId = 'user-m6-test'

    // Setup: create project_config document
    await db.collection('project_configs_m6').insertOne({
      projectId,
      settings: { dashboardIds: [] as string[] },
    })

    const session = mongoClient.startSession()
    try {
      await session.withTransaction(async () => {
        // Step 1: Insert dashboard
        await db.collection('dashboards_m6').insertOne(
          { _id: dashboardId, projectId, name: 'Test Dashboard', widgets: [], createdBy: userId } as unknown as Document,
          { session }
        )

        // Step 2: Update project_configs to add dashboardId
        await db.collection('project_configs_m6').updateOne(
          { projectId },
          { $push: { 'settings.dashboardIds': dashboardId } as unknown as Document },
          { session }
        )

        // Step 3: Insert audit event
        await db.collection('events_m6').insertOne(
          {
            _id: `audit-${dashboardId}`,
            type: 'audit',
            action: 'dashboard_created',
            dashboardId,
            projectId,
            userId,
            createdAt: new Date(),
          } as unknown as Document,
          { session }
        )
      })
    } finally {
      await session.endSession()
    }

    // Verify all 3 writes committed
    const dash = await db.collection('dashboards_m6').findOne(byId(dashboardId))
    expect(dash).not.toBeNull()

    const config = await db.collection('project_configs_m6').findOne({ projectId })
    expect(config!['settings'].dashboardIds).toContain(dashboardId)

    const audit = await db.collection('events_m6').findOne(byId(`audit-${dashboardId}`))
    expect(audit).not.toBeNull()
  })

  it('all 3 writes roll back when step 2 throws', async () => {
    const dashboardId = `dash-fail-${Date.now()}`
    const projectId = `proj-m6-fail-${Date.now()}`
    const userId = 'user-m6-fail'

    const session = mongoClient.startSession()
    let threw = false

    try {
      await session.withTransaction(async () => {
        // Step 1: Insert dashboard
        await db.collection('dashboards_m6').insertOne(
          { _id: dashboardId, projectId, name: 'Failing Dashboard', widgets: [], createdBy: userId } as unknown as Document,
          { session }
        )

        // Step 3: Insert audit event inside the transaction BEFORE the failure so
        // the test can prove it is rolled back alongside step 1 (not merely skipped).
        await db.collection('events_m6').insertOne(
          {
            _id: `audit-${dashboardId}`,
            type: 'audit',
            action: 'dashboard_created',
            dashboardId,
            projectId,
            userId,
            createdAt: new Date(),
          } as unknown as Document,
          { session }
        )

        // Step 2: Simulate project_configs update failure — aborts the transaction,
        // rolling back both the dashboard insert (step 1) and audit insert (step 3).
        throw new Error('Simulated step 2 failure')
      })
    } catch {
      threw = true
    } finally {
      await session.endSession()
    }

    expect(threw).toBe(true)

    // Step 1 (dashboard) must have been rolled back
    const dash = await db.collection('dashboards_m6').findOne(byId(dashboardId))
    expect(dash).toBeNull()

    // Step 3 (audit event) was inserted within the transaction and must also be rolled back
    const audit = await db.collection('events_m6').findOne(byId(`audit-${dashboardId}`))
    expect(audit).toBeNull()
  })
})
