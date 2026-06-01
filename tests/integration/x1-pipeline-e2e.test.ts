/**
 * X1: End-to-End Ingestion Pipeline
 *
 * Verifies that one call to processEvent() results in correct final state
 * across all four stores:
 *   1. MongoDB   — events collection contains the document
 *   2. Elasticsearch — document is searchable by eventId
 *   3. PostgreSQL — monthly_usage counter is incremented exactly once
 *   4. MongoDB   — pipeline_metrics stages are recorded
 *   5. Redis     — leaderboard ZINCRBY reflects the new event
 *
 * Also verifies idempotency: processing the same eventId twice must NOT
 * double-count in PostgreSQL.
 */

import { describe, it, expect, beforeAll, afterAll } from 'vitest'
import { Client as ESClient } from '@elastic/elasticsearch'
import {
  processEvent,
  setupTestDatabases,
  teardownTestDatabases,
  createTestTenant,
  getPgPool,
  getMongoDb,
  getRedisClient,
  getContainer,
} from '../helpers/setup.js'
import { STREAM_KEY, CONSUMER_GROUP } from '../../src/db/redis.js'

// ---------------------------------------------------------------------------
// Suite setup
// ---------------------------------------------------------------------------

describe('X1: End-to-End Ingestion Pipeline', () => {
  let testTenant: Awaited<ReturnType<typeof createTestTenant>>

  beforeAll(async () => {
    await setupTestDatabases()
    testTenant = await createTestTenant()
  })

  afterAll(teardownTestDatabases)

  // -------------------------------------------------------------------------
  // Happy path: one event lands in all 4 stores
  // -------------------------------------------------------------------------

  it('processes one event and asserts correct final state in all 4 stores', async () => {
    const eventId = crypto.randomUUID()
    const traceId = crypto.randomUUID()
    const now = new Date()

    // Invoke the domain function directly (simulates the ingest-worker side)
    await processEvent({
      eventId,
      traceId,
      projectId: testTenant.projectId,
      tenantId: testTenant.tenantId,
      planId: testTenant.planId,
      raw: {
        type: 'error',
        severity: 'error',
        message: 'X1 test: payment timeout',
        fingerprint: 'x1-fp-001',
        occurredAt: now.toISOString(),
        tags: { env: 'production', service: 'payments' },
      },
    })

    // Allow any fire-and-forget writes (e.g. pipelineMetrics) to settle
    await new Promise<void>((r) => setTimeout(r, 500))

    // 1. Assert event document in MongoDB events collection
    const mongoDoc = await getMongoDb()
      .collection('events')
      .findOne({ _id: eventId } as unknown as import("mongodb").Filter<import("mongodb").Document>)

    expect(mongoDoc).not.toBeNull()
    expect(mongoDoc!['message']).toBe('X1 test: payment timeout')
    expect(mongoDoc!['traceId']).toBe(traceId)
    expect(mongoDoc!['projectId']).toBe(testTenant.projectId)

    // 2. Assert document indexed in Elasticsearch
    // Retry up to 5 times because ES write visibility is near-real-time (~1 s)
    const es = new ESClient({
      node: process.env['ES_URL'] ?? 'http://localhost:9201',
    })

    // es.get() does not support wildcard patterns; use es.search() with an ids query instead.
    let esDoc: Record<string, unknown> | null = null
    for (let attempt = 0; attempt < 5; attempt++) {
      try {
        const pattern = `logs-${testTenant.projectId}-*`
        const result = await es.search({
          index: pattern,
          body: { query: { ids: { values: [eventId] } } },
        }).catch(() => null)
        const hit = (result as any)?.hits?.hits?.[0]
        if (hit) { esDoc = hit; break }
      } catch {
        // swallow and retry
      }
      await new Promise<void>((r) => setTimeout(r, 500))
    }
    expect(esDoc).not.toBeNull()

    // 3. Assert monthly_usage row incremented in PostgreSQL
    const pg = getPgPool()
    const usageResult = await pg.query<{ event_count: string }>(
      `SELECT event_count
         FROM monthly_usage
        WHERE tenant_id = $1
          AND year  = $2
          AND month = $3`,
      [testTenant.tenantId, now.getUTCFullYear(), now.getUTCMonth() + 1],
    )
    expect(usageResult.rows.length).toBeGreaterThan(0)
    expect(Number(usageResult.rows[0]!['event_count'])).toBeGreaterThanOrEqual(1)

    // 4. Assert pipeline_metrics stage records written to MongoDB
    const metrics = await getMongoDb()
      .collection('pipeline_metrics')
      .find({ 'meta.traceId': traceId })
      .toArray()

    // traceStage stores meta.traceId — expect at minimum the 'mongo' stage
    expect(metrics.length).toBeGreaterThan(0)
    const stageNames = metrics.map((m) => m['stage'] as string)
    expect(stageNames).toContain('mongo')

    // 5. Assert leaderboard ZINCRBY updated in Redis
    const redis = getRedisClient()
    const dateKey = `${now.getUTCFullYear()}-${String(now.getUTCMonth() + 1).padStart(2, '0')}-${String(now.getUTCDate()).padStart(2, '0')}`
    const score = await redis.zscore(`leaderboard:${dateKey}`, testTenant.projectId)
    expect(Number(score)).toBeGreaterThanOrEqual(1)
  }, 30_000)

  // -------------------------------------------------------------------------
  // Stream consumer: XADD → XREADGROUP → processEvent → XACK
  // -------------------------------------------------------------------------

  it('stream consumer: XADD message is read via XREADGROUP, processed, and ACKed', async () => {
    const container = getContainer()
    const redis = getRedisClient()
    const eventId = crypto.randomUUID()
    const traceId = crypto.randomUUID()

    // Ensure consumer group exists
    await container.redis.ensureConsumerGroup(STREAM_KEY, CONSUMER_GROUP)

    // XADD the raw event to the stream (same format the HTTP ingest route uses)
    await redis.xadd(
      STREAM_KEY, '*',
      'eventId', eventId,
      'traceId', traceId,
      'projectId', testTenant.projectId,
      'tenantId', testTenant.tenantId,
      'planId', testTenant.planId,
      'type', 'log',
      'severity', 'info',
      'message', 'X1 stream consumer XACK test',
      'fingerprint', 'x1-xack-fp',
    )

    // XREADGROUP — mimic what IngestConsumer does
    const streamResults = await redis.xreadgroup(
      'GROUP', CONSUMER_GROUP, 'test-x1-consumer',
      'COUNT', 1,
      'BLOCK', 500,
      'STREAMS', STREAM_KEY, '>',
    ) as Array<[string, Array<[string, string[]]>]> | null

    expect(streamResults).not.toBeNull()
    const [, messages] = streamResults![0]!
    const [msgId, fields] = messages![0]!
    expect(msgId).toBeDefined()

    // The message should be in XPENDING before ACK
    const pendingBefore = await redis.xpending(STREAM_KEY, CONSUMER_GROUP, msgId, msgId, 10) as unknown[]
    expect(pendingBefore.length).toBeGreaterThan(0)

    // Process using the ingestion service (what IngestConsumer.start() does)
    const data: Record<string, string> = {}
    for (let i = 0; i < fields.length - 1; i += 2) {
      const k = fields[i]
      const v = fields[i + 1]
      if (k !== undefined && v !== undefined) data[k] = v
    }
    await container.ingestion.processEvent({
      eventId: data['eventId'] ?? msgId,
      traceId: data['traceId'] ?? '',
      projectId: data['projectId'] ?? '',
      tenantId: data['tenantId'] ?? '',
      planId: data['planId'] ?? '',
      raw: {
        type: (data['type'] ?? 'log') as 'log',
        severity: (data['severity'] ?? 'info') as 'info',
        message: data['message'] ?? '',
        fingerprint: data['fingerprint'],
      },
    })

    // XACK the message (what IngestConsumer does after processEvent succeeds)
    await redis.xack(STREAM_KEY, CONSUMER_GROUP, msgId)

    // The message should no longer be in XPENDING
    const pendingAfter = await redis.xpending(STREAM_KEY, CONSUMER_GROUP, msgId, msgId, 10) as unknown[]
    expect(pendingAfter.length).toBe(0)

    // And the event should have landed in MongoDB
    const mongoDoc = await getMongoDb()
      .collection('events')
      .findOne({ _id: eventId } as unknown as import('mongodb').Filter<import('mongodb').Document>)
    expect(mongoDoc).not.toBeNull()
  }, 20_000)

  // -------------------------------------------------------------------------
  // Idempotency: same eventId twice must not double-count in PG
  // -------------------------------------------------------------------------

  it('idempotency: processing same eventId twice does not double-count in PG', async () => {
    const eventId = crypto.randomUUID()
    const traceId = crypto.randomUUID()
    const now = new Date()

    const payload = {
      eventId,
      traceId,
      projectId: testTenant.projectId,
      tenantId: testTenant.tenantId,
      planId: testTenant.planId,
      raw: {
        type: 'log' as const,
        severity: 'info' as const,
        message: 'idempotency test event',
        fingerprint: 'x1-idem-001',
        occurredAt: now.toISOString(),
      },
    }

    const pg = getPgPool()

    // Snapshot the counter before processing
    const before = await pg.query<{ event_count: string }>(
      `SELECT event_count
         FROM monthly_usage
        WHERE tenant_id = $1
          AND year  = $2
          AND month = $3`,
      [testTenant.tenantId, now.getUTCFullYear(), now.getUTCMonth() + 1],
    )
    const beforeCount = Number(before.rows[0]?.['event_count'] ?? 0)

    // Process the exact same event twice
    await processEvent(payload)
    await processEvent(payload)
    await new Promise<void>((r) => setTimeout(r, 300))

    // Read the counter after processing
    const after = await pg.query<{ event_count: string }>(
      `SELECT event_count
         FROM monthly_usage
        WHERE tenant_id = $1
          AND year  = $2
          AND month = $3`,
      [testTenant.tenantId, now.getUTCFullYear(), now.getUTCMonth() + 1],
    )
    const afterCount = Number(after.rows[0]?.['event_count'] ?? 0)

    // usage_dedup gate ensures the counter moves by exactly 1, not 2
    expect(afterCount - beforeCount).toBe(1)
  }, 15_000)
})
