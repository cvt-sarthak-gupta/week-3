/**
 * X4: Query Complexity & Performance Budget
 *
 * Each test runs its target operation, asserts the timing budget from the spec,
 * and logs the query-plan output (EXPLAIN ANALYZE / executionStats / ES profile)
 * so the evidence is captured in CI logs.
 *
 * Spec targets (seeded dataset):
 *   P3  PostgreSQL quota report      10k tenants, 12 months   < 200 ms
 *   M3  MongoDB error pipeline        1M events                < 2 000 ms
 *   E3  Elasticsearch full-text        5M log docs              < 500 ms
 *   R1  Redis Lua rate-limit check    N/A (in-memory)          < 5 ms p95
 *   X1  Full ingestion pipeline       single event             < 100 ms p95
 *
 * Tests here run against the test databases (small dataset); they will be faster
 * than the targets.  Full-scale benchmark evidence is in docs/PERFORMANCE-REPORT.json.
 */

import { describe, it, expect, beforeAll } from 'vitest'
import { randomUUID } from 'node:crypto'
import {
  getPgPool,
  getMongoDb,
  getEsClient,
  getContainer,
  createTestTenant,
} from '../helpers/setup.js'

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function p95(samples: number[]): number {
  const sorted = [...samples].sort((a, b) => a - b)
  const idx = Math.ceil(0.95 * sorted.length) - 1
  return sorted[Math.max(0, idx)]!
}

// Quota-report CTE (same query as P3 tests)
function quotaReportSql(): string {
  return `
    WITH month_agg AS (
      SELECT tenant_id, SUM(event_count) AS this_month_events
      FROM monthly_usage
      WHERE year = $1 AND month = $2
      GROUP BY tenant_id
    ),
    prev_month_agg AS (
      SELECT tenant_id, SUM(event_count) AS prev_month_events
      FROM monthly_usage
      WHERE year = $3 AND month = $4
      GROUP BY tenant_id
    ),
    ranked AS (
      SELECT
        t.id                                   AS tenant_id,
        t.name,
        p.name                                 AS plan_name,
        p.event_quota_per_month                AS quota,
        COALESCE(m.this_month_events, 0)       AS this_month_events,
        COALESCE(pm.prev_month_events, 0)      AS prev_month_events,
        RANK() OVER (
          ORDER BY COALESCE(m.this_month_events, 0) DESC
        )                                      AS rank,
        CASE WHEN p.event_quota_per_month > 0
          THEN ROUND(
            COALESCE(m.this_month_events, 0)::NUMERIC
              / p.event_quota_per_month * 100, 2)
          ELSE 0
        END                                    AS usage_pct,
        COALESCE(m.this_month_events, 0)
          > (p.event_quota_per_month * 0.8)    AS exceeded_80pct,
        CASE WHEN COALESCE(pm.prev_month_events, 0) > 0
          THEN ROUND(
            (COALESCE(m.this_month_events, 0) - pm.prev_month_events)::NUMERIC
              / pm.prev_month_events * 100, 2)
          ELSE NULL
        END                                    AS mom_growth_pct
      FROM tenants t
      JOIN plans p ON p.id = t.plan_id
      LEFT JOIN month_agg  m  ON m.tenant_id  = t.id
      LEFT JOIN prev_month_agg pm ON pm.tenant_id = t.id
    )
    SELECT * FROM ranked ORDER BY rank LIMIT 100
  `
}

// ---------------------------------------------------------------------------
// Shared fixtures
// ---------------------------------------------------------------------------

let x4PlanId: string
let x4Fixture: Awaited<ReturnType<typeof createTestTenant>>
const x4ProjectId = `x4-perf-${randomUUID().slice(0, 8)}`

beforeAll(async () => {
  const pool = getPgPool()

  // Seed a plan for P3 tenants
  x4PlanId = randomUUID()
  await pool.query(
    `INSERT INTO plans (id, name, event_quota_per_month, retention_days, max_projects, price_cents)
     VALUES ($1, 'X4 Plan', 500000, 90, 50, 0)
     ON CONFLICT DO NOTHING`,
    [x4PlanId],
  )

  const now = new Date()
  const prev = new Date(now)
  prev.setMonth(prev.getMonth() - 1)

  // Seed 20 tenants each with two months of usage (small but sufficient to exercise the query)
  for (let i = 0; i < 20; i++) {
    const tid = randomUUID()
    const slug = `x4-tenant-${i}-${Date.now()}`
    await pool.query(
      `INSERT INTO tenants (id, name, slug, plan_id) VALUES ($1, $2, $3, $4)
       ON CONFLICT DO NOTHING`,
      [tid, `X4 Tenant ${i}`, slug, x4PlanId],
    )
    await pool.query(
      `INSERT INTO monthly_usage (tenant_id, year, month, event_count)
       VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING`,
      [tid, now.getFullYear(), now.getMonth() + 1, (i + 1) * 5000],
    )
    await pool.query(
      `INSERT INTO monthly_usage (tenant_id, year, month, event_count)
       VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING`,
      [tid, prev.getFullYear(), prev.getMonth() + 1, (i + 1) * 4000],
    )
  }

  // Seed MongoDB events for M3
  const db = getMongoDb()
  const fingerprints = Array.from({ length: 20 }, (_, i) => `fp-x4-${i}`)
  const severities = ['info', 'warn', 'error', 'fatal']
  const browsers = ['Chrome', 'Firefox', 'Safari']
  const eventsToInsert = Array.from({ length: 500 }, (_, i) => ({
    _id: `x4-evt-${i}-${randomUUID()}`,
    projectId: x4ProjectId,
    type: 'error',
    severity: severities[i % severities.length],
    message: `X4 test error message number ${i}`,
    fingerprint: fingerprints[i % fingerprints.length],
    occurredAt: new Date(Date.now() - (i % (7 * 24 * 60 * 60 * 1000))),
    ingestedAt: new Date(),
    tags: {},
    deviceContext: { browser: browsers[i % browsers.length] },
    userContext: i % 3 === 0 ? { userId: `user-${i % 50}` } : undefined,
    payload: {},
  }))
  await db.collection('events').insertMany(eventsToInsert, { ordered: false }).catch(() => {})

  // Seed Elasticsearch documents for E3
  const es = getEsClient()
  const indexName = `logs-${x4ProjectId}-${new Date().toISOString().slice(0, 7).replace('-', '.')}`
  await es.indices.create({
    index: indexName,
    mappings: {
      properties: {
        message:     { type: 'text' },
        severity:    { type: 'keyword' },
        stackTrace:  { type: 'text' },
        occurredAt:  { type: 'date' },
        projectId:   { type: 'keyword' },
        fingerprint: { type: 'keyword' },
        tags: {
          type: 'nested',
          properties: {
            key:   { type: 'keyword' },
            value: { type: 'keyword' },
          },
        },
      },
    },
  }).catch(() => {}) // ignore already-exists errors

  await es.indices.putAlias({ index: indexName, name: `logs-${x4ProjectId}-active` }).catch(() => {})

  const esDocs = Array.from({ length: 200 }, (_, i) => ({
    projectId: x4ProjectId,
    message:   `X4 error NullPointerException in PaymentsService iteration ${i}`,
    severity:  i % 10 === 0 ? 'fatal' : i % 4 === 0 ? 'error' : 'info',
    fingerprint: fingerprints[i % fingerprints.length],
    occurredAt: new Date(Date.now() - i * 60_000).toISOString(),
    tags: [{ key: 'env', value: i % 2 === 0 ? 'production' : 'staging' }],
    stackTrace: `Error: test\n  at PaymentsService.process (payments.ts:${i})`,
  }))

  const bulkBody = esDocs.flatMap(doc => [
    { index: { _index: indexName } },
    doc,
  ])
  await es.bulk({ body: bulkBody, refresh: true })

  // Create tenant fixture for X1 pipeline test
  x4Fixture = await createTestTenant()
}, 60_000)

// ---------------------------------------------------------------------------
// P3 — PostgreSQL Quota Report < 200 ms
// ---------------------------------------------------------------------------

describe('X4-P3: PostgreSQL Quota Report Performance', () => {
  it('executes in < 200ms and captures EXPLAIN ANALYZE output', async () => {
    const pool = getPgPool()
    const now = new Date()
    const prev = new Date(now)
    prev.setMonth(prev.getMonth() - 1)

    const params = [now.getFullYear(), now.getMonth() + 1, prev.getFullYear(), prev.getMonth() + 1]

    // Warm up the planner cache
    await pool.query(quotaReportSql(), params)

    // Timed run
    const t0 = performance.now()
    const result = await pool.query(quotaReportSql(), params)
    const durationMs = performance.now() - t0

    // Capture EXPLAIN (ANALYZE, BUFFERS) output for submission
    const explain = await pool.query<{ 'QUERY PLAN': string }>(
      `EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT) ${quotaReportSql()}`,
      params,
    )
    console.log('\n[X4-P3] EXPLAIN ANALYZE:\n', explain.rows.map(r => r['QUERY PLAN']).join('\n'))
    console.log(`[X4-P3] Observed: ${durationMs.toFixed(1)}ms  Target: <200ms`)

    expect(result.rows.length).toBeGreaterThan(0)
    expect(durationMs).toBeLessThan(200)
  })
})

// ---------------------------------------------------------------------------
// M3 — MongoDB Error Intelligence Pipeline < 2000 ms
// ---------------------------------------------------------------------------

describe('X4-M3: MongoDB Error Intelligence Pipeline Performance', () => {
  it('executes in < 2000ms and captures executionStats', async () => {
    const db = getMongoDb()
    const now = new Date()
    const windowStart = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000)

    const pipeline = [
      { $match: { projectId: x4ProjectId, occurredAt: { $gte: windowStart } } },
      {
        $facet: {
          topErrors: [
            { $group: { _id: '$fingerprint', message: { $first: '$message' }, count: { $sum: 1 }, firstSeen: { $min: '$occurredAt' }, lastSeen: { $max: '$occurredAt' }, affectedUsers: { $addToSet: '$userContext.userId' } } },
            { $sort: { count: -1 } },
            { $limit: 10 },
            { $project: { _id: 0, fingerprint: '$_id', message: 1, count: 1, firstSeen: 1, lastSeen: 1 } },
          ],
          hourlyHistogram: [
            { $group: { _id: { $dateToString: { format: '%Y-%m-%dT%H:00:00Z', date: '$occurredAt', timezone: 'UTC' } }, count: { $sum: 1 } } },
            { $sort: { _id: 1 } },
          ],
          severityBrowser: [
            { $group: { _id: { severity: '$severity', browser: '$deviceContext.browser' }, count: { $sum: 1 } } },
            { $sort: { count: -1 } },
          ],
          newFingerprints: [
            { $group: { _id: '$fingerprint', firstSeen: { $min: '$occurredAt' } } },
            { $match: { firstSeen: { $gte: new Date(now.getTime() - 24 * 60 * 60 * 1000) } } },
          ],
        },
      },
    ]

    // Capture executionStats via db.command
    const explainResult = await db.command({
      explain: { aggregate: 'events', pipeline, cursor: {} },
      verbosity: 'executionStats',
    })
    const execStats = explainResult?.stages?.[0]?.['$cursor']?.executionStats ?? explainResult?.executionStats ?? {}
    console.log('\n[X4-M3] executionStats:', JSON.stringify({
      nReturned:          execStats.nReturned ?? 'n/a',
      totalDocsExamined:  execStats.totalDocsExamined ?? 'n/a',
      totalKeysExamined:  execStats.totalKeysExamined ?? 'n/a',
      executionTimeMillis: execStats.executionTimeMillis ?? 'n/a',
    }, null, 2))

    // Timed production run
    const t0 = performance.now()
    const rows = await db.collection('events').aggregate(pipeline).toArray()
    const durationMs = performance.now() - t0

    console.log(`[X4-M3] Observed: ${durationMs.toFixed(1)}ms  Target: <2000ms`)

    expect(rows.length).toBeGreaterThan(0)
    expect(durationMs).toBeLessThan(2000)
  })
})

// ---------------------------------------------------------------------------
// E3 — Elasticsearch Full-Text Search < 500 ms
// ---------------------------------------------------------------------------

describe('X4-E3: Elasticsearch Full-Text Search Performance', () => {
  it('executes in < 500ms and captures ES profile output', async () => {
    const es = getEsClient()
    const alias = `logs-${x4ProjectId}-active`

    const searchBody = {
      size: 20,
      query: {
        bool: {
          must: [
            {
              multi_match: {
                query: 'NullPointerException PaymentsService',
                fields: ['message', 'stackTrace'],
                type: 'best_fields' as const,
                tie_breaker: 0.3,
              },
            },
          ],
          filter: [
            {
              range: {
                occurredAt: {
                  gte: new Date(Date.now() - 7 * 24 * 60 * 60 * 1000).toISOString(),
                },
              },
            },
          ],
          should: [
            {
              nested: {
                path: 'tags',
                query: { bool: { must: [{ term: { 'tags.key': 'env' } }, { term: { 'tags.value': 'production' } }] } },
                score_mode: 'avg',
                boost: 1.5,
              },
            },
          ],
          must_not: [{ term: { severity: 'debug' } }],
        },
      },
      highlight: {
        fields: { message: { number_of_fragments: 1, fragment_size: 150 } },
      },
    }

    // Warm-up run (also captures profile)
    const warmup = await es.search({ index: alias, ...searchBody, profile: true })
    const shards = (warmup as any).profile?.shards ?? []
    if (shards.length > 0) {
      const firstQuery = shards[0]?.searches?.[0]?.query?.[0] ?? {}
      console.log('\n[X4-E3] ES profile (first shard, top query node):', JSON.stringify({
        type:        firstQuery.type ?? 'n/a',
        description: (firstQuery.description ?? '').slice(0, 120),
        time_in_nanos: firstQuery.time_in_nanos ?? 'n/a',
      }, null, 2))
    }

    // Timed run
    const t0 = performance.now()
    const response = await es.search({ index: alias, ...searchBody })
    const wallMs = performance.now() - t0
    const esTookMs = (response as any).took ?? 0

    console.log(`[X4-E3] ES took: ${esTookMs}ms  Wall-clock: ${wallMs.toFixed(1)}ms  Target: <500ms`)

    expect(esTookMs).toBeLessThan(500)
    expect(wallMs).toBeLessThan(500)
  })
})

// ---------------------------------------------------------------------------
// R1 — Redis Lua Sliding-Window Rate Limit < 5 ms p95
// ---------------------------------------------------------------------------

describe('X4-R1: Redis Lua Rate-Limit Check Performance', () => {
  it('p95 of 20 consecutive calls is < 5ms', async () => {
    const container = getContainer()
    const apiKey = `x4-rl-${randomUUID()}`
    const config = { windowMs: 60_000, maxRequests: 1000 }

    // Warm up (script SHA already loaded in container.initialize)
    await container.rateLimit.checkRateLimit(apiKey, config)

    const latencies: number[] = []
    for (let i = 0; i < 20; i++) {
      const t0 = performance.now()
      await container.rateLimit.checkRateLimit(apiKey, config)
      latencies.push(performance.now() - t0)
    }

    const p95ms = p95(latencies)
    const p50ms = latencies.sort((a, b) => a - b)[Math.floor(latencies.length / 2)]!

    console.log(`[X4-R1] p50: ${p50ms.toFixed(2)}ms  p95: ${p95ms.toFixed(2)}ms  Target: p95 <5ms`)
    console.log('[X4-R1] Note: loopback Docker adds ~0.5-1ms vs bare-metal Redis.')

    expect(p95ms).toBeLessThan(5)
  })
})

// ---------------------------------------------------------------------------
// X1 — Full Ingestion Pipeline p95 < 100 ms
// ---------------------------------------------------------------------------

describe('X4-X1: Full Ingestion Pipeline Performance (p95)', () => {
  it('p95 of 20 end-to-end processEvent calls is < 100ms', async () => {
    const container = getContainer()
    const { projectId, tenantId, planId } = x4Fixture

    const latencies: number[] = []
    for (let i = 0; i < 20; i++) {
      const t0 = performance.now()
      await container.ingestion.processEvent({
        eventId:   randomUUID(),
        traceId:   randomUUID(),
        projectId,
        tenantId,
        planId,
        raw: {
          type:     'error',
          severity: i % 5 === 0 ? 'fatal' : 'error',
          message:  `X4 pipeline perf test event ${i}`,
        },
      })
      latencies.push(performance.now() - t0)
    }

    const p95ms = p95(latencies)
    const p50ms = [...latencies].sort((a, b) => a - b)[Math.floor(latencies.length / 2)]!

    console.log(`[X4-X1] p50: ${p50ms.toFixed(1)}ms  p95: ${p95ms.toFixed(1)}ms  Target: <100ms`)
    console.log('[X4-X1] Stage breakdown available in pipeline_metrics collection.')

    expect(p95ms).toBeLessThan(100)
  }, 60_000)
})
