import { describe, it, expect, beforeAll, afterAll } from 'vitest'
import { Client } from '@elastic/elasticsearch'

const esClient = new Client({ node: process.env['ES_URL'] ?? 'http://localhost:9201' })
const PROJECT_ID = `e4-dash-${Date.now()}`
const INDEX = `logs-${PROJECT_ID}`

describe('E4: Dashboard Aggregation', () => {
  beforeAll(async () => {
    await esClient.indices.create({
      index: INDEX,
      body: {
        mappings: {
          properties: {
            projectId: { type: 'keyword' },
            severity: { type: 'keyword' },
            message: { type: 'text' },
            occurredAt: { type: 'date' },
            fingerprint: { type: 'keyword' },
            responseTimeMs: { type: 'float' },
            tags: {
              type: 'nested',
              properties: {
                env: { type: 'keyword' },
                service: { type: 'keyword' },
              },
            },
          },
        },
      },
    })

    const severities = ['error', 'fatal', 'warn', 'info']
    const ops: unknown[] = []
    for (let i = 0; i < 100; i++) {
      const daysAgo = Math.floor(i / 15)
      const occurredAt = new Date(Date.now() - daysAgo * 86_400_000 - i * 3_600_000).toISOString()
      ops.push({ index: { _index: INDEX } })
      ops.push({
        projectId: PROJECT_ID,
        severity: severities[i % severities.length],
        message: `Dashboard test event ${i}`,
        occurredAt,
        fingerprint: `fp-e4-${i % 10}`,
        responseTimeMs: 50 + (i % 450),
        tags: [{ env: i % 2 === 0 ? 'prod' : 'staging', service: 'api' }],
      })
    }
    await esClient.bulk({ refresh: true, body: ops })
  })

  afterAll(async () => {
    await esClient.indices.delete({ index: INDEX }).catch(() => {})
  })

  it('single _search returns timeseries, severity breakdown, percentiles, and unique fingerprints', async () => {
    const sevenDaysAgo = new Date(Date.now() - 7 * 86_400_000).toISOString()
    const start = performance.now()

    const result = await esClient.search({
      index: INDEX,
      body: {
        size: 0,
        query: {
          bool: {
            filter: [
              { term: { projectId: PROJECT_ID } },
              { range: { occurredAt: { gte: sevenDaysAgo } } },
            ],
            must_not: [{ term: { severity: 'debug' } }],
          },
        },
        aggs: {
          timeseries: {
            date_histogram: {
              field: 'occurredAt',
              calendar_interval: 'hour',
            },
            aggs: {
              by_severity: { terms: { field: 'severity', size: 10 } },
            },
          },
          by_severity_top: {
            terms: { field: 'severity', size: 10 },
            aggs: {
              top_events: {
                top_hits: { size: 3, _source: ['message', 'occurredAt', 'fingerprint'] },
              },
            },
          },
          percentiles: {
            filter: { exists: { field: 'responseTimeMs' } },
            aggs: {
              response_time: {
                percentiles: { field: 'responseTimeMs', percents: [50, 95, 99] },
              },
            },
          },
          unique_fingerprints: {
            cardinality: { field: 'fingerprint' },
          },
          anomalous_terms: {
            significant_terms: { field: 'fingerprint' },
          },
        },
      },
    }) as unknown as { aggregations: Record<string, unknown> }

    const elapsed = performance.now() - start
    console.log(`E4 dashboard elapsed: ${elapsed.toFixed(0)}ms`)

    const aggs = result.aggregations
    expect(aggs).toBeDefined()

    const timeseries = aggs['timeseries'] as { buckets: unknown[] }
    expect(Array.isArray(timeseries.buckets)).toBe(true)
    expect(timeseries.buckets.length).toBeGreaterThan(0)

    const bySeverityTop = aggs['by_severity_top'] as { buckets: unknown[] }
    expect(bySeverityTop.buckets.length).toBeGreaterThan(0)

    const percentiles = aggs['percentiles'] as {
      response_time: { values: Record<string, number> }
    }
    const p50 = percentiles.response_time.values['50.0']
    const p95 = percentiles.response_time.values['95.0']
    expect(p50).toBeGreaterThan(0)
    expect(p95).toBeGreaterThanOrEqual(p50!)

    const uniqueFingerprints = aggs['unique_fingerprints'] as { value: number }
    expect(uniqueFingerprints.value).toBeGreaterThan(0)
    expect(uniqueFingerprints.value).toBeLessThanOrEqual(10)

    expect(elapsed).toBeLessThan(3000)
  }, 30_000)

  it('timeseries day buckets with doc_count > 0 have severity sub-aggs', async () => {
    const result = await esClient.search({
      index: INDEX,
      body: {
        size: 0,
        query: { bool: { filter: [{ term: { projectId: PROJECT_ID } }] } },
        aggs: {
          timeseries: {
            date_histogram: { field: 'occurredAt', calendar_interval: 'day' },
            aggs: { severity: { terms: { field: 'severity' } } },
          },
        },
      },
    }) as unknown as { aggregations: Record<string, unknown> }

    const aggs = result.aggregations
    const timeseries = aggs['timeseries'] as {
      buckets: { doc_count: number; severity: { buckets: unknown[] } }[]
    }
    for (const bucket of timeseries.buckets) {
      if (bucket.doc_count > 0) {
        expect(bucket.severity.buckets.length).toBeGreaterThan(0)
      }
    }
  })
})
