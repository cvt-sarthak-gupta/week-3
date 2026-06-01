import { describe, it, expect, beforeAll, afterAll } from 'vitest'
import { Client } from '@elastic/elasticsearch'
import { getContainer, applyPolicyForProject } from '../helpers/setup.js'

const esClient = new Client({ node: process.env['ES_URL'] ?? 'http://localhost:9201' })
const PROJECT_ID = `e4-dash-${Date.now()}`
// Use proper alias-backed index so getDashboardReport() works (it uses aliasName())
const ALIAS = `logs-${PROJECT_ID}-active`

describe('E4: Dashboard Aggregation', () => {
  beforeAll(async () => {
    // Create a proper index+alias structure so getDashboardReport() can use aliasName()
    await applyPolicyForProject(PROJECT_ID, 90)

    const severities = ['error', 'fatal', 'warn', 'info']
    const ops: unknown[] = []
    for (let i = 0; i < 100; i++) {
      const daysAgo = Math.floor(i / 15)
      const occurredAt = new Date(Date.now() - daysAgo * 86_400_000 - i * 3_600_000).toISOString()
      ops.push({ index: { _index: ALIAS } })
      ops.push({
        projectId: PROJECT_ID,
        severity: severities[i % severities.length],
        message: `Dashboard test event ${i}`,
        occurredAt,
        fingerprint: `fp-e4-${i % 10}`,
        // payload.responseTimeMs — matches production mapping (nested under payload object)
        payload: { responseTimeMs: 50 + (i % 450) },
        tags: [{ key: 'env', value: i % 2 === 0 ? 'prod' : 'staging' }],
      })
    }
    await esClient.bulk({ refresh: true, operations: ops })
  })

  afterAll(async () => {
    await esClient.indices.delete({ index: `logs-${PROJECT_ID}-*` }).catch(() => {})
  })

  it('single _search returns timeseries, severity breakdown, percentiles, cardinality, and significant terms', async () => {
    const sevenDaysAgo = new Date(Date.now() - 7 * 86_400_000).toISOString()
    const start = performance.now()

    const result = await esClient.search({
      index: ALIAS,
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
            calendar_interval: 'hour' as const,
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
        response_time_percentiles: {
          filter: { exists: { field: 'payload.responseTimeMs' } },
          aggs: {
            percentiles: {
              percentiles: { field: 'payload.responseTimeMs', percents: [50, 95, 99] },
            },
          },
        },
        unique_fingerprints: {
          cardinality: { field: 'fingerprint' },
        },
        recent_hour_terms: {
          filter: { range: { occurredAt: { gte: 'now-1h' } } },
          aggs: {
            significant_error_terms: {
              significant_terms: {
                field: 'message.keyword',
                background_filter: { term: { projectId: PROJECT_ID } },
              },
            },
          },
        },
      },
    }) as unknown as { aggregations: Record<string, unknown> }

    const elapsed = performance.now() - start
    console.log(`E4 dashboard elapsed: ${elapsed.toFixed(0)}ms`)

    const aggs = result.aggregations
    expect(aggs).toBeDefined()

    // Date histogram
    const timeseries = aggs['timeseries'] as { buckets: unknown[] }
    expect(Array.isArray(timeseries.buckets)).toBe(true)
    expect(timeseries.buckets.length).toBeGreaterThan(0)

    // Severity terms with top_hits sub-agg (3 most recent per severity)
    const bySeverityTop = aggs['by_severity_top'] as {
      buckets: Array<{ key: string; doc_count: number; top_events: { hits: { hits: unknown[] } } }>
    }
    expect(bySeverityTop.buckets.length).toBeGreaterThan(0)
    for (const bucket of bySeverityTop.buckets) {
      expect(bucket.top_events.hits.hits.length).toBeLessThanOrEqual(3)
    }

    // Percentiles on payload.responseTimeMs
    const responseTimePercentiles = aggs['response_time_percentiles'] as {
      percentiles: { values: Record<string, number> }
    }
    const p50 = responseTimePercentiles.percentiles.values['50.0']
    const p95 = responseTimePercentiles.percentiles.values['95.0']
    expect(p50).toBeGreaterThan(0)
    expect(p95).toBeGreaterThanOrEqual(p50!)

    // Cardinality on fingerprint
    const uniqueFingerprints = aggs['unique_fingerprints'] as { value: number }
    expect(uniqueFingerprints.value).toBeGreaterThan(0)
    expect(uniqueFingerprints.value).toBeLessThanOrEqual(10)

    // significant_terms foreground=1h vs 7d background
    const recentHourTerms = aggs['recent_hour_terms'] as {
      significant_error_terms: { buckets: unknown[] }
    }
    expect(recentHourTerms).toBeDefined()
    expect(Array.isArray(recentHourTerms.significant_error_terms.buckets)).toBe(true)

    expect(elapsed).toBeLessThan(3000)
  }, 30_000)

  it('getDashboardReport() returns all required aggregation fields', async () => {
    const container = getContainer()
    const report = await container.reports.getDashboardReport(PROJECT_ID) as Record<string, unknown>

    expect(report).toHaveProperty('eventsOverTime')
    expect(report).toHaveProperty('bySeverity')
    expect(report).toHaveProperty('responseTimePercentiles')
    expect(report).toHaveProperty('uniqueErrorCount')
    expect(report).toHaveProperty('significantErrorTerms')
    expect(report).toHaveProperty('errorCount')
    expect(report).toHaveProperty('totalCount')

    expect(Array.isArray(report['eventsOverTime'])).toBe(true)
    expect(Array.isArray(report['bySeverity'])).toBe(true)
    expect(Array.isArray(report['significantErrorTerms'])).toBe(true)
    expect(typeof report['uniqueErrorCount']).toBe('number')
  }, 15_000)

  it('timeseries day buckets with doc_count > 0 have severity sub-aggs', async () => {
    const result = await esClient.search({
      index: ALIAS,
      size: 0,
      query: { bool: { filter: [{ term: { projectId: PROJECT_ID } }] } },
      aggs: {
        timeseries: {
          date_histogram: { field: 'occurredAt', calendar_interval: 'day' as const },
          aggs: { severity: { terms: { field: 'severity' } } },
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
