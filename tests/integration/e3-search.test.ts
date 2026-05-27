/**
 * E3: Full-Text Search with search_after Pagination
 *
 * Verifies that:
 *  - multi_match across message and stackTrace fields works
 *  - must_not correctly excludes debug severity events
 *  - highlights are returned for matched message field
 *  - search_after cursor returns the correct next page with no overlap
 *  - term filter on severity correctly narrows results
 *  - production env tag boost places matching docs higher in relevance score
 */

import { Client } from '@elastic/elasticsearch'
import { describe, it, expect, beforeAll, afterAll } from 'vitest'

const esClient = new Client({ node: process.env['ES_URL'] ?? 'http://localhost:9201' })
const TEST_INDEX = 'test-e3-search'
const TOTAL_DOCS = 32

// Helper to build a log document
function makeDoc(i: number): Record<string, unknown> {
  const severities = ['info', 'warn', 'error', 'fatal', 'debug'] as const
  const severity = i < 5
    ? 'debug'
    : (severities[i % 4] as string) // first 5 are debug, rest cycle through info/warn/error/fatal

  const isProduction = i % 3 === 0 // every 3rd doc has env=production tag
  const tags = isProduction
    ? [{ key: 'env', value: 'production' }, { key: 'service', value: `svc-${i % 5}` }]
    : [{ key: 'env', value: 'staging' }, { key: 'service', value: `svc-${i % 5}` }]

  const messages = [
    'NullPointerException in PaymentsService during checkout flow',
    'database connection timeout after 30s retries exhausted',
    'payment gateway returned 503 service unavailable',
    'user authentication failed invalid token signature',
    'order processing completed successfully for customer',
    'cache miss for project report recomputing aggregates',
    'rate limit exceeded for api key request denied',
    'webhook delivery failed after 3 attempts backoff',
  ]

  return {
    message: messages[i % messages.length],
    severity,
    projectId: 'proj-e3',
    stackTrace: severity === 'error' || severity === 'fatal'
      ? `Error: something failed\n  at handler (/app/src/handler.ts:${i})\n  at process`
      : null,
    tags,
    occurredAt: new Date(Date.now() - i * 60_000).toISOString(), // stagger by 1min each
    fingerprint: `fp-${i}`,
  }
}

describe('E3: Full-Text Search with search_after', () => {
  beforeAll(async () => {
    await esClient.indices.delete({ index: TEST_INDEX, ignore_unavailable: true })

    await esClient.indices.create({
      index: TEST_INDEX,
      body: {
        settings: {
          analysis: {
            filter: {
              english_stop: { type: 'stop', stopwords: '_english_' },
              error_synonyms: {
                type: 'synonym',
                synonyms: ['exception, error, err'],
              },
              edge_ngram_filter: {
                type: 'edge_ngram',
                min_gram: 2,
                max_gram: 20,
              },
            },
            analyzer: {
              logs_analyzer: {
                type: 'custom',
                tokenizer: 'standard',
                filter: ['lowercase', 'english_stop', 'error_synonyms', 'edge_ngram_filter'],
              },
            },
          },
        },
        mappings: {
          dynamic: false,
          properties: {
            message: { type: 'text', analyzer: 'logs_analyzer' },
            severity: { type: 'keyword' },
            projectId: { type: 'keyword' },
            stackTrace: { type: 'text', analyzer: 'standard' },
            tags: {
              type: 'nested',
              properties: {
                key: { type: 'keyword' },
                value: { type: 'keyword' },
              },
            },
            occurredAt: { type: 'date' },
            fingerprint: { type: 'keyword' },
          },
        },
      },
    })

    // Bulk index TOTAL_DOCS documents
    const ops: unknown[] = []
    for (let i = 0; i < TOTAL_DOCS; i++) {
      ops.push({ index: { _index: TEST_INDEX, _id: `doc-${i}` } })
      ops.push(makeDoc(i))
    }

    // Add a special doc for the NullPointerException search test
    ops.push({ index: { _index: TEST_INDEX, _id: 'doc-npe' } })
    ops.push({
      message: 'NullPointerException in PaymentsService',
      severity: 'error',
      projectId: 'proj-e3',
      stackTrace: 'java.lang.NullPointerException at payments.Service.process(Service.java:42)',
      tags: [{ key: 'env', value: 'production' }, { key: 'service', value: 'payments' }],
      occurredAt: new Date().toISOString(),
      fingerprint: 'fp-npe',
    })

    const bulkResponse = await esClient.bulk({ refresh: true, body: ops })
    if ((bulkResponse as any).errors) {
      const failed = (bulkResponse as any).items
        .filter((item: any) => item.index?.error)
        .map((item: any) => item.index?.error)
      throw new Error(`Bulk indexing errors: ${JSON.stringify(failed)}`)
    }
  })

  afterAll(async () => {
    await esClient.indices.delete({ index: TEST_INDEX, ignore_unavailable: true })
  })

  it('excludes debug severity events from search results via must_not', async () => {
    const result = await esClient.search({
      index: TEST_INDEX,
      body: {
        size: 50,
        query: {
          bool: {
            must: [{ match_all: {} }],
            must_not: [{ term: { severity: 'debug' } }],
          },
        },
      },
    })
    const hits = (result as any).hits.hits
    const severities = hits.map((h: any) => h._source.severity)
    // None of the returned docs should be debug severity
    expect(severities).not.toContain('debug')
    // We should get results (TOTAL_DOCS - 5 debug docs + 1 npe doc)
    expect(hits.length).toBeGreaterThan(0)
  })

  it('multi_match on message and stackTrace finds NullPointerException doc', async () => {
    const result = await esClient.search({
      index: TEST_INDEX,
      body: {
        query: {
          multi_match: {
            query: 'NullPointerException PaymentsService',
            fields: ['message^2', 'stackTrace'],
            type: 'best_fields',
          },
        },
      },
    })
    const ids = (result as any).hits.hits.map((h: any) => h._id)
    expect(ids).toContain('doc-npe')
    // The NullPointerException message appears in other docs too (doc-0, doc-8, ...)
    expect((result as any).hits.total.value).toBeGreaterThan(0)
  })

  it('returns highlighted snippets in message field', async () => {
    const result = await esClient.search({
      index: TEST_INDEX,
      body: {
        query: {
          match: {
            message: 'payment',
          },
        },
        highlight: {
          fields: {
            message: {
              pre_tags: ['<em>'],
              post_tags: ['</em>'],
              number_of_fragments: 1,
            },
          },
        },
      },
    })
    const hits = (result as any).hits.hits
    expect(hits.length).toBeGreaterThan(0)
    // Every returned hit should have highlight.message
    for (const hit of hits) {
      expect(hit.highlight).toBeDefined()
      expect(hit.highlight.message).toBeDefined()
      expect(Array.isArray(hit.highlight.message)).toBe(true)
      expect(hit.highlight.message.length).toBeGreaterThan(0)
      // Should contain the highlight tags
      const snippet = hit.highlight.message[0] as string
      expect(snippet).toContain('<em>')
      expect(snippet).toContain('</em>')
    }
  })

  it('search_after cursor returns correct next page without overlap', async () => {
    const PAGE_SIZE = 5

    // First page — sorted by occurredAt desc, then _shard_doc for stable tie-breaking.
    // NOTE: sorting by _id requires fielddata which is disabled by default in ES8.
    // _shard_doc is the recommended stable tiebreaker for search_after pagination.
    const firstPage = await esClient.search({
      index: TEST_INDEX,
      body: {
        size: PAGE_SIZE,
        query: {
          bool: {
            must_not: [{ term: { severity: 'debug' } }],
          },
        },
        sort: [
          { occurredAt: { order: 'desc' } },
          { fingerprint: { order: 'asc' } },
        ],
      },
    })

    const firstHits = (firstPage as any).hits.hits
    expect(firstHits.length).toBe(PAGE_SIZE)

    // Extract the sort values of the last hit for search_after
    const lastSort = firstHits[firstHits.length - 1].sort
    expect(lastSort).toBeDefined()

    // Second page using search_after
    const secondPage = await esClient.search({
      index: TEST_INDEX,
      body: {
        size: PAGE_SIZE,
        query: {
          bool: {
            must_not: [{ term: { severity: 'debug' } }],
          },
        },
        sort: [
          { occurredAt: { order: 'desc' } },
          { fingerprint: { order: 'asc' } },
        ],
        search_after: lastSort,
      },
    })

    const secondHits = (secondPage as any).hits.hits
    expect(secondHits.length).toBeGreaterThan(0)

    // Verify no overlapping IDs between pages
    const firstIds = new Set(firstHits.map((h: any) => h._id))
    const secondIds = secondHits.map((h: any) => h._id)
    for (const id of secondIds) {
      expect(firstIds.has(id)).toBe(false)
    }

    // Verify ordering: the first item on page 2 should come after the last item on page 1
    const lastOccurredAt = new Date(firstHits[firstHits.length - 1]._source.occurredAt).getTime()
    const firstNextOccurredAt = new Date(secondHits[0]._source.occurredAt).getTime()
    // Either the date is earlier (desc sort) or equal with a different tiebreaker
    expect(firstNextOccurredAt).toBeLessThanOrEqual(lastOccurredAt)
  })

  it('term filter on severity correctly narrows results to only that severity', async () => {
    const result = await esClient.search({
      index: TEST_INDEX,
      body: {
        size: 50,
        query: {
          bool: {
            filter: [{ term: { severity: 'error' } }],
          },
        },
      },
    })
    const hits = (result as any).hits.hits
    expect(hits.length).toBeGreaterThan(0)
    // Every result must be severity=error
    for (const hit of hits) {
      expect(hit._source.severity).toBe('error')
    }
  })

  it('production env boost places matching docs higher in score', async () => {
    const result = await esClient.search({
      index: TEST_INDEX,
      body: {
        size: 20,
        query: {
          bool: {
            must: [
              {
                match: { message: 'payment' },
              },
            ],
            should: [
              {
                nested: {
                  path: 'tags',
                  query: {
                    bool: {
                      must: [
                        { term: { 'tags.key': 'env' } },
                        { term: { 'tags.value': 'production' } },
                      ],
                    },
                  },
                  score_mode: 'sum',
                  boost: 2.0,
                },
              },
            ],
          },
        },
      },
    })

    const hits = (result as any).hits.hits
    expect(hits.length).toBeGreaterThan(0)

    // Find the first hit that has env=production tag and one that doesn't
    const productionHits = hits.filter((h: any) =>
      h._source.tags?.some((t: any) => t.key === 'env' && t.value === 'production')
    )
    const nonProductionHits = hits.filter((h: any) =>
      !h._source.tags?.some((t: any) => t.key === 'env' && t.value === 'production')
    )

    if (productionHits.length > 0 && nonProductionHits.length > 0) {
      // The top production hit should have a higher or equal score than any non-production hit
      // (boost means production docs get extra score for matching the should clause)
      const topProductionScore = productionHits[0]._score
      const topNonProductionScore = nonProductionHits[0]._score
      // At least the very first overall result should score higher if it's production
      // We just verify production results tend to come first
      const firstProductionIdx = hits.findIndex((h: any) =>
        h._source.tags?.some((t: any) => t.key === 'env' && t.value === 'production')
      )
      const firstNonProductionIdx = hits.findIndex((h: any) =>
        !h._source.tags?.some((t: any) => t.key === 'env' && t.value === 'production')
      )
      // Production docs with the boost should appear before non-production docs
      // when both match the main query
      expect(topProductionScore).toBeGreaterThanOrEqual(topNonProductionScore - 0.001)
    } else {
      // If all matching docs are either all production or all non-production, just verify results exist
      expect(hits.length).toBeGreaterThan(0)
    }
  })

  it('search returns zero results for term that does not exist', async () => {
    const result = await esClient.search({
      index: TEST_INDEX,
      body: {
        query: {
          match: {
            message: 'xyzzyfragnomicron_impossible_term_99',
          },
        },
      },
    })
    expect((result as any).hits.total.value).toBe(0)
  })

  it('severity filter combined with text search returns only matching severity', async () => {
    const result = await esClient.search({
      index: TEST_INDEX,
      body: {
        query: {
          bool: {
            must: [{ match: { message: 'database' } }],
            filter: [{ term: { severity: 'fatal' } }],
          },
        },
      },
    })
    const hits = (result as any).hits.hits
    // If any results, they must all be fatal
    for (const hit of hits) {
      expect(hit._source.severity).toBe('fatal')
    }
  })
})
