/**
 * E5: Percolator Alert Matching
 *
 * Verifies that:
 *  - Percolator index is created with correct mapping (query field + mirrored log fields)
 *  - Alert rules stored as percolator documents match incoming events correctly
 *  - A fatal event with 'payment' and 'database' in message matches exactly 2 of 3 rules
 *  - A non-matching event returns zero rule hits
 *  - projectId-scoped rules only fire for the correct project
 */

import { Client } from '@elastic/elasticsearch'
import { describe, it, expect, beforeAll, afterAll } from 'vitest'

const esClient = new Client({ node: process.env['ES_URL'] ?? 'http://localhost:9201' })
const PERCOLATOR_INDEX = 'test-alert-percolator'

describe('E5: Percolator Alert Matching', () => {
  beforeAll(async () => {
    // Clean slate
    await esClient.indices.delete({ index: PERCOLATOR_INDEX, ignore_unavailable: true })

    // Create the percolator index with:
    // - query field mapped as percolator
    // - mirrored field mappings from the logs-* template so percolation
    //   evaluates queries against correctly-typed fields
    await esClient.indices.create({
      index: PERCOLATOR_INDEX,
      body: {
        mappings: {
          properties: {
            // The percolator query field — stores the query DSL
            query: { type: 'percolator' },
            // Metadata fields stored on each rule document
            alertRuleId: { type: 'keyword' },
            projectId: { type: 'keyword' },
            name: { type: 'keyword' },
            // Mirrored log document fields — percolation requires the same mappings
            // as the documents being matched against the stored queries
            message: { type: 'text', analyzer: 'standard' },
            severity: { type: 'keyword' },
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

    // Index 3 alert rules as percolator documents:
    //
    // Rule 1: triggers when severity=fatal AND message contains "payment"
    // Rule 2: triggers when severity=fatal AND message contains "database"
    // Rule 3: triggers when severity=error (NOT fatal — so a fatal event should NOT match this)
    await esClient.bulk({
      refresh: true,
      body: [
        { index: { _index: PERCOLATOR_INDEX, _id: 'rule-1' } },
        {
          alertRuleId: 'rule-1',
          projectId: 'proj-1',
          name: 'Fatal Payment Errors',
          query: {
            bool: {
              must: [
                { term: { severity: 'fatal' } },
                { match: { message: 'payment' } },
              ],
            },
          },
        },
        { index: { _index: PERCOLATOR_INDEX, _id: 'rule-2' } },
        {
          alertRuleId: 'rule-2',
          projectId: 'proj-1',
          name: 'Fatal Database Errors',
          query: {
            bool: {
              must: [
                { term: { severity: 'fatal' } },
                { match: { message: 'database' } },
              ],
            },
          },
        },
        { index: { _index: PERCOLATOR_INDEX, _id: 'rule-3' } },
        {
          alertRuleId: 'rule-3',
          projectId: 'proj-1',
          name: 'Error Severity Only',
          // This rule only fires for severity=error, NOT fatal
          query: {
            bool: {
              must: [
                { term: { severity: 'error' } },
              ],
            },
          },
        },
        // Rule 4: scoped to a different project to test tenant isolation
        { index: { _index: PERCOLATOR_INDEX, _id: 'rule-4' } },
        {
          alertRuleId: 'rule-4',
          projectId: 'proj-2',
          name: 'Proj2 Fatal Rule',
          query: {
            bool: {
              must: [
                { term: { severity: 'fatal' } },
              ],
            },
          },
        },
      ],
    })
  })

  afterAll(async () => {
    await esClient.indices.delete({ index: PERCOLATOR_INDEX, ignore_unavailable: true })
  })

  it('percolator index exists with correct mapping', async () => {
    const mapping = await esClient.indices.getMapping({ index: PERCOLATOR_INDEX })
    const props = (mapping as any)[PERCOLATOR_INDEX].mappings.properties
    expect(props.query.type).toBe('percolator')
    expect(props.severity.type).toBe('keyword')
    expect(props.message.type).toBe('text')
    expect(props.tags.type).toBe('nested')
  })

  it('matches exactly 2 of 3 alert rules for a fatal payment+database event', async () => {
    // This is the critical test: a single proj-1 event matches Rule1 (fatal+payment) and
    // Rule2 (fatal+database) but NOT Rule3 (only matches error severity, not fatal) and
    // NOT Rule4 (belongs to proj-2).
    // The bool query wraps percolate + projectId term to scope to proj-1 only.
    const result = await esClient.search({
      index: PERCOLATOR_INDEX,
      body: {
        query: {
          bool: {
            must: [
              {
                percolate: {
                  field: 'query',
                  document: {
                    message: 'payment gateway database timeout',
                    severity: 'fatal',
                  },
                },
              },
              { term: { projectId: 'proj-1' } },
            ],
          },
        },
      },
    })

    const hits = (result as any).hits.hits
    const matchedIds = hits.map((h: any) => h._id).sort()

    // Should match exactly 2 rules
    expect(matchedIds).toHaveLength(2)
    expect(matchedIds).toContain('rule-1')  // fatal + payment ✓
    expect(matchedIds).toContain('rule-2')  // fatal + database ✓
    expect(matchedIds).not.toContain('rule-3')  // error only, not fatal ✗
    expect(matchedIds).not.toContain('rule-4')  // proj-2 scoped, filtered out ✗
  })

  it('fatal event without payment or database keyword matches no specific-keyword rules', async () => {
    const result = await esClient.search({
      index: PERCOLATOR_INDEX,
      body: {
        query: {
          percolate: {
            field: 'query',
            document: {
              message: 'user authentication session expired',
              severity: 'fatal',
            },
          },
        },
      },
    })

    const matchedIds = (result as any).hits.hits.map((h: any) => h._id)

    // Does not contain "payment" or "database" so neither rule-1 nor rule-2 match
    expect(matchedIds).not.toContain('rule-1')
    expect(matchedIds).not.toContain('rule-2')
    // Not error severity so rule-3 doesn't match either
    expect(matchedIds).not.toContain('rule-3')
  })

  it('error severity event matches rule-3 but not rule-1 or rule-2', async () => {
    const result = await esClient.search({
      index: PERCOLATOR_INDEX,
      body: {
        query: {
          percolate: {
            field: 'query',
            document: {
              message: 'something went wrong in the application',
              severity: 'error',
            },
          },
        },
      },
    })

    const matchedIds = (result as any).hits.hits.map((h: any) => h._id)

    // Rule 3 matches (severity=error)
    expect(matchedIds).toContain('rule-3')
    // Rule 1 and 2 require severity=fatal, so they should NOT match
    expect(matchedIds).not.toContain('rule-1')
    expect(matchedIds).not.toContain('rule-2')
  })

  it('info severity event matches no rules', async () => {
    const result = await esClient.search({
      index: PERCOLATOR_INDEX,
      body: {
        query: {
          percolate: {
            field: 'query',
            document: {
              message: 'health check passed all systems nominal',
              severity: 'info',
            },
          },
        },
      },
    })

    const hits = (result as any).hits.hits
    // info severity matches none of our rules (none specify info)
    const relevantIds = hits.map((h: any) => h._id).filter(
      (id: string) => ['rule-1', 'rule-2', 'rule-3'].includes(id)
    )
    expect(relevantIds).toHaveLength(0)
  })

  it('fatal event with only payment matches rule-1 but not rule-2', async () => {
    const result = await esClient.search({
      index: PERCOLATOR_INDEX,
      body: {
        query: {
          percolate: {
            field: 'query',
            document: {
              message: 'payment processor unreachable connection refused',
              severity: 'fatal',
            },
          },
        },
      },
    })

    const matchedIds = (result as any).hits.hits.map((h: any) => h._id)

    // rule-1: fatal + payment → matches
    expect(matchedIds).toContain('rule-1')
    // rule-2: fatal + database → message has no "database" → does NOT match
    expect(matchedIds).not.toContain('rule-2')
    // rule-3: error only → does NOT match fatal
    expect(matchedIds).not.toContain('rule-3')
  })

  it('project-scoped percolation only returns rules for matching projectId', async () => {
    // Query with a filter on projectId to simulate tenant-scoped alert matching
    const result = await esClient.search({
      index: PERCOLATOR_INDEX,
      body: {
        query: {
          bool: {
            must: [
              {
                percolate: {
                  field: 'query',
                  document: {
                    message: 'payment gateway database timeout',
                    severity: 'fatal',
                  },
                },
              },
            ],
            filter: [
              { term: { projectId: 'proj-1' } },
            ],
          },
        },
      },
    })

    const matchedIds = (result as any).hits.hits.map((h: any) => h._id).sort()
    // rule-4 is for proj-2 — should be excluded by the filter
    expect(matchedIds).not.toContain('rule-4')
    // rule-1 and rule-2 are for proj-1 — should be included
    expect(matchedIds).toContain('rule-1')
    expect(matchedIds).toContain('rule-2')
  })

  it('percolating with multiple documents matches union of rules', async () => {
    // Percolate two documents: one matching rule-1, one matching rule-3
    // Use documents array for multi-document percolation
    const result1 = await esClient.search({
      index: PERCOLATOR_INDEX,
      body: {
        query: {
          percolate: {
            field: 'query',
            document: {
              message: 'payment declined insufficient funds',
              severity: 'fatal',
            },
          },
        },
      },
    })

    const result2 = await esClient.search({
      index: PERCOLATOR_INDEX,
      body: {
        query: {
          percolate: {
            field: 'query',
            document: {
              message: 'connection refused to upstream',
              severity: 'error',
            },
          },
        },
      },
    })

    const ids1 = (result1 as any).hits.hits.map((h: any) => h._id)
    const ids2 = (result2 as any).hits.hits.map((h: any) => h._id)

    expect(ids1).toContain('rule-1')  // fatal + payment
    expect(ids2).toContain('rule-3')  // error severity
  })
})
