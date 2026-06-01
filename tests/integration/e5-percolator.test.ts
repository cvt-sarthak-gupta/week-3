/**
 * E5: Percolator Alert Matching
 *
 * Uses the PRODUCTION percolator index ('alert_percolator') so we validate
 * that ensurePercolatorIndex() creates it with the correct mapping.
 *
 * Verifies that:
 *  - The production percolator index exists with correct mapping
 *  - Alert rules stored as percolator documents match incoming events
 *  - A fatal event with 'payment' and 'database' matches exactly 2 of 3 rules
 *  - A non-matching event returns zero rule hits
 *  - projectId-scoped rules only fire for the correct project
 */

import { Client } from '@elastic/elasticsearch'
import { describe, it, expect, beforeAll, afterAll } from 'vitest'

const esClient = new Client({ node: process.env['ES_URL'] ?? 'http://localhost:9201' })

// Use the production percolator index name so this test validates the real
// ensurePercolatorIndex() output rather than a test-only clone.
const PERCOLATOR_INDEX = 'alert_percolator'

// IDs we index during this test — cleaned up in afterAll.
const TEST_RULE_IDS = ['e5-rule-1', 'e5-rule-2', 'e5-rule-3', 'e5-rule-4']

describe('E5: Percolator Alert Matching', () => {
  beforeAll(async () => {
    // The production setup call creates this index if it doesn't exist.
    // Seed our test rules; use unique IDs to avoid colliding with other tests.
    await esClient.bulk({
      refresh: true,
      operations: [
        { index: { _index: PERCOLATOR_INDEX, _id: 'e5-rule-1' } },
        {
          alertRuleId: 'e5-rule-1',
          projectId: 'e5-proj-1',
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
        { index: { _index: PERCOLATOR_INDEX, _id: 'e5-rule-2' } },
        {
          alertRuleId: 'e5-rule-2',
          projectId: 'e5-proj-1',
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
        { index: { _index: PERCOLATOR_INDEX, _id: 'e5-rule-3' } },
        {
          alertRuleId: 'e5-rule-3',
          projectId: 'e5-proj-1',
          name: 'Error Severity Only',
          // This rule fires for severity=error — a fatal event must NOT match.
          query: {
            bool: { must: [{ term: { severity: 'error' } }] },
          },
        },
        // Rule scoped to a different project to test tenant isolation.
        { index: { _index: PERCOLATOR_INDEX, _id: 'e5-rule-4' } },
        {
          alertRuleId: 'e5-rule-4',
          projectId: 'e5-proj-2',
          name: 'Proj2 Fatal Rule',
          query: {
            bool: { must: [{ term: { severity: 'fatal' } }] },
          },
        },
      ],
    })
  })

  afterAll(async () => {
    // Clean up only the documents we inserted so the production index is not destroyed.
    const deleteOps = TEST_RULE_IDS.flatMap((id) => [
      { delete: { _index: PERCOLATOR_INDEX, _id: id } },
    ])
    await esClient.bulk({ operations: deleteOps }).catch(() => {})
  })

  it('production percolator index exists with correct mapping', async () => {
    const mapping = await esClient.indices.getMapping({ index: PERCOLATOR_INDEX })
    const props = (mapping as Record<string, { mappings: { properties: Record<string, { type: string }> } }>)[PERCOLATOR_INDEX]?.mappings?.properties
    expect(props?.['query']?.type).toBe('percolator')
    expect(props?.['severity']?.type).toBe('keyword')
    expect(props?.['message']?.type).toBe('text')
    expect(props?.['tags']?.type).toBe('nested')
  })

  it('matches exactly 2 of 3 project-1 rules for a fatal payment+database event', async () => {
    const result = await esClient.search({
      index: PERCOLATOR_INDEX,
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
            { term: { projectId: 'e5-proj-1' } },
          ],
        },
      },
    })

    const hits = result.hits.hits
    const matchedIds = hits
      .map((h) => h._id)
      .filter((id): id is string => typeof id === 'string' && TEST_RULE_IDS.includes(id))
      .sort()

    expect(matchedIds).toHaveLength(2)
    expect(matchedIds).toContain('e5-rule-1')  // fatal + payment ✓
    expect(matchedIds).toContain('e5-rule-2')  // fatal + database ✓
    expect(matchedIds).not.toContain('e5-rule-3') // error only ✗
    expect(matchedIds).not.toContain('e5-rule-4') // proj-2 ✗
  })

  it('fatal event without payment or database keyword matches no specific-keyword rules', async () => {
    const result = await esClient.search({
      index: PERCOLATOR_INDEX,
      query: {
        percolate: {
          field: 'query',
          document: {
            message: 'user authentication session expired',
            severity: 'fatal',
          },
        },
      },
    })

    const ids = (id: string | undefined): id is string => typeof id === 'string'
    const matchedIds = result.hits.hits.map((h) => h._id).filter(ids)
    expect(matchedIds).not.toContain('e5-rule-1')
    expect(matchedIds).not.toContain('e5-rule-2')
    expect(matchedIds).not.toContain('e5-rule-3')
  })

  it('error severity event matches rule-3 but not rule-1 or rule-2', async () => {
    const result = await esClient.search({
      index: PERCOLATOR_INDEX,
      query: {
        percolate: {
          field: 'query',
          document: {
            message: 'something went wrong in the application',
            severity: 'error',
          },
        },
      },
    })

    const strIds = result.hits.hits.map((h) => h._id).filter((id): id is string => typeof id === 'string')
    expect(strIds).toContain('e5-rule-3')
    expect(strIds).not.toContain('e5-rule-1')
    expect(strIds).not.toContain('e5-rule-2')
  })

  it('info severity event matches no test rules', async () => {
    const result = await esClient.search({
      index: PERCOLATOR_INDEX,
      query: {
        percolate: {
          field: 'query',
          document: {
            message: 'health check passed all systems nominal',
            severity: 'info',
          },
        },
      },
    })

    const relevantMatches = result.hits.hits
      .map((h) => h._id)
      .filter((id): id is string => typeof id === 'string' && TEST_RULE_IDS.includes(id))

    expect(relevantMatches).toHaveLength(0)
  })

  it('fatal event with only payment matches rule-1 but not rule-2', async () => {
    const result = await esClient.search({
      index: PERCOLATOR_INDEX,
      query: {
        percolate: {
          field: 'query',
          document: {
            message: 'payment processor unreachable connection refused',
            severity: 'fatal',
          },
        },
      },
    })

    const strIds = result.hits.hits.map((h) => h._id).filter((id): id is string => typeof id === 'string')
    expect(strIds).toContain('e5-rule-1')      // fatal + payment → ✓
    expect(strIds).not.toContain('e5-rule-2')  // fatal + database → ✗ (no "database")
    expect(strIds).not.toContain('e5-rule-3')  // error only → ✗
  })

  it('project-scoped percolation excludes rules from other projects', async () => {
    const result = await esClient.search({
      index: PERCOLATOR_INDEX,
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
          filter: [{ term: { projectId: 'e5-proj-1' } }],
        },
      },
    })

    const matchedIds = result.hits.hits
      .map((h) => h._id)
      .filter((id): id is string => typeof id === 'string' && TEST_RULE_IDS.includes(id))
    expect(matchedIds).not.toContain('e5-rule-4')  // proj-2 rule excluded by filter
    expect(matchedIds).toContain('e5-rule-1')
    expect(matchedIds).toContain('e5-rule-2')
  })
})
