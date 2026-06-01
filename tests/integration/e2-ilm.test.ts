/**
 * E2: ILM Lifecycle Policies
 *
 * With the dynamic-per-retentionDays implementation, policies are named
 * `logs-retention-<N>d` and created on demand for the exact retention value.
 * `ensureIlmPolicies()` pre-creates the three common values (30, 90, 365).
 */
import { describe, it, expect, beforeAll } from 'vitest'
import { Client } from '@elastic/elasticsearch'
import {
  ensureIlmPolicies,
  resolveTierPolicy,
  applyPolicyForProject,
  setupTestDatabases,
} from '../helpers/setup.js'

const esClient = new Client({ node: process.env['ES_URL'] ?? 'http://localhost:9201' })

describe('E2: ILM Tier Policies', () => {
  beforeAll(async () => {
    await setupTestDatabases()
    await ensureIlmPolicies()
  })

  it('logs-retention-30d ILM policy exists with correct phases', async () => {
    const result = await esClient.ilm.getLifecycle() as unknown as Record<string, unknown>
    expect(result).toHaveProperty('logs-retention-30d')
    const policy = result['logs-retention-30d'] as Record<string, unknown>
    const phases = (policy['policy'] as Record<string, unknown>)?.['phases'] as Record<string, unknown>

    // Hot phase: rollover at 7d and 5gb
    expect(phases).toHaveProperty('hot')
    const hotActions = (phases['hot'] as Record<string, unknown>)?.['actions'] as Record<string, unknown>
    expect(hotActions).toHaveProperty('rollover')
    const rollover = hotActions['rollover'] as Record<string, unknown>
    expect(rollover['max_age']).toBe('7d')
    expect(rollover['max_primary_shard_size']).toBe('5gb')

    // Warm phase: forcemerge 1 segment + shrink to 1 shard
    expect(phases).toHaveProperty('warm')
    const warmActions = (phases['warm'] as Record<string, unknown>)?.['actions'] as Record<string, unknown>
    expect(warmActions).toHaveProperty('forcemerge')
    expect((warmActions['forcemerge'] as Record<string, unknown>)?.['max_num_segments']).toBe(1)
    expect(warmActions).toHaveProperty('shrink')
    expect((warmActions['shrink'] as Record<string, unknown>)?.['number_of_shards']).toBe(1)

    // Cold phase: exists, does NOT contain a freeze action (deprecated in ES 8.x)
    expect(phases).toHaveProperty('cold')
    const coldActions = (phases['cold'] as Record<string, unknown>)?.['actions'] as Record<string, unknown> | undefined
    expect(coldActions).not.toHaveProperty('freeze')

    // Delete phase exists and uses the exact retention value (30d).
    expect(phases).toHaveProperty('delete')
    const deleteActions = phases['delete'] as Record<string, unknown>
    expect(deleteActions['min_age']).toBe('30d')
  })

  it('logs-retention-90d ILM policy exists with 90d delete phase', async () => {
    const result = await esClient.ilm.getLifecycle() as unknown as Record<string, unknown>
    expect(result).toHaveProperty('logs-retention-90d')
    const phases = ((result['logs-retention-90d'] as Record<string, unknown>)?.['policy'] as Record<string, unknown>)?.['phases'] as Record<string, unknown>
    expect((phases['delete'] as Record<string, unknown>)?.['min_age']).toBe('90d')
  })

  it('logs-retention-365d ILM policy exists with 365d delete phase', async () => {
    const result = await esClient.ilm.getLifecycle() as unknown as Record<string, unknown>
    expect(result).toHaveProperty('logs-retention-365d')
    const phases = ((result['logs-retention-365d'] as Record<string, unknown>)?.['policy'] as Record<string, unknown>)?.['phases'] as Record<string, unknown>
    expect((phases['delete'] as Record<string, unknown>)?.['min_age']).toBe('365d')
  })

  it('resolveTierPolicy returns the exact per-retentionDays policy name', () => {
    // Policy name matches the exact retention value — no bucketing.
    expect(resolveTierPolicy(30)).toBe('logs-retention-30d')
    expect(resolveTierPolicy(45)).toBe('logs-retention-45d')   // was incorrectly rounded to 90d before
    expect(resolveTierPolicy(90)).toBe('logs-retention-90d')
    expect(resolveTierPolicy(365)).toBe('logs-retention-365d')
    expect(resolveTierPolicy(7)).toBe('logs-retention-7d')
  })

  it('applyPolicyForProject creates a per-retentionDays policy and index alias', async () => {
    const projectId = `e2-alias-${Date.now()}`
    // Use a non-standard retention value (45d) to prove dynamic policy creation.
    await applyPolicyForProject(projectId, 45)

    // The per-project policy must have been created.
    const policies = await esClient.ilm.getLifecycle() as unknown as Record<string, unknown>
    expect(policies).toHaveProperty('logs-retention-45d')

    // The alias must point to the newly created index.
    const aliasResult = await esClient.indices.getAlias({ name: `logs-${projectId}-active` }) as unknown as Record<string, unknown>
    expect(Object.keys(aliasResult).length).toBeGreaterThan(0)

    await esClient.indices.delete({ index: `logs-${projectId}-*` }).catch(() => {})
  }, 20_000)
})
