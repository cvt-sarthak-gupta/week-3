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

  it('logs-tier-30d ILM policy exists with correct phases', async () => {
    const result = await esClient.ilm.getLifecycle() as unknown as Record<string, unknown>
    expect(result).toHaveProperty('logs-tier-30d')
    const policy = (result['logs-tier-30d'] as Record<string, unknown>)
    const phases = ((policy['policy'] as Record<string, unknown>)?.['phases'] as Record<string, unknown>)

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

    // Delete phase: exists
    expect(phases).toHaveProperty('delete')
  })

  it('logs-tier-90d ILM policy exists', async () => {
    const result = await esClient.ilm.getLifecycle() as unknown as Record<string, unknown>
    expect(result).toHaveProperty('logs-tier-90d')
  })

  it('logs-tier-365d ILM policy exists', async () => {
    const result = await esClient.ilm.getLifecycle() as unknown as Record<string, unknown>
    expect(result).toHaveProperty('logs-tier-365d')
  })

  it('project requesting ≤30d retention maps to logs-tier-30d', () => {
    expect(resolveTierPolicy(30)).toBe('logs-tier-30d')
    expect(resolveTierPolicy(1)).toBe('logs-tier-30d')
    expect(resolveTierPolicy(29)).toBe('logs-tier-30d')
  })

  it('project requesting 31–90d retention maps to logs-tier-90d', () => {
    expect(resolveTierPolicy(31)).toBe('logs-tier-90d')
    expect(resolveTierPolicy(60)).toBe('logs-tier-90d')
    expect(resolveTierPolicy(90)).toBe('logs-tier-90d')
  })

  it('project requesting >90d retention maps to logs-tier-365d', () => {
    expect(resolveTierPolicy(91)).toBe('logs-tier-365d')
    expect(resolveTierPolicy(180)).toBe('logs-tier-365d')
    expect(resolveTierPolicy(365)).toBe('logs-tier-365d')
  })

  it('applyPolicyForProject creates index and alias for the project', async () => {
    const projectId = `e2-alias-${Date.now()}`
    await applyPolicyForProject(projectId, 90)

    const aliasResult = await esClient.indices.getAlias({ name: `logs-${projectId}-active` }) as unknown as Record<string, unknown>
    expect(Object.keys(aliasResult).length).toBeGreaterThan(0)

    await esClient.indices.delete({ index: `logs-${projectId}-*` }).catch(() => {})
  }, 20_000)
})
