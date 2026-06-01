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
    expect(phases).toHaveProperty('hot')
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
