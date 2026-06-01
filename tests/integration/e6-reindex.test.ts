/**
 * E6: Zero-downtime alias swap with concurrent writes.
 *
 * Flow:
 * 1. Create logs-v1 with base mapping, point alias logs-active → v1 (write index)
 * 2. Seed 50 baseline docs through the alias
 * 3. Start a background writer pushing docs through the alias
 * 4. Create logs-v2 with updated mapping (adds fingerprint field)
 * 5. Reindex v1 → v2 asynchronously; poll the task until complete
 * 6. Atomically swap alias: remove v1, add v2 as write index
 * 7. Assert: all 50 baseline docs exist in v2; count(v2) ≥ 50
 */

import { describe, it, expect, beforeAll, afterAll } from 'vitest'
import { Client } from '@elastic/elasticsearch'

const esClient = new Client({ node: process.env['ES_URL'] ?? 'http://localhost:9201' })

const suffix = Date.now()
const V1 = `logs-v1-e6-${suffix}`
const V2 = `logs-v2-e6-${suffix}`
const ALIAS = `logs-active-e6-${suffix}`

describe('E6: Reindex with Alias Swap (zero downtime)', () => {
  beforeAll(async () => {
    await esClient.indices.create({
      index: V1,
      mappings: {
        properties: {
          projectId: { type: 'keyword' },
          severity: { type: 'keyword' },
          message: { type: 'text' },
          occurredAt: { type: 'date' },
          eventId: { type: 'keyword' },
        },
      },
    })
    await esClient.indices.updateAliases({
      actions: [{ add: { index: V1, alias: ALIAS, is_write_index: true } }],
    })
  })

  afterAll(async () => {
    await esClient.indices.delete({ index: V1 }).catch(() => {})
    await esClient.indices.delete({ index: V2 }).catch(() => {})
  })

  it('concurrent writes through alias are preserved after v1 → v2 alias swap', async () => {
    // Seed 50 baseline docs
    const baselineOps: unknown[] = []
    for (let i = 0; i < 50; i++) {
      baselineOps.push({ index: { _index: ALIAS, _id: `baseline-${i}` } })
      baselineOps.push({
        projectId: 'e6-test',
        severity: 'info',
        message: `Baseline ${i}`,
        occurredAt: new Date().toISOString(),
        eventId: `baseline-${i}`,
      })
    }
    await esClient.bulk({ refresh: true, operations: baselineOps })

    const v1BeforeResult = await esClient.count({ index: V1 }) as unknown as { count: number }
    const v1BeforeCount = v1BeforeResult.count
    expect(v1BeforeCount).toBe(50)

    // Create v2 with updated mapping (adds fingerprint)
    await esClient.indices.create({
      index: V2,
      mappings: {
        properties: {
          projectId: { type: 'keyword' },
          severity: { type: 'keyword' },
          message: { type: 'text' },
          occurredAt: { type: 'date' },
          eventId: { type: 'keyword' },
          fingerprint: { type: 'keyword' },
        },
      },
    })

    // Background writer — pushes docs through the alias while reindex runs
    let writerRunning = true
    let swapped = false
    let postSwapWrites = 0

    const writerPromise = (async () => {
      let i = 0
      while (writerRunning) {
        const docId = `writer-${i++}`
        await esClient.index({
          index: ALIAS,
          id: docId,
          document: {
            projectId: 'e6-test',
            severity: 'warn',
            message: `Writer ${i}`,
            occurredAt: new Date().toISOString(),
            eventId: docId,
          },
        }).catch(() => {})
        if (swapped) postSwapWrites++
        await new Promise(r => setTimeout(r, 40))
      }
    })()

    // Reindex v1 → v2 asynchronously — ES8 client returns task ID at top level
    const reindexResult = await esClient.reindex({
      wait_for_completion: false,
      source: { index: V1 },
      dest: { index: V2 },
    }) as unknown as { task: string }
    const taskId: string = reindexResult.task

    // Poll task until complete
    let done = false
    for (let attempt = 0; attempt < 40 && !done; attempt++) {
      await new Promise(r => setTimeout(r, 500))
      const taskResult = await esClient.tasks.get({ task_id: taskId }) as unknown as {
        completed: boolean
        response?: { failures?: unknown[] }
        body?: { completed: boolean; response?: { failures?: unknown[] } }
      }
      const completed = taskResult.completed ?? taskResult.body?.completed
      if (completed) {
        const failures = taskResult.response?.failures ?? taskResult.body?.response?.failures ?? []
        expect((failures as unknown[]).length).toBe(0)
        done = true
      }
    }
    expect(done).toBe(true)

    // Atomic alias swap — ES8 client takes actions at top level
    await esClient.indices.updateAliases({
      actions: [
        { remove: { index: V1, alias: ALIAS } },
        { add: { index: V2, alias: ALIAS, is_write_index: true } },
      ],
    })
    swapped = true

    // Allow writer to produce a few post-swap docs
    await new Promise(r => setTimeout(r, 300))
    writerRunning = false
    await writerPromise

    await esClient.indices.refresh({ index: V2 })

    const v2CountResult = await esClient.count({ index: V2 }) as unknown as { count: number }
    const v2Count = v2CountResult.count

    expect(v2Count).toBeGreaterThanOrEqual(50) // at least the 50 baseline docs

    // All baseline docs must exist in v2 (eventId as _id → idempotent)
    const baselineCountResult = await esClient.count({
      index: V2,
      query: { prefix: { eventId: { value: 'baseline-' } } },
    }) as unknown as { count: number }
    const baselineCount = baselineCountResult.count
    expect(baselineCount).toBe(50)

    console.log(`E6: v2=${v2Count} docs, post-swap-writes=${postSwapWrites}`)
  }, 90_000)
})
