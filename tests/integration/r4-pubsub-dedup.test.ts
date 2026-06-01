import { describe, it, expect, afterEach } from 'vitest'
import { Redis } from 'ioredis'

const REDIS_OPTS = {
  host: process.env['REDIS_HOST'] ?? 'localhost',
  port: Number(process.env['REDIS_PORT'] ?? 6380),
}

describe('R4: Pub/Sub Alert Deduplication', () => {
  const clients: Redis[] = []
  afterEach(async () => {
    await Promise.all(clients.map(c => c.quit().catch(() => {})))
    clients.length = 0
  })

  function makeClient() {
    const c = new Redis(REDIS_OPTS)
    clients.push(c)
    return c
  }

  it('3 subscribers (psubscribe pattern) all receive the message but exactly 1 fires via SET NX lock', async () => {
    const publisher = makeClient()
    const sub1 = makeClient()
    const sub2 = makeClient()
    const sub3 = makeClient()
    const lockClient = makeClient()

    const alertRuleId = `rule-${Date.now()}`
    const eventId = `event-${Date.now()}`
    // Lock key is per-alertRuleId only (60s cooldown), matching production AlertService.fireDedupAlert
    const lockKey = `fire-lock:${alertRuleId}`
    const projectId = `proj-test-${Date.now()}`
    const channel = `alerts:fatal:${projectId}`

    let firedCount = 0
    const receivedBy: string[] = []

    async function handleAlert(message: string, name: string) {
      receivedBy.push(name)
      // Race to acquire the dedup lock (per-rule, 60s TTL — matches production)
      const won = await lockClient.set(lockKey, name, 'EX', 60, 'NX')
      if (won === 'OK') firedCount++
    }

    // Use psubscribe (pattern subscribe) — matches production AlertSubscriber which uses
    // psubscribe('alerts:fatal:*'). pmessage event receives (pattern, channel, message).
    await Promise.all([sub1, sub2, sub3].map(s => s.psubscribe('alerts:fatal:*')))

    sub1.on('pmessage', (_, _ch, msg) => handleAlert(msg, 'sub1'))
    sub2.on('pmessage', (_, _ch, msg) => handleAlert(msg, 'sub2'))
    sub3.on('pmessage', (_, _ch, msg) => handleAlert(msg, 'sub3'))

    // Wait for subscriptions to register
    await new Promise(r => setTimeout(r, 150))

    // Publish the alert
    await publisher.publish(channel, JSON.stringify({ alertRuleId, eventId }))

    // Wait for processing
    await new Promise(r => setTimeout(r, 500))

    expect(receivedBy.length).toBe(3)  // all 3 received it
    expect(firedCount).toBe(1)         // exactly 1 fired (per-rule dedup)
  }, 10_000)

  it('second event for same rule within TTL does NOT fire again (per-rule cooldown)', async () => {
    const publisher = makeClient()
    const sub = makeClient()
    const lockClient = makeClient()

    const alertRuleId = `rule-cooldown-${Date.now()}`
    const channel = `alerts:fatal:proj-cooldown-${Date.now()}`
    let firedCount = 0

    await sub.psubscribe('alerts:fatal:*')
    sub.on('pmessage', async (_, _ch, msg) => {
      const { alertRuleId: ruleId } = JSON.parse(msg) as { alertRuleId: string }
      // Per-rule lock — same key for both events
      const lockKey = `fire-lock:${ruleId}`
      const won = await lockClient.set(lockKey, 'sub', 'EX', 60, 'NX')
      if (won === 'OK') firedCount++
    })

    await new Promise(r => setTimeout(r, 100))

    // Two different events matching the same rule — only FIRST should fire (lock held for 60s)
    await publisher.publish(channel, JSON.stringify({ alertRuleId, eventId: 'event-A' }))
    await publisher.publish(channel, JSON.stringify({ alertRuleId, eventId: 'event-B' }))

    await new Promise(r => setTimeout(r, 400))
    // Only 1 fires — the per-rule lock prevents the second burst within the TTL window
    expect(firedCount).toBe(1)
  }, 10_000)

  it('different alert rules each fire independently (locks are per-rule)', async () => {
    const publisher = makeClient()
    const sub = makeClient()
    const lockClient = makeClient()

    const ruleA = `rule-a-${Date.now()}`
    const ruleB = `rule-b-${Date.now()}`
    const channel = `alerts:fatal:proj-multi-${Date.now()}`
    let firedCount = 0

    await sub.psubscribe('alerts:fatal:*')
    sub.on('pmessage', async (_, _ch, msg) => {
      const { alertRuleId } = JSON.parse(msg) as { alertRuleId: string }
      const lockKey = `fire-lock:${alertRuleId}`
      const won = await lockClient.set(lockKey, 'sub', 'EX', 60, 'NX')
      if (won === 'OK') firedCount++
    })

    await new Promise(r => setTimeout(r, 100))

    // Two different rules — each has its own lock key, both should fire
    await publisher.publish(channel, JSON.stringify({ alertRuleId: ruleA, eventId: 'event-A' }))
    await publisher.publish(channel, JSON.stringify({ alertRuleId: ruleB, eventId: 'event-B' }))

    await new Promise(r => setTimeout(r, 400))
    expect(firedCount).toBe(2)
  }, 10_000)
})
