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

  it('3 subscribers all receive the message but exactly 1 fires via SET NX lock', async () => {
    const publisher = makeClient()
    const sub1 = makeClient()
    const sub2 = makeClient()
    const sub3 = makeClient()
    const lockClient = makeClient()

    const alertRuleId = `rule-${Date.now()}`
    const eventId = `event-${Date.now()}`
    const lockKey = `fire-lock:${alertRuleId}:${eventId}`
    const channel = `alerts:fatal:proj-test-${Date.now()}`

    let firedCount = 0
    const receivedBy: string[] = []

    async function handleAlert(message: string, name: string) {
      receivedBy.push(name)
      // Race to acquire the dedup lock
      const won = await lockClient.set(lockKey, name, 'EX', 60, 'NX')
      if (won === 'OK') firedCount++
    }

    // Subscribe all 3
    await Promise.all([sub1, sub2, sub3].map(s => s.subscribe(channel)))

    sub1.on('message', (_, msg) => handleAlert(msg, 'sub1'))
    sub2.on('message', (_, msg) => handleAlert(msg, 'sub2'))
    sub3.on('message', (_, msg) => handleAlert(msg, 'sub3'))

    // Wait for subscriptions to register
    await new Promise(r => setTimeout(r, 150))

    // Publish the alert
    await publisher.publish(channel, JSON.stringify({ alertRuleId, eventId }))

    // Wait for processing
    await new Promise(r => setTimeout(r, 500))

    expect(receivedBy.length).toBe(3)  // all 3 received it
    expect(firedCount).toBe(1)         // exactly 1 fired
  }, 10_000)

  it('second message with different eventId fires again (lock is per-event)', async () => {
    const publisher = makeClient()
    const sub = makeClient()
    const lockClient = makeClient()

    const alertRuleId = `rule-${Date.now()}`
    const channel = `alerts:fatal:proj-dedup-${Date.now()}`
    let firedCount = 0

    await sub.subscribe(channel)
    sub.on('message', async (_, msg) => {
      const { eventId } = JSON.parse(msg) as { eventId: string }
      const lockKey = `fire-lock:${alertRuleId}:${eventId}`
      const won = await lockClient.set(lockKey, 'sub', 'EX', 60, 'NX')
      if (won === 'OK') firedCount++
    })

    await new Promise(r => setTimeout(r, 100))

    // Two different events — should both fire
    await publisher.publish(channel, JSON.stringify({ alertRuleId, eventId: 'event-A' }))
    await publisher.publish(channel, JSON.stringify({ alertRuleId, eventId: 'event-B' }))

    await new Promise(r => setTimeout(r, 400))
    expect(firedCount).toBe(2)
  }, 10_000)
})
