/**
 * R3: Redis Streams — Zero-Loss Ingestion
 *
 * Verifies that:
 *  - 1000 produced events are fully consumed with zero loss
 *  - XPENDING detects unacknowledged messages after a simulated crash
 *  - XACK correctly clears the pending entry list
 *  - Consumer group handles large batches reliably
 *  - Stream XLEN matches the number of produced messages
 */

import { Redis } from 'ioredis'
import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'vitest'

const testRedis = new Redis({ host: 'localhost', port: 6380, lazyConnect: true })

// Unique stream and group names per test run to avoid cross-test contamination
const BASE_STREAM = `test-stream-${Date.now()}`
const TEST_GROUP = 'test-ingesters'

describe('R3: Redis Streams Zero-Loss Ingestion', () => {
  beforeAll(async () => {
    await testRedis.connect()
  })

  afterAll(async () => {
    // Clean up all streams created during this test run
    const keys = await testRedis.keys(`test-stream-*`)
    if (keys.length > 0) {
      await testRedis.del(...keys)
    }
    await testRedis.quit()
  })

  it('1000 produced events are fully consumed with zero loss', async () => {
    const streamKey = `${BASE_STREAM}-zero-loss`
    const groupName = TEST_GROUP

    // Create stream and consumer group
    await testRedis.xgroup('CREATE', streamKey, groupName, '$', 'MKSTREAM')

    // Producer: add 1000 messages to the stream
    const eventIds = new Set<string>()
    const pipeline = testRedis.pipeline()
    for (let i = 0; i < 1000; i++) {
      pipeline.xadd(
        streamKey,
        '*',
        'eventId', `event-${i}`,
        'projectId', 'proj-1',
        'data', JSON.stringify({ index: i, message: `log entry ${i}` })
      )
    }
    const addResults = await pipeline.exec()

    // Collect all Redis-assigned stream IDs
    if (addResults) {
      for (const [err] of addResults) {
        if (err) throw err
      }
    }

    // Verify stream length
    const streamLen = await testRedis.xlen(streamKey)
    expect(streamLen).toBe(1000)

    // Populate our expected set
    for (let i = 0; i < 1000; i++) {
      eventIds.add(`event-${i}`)
    }
    expect(eventIds.size).toBe(1000)

    // Consumer: process all messages in batches of 100
    const processed = new Set<string>()
    let attempts = 0
    const maxAttempts = 50  // safety valve: 50 * 100 = 5000 possible reads

    while (processed.size < 1000 && attempts < maxAttempts) {
      const messages = await testRedis.xreadgroup(
        'GROUP', groupName, 'consumer-1',
        'COUNT', '100',
        'STREAMS', streamKey, '>'
      ) as Array<[string, Array<[string, string[]]>]> | null

      if (!messages || messages.length === 0) {
        attempts++
        continue
      }

      for (const [, entries] of messages) {
        const ackIds: string[] = []
        for (const [msgId, fields] of entries) {
          // Fields are stored as alternating key-value array: ['eventId', 'event-0', 'projectId', 'proj-1', ...]
          const fieldMap: Record<string, string> = {}
          for (let j = 0; j < fields.length; j += 2) {
            fieldMap[fields[j]!] = fields[j + 1]!
          }
          const eventId = fieldMap['eventId']
          if (eventId) {
            processed.add(eventId)
          }
          ackIds.push(msgId)
        }
        // Batch acknowledge all processed messages
        if (ackIds.length > 0) {
          await testRedis.xack(streamKey, groupName, ...ackIds)
        }
      }
      attempts++
    }

    expect(processed.size).toBe(1000)

    // Verify the processed set matches the produced set exactly
    for (let i = 0; i < 1000; i++) {
      expect(processed.has(`event-${i}`)).toBe(true)
    }

    // Verify XPENDING shows 0 unacked messages — everything was acknowledged
    const pending = await testRedis.xpending(streamKey, groupName, '-', '+', 10) as unknown[]
    expect(pending.length).toBe(0)
  }, 60_000)

  it('XPENDING detects unacknowledged messages after simulated crash', async () => {
    const streamKey = `${BASE_STREAM}-crash-sim`
    const groupName = `${TEST_GROUP}-crash`

    await testRedis.xgroup('CREATE', streamKey, groupName, '$', 'MKSTREAM')

    // Add a message
    await testRedis.xadd(streamKey, '*', 'eventId', 'crash-event', 'data', '{"severity":"fatal"}')

    // Consume but deliberately do NOT acknowledge — simulates a worker crash
    const msgs = await testRedis.xreadgroup(
      'GROUP', groupName, 'consumer-crash',
      'COUNT', '1',
      'STREAMS', streamKey, '>'
    ) as Array<[string, Array<[string, string[]]>]>

    expect(msgs).not.toBeNull()
    expect(msgs.length).toBeGreaterThan(0)
    const crashMsgId = msgs[0]![1][0]![0]

    // XPENDING should now show 1 unacknowledged message
    const pending = await testRedis.xpending(streamKey, groupName, '-', '+', 10) as unknown[]
    expect(pending.length).toBeGreaterThan(0)

    // The pending entry should be for our consumer
    const pendingEntry = (pending[0] as any)
    // ioredis xpending returns: [id, consumerName, idleMs, deliveryCount]
    expect(pendingEntry[0]).toBe(crashMsgId)
    expect(pendingEntry[1]).toBe('consumer-crash')
    expect(Number(pendingEntry[3])).toBe(1)  // delivered once, never acked

    // Now ack it to clean up
    await testRedis.xack(streamKey, groupName, crashMsgId)

    // XPENDING should now be empty
    const pendingAfterAck = await testRedis.xpending(streamKey, groupName, '-', '+', 10) as unknown[]
    expect(pendingAfterAck.length).toBe(0)
  })

  it('XCLAIM allows a second consumer to take over unacked messages', async () => {
    const streamKey = `${BASE_STREAM}-xclaim`
    const groupName = `${TEST_GROUP}-xclaim`

    await testRedis.xgroup('CREATE', streamKey, groupName, '$', 'MKSTREAM')

    // Add a message and consume without acking
    await testRedis.xadd(streamKey, '*', 'eventId', 'xclaim-event', 'data', '{}')
    const msgs = await testRedis.xreadgroup(
      'GROUP', groupName, 'original-consumer',
      'COUNT', '1',
      'STREAMS', streamKey, '>'
    ) as Array<[string, Array<[string, string[]]>]>

    const msgId = msgs[0]![1][0]![0]

    // Wait a small amount so the message has some idle time
    await new Promise(r => setTimeout(r, 100))

    // Claim the message with a 0ms min-idle-time (unconditional claim)
    const claimed = await testRedis.xclaim(
      streamKey, groupName, 'rescue-consumer', 0, msgId
    ) as unknown[]

    expect(claimed.length).toBeGreaterThan(0)

    // Now XPENDING should show the message under rescue-consumer
    const pending = await testRedis.xpending(streamKey, groupName, '-', '+', 10) as unknown[]
    expect(pending.length).toBeGreaterThan(0)
    const entry = pending[0] as any
    expect(entry[1]).toBe('rescue-consumer')

    // Cleanup
    await testRedis.xack(streamKey, groupName, msgId)
  })

  it('consumer group with MKSTREAM flag creates stream automatically', async () => {
    const newStream = `${BASE_STREAM}-mkstream-auto`
    const newGroup = `${TEST_GROUP}-auto`

    // Stream does not exist yet — MKSTREAM should create it
    await testRedis.xgroup('CREATE', newStream, newGroup, '$', 'MKSTREAM')

    // Verify the stream now exists (XLEN returns 0 for empty stream)
    const len = await testRedis.xlen(newStream)
    expect(len).toBe(0)

    // Cleanup
    await testRedis.del(newStream)
  })

  it('BUSYGROUP error is raised when consumer group already exists', async () => {
    const streamKey = `${BASE_STREAM}-busygroup`
    const groupName = `${TEST_GROUP}-busy`

    await testRedis.xgroup('CREATE', streamKey, groupName, '$', 'MKSTREAM')

    // Creating the same group again should throw BUSYGROUP
    await expect(
      testRedis.xgroup('CREATE', streamKey, groupName, '$', 'MKSTREAM')
    ).rejects.toThrow(/BUSYGROUP/)
  })

  it('messages with multiple field pairs are parsed correctly', async () => {
    const streamKey = `${BASE_STREAM}-multifield`
    const groupName = `${TEST_GROUP}-multifield`

    await testRedis.xgroup('CREATE', streamKey, groupName, '$', 'MKSTREAM')

    const payload = JSON.stringify({ level: 'error', code: 500 })
    await testRedis.xadd(
      streamKey, '*',
      'eventId', 'mf-event-1',
      'projectId', 'proj-mf',
      'severity', 'error',
      'data', payload
    )

    const msgs = await testRedis.xreadgroup(
      'GROUP', groupName, 'consumer-mf',
      'COUNT', '1',
      'STREAMS', streamKey, '>'
    ) as Array<[string, Array<[string, string[]]>]>

    expect(msgs.length).toBeGreaterThan(0)
    const [msgId, fields] = msgs[0]![1][0]!

    // Parse the alternating key-value array
    const fieldMap: Record<string, string> = {}
    for (let j = 0; j < fields.length; j += 2) {
      fieldMap[fields[j]!] = fields[j + 1]!
    }

    expect(fieldMap['eventId']).toBe('mf-event-1')
    expect(fieldMap['projectId']).toBe('proj-mf')
    expect(fieldMap['severity']).toBe('error')
    expect(fieldMap['data']).toBe(payload)

    await testRedis.xack(streamKey, groupName, msgId)
  })
})
