import { describe, it, expect, beforeAll, afterAll } from 'vitest'
import { MongoClient } from 'mongodb'
import { Redis } from 'ioredis'

const mongoClient = new MongoClient(
  process.env['MONGO_URL'] ?? 'mongodb://localhost:27018/pulseboard_test?directConnection=true'
)
const testRedis = new Redis({
  host: process.env['REDIS_HOST'] ?? 'localhost',
  port: Number(process.env['REDIS_PORT'] ?? 6380),
  lazyConnect: true,
})
const PROJECT_ID = `m4-test-proj-${Date.now()}`
const CHANNEL = `alerts:fatal:${PROJECT_ID}`

describe('M4: Change Stream → Redis Pub/Sub', () => {
  beforeAll(async () => {
    await mongoClient.connect()
    await testRedis.connect()
  })

  afterAll(async () => {
    await mongoClient.db('pulseboard_test').collection('events').deleteMany({ projectId: PROJECT_ID })
    await testRedis.quit()
    await mongoClient.close()
  })

  it('fatal event published to Redis within 500ms via change stream subscriber', async () => {
    // Subscriber: listen on the alerts channel
    const subRedis = new Redis({
      host: process.env['REDIS_HOST'] ?? 'localhost',
      port: Number(process.env['REDIS_PORT'] ?? 6380),
    })

    let received: unknown = null
    const receivePromise = new Promise<unknown>((resolve) => {
      subRedis.subscribe(CHANNEL, () => {
        subRedis.on('message', (_ch, msg) => {
          received = JSON.parse(msg)
          resolve(received)
        })
      })
    })

    // Open change stream in the background (simulating the worker)
    const db = mongoClient.db('pulseboard_test')
    const col = db.collection('events')
    const cs = col.watch([
      { $match: { 'fullDocument.severity': 'fatal', operationType: 'insert' } },
    ], { fullDocument: 'updateLookup' })

    // Change stream listener: publish to Redis
    const streamPromise = new Promise<void>((resolve, reject) => {
      cs.on('change', async (change) => {
        try {
          const doc = (change as { fullDocument?: Record<string, unknown> }).fullDocument
          if (!doc) return
          await testRedis.publish(`alerts:fatal:${doc['projectId']}`, JSON.stringify(doc))
          resolve()
        } catch (err) {
          reject(err)
        }
      })
      cs.on('error', reject)
    })

    // Give change stream time to register
    await new Promise(r => setTimeout(r, 200))

    // Insert a fatal event
    const eventId = `m4-fatal-${Date.now()}`
    const insertStart = Date.now()
    await col.insertOne({
      _id: eventId as unknown as import('mongodb').ObjectId,
      projectId: PROJECT_ID,
      type: 'error',
      severity: 'fatal',
      message: 'Fatal: database connection pool exhausted',
      occurredAt: new Date(),
      ingestedAt: new Date(),
      fingerprint: 'fp-m4-fatal',
    })

    // Wait for both: change stream fires + Redis receive
    const timeoutPromise = new Promise<null>((_, reject) =>
      setTimeout(() => reject(new Error('Timeout: no message received in 500ms')), 500)
    )

    try {
      await Promise.race([receivePromise, timeoutPromise])
      const elapsed = Date.now() - insertStart
      console.log(`M4: fatal event published to Redis in ${elapsed}ms`)
      expect(elapsed).toBeLessThan(500)
      expect(received).not.toBeNull()
    } finally {
      await cs.close()
      await subRedis.quit()
    }
  }, 10_000)

  it('non-fatal events do not trigger the change stream filter', async () => {
    const db = mongoClient.db('pulseboard_test')
    const col = db.collection('events')

    let changeCount = 0
    const cs = col.watch([
      { $match: { 'fullDocument.severity': 'fatal', operationType: 'insert' } },
    ], { fullDocument: 'updateLookup' })

    cs.on('change', () => { changeCount++ })

    await new Promise(r => setTimeout(r, 100))

    // Insert non-fatal events
    await col.insertMany([
      { _id: `m4-info-${Date.now()}`, projectId: PROJECT_ID, type: 'log', severity: 'info', message: 'info', occurredAt: new Date(), ingestedAt: new Date(), fingerprint: 'fp-info' },
      { _id: `m4-warn-${Date.now()}`, projectId: PROJECT_ID, type: 'log', severity: 'warn', message: 'warn', occurredAt: new Date(), ingestedAt: new Date(), fingerprint: 'fp-warn' },
    ] as unknown as Document[])

    await new Promise(r => setTimeout(r, 300))
    await cs.close()

    expect(changeCount).toBe(0)
  }, 5_000)
})
