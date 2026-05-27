/**
 * M1: MongoDB collection-level schema validation
 *
 * Verifies that:
 *  - Required fields (fingerprint, projectId, type, severity, message, occurredAt, ingestedAt) are enforced
 *  - Invalid enum values for `type` and `severity` are rejected
 *  - Valid documents with optional fields (payload, tags, userContext) are accepted
 *  - Minimal valid documents (no optional fields) are accepted
 */

import { describe, it, expect, beforeAll, afterAll } from 'vitest'
import { getMongoDb } from '../helpers/setup.js'

// Document shape used by this test collection — _id is a UUID string, not ObjectId
interface EventDoc {
  _id: string;
  projectId?: string;
  type?: string;
  severity?: string;
  message?: string;
  occurredAt?: Date;
  ingestedAt?: Date;
  fingerprint?: string;
  payload?: Record<string, unknown>;
  tags?: Record<string, unknown>;
  userContext?: Record<string, unknown>;
  stackTrace?: unknown[];
  [key: string]: unknown;
}

// Name for the test-isolated collection
const COL = 'events_m1_validator_test'

const VALIDATOR = {
  $jsonSchema: {
    bsonType: 'object',
    required: [
      '_id',
      'projectId',
      'type',
      'severity',
      'message',
      'occurredAt',
      'ingestedAt',
      'fingerprint',
    ],
    properties: {
      _id:         { bsonType: 'string' },
      projectId:   { bsonType: 'string' },
      type:        { enum: ['error', 'log', 'metric', 'custom'] },
      severity:    { enum: ['debug', 'info', 'warn', 'error', 'fatal'] },
      message:     { bsonType: 'string' },
      occurredAt:  { bsonType: 'date' },
      ingestedAt:  { bsonType: 'date' },
      fingerprint: { bsonType: 'string' },
      payload:     {},  // any type
      tags:        { bsonType: 'object' },
      userContext: { bsonType: 'object' },
      stackTrace:  { bsonType: 'array' },
    },
    additionalProperties: true,
  },
}

beforeAll(async () => {
  const db = getMongoDb()

  // Drop any leftover collection from a previous run
  await db.collection(COL).drop().catch(() => {})

  // Create collection with strict schema validation
  await db.createCollection(COL, {
    validator: VALIDATOR,
    validationLevel: 'strict',
    validationAction: 'error',
  })
})

afterAll(async () => {
  await getMongoDb().collection(COL).drop().catch(() => {})
})

function col() {
  return getMongoDb().collection<EventDoc>(COL)
}

// Helper that builds a minimal valid document
function minimalEvent(overrides: Record<string, unknown> = {}): EventDoc {
  return {
    _id:         crypto.randomUUID(),
    projectId:   'proj-test-1',
    type:        'error',
    severity:    'error',
    message:     'Test error message',
    occurredAt:  new Date(),
    ingestedAt:  new Date(),
    fingerprint: 'fp-abc123',
    ...overrides,
  }
}

describe('M1: MongoDB Schema Validation', () => {
  it('rejects event missing required `fingerprint` field', async () => {
    const doc = minimalEvent()
    delete doc['fingerprint']

    await expect(col().insertOne(doc as any)).rejects.toThrow()
  })

  it('rejects event missing required `projectId` field', async () => {
    const doc = minimalEvent()
    delete doc['projectId']

    await expect(col().insertOne(doc as any)).rejects.toThrow()
  })

  it('rejects event missing required `message` field', async () => {
    const doc = minimalEvent()
    delete doc['message']

    await expect(col().insertOne(doc as any)).rejects.toThrow()
  })

  it('rejects event with invalid `type` enum value', async () => {
    await expect(
      col().insertOne(minimalEvent({ type: 'unknown' }) as any),
    ).rejects.toThrow()
  })

  it('rejects event with invalid `severity` enum value', async () => {
    await expect(
      col().insertOne(minimalEvent({ severity: 'critical' }) as any),
    ).rejects.toThrow()
  })

  it('rejects event missing both `occurredAt` and `ingestedAt`', async () => {
    const doc = minimalEvent()
    delete doc['occurredAt']
    delete doc['ingestedAt']

    await expect(col().insertOne(doc as any)).rejects.toThrow()
  })

  it('accepts a minimal valid event (no optional fields)', async () => {
    const eventId = crypto.randomUUID()
    const result = await col().insertOne(
      minimalEvent({ _id: eventId }),
    )
    expect(result.acknowledged).toBe(true)
    await col().deleteOne({ _id: eventId })
  })

  it('accepts a valid event with arbitrary payload', async () => {
    const eventId = crypto.randomUUID()
    const result = await col().insertOne(
      minimalEvent({
        _id:     eventId,
        payload: { responseTimeMs: 500, nested: { deep: true } },
      }),
    )
    expect(result.acknowledged).toBe(true)
    await col().deleteOne({ _id: eventId })
  })

  it('accepts a valid `log` type event with `info` severity', async () => {
    const eventId = crypto.randomUUID()
    const result = await col().insertOne(
      minimalEvent({ _id: eventId, type: 'log', severity: 'info', message: 'minimal log' }),
    )
    expect(result.acknowledged).toBe(true)
    await col().deleteOne({ _id: eventId })
  })

  it('accepts a valid `metric` event with `debug` severity', async () => {
    const eventId = crypto.randomUUID()
    const result = await col().insertOne(
      minimalEvent({ _id: eventId, type: 'metric', severity: 'debug', message: 'perf metric' }),
    )
    expect(result.acknowledged).toBe(true)
    await col().deleteOne({ _id: eventId })
  })

  it('accepts event with optional userContext and tags', async () => {
    const eventId = crypto.randomUUID()
    const result = await col().insertOne(
      minimalEvent({
        _id:         eventId,
        userContext: { userId: 'u-1', email: 'test@example.com', ip: '127.0.0.1' },
        tags:        { env: 'production', service: 'api' },
      }),
    )
    expect(result.acknowledged).toBe(true)
    await col().deleteOne({ _id: eventId })
  })

  it('accepts event with a stackTrace array', async () => {
    const eventId = crypto.randomUUID()
    const result = await col().insertOne(
      minimalEvent({
        _id: eventId,
        stackTrace: [
          { file: 'app.js', line: 42, column: 8, function: 'handleRequest' },
        ],
      }),
    )
    expect(result.acknowledged).toBe(true)
    await col().deleteOne({ _id: eventId })
  })
})
