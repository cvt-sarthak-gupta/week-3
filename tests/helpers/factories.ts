/**
 * tests/helpers/factories.ts
 * Factory functions for test data.
 * Each returns a minimal valid object for that type with sensible defaults.
 * Pass `overrides` to customise individual fields.
 */

import { randomUUID } from 'node:crypto';
import type { EventIngest } from '../../src/schemas/event.js';
import type { AlertRule } from '../../src/schemas/alert.js';

// ---------------------------------------------------------------------------
// Word lists (small, fast)
// ---------------------------------------------------------------------------

const ADJECTIVES = ['rapid', 'bright', 'smart', 'swift', 'calm', 'keen', 'bold'];
const NOUNS      = ['falcon', 'hawk', 'node', 'core', 'grid', 'hub', 'beam'];
const VERBS      = ['track', 'watch', 'monitor', 'log', 'audit', 'stream'];
const OBJECTS    = ['payments', 'events', 'users', 'sessions', 'orders', 'logs'];
const DOMAINS    = ['example.com', 'test.dev', 'acme.io', 'corp.net'];

function pick<T>(arr: T[]): T {
  return arr[Math.floor(Math.random() * arr.length)]!;
}

function randInt(min: number, max: number): number {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

let _counter = 0;
function nextId(): number {
  return ++_counter;
}

// ---------------------------------------------------------------------------
// makeEvent — minimal valid EventIngest
// ---------------------------------------------------------------------------

export function makeEvent(overrides?: Partial<EventIngest>): EventIngest {
  return {
    type:     'log',
    severity: 'info',
    message:  `Test log message #${nextId()}`,
    ...overrides,
  };
}

// ---------------------------------------------------------------------------
// makeTenant — minimal valid tenant body for API / DB insertion
// ---------------------------------------------------------------------------

export function makeTenant(): { name: string; slug: string } {
  const id = nextId();
  return {
    name: `${pick(ADJECTIVES)} ${pick(NOUNS)} Co`,
    slug: `${pick(ADJECTIVES)}-${pick(NOUNS)}-${id}`.toLowerCase(),
  };
}

// ---------------------------------------------------------------------------
// makeUser — minimal valid user body
// ---------------------------------------------------------------------------

export function makeUser(): { email: string; password: string; fullName: string } {
  const id = nextId();
  return {
    email:    `testuser-${id}@${pick(DOMAINS)}`,
    password: `P@ssword${id}!Secure`,
    fullName: `Test User ${id}`,
  };
}

// ---------------------------------------------------------------------------
// makeProject — minimal valid project body for a given tenant
// ---------------------------------------------------------------------------

export function makeProject(tenantId: string): { name: string; slug: string; tenantId: string } {
  const id = nextId();
  return {
    tenantId,
    name: `${pick(VERBS)}-${pick(OBJECTS)}-${id}`,
    slug: `${pick(VERBS)}-${pick(OBJECTS)}-${id}`.toLowerCase(),
  };
}

// ---------------------------------------------------------------------------
// makeAlertRule — minimal valid AlertRule for a given project
// ---------------------------------------------------------------------------

export function makeAlertRule(projectId: string): AlertRule & { projectId: string } {
  const id = nextId();
  return {
    projectId,
    name:                `Test Alert Rule ${id}`,
    conditionType:       'threshold',
    threshold:           10,
    windowSeconds:       300,
    notificationChannel: `https://hooks.example.com/alert-${id}`,
    isEnabled:           true,
    esQuery:             { bool: { filter: [{ term: { severity: 'error' } }] } },
  };
}

// ---------------------------------------------------------------------------
// makeStackFrame — a realistic stack frame for error events
// ---------------------------------------------------------------------------

export function makeStackFrame(overrides?: Partial<EventIngest['stackTrace'] extends Array<infer F> | undefined ? NonNullable<EventIngest['stackTrace']>[number] : never>): NonNullable<EventIngest['stackTrace']>[number] {
  return {
    filename: `src/services/order-service.ts`,
    function: `processOrder`,
    line:     randInt(10, 500),
    column:   randInt(1, 80),
    context:  `throw new Error('Payment declined');`,
    ...overrides,
  };
}

// ---------------------------------------------------------------------------
// makeErrorEvent — a fuller error EventIngest with stack trace
// ---------------------------------------------------------------------------

export function makeErrorEvent(overrides?: Partial<EventIngest>): EventIngest {
  const id = nextId();
  return {
    type:       'error',
    severity:   'error',
    message:    `Unhandled exception: Cannot read property 'id' of undefined (test #${id})`,
    stackTrace: [
      makeStackFrame({ filename: 'src/routes/orders.ts', function: 'createOrder', line: 42 }),
      makeStackFrame({ filename: 'src/lib/middleware.ts', function: 'validateRequest', line: 18 }),
    ],
    tags:        { env: 'staging', service: 'order-service' },
    userContext: {
      userId: randomUUID(),
      email:  `user-${id}@${pick(DOMAINS)}`,
      ip:     `10.0.${randInt(0, 255)}.${randInt(1, 254)}`,
    },
    occurredAt: new Date().toISOString(),
    ...overrides,
  };
}

// ---------------------------------------------------------------------------
// makeMetricEvent — a metric EventIngest with payload
// ---------------------------------------------------------------------------

export function makeMetricEvent(overrides?: Partial<EventIngest>): EventIngest {
  const id = nextId();
  return {
    type:     'metric',
    severity: 'info',
    message:  `Response time metric #${id}`,
    payload:  {
      responseTimeMs: randInt(50, 2000),
      statusCode:     200,
      endpoint:       `/api/v1/orders/${id}`,
    },
    tags: { env: 'production', service: 'api-gateway' },
    occurredAt: new Date().toISOString(),
    ...overrides,
  };
}
