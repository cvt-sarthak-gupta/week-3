/**
 * k6 load test — Ingest endpoint
 *
 * Target: 10,000 events/minute (≈167 RPS single-event, or fewer RPS with BATCH_SIZE>1).
 * p95 latency budget: <100ms.
 *
 * Key env vars:
 *   API_URL    Base URL (default: http://localhost:3000)
 *   API_KEY    PulseBoard API key
 *   BATCH_SIZE Events per request; 1=single, 5-10=batch mode (default: 1)
 *   EXTENDED   Set to "1" for a 15-minute soak run (default: 2-minute hold)
 *
 * Run (basic):
 *   k6 run tests/load/ingest.k6.js
 *
 * Run (batch mode — 5 events/request = ~33 RPS for 10k events/min):
 *   k6 run --env BATCH_SIZE=5 tests/load/ingest.k6.js
 *
 * Run (extended soak):
 *   k6 run --env EXTENDED=1 tests/load/ingest.k6.js
 */

import http from 'k6/http'
import { check } from 'k6'
import { Counter, Rate, Trend, Gauge } from 'k6/metrics'

const apiUrl = __ENV.API_URL || 'http://localhost:3000'
const apiKey  = __ENV.API_KEY  || 'a1b2c3d4-e5f6-7890-abcd-ef1234567890'

const batchSize = parseInt(__ENV.BATCH_SIZE || '1', 10)
const extended  = __ENV.EXTENDED === '1'

// If batch mode, target RPS is 10000/min ÷ batchSize. Single-event = 167 RPS.
const targetRps = Math.ceil(10000 / 60 / batchSize)  // events-per-minute → RPS

// Hold duration: 2 min standard, 15 min extended
const holdDuration = extended ? '15m' : '2m'

// ---------------------------------------------------------------------------
// Custom metrics
// ---------------------------------------------------------------------------
const ingestErrors       = new Counter('ingest_errors')
const ingestOkRate       = new Rate('ingest_ok_rate')
const ingestDuration     = new Trend('ingest_duration_ms', true)
const eventsAccepted     = new Counter('events_accepted')
const droppedIter        = new Counter('ingest_dropped_iter')
const steadyStateRps     = new Gauge('steady_state_rps_snapshot')

// ---------------------------------------------------------------------------
// Scenario configuration
// ---------------------------------------------------------------------------
export const options = {
  scenarios: {
    ramp_and_hold: {
      executor: 'ramping-arrival-rate',
      startRate: 10,
      timeUnit: '1s',
      // Pre-allocate more VUs than strictly needed to prevent dropped iterations.
      // At p95=6ms, 167 RPS needs ~167*0.006=1 concurrent VU. 200 pre-allocated
      // gives large headroom for spikes without relying on dynamic VU creation.
      preAllocatedVUs: 200,
      maxVUs: 500,
      stages: [
        { duration: '30s',  target: targetRps },  // ramp to target
        { duration: holdDuration, target: targetRps },  // hold
        { duration: '15s',  target: 0 },          // ramp down
      ],
    },
  },
  thresholds: {
    ingest_duration_ms:   ['p(95)<100'],
    ingest_ok_rate:       ['rate>0.99'],
    http_req_duration:    ['p(95)<100'],
    http_req_failed:      ['rate<0.01'],
  },
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------
const severities = ['error', 'fatal', 'warn', 'info']
const browsers   = ['chrome', 'firefox', 'safari', 'edge']
const services   = ['api-gateway', 'auth-service', 'payment-service', 'user-service']

function randomItem(arr) {
  return arr[Math.floor(Math.random() * arr.length)]
}

function buildEvent() {
  const severity = randomItem(severities)
  const browser  = randomItem(browsers)
  const service  = randomItem(services)
  return {
    type:        'error',
    severity,
    message:     `k6 load test event [${severity}] from ${browser} on ${service}`,
    fingerprint: `fp-k6-${severity}-${service}`,
    occurredAt:  new Date().toISOString(),
    userContext:  { userId: `user-${Math.floor(Math.random() * 1000)}` },
    deviceContext: { browser },
    payload: {
      eventId:        `k6-${Date.now()}-${Math.random().toString(36).slice(2, 10)}`,
      responseTimeMs: Math.floor(Math.random() * 500),
    },
    tags: { env: 'load-test', service },
  }
}

// ---------------------------------------------------------------------------
// Test function
// ---------------------------------------------------------------------------
let _iterStart = 0

export function setup() {
  _iterStart = Date.now()
  return { startMs: Date.now() }
}

export default function () {
  const body = batchSize === 1 ? buildEvent() : Array.from({ length: batchSize }, buildEvent)
  const payload = JSON.stringify(body)

  const params = {
    headers: {
      'Content-Type': 'application/json',
      'X-PulseBoard-Key': apiKey,
    },
    timeout: '5s',
  }

  const start  = Date.now()
  const res    = http.post(`${apiUrl}/v1/ingest`, payload, params)
  const elapsed = Date.now() - start

  ingestDuration.add(elapsed)

  let accepted = 0
  let ok = false

  if (batchSize === 1) {
    ok = check(res, {
      'status is 202': (r) => r.status === 202,
      'has eventId':   (r) => {
        try { return JSON.parse(r.body).eventId !== undefined } catch { return false }
      },
    })
    if (ok) accepted = 1
  } else {
    ok = check(res, {
      'status is 202':    (r) => r.status === 202,
      'accepted matches': (r) => {
        try { return JSON.parse(r.body).accepted === batchSize } catch { return false }
      },
    })
    if (ok) accepted = batchSize
  }

  ingestOkRate.add(ok ? 1 : 0)
  eventsAccepted.add(accepted)

  if (!ok) {
    ingestErrors.add(1)
    console.error(`Ingest failed: status=${res.status} body=${res.body?.slice(0, 200)}`)
  }
}

// ---------------------------------------------------------------------------
// Summary
// ---------------------------------------------------------------------------
export function handleSummary(data) {
  const dur    = (data.state?.testRunDurationMs ?? 0) / 1000          // seconds
  const rps    = data.metrics['http_reqs']?.values?.rate ?? 0
  const total  = data.metrics['http_reqs']?.values?.count ?? 0
  const p95    = data.metrics['ingest_duration_ms']?.values?.['p(95)'] ?? 0
  const okRate = (data.metrics['ingest_ok_rate']?.values?.rate ?? 0) * 100
  const dropped = data.metrics['dropped_iterations']?.values?.count ?? 0
  const accepted = data.metrics['events_accepted']?.values?.count ?? 0

  // Estimate hold-phase RPS: subtract ramp-up and ramp-down expected counts
  // Ramp-up   (30s): average = (10 + targetRps) / 2
  // Ramp-down (15s): average = targetRps / 2
  const rampUpExpected   = ((10 + targetRps) / 2) * 30
  const rampDownExpected = (targetRps / 2) * 15
  const holdSeconds      = extended ? 900 : 120
  const holdRequests     = Math.max(0, total - rampUpExpected - rampDownExpected)
  const holdRps          = holdSeconds > 0 ? (holdRequests / holdSeconds) : 0
  const eventsPerMin     = holdRps * batchSize * 60

  const modeLabel = batchSize > 1 ? `batch(${batchSize} events/req)` : 'single-event'

  const stdout = `
=== Ingest Load Test Summary (${modeLabel}) ===
Overall avg RPS:      ${rps.toFixed(1)} RPS
Hold-phase est. RPS:  ${holdRps.toFixed(1)} RPS  (target: ≥${targetRps} RPS)
Events/minute (hold): ${eventsPerMin.toFixed(0)}  (target: ≥10,000)
p95 latency:          ${p95.toFixed(0)}ms  (budget: <100ms)
Success rate:         ${okRate.toFixed(2)}%  (budget: >99%)
Total requests:       ${total}
Events accepted:      ${accepted}
Dropped iterations:   ${dropped}  (should be <0.5% of total)
Test duration:        ${dur.toFixed(0)}s
${dropped / Math.max(total, 1) > 0.005 ? '⚠ Dropped iteration rate exceeds 0.5% — consider increasing preAllocatedVUs' : '✓ Dropped iteration rate within budget'}
${'='.repeat(48)}
`

  return {
    stdout,
    'tests/load/ingest-summary.json': JSON.stringify(data, null, 2),
  }
}
