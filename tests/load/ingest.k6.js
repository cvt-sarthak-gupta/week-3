/**
 * k6 load test — Ingest endpoint
 *
 * Target: 10,000 events/minute (≈167 RPS) with p95 < 100ms.
 *
 * Run:
 *   k6 run tests/load/ingest.k6.js
 *   k6 run --env API_URL=http://localhost:3000 tests/load/ingest.k6.js
 */

import http from 'k6/http'
import { check, sleep } from 'k6'
import { Counter, Rate, Trend } from 'k6/metrics'

const apiUrl = __ENV.API_URL || 'http://localhost:3000'

// Obtain a test API key via env, or use the seed default
const apiKey = __ENV.API_KEY || 'test-api-key-seed-001'

// Custom metrics
const ingestErrors = new Counter('ingest_errors')
const ingestOkRate = new Rate('ingest_ok_rate')
const ingestDuration = new Trend('ingest_duration_ms', true)

export const options = {
  scenarios: {
    ramp_and_hold: {
      executor: 'ramping-arrival-rate',
      startRate: 10,
      timeUnit: '1s',
      preAllocatedVUs: 50,
      maxVUs: 200,
      stages: [
        { duration: '30s', target: 167 }, // ramp to 10k/min
        { duration: '2m', target: 167 },  // hold at 10k/min
        { duration: '15s', target: 0 },   // ramp down
      ],
    },
  },
  thresholds: {
    // p95 under 100ms
    ingest_duration_ms: ['p(95)<100'],
    // Less than 1% errors
    ingest_ok_rate: ['rate>0.99'],
    // Overall http_req_duration also tracked
    http_req_duration: ['p(95)<100'],
    http_req_failed: ['rate<0.01'],
  },
}

const severities = ['error', 'fatal', 'warn', 'info']
const browsers = ['chrome', 'firefox', 'safari', 'edge']

function randomItem(arr) {
  return arr[Math.floor(Math.random() * arr.length)]
}

export default function () {
  const severity = randomItem(severities)
  const browser = randomItem(browsers)
  const eventId = `k6-${Date.now()}-${Math.random().toString(36).slice(2, 10)}`

  const payload = JSON.stringify({
    type: 'error',
    severity,
    message: `k6 load test event [${severity}] from ${browser}`,
    fingerprint: `fp-k6-${severity}`,
    occurredAt: new Date().toISOString(),
    userContext: { userId: `user-${Math.floor(Math.random() * 1000)}` },
    deviceContext: { browser },
    payload: {
      eventId,
      responseTimeMs: Math.floor(Math.random() * 500),
    },
    tags: { env: 'load-test', service: 'k6' },
  })

  const params = {
    headers: {
      'Content-Type': 'application/json',
      'X-PulseBoard-Key': apiKey,
    },
    timeout: '5s',
  }

  const start = Date.now()
  const res = http.post(`${apiUrl}/v1/ingest`, payload, params)
  const elapsed = Date.now() - start

  ingestDuration.add(elapsed)

  const ok = check(res, {
    'status is 202': (r) => r.status === 202,
    'has eventId in response': (r) => {
      try {
        return JSON.parse(r.body).eventId !== undefined
      } catch {
        return false
      }
    },
  })

  ingestOkRate.add(ok ? 1 : 0)
  if (!ok) {
    ingestErrors.add(1)
    console.error(`Ingest failed: status=${res.status} body=${res.body?.slice(0, 200)}`)
  }

  // No sleep needed — arrival-rate executor manages concurrency
}

export function handleSummary(data) {
  const p95 = data.metrics['ingest_duration_ms']?.values?.['p(95)'] ?? 0
  const okRate = (data.metrics['ingest_ok_rate']?.values?.rate ?? 0) * 100
  const rps = data.metrics['http_reqs']?.values?.rate ?? 0

  return {
    stdout: `
=== Ingest Load Test Summary ===
Throughput:     ${rps.toFixed(1)} RPS (target: ≥167 RPS = 10k/min)
p95 latency:    ${p95.toFixed(0)}ms (budget: <100ms)
Success rate:   ${okRate.toFixed(2)}% (budget: >99%)
================================
`,
    'tests/load/ingest-summary.json': JSON.stringify(data, null, 2),
  }
}
