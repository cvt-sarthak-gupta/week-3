/**
 * k6 spike test — Sudden burst traffic
 *
 * Simulates worst-case concurrency spikes that expose:
 *   - Redis stream backpressure / MAXLEN trimming
 *   - Rate limiter behavior under burst (Lua sliding window)
 *   - Circuit breaker activation under connection saturation
 *   - PG connection pool exhaustion (pool max = 20 by default)
 *   - k6 dropped iteration rate (VU starvation)
 *   - Recovery: does throughput return to normal after spike?
 *
 * Profile:
 *   0–10s   : baseline (10 RPS) — establishes normal
 *   10–20s  : sudden spike to 500 RPS (50× baseline)
 *   20–30s  : hold spike
 *   30–40s  : drop back to baseline
 *   40–60s  : recovery observation — does system self-heal?
 *
 * Run:
 *   k6 run tests/load/spike.k6.js
 *   k6 run --env API_URL=http://localhost:3000 --env API_KEY=<key> tests/load/spike.k6.js
 */

import http from 'k6/http'
import { check, sleep } from 'k6'
import { Counter, Rate, Trend, Gauge } from 'k6/metrics'

const apiUrl = __ENV.API_URL || 'http://localhost:3000'
const apiKey  = __ENV.API_KEY  || 'a1b2c3d4-e5f6-7890-abcd-ef1234567890'

// ---------------------------------------------------------------------------
// Custom metrics
// ---------------------------------------------------------------------------
const spikeErrors      = new Counter('spike_errors')
const spikeOkRate      = new Rate('spike_ok_rate')
const spikeDuration    = new Trend('spike_duration_ms', true)
const rateLimitedCount = new Counter('spike_rate_limited')
const serviceUnavail   = new Counter('spike_service_unavail')

// ---------------------------------------------------------------------------
// Scenarios
// ---------------------------------------------------------------------------
export const options = {
  summaryTrendStats: ['avg', 'min', 'med', 'max', 'p(90)', 'p(95)', 'p(99)'],
  scenarios: {
    spike_test: {
      executor: 'ramping-arrival-rate',
      startRate: 10,
      timeUnit: '1s',
      // Generous VU pool: spike needs ~500 × 0.01s ≈ 5 concurrent VUs,
      // but connection overhead during the spike can need more
      preAllocatedVUs: 300,
      maxVUs: 800,
      stages: [
        { duration: '10s', target: 10   },  // baseline
        { duration: '10s', target: 500  },  // sudden spike
        { duration: '10s', target: 500  },  // hold spike
        { duration: '5s',  target: 10   },  // drop
        { duration: '25s', target: 10   },  // recovery observation
      ],
    },
  },
  thresholds: {
    // During a spike we tolerate higher error rate — but recovery must work
    spike_ok_rate:     ['rate>0.70'],    // accept up to 30% errors during spike
    spike_duration_ms: ['p(99)<2000'],   // p99 must stay under 2s even at 500 RPS
    http_req_failed:   ['rate<0.30'],
  },
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------
const severities = ['error', 'warn', 'info']
const services   = ['api-gateway', 'auth-service', 'payment-service']

function randomItem(arr) {
  return arr[Math.floor(Math.random() * arr.length)]
}

// ---------------------------------------------------------------------------
// Default test function
// ---------------------------------------------------------------------------
export default function () {
  const severity = randomItem(severities)
  const service  = randomItem(services)

  const payload = JSON.stringify({
    type: 'error',
    severity,
    message: `spike-test [${severity}] from ${service}`,
    fingerprint: `spike-${severity}`,
    occurredAt: new Date().toISOString(),
    tags: { env: 'spike-test', service },
    payload: { ts: Date.now() },
  })

  const params = {
    headers: {
      'Content-Type': 'application/json',
      'X-PulseBoard-Key': apiKey,
    },
    timeout: '3s',
  }

  const start   = Date.now()
  const res     = http.post(`${apiUrl}/v1/ingest`, payload, params)
  const elapsed = Date.now() - start

  spikeDuration.add(elapsed)

  // Classify response
  const status = res.status
  if (status === 429) {
    rateLimitedCount.add(1)
  } else if (status === 503 || status === 502) {
    serviceUnavail.add(1)
  }

  const ok = check(res, {
    'accepted or rate-limited': (r) => r.status === 202 || r.status === 429,
    'no 5xx errors': (r) => r.status < 500,
  })

  spikeOkRate.add(ok ? 1 : 0)
  if (!ok) {
    spikeErrors.add(1)
    if (status !== 503) {
      // Only log unexpected failures (503 during spike is expected/acceptable)
      console.warn(`Spike unexpected failure: status=${status}`)
    }
  }
}

// ---------------------------------------------------------------------------
// Summary
// ---------------------------------------------------------------------------
export function handleSummary(data) {
  const dur          = (data.state?.testRunDurationMs ?? 0) / 1000
  const rps          = data.metrics['http_reqs']?.values?.rate ?? 0
  const total        = data.metrics['http_reqs']?.values?.count ?? 0
  const p99          = data.metrics['spike_duration_ms']?.values?.['p(99)'] ?? 0
  const okRate       = (data.metrics['spike_ok_rate']?.values?.rate ?? 0) * 100
  const rateLimited  = data.metrics['spike_rate_limited']?.values?.count ?? 0
  const unavail      = data.metrics['spike_service_unavail']?.values?.count ?? 0
  const dropped      = data.metrics['dropped_iterations']?.values?.count ?? 0
  const errors       = data.metrics['spike_errors']?.values?.count ?? 0

  return {
    stdout: `
=== Spike Load Test Summary ===
Overall avg RPS:     ${rps.toFixed(1)} RPS
Total requests:      ${total}
p99 latency:         ${p99.toFixed(0)}ms  (budget: <2000ms)
Acceptance rate:     ${okRate.toFixed(1)}%  (budget: >70%)
Rate-limited (429):  ${rateLimited}  (expected during spike — proves limiter works)
Service unavail:     ${unavail}  (503/502 during spike)
Unexpected errors:   ${errors}
Dropped iterations:  ${dropped}
Test duration:       ${dur.toFixed(0)}s

Spike diagnosis:
  ${rateLimited > 0 ? '✓' : '?'} Rate limiter active under spike
  ${unavail > 0 ? '⚠ Circuit breaker / backpressure triggered — check recovery' : '✓ No 503s — system absorbed spike'}
  ${dropped > 50 ? '⚠ Many dropped iterations — system was overloaded at peak' : '✓ Dropped iterations within bounds'}
${'='.repeat(35)}
`,
    'tests/load/spike-summary.json': JSON.stringify(data, null, 2),
  }
}
