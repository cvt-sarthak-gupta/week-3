/**
 * k6 load test — Search endpoint
 *
 * Covers:
 *   - Cache warmup phase: pre-populates Redis before measuring
 *   - Weighted query distribution: realistic traffic skew (top terms appear far more)
 *   - Stable cache keys: uses fixed `from`/`to` date windows, not random per-request
 *   - Cache hit tracking: reads `cacheHit` field from response body (server-side flag)
 *     AND falls back to `X-Cache` header for defence-in-depth
 *   - Extended soak mode via EXTENDED=1 (15-minute run)
 *
 * Run:
 *   k6 run tests/load/search.k6.js
 *   k6 run --env API_URL=http://localhost:3000 --env AUTH_TOKEN=<jwt> \
 *           --env PROJECT_ID=<id> --env TENANT_ID=<id> tests/load/search.k6.js
 *   k6 run --env EXTENDED=1 tests/load/search.k6.js
 */

import http from 'k6/http'
import { check, group } from 'k6'
import { Counter, Rate, Trend } from 'k6/metrics'

const apiUrl    = __ENV.API_URL    || 'http://localhost:3000'
const authToken = __ENV.AUTH_TOKEN || ''
const projectId = __ENV.PROJECT_ID || '00000000-0000-0000-0000-000000000003'
const tenantId  = __ENV.TENANT_ID  || '00000000-0000-0000-0000-000000000002'
const extended  = __ENV.EXTENDED === '1'

// ---------------------------------------------------------------------------
// Custom metrics
// ---------------------------------------------------------------------------
const searchErrors   = new Counter('search_errors')
const searchOkRate   = new Rate('search_ok_rate')
const searchDuration = new Trend('search_duration_ms', true)
const cacheHitRate   = new Rate('search_cache_hit_rate')
const warmupOkRate   = new Rate('warmup_ok_rate')

// ---------------------------------------------------------------------------
// Scenario configuration
// ---------------------------------------------------------------------------

// Warmup: 10s at 5 RPS to populate Redis before we start measuring
// Main:   2 min at 30 RPS (standard) or 15 min (extended)
const mainDuration = extended ? '15m' : '2m'

export const options = {
  summaryTrendStats: ['avg', 'min', 'med', 'max', 'p(90)', 'p(95)', 'p(99)'],
  scenarios: {
    // Phase 1 — cache warmup (not measured in thresholds)
    cache_warmup: {
      executor: 'constant-arrival-rate',
      rate: 5,
      timeUnit: '1s',
      duration: '10s',
      preAllocatedVUs: 10,
      maxVUs: 20,
      gracefulStop: '5s',
      env: { PHASE: 'warmup' },
    },
    // Phase 2 — measured load (starts after warmup)
    steady_load: {
      executor: 'constant-arrival-rate',
      rate: 30,           // 30 RPS = 1800 searches/min
      timeUnit: '1s',
      duration: mainDuration,
      preAllocatedVUs: 50,
      maxVUs: 150,
      startTime: '15s',   // 10s warmup + 5s grace
      env: { PHASE: 'main' },
    },
  },
  thresholds: {
    // Only apply latency thresholds to the main scenario
    'search_duration_ms{phase:main}': ['p(95)<500', 'p(99)<1000'],
    search_ok_rate:      ['rate>0.99'],
    http_req_duration:   ['p(95)<500'],
    http_req_failed:     ['rate<0.01'],
    // Expect ≥80% cache hit rate in the main phase (after warmup)
    'search_cache_hit_rate{phase:main}': ['rate>0.8'],
  },
}

// ---------------------------------------------------------------------------
// Query distribution — Zipf-like skew
//
// In production, users repeat the same dashboard queries constantly.
// Top 3 terms account for ~65% of traffic; long-tail fills the rest.
// Using fixed weights ensures the same cache keys are hit repeatedly.
// ---------------------------------------------------------------------------
const weightedTerms = [
  // [term, weight] — higher weight = more frequent
  ['error',             30],
  ['timeout',           20],
  ['database',          15],
  ['connection refused', 8],
  ['null pointer',       7],
  ['payment',            6],
  ['authentication',     5],
  ['session expired',    4],
  ['rate limit',         3],
  ['invalid request',    2],
]

// Build a flat array for O(1) weighted selection
const termPool = []
for (const [term, weight] of weightedTerms) {
  for (let i = 0; i < weight; i++) termPool.push(term)
}

const severities = ['error', 'fatal', 'warn']

// Fixed date windows — computed once at test start so they're stable cache keys.
// Using ISO strings ensures the server caches consistently across all VUs.
const now      = new Date()
const from7d   = new Date(now - 7  * 24 * 3600 * 1000).toISOString()
const from30d  = new Date(now - 30 * 24 * 3600 * 1000).toISOString()
const toNow    = now.toISOString()

// Only 2 date windows: last 7 days (80% of queries) and last 30 days (20%)
// This reduces unique cache keys while keeping the test realistic
function pickDateWindow() {
  return Math.random() < 0.8
    ? { from: from7d,  to: toNow }
    : { from: from30d, to: toNow }
}

function randomItem(arr) {
  return arr[Math.floor(Math.random() * arr.length)]
}

function pickTerm() {
  return termPool[Math.floor(Math.random() * termPool.length)]
}

// ---------------------------------------------------------------------------
// Shared request logic
// ---------------------------------------------------------------------------
function doSearch(phase) {
  const q        = pickTerm()
  const severity = Math.random() > 0.4 ? randomItem(severities) : null  // 60% include severity
  const window   = pickDateWindow()

  let qs = `q=${encodeURIComponent(q)}&from=${encodeURIComponent(window.from)}&to=${encodeURIComponent(window.to)}`
  if (severity) qs += `&severity=${severity}`

  const headers = { 'Content-Type': 'application/json' }
  if (authToken) headers['Authorization'] = `Bearer ${authToken}`

  const start = Date.now()
  const res   = http.get(
    `${apiUrl}/v1/tenants/${tenantId}/projects/${projectId}/logs/search?${qs}`,
    { headers, timeout: '10s', tags: { phase } },
  )
  const elapsed = Date.now() - start

  return { res, elapsed, phase }
}

// ---------------------------------------------------------------------------
// Warmup phase — populate cache, no strict checks
// ---------------------------------------------------------------------------
function runWarmup() {
  const { res } = doSearch('warmup')
  const ok = check(res, {
    'warmup 2xx': (r) => r.status >= 200 && r.status < 300,
  })
  warmupOkRate.add(ok ? 1 : 0)
  if (!ok) {
    console.warn(`[warmup] status=${res.status} body=${res.body?.slice(0, 100)}`)
  }
}

// ---------------------------------------------------------------------------
// Main phase — measured load
// ---------------------------------------------------------------------------
function runMain() {
  const { res, elapsed } = doSearch('main')

  searchDuration.add(elapsed, { phase: 'main' })

  const ok = check(res, {
    'status 200 or 206': (r) => r.status === 200 || r.status === 206,
    'has hits array': (r) => {
      try {
        const body = JSON.parse(r.body)
        return Array.isArray(body.hits) || Array.isArray(body.results)
      } catch { return false }
    },
  })

  searchOkRate.add(ok ? 1 : 0)

  // Cache hit detection: prefer response body field (server-side truth),
  // fall back to X-Cache header.
  let isHit = false
  try {
    const body = JSON.parse(res.body)
    if (typeof body.cacheHit === 'boolean') {
      isHit = body.cacheHit
    } else {
      // Header fallback (case-insensitive)
      const h = (res.headers['X-Cache'] ?? res.headers['x-cache'] ?? '').toLowerCase()
      isHit = h === 'hit' || h === 'stale'
    }
  } catch {
    const h = (res.headers['X-Cache'] ?? res.headers['x-cache'] ?? '').toLowerCase()
    isHit = h === 'hit' || h === 'stale'
  }

  cacheHitRate.add(isHit ? 1 : 0, { phase: 'main' })

  if (!ok) {
    searchErrors.add(1)
    console.error(`Search failed: status=${res.status} body=${res.body?.slice(0, 200)}`)
  }
}

// ---------------------------------------------------------------------------
// Default export — dispatches by PHASE env (set per scenario)
// ---------------------------------------------------------------------------
export default function () {
  if (__ENV.PHASE === 'warmup') {
    runWarmup()
  } else {
    runMain()
  }
}

// ---------------------------------------------------------------------------
// Summary
// ---------------------------------------------------------------------------
export function handleSummary(data) {
  const p95      = data.metrics['search_duration_ms']?.values?.['p(95)'] ?? 0
  const p99      = data.metrics['search_duration_ms']?.values?.['p(99)'] ?? 0
  const okRate   = (data.metrics['search_ok_rate']?.values?.rate ?? 0) * 100
  const cacheRate = (data.metrics['search_cache_hit_rate']?.values?.rate ?? 0) * 100
  const rps      = data.metrics['http_reqs']?.values?.rate ?? 0
  const total    = data.metrics['http_reqs']?.values?.count ?? 0
  const dur      = (data.state?.testRunDurationMs ?? 0) / 1000

  // Unique cache key estimate:
  // Queries: 10 terms × (0 severity + 3 severities) × 2 date windows = 80 unique keys
  // With 30 RPS × mainDuration, steady-state hit rate should be ≥95%
  const expectedKeys = 10 * 4 * 2
  const mainSeconds  = extended ? 900 : 120
  const expectedHitRate = Math.max(0, (30 * mainSeconds - expectedKeys) / (30 * mainSeconds)) * 100

  const cacheStatus = cacheRate >= 80
    ? `✓ ${cacheRate.toFixed(1)}% (budget: ≥80%)`
    : `✗ ${cacheRate.toFixed(1)}% — expected ≥80% after warmup (${expectedKeys} unique keys; check Redis connectivity or X-Cache header)`

  return {
    stdout: `
=== Search Load Test Summary ===
Throughput:     ${rps.toFixed(1)} RPS  (${total} total requests)
p95 latency:    ${p95.toFixed(0)}ms  (budget: <500ms)
p99 latency:    ${p99.toFixed(0)}ms  (budget: <1000ms)
Success rate:   ${okRate.toFixed(2)}%  (budget: >99%)
Cache hit rate: ${cacheStatus}
Expected rate:  ~${expectedHitRate.toFixed(0)}% after ${expectedKeys}-key warmup
Test duration:  ${dur.toFixed(0)}s
${'='.repeat(42)}
`,
    'tests/load/search-summary.json': JSON.stringify(data, null, 2),
  }
}
