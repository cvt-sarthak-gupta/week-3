/**
 * k6 soak test — Long-duration stability
 *
 * Runs at 50% of target throughput (≈83 RPS ingest + 15 RPS search) for an
 * extended period to reveal gradual degradation that short tests miss:
 *
 *   - Node.js heap growth / memory leaks (tenantPlanCache, planLimitCache Map growth)
 *   - Redis stream consumer lag accumulation
 *   - Elasticsearch segment merge pressure / heap growth
 *   - PostgreSQL WAL accumulation / connection pool saturation
 *   - Latency creep: p99 rising over time (event loop lag)
 *   - Redis key count growth (leaderboard keys not expiring)
 *
 * Duration: 15 minutes (default) or 30 minutes (EXTENDED=1)
 *
 * Observability — check these during the test:
 *   curl http://localhost:3000/v1/health
 *   redis-cli info memory
 *   curl http://localhost:9200/_cat/nodes?v&h=heap.percent,name
 *
 * Run:
 *   k6 run tests/load/soak.k6.js
 *   k6 run --env EXTENDED=1 tests/load/soak.k6.js
 */

import http from 'k6/http'
import { check, sleep } from 'k6'
import { Counter, Rate, Trend } from 'k6/metrics'

const apiUrl    = __ENV.API_URL    || 'http://localhost:3000'
const apiKey    = __ENV.API_KEY    || 'a1b2c3d4-e5f6-7890-abcd-ef1234567890'
const authToken = __ENV.AUTH_TOKEN || ''
const projectId = __ENV.PROJECT_ID || '00000000-0000-0000-0000-000000000003'
const tenantId  = __ENV.TENANT_ID  || '00000000-0000-0000-0000-000000000002'
const extended  = __ENV.EXTENDED === '1'

const totalDuration = extended ? '30m' : '15m'

// ---------------------------------------------------------------------------
// Custom metrics
// ---------------------------------------------------------------------------
const soakIngestOk = new Rate('soak_ingest_ok_rate')
const soakSearchOk = new Rate('soak_search_ok_rate')
const soakIngestMs = new Trend('soak_ingest_duration_ms', true)
const soakSearchMs = new Trend('soak_search_duration_ms', true)
const soakHealthOk = new Rate('soak_health_ok_rate')
const soakErrors   = new Counter('soak_errors')

// ---------------------------------------------------------------------------
// Options
// ---------------------------------------------------------------------------
export const options = {
  summaryTrendStats: ['avg', 'min', 'med', 'max', 'p(90)', 'p(95)', 'p(99)'],
  scenarios: {
    // Ingest at ~50% target: 83 RPS
    soak_ingest: {
      executor: 'constant-arrival-rate',
      rate: 83,
      timeUnit: '1s',
      duration: totalDuration,
      preAllocatedVUs: 50,
      maxVUs: 150,
      gracefulStop: '10s',
      env: { ROLE: 'ingest' },
    },
    // Search at 50% target: 15 RPS
    soak_search: {
      executor: 'constant-arrival-rate',
      rate: 15,
      timeUnit: '1s',
      duration: totalDuration,
      preAllocatedVUs: 20,
      maxVUs: 60,
      gracefulStop: '10s',
      env: { ROLE: 'search' },
    },
    // Health probe: 1 VU, sleeps 30s between checks
    health_probe: {
      executor: 'constant-vus',
      vus: 1,
      duration: totalDuration,
      gracefulStop: '5s',
      env: { ROLE: 'health' },
    },
  },
  thresholds: {
    soak_ingest_duration_ms: ['p(95)<200', 'p(99)<500'],
    soak_search_duration_ms: ['p(95)<800', 'p(99)<2000'],
    soak_ingest_ok_rate:     ['rate>0.99'],
    soak_search_ok_rate:     ['rate>0.99'],
    soak_health_ok_rate:     ['rate>0.99'],
    http_req_failed:         ['rate<0.01'],
  },
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------
const severities = ['error', 'fatal', 'warn', 'info']
const services   = ['api-gateway', 'auth-service', 'payment-service', 'user-service']
const queryTerms = [
  'error', 'timeout', 'database', 'connection refused', 'null pointer',
  'payment', 'authentication', 'session expired', 'rate limit', 'invalid request',
]

function randomItem(arr) {
  return arr[Math.floor(Math.random() * arr.length)]
}

// Fixed date window — stable across the test run for consistent cache keys
const runStart = new Date()
const from7d   = new Date(runStart - 7 * 24 * 3600 * 1000).toISOString()
const toNow    = runStart.toISOString()

// ---------------------------------------------------------------------------
// Ingest
// ---------------------------------------------------------------------------
function doIngest() {
  const severity = randomItem(severities)
  const service  = randomItem(services)

  const start = Date.now()
  const res   = http.post(
    `${apiUrl}/v1/ingest`,
    JSON.stringify({
      type:        'error',
      severity,
      message:     `soak-test [${severity}] from ${service}`,
      fingerprint: `soak-${severity}-${service}`,
      occurredAt:  new Date().toISOString(),
      tags:        { env: 'soak-test', service },
    }),
    {
      headers: {
        'Content-Type':    'application/json',
        'X-PulseBoard-Key': apiKey,
      },
      timeout: '5s',
    },
  )
  soakIngestMs.add(Date.now() - start)

  const ok = check(res, { 'ingest 202': (r) => r.status === 202 })
  soakIngestOk.add(ok ? 1 : 0)
  if (!ok) soakErrors.add(1)
}

// ---------------------------------------------------------------------------
// Search
// ---------------------------------------------------------------------------
function doSearch() {
  const q        = randomItem(queryTerms)
  const severity = Math.random() > 0.5 ? randomItem(['error', 'fatal', 'warn']) : null
  let qs = `q=${encodeURIComponent(q)}&from=${encodeURIComponent(from7d)}&to=${encodeURIComponent(toNow)}`
  if (severity) qs += `&severity=${severity}`

  const headers = { 'Content-Type': 'application/json' }
  if (authToken) headers['Authorization'] = `Bearer ${authToken}`

  const start = Date.now()
  const res   = http.get(
    `${apiUrl}/v1/tenants/${tenantId}/projects/${projectId}/logs/search?${qs}`,
    { headers, timeout: '10s' },
  )
  soakSearchMs.add(Date.now() - start)

  const ok = check(res, {
    'search 2xx': (r) => r.status >= 200 && r.status < 300,
    'has hits':   (r) => {
      try { return Array.isArray(JSON.parse(r.body).hits) } catch { return false }
    },
  })
  soakSearchOk.add(ok ? 1 : 0)
  if (!ok) soakErrors.add(1)
}

// ---------------------------------------------------------------------------
// Health probe
// ---------------------------------------------------------------------------
function doHealthProbe() {
  const res = http.get(`${apiUrl}/v1/health`, { timeout: '3s' })
  const ok  = check(res, { 'health 200': (r) => r.status === 200 })
  soakHealthOk.add(ok ? 1 : 0)
  if (!ok) {
    console.error(`[soak] Health probe FAILED at ${new Date().toISOString()}: status=${res.status}`)
    soakErrors.add(1)
  }
}

// ---------------------------------------------------------------------------
// Default export
// ---------------------------------------------------------------------------
export default function () {
  const role = __ENV.ROLE

  if (role === 'ingest') {
    doIngest()
  } else if (role === 'search') {
    doSearch()
  } else {
    // health_probe — sleep 30s between checks to avoid flooding
    doHealthProbe()
    sleep(30)
  }
}

// ---------------------------------------------------------------------------
// Summary
// ---------------------------------------------------------------------------
export function handleSummary(data) {
  const dur       = (data.state?.testRunDurationMs ?? 0) / 1000
  const iP95      = data.metrics['soak_ingest_duration_ms']?.values?.['p(95)'] ?? 0
  const iP99      = data.metrics['soak_ingest_duration_ms']?.values?.['p(99)'] ?? 0
  const sP95      = data.metrics['soak_search_duration_ms']?.values?.['p(95)'] ?? 0
  const sP99      = data.metrics['soak_search_duration_ms']?.values?.['p(99)'] ?? 0
  const iOk       = (data.metrics['soak_ingest_ok_rate']?.values?.rate ?? 0) * 100
  const sOk       = (data.metrics['soak_search_ok_rate']?.values?.rate ?? 0) * 100
  const hOk       = (data.metrics['soak_health_ok_rate']?.values?.rate ?? 0) * 100
  const totalReqs = data.metrics['http_reqs']?.values?.count ?? 0
  const errors    = data.metrics['soak_errors']?.values?.count ?? 0

  return {
    stdout: `
=== Soak Test Summary (${(dur / 60).toFixed(0)} min) ===
Total requests:        ${totalReqs}
Ingest p95/p99:        ${iP95.toFixed(0)}ms / ${iP99.toFixed(0)}ms  (budget: 200/500ms)
Search p95/p99:        ${sP95.toFixed(0)}ms / ${sP99.toFixed(0)}ms  (budget: 800/2000ms)
Ingest success:        ${iOk.toFixed(2)}%
Search success:        ${sOk.toFixed(2)}%
Health probe success:  ${hOk.toFixed(2)}%
Total errors:          ${errors}

Post-test manual checks:
  □ Redis memory stable?  redis-cli info memory | grep used_memory_human
  □ ES heap stable?       curl :9200/_cat/nodes?v&h=heap.percent,name
  □ PG connections ok?    psql -c "SELECT count(*) FROM pg_stat_activity"
  □ Node heap flat?       Compare first/last 5-min memory reading
${'='.repeat(40)}
`,
    'tests/load/soak-summary.json': JSON.stringify(data, null, 2),
  }
}
