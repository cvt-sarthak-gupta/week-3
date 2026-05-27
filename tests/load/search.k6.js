/**
 * k6 load test — Search endpoint
 *
 * Target: search responses with p95 < 500ms under concurrent load.
 *
 * Requires seed data to be loaded and at least one project with logs indexed.
 *
 * Run:
 *   k6 run tests/load/search.k6.js
 *   k6 run --env API_URL=http://localhost:3000 --env AUTH_TOKEN=<jwt> --env PROJECT_ID=<id> tests/load/search.k6.js
 */

import http from 'k6/http'
import { check, sleep } from 'k6'
import { Counter, Rate, Trend } from 'k6/metrics'

const apiUrl = __ENV.API_URL || 'http://localhost:3000'
const authToken = __ENV.AUTH_TOKEN || ''
const projectId = __ENV.PROJECT_ID || '00000000-0000-0000-0000-000000000003'
const tenantId = __ENV.TENANT_ID || '00000000-0000-0000-0000-000000000002'

const searchErrors = new Counter('search_errors')
const searchOkRate = new Rate('search_ok_rate')
const searchDuration = new Trend('search_duration_ms', true)
const cacheHitRate = new Rate('search_cache_hit_rate')

export const options = {
  scenarios: {
    steady_load: {
      executor: 'constant-arrival-rate',
      rate: 30,          // 30 RPS = 1800 searches/min
      timeUnit: '1s',
      duration: '2m',
      preAllocatedVUs: 40,
      maxVUs: 100,
    },
  },
  thresholds: {
    search_duration_ms: ['p(95)<500', 'p(99)<1000'],
    search_ok_rate: ['rate>0.99'],
    http_req_duration: ['p(95)<500'],
    http_req_failed: ['rate<0.01'],
  },
}

const queryTerms = [
  'error',
  'database',
  'timeout',
  'connection refused',
  'null pointer',
  'payment',
  'authentication',
  'session expired',
  'rate limit',
  'invalid request',
]

const severities = ['error', 'fatal', 'warn']

function randomItem(arr) {
  return arr[Math.floor(Math.random() * arr.length)]
}

export default function () {
  const q = randomItem(queryTerms)
  const severity = Math.random() > 0.5 ? randomItem(severities) : null
  const days = [1, 7, 14, 30][Math.floor(Math.random() * 4)]

  let qs = `q=${encodeURIComponent(q)}&days=${days}`
  if (severity) qs += `&severity=${severity}`

  const headers = {
    'Content-Type': 'application/json',
  }
  if (authToken) {
    headers['Authorization'] = `Bearer ${authToken}`
  }

  const start = Date.now()
  const res = http.get(
    `${apiUrl}/v1/tenants/${tenantId}/projects/${projectId}/logs/search?${qs}`,
    { headers, timeout: '10s' },
  )
  const elapsed = Date.now() - start

  searchDuration.add(elapsed)

  const ok = check(res, {
    'status 200 or 206': (r) => r.status === 200 || r.status === 206,
    'has hits array': (r) => {
      try {
        const body = JSON.parse(r.body)
        return Array.isArray(body.hits) || Array.isArray(body.results)
      } catch {
        return false
      }
    },
  })

  searchOkRate.add(ok ? 1 : 0)

  // Track cache hits via X-Cache header (if implemented)
  const cacheHeader = res.headers['X-Cache'] || res.headers['x-cache'] || ''
  cacheHitRate.add(cacheHeader.toLowerCase().includes('hit') ? 1 : 0)

  if (!ok) {
    searchErrors.add(1)
    console.error(`Search failed: status=${res.status} body=${res.body?.slice(0, 200)}`)
  }
}

export function handleSummary(data) {
  const p95 = data.metrics['search_duration_ms']?.values?.['p(95)'] ?? 0
  const p99 = data.metrics['search_duration_ms']?.values?.['p(99)'] ?? 0
  const okRate = (data.metrics['search_ok_rate']?.values?.rate ?? 0) * 100
  const cacheRate = (data.metrics['search_cache_hit_rate']?.values?.rate ?? 0) * 100
  const rps = data.metrics['http_reqs']?.values?.rate ?? 0

  return {
    stdout: `
=== Search Load Test Summary ===
Throughput:     ${rps.toFixed(1)} RPS
p95 latency:    ${p95.toFixed(0)}ms (budget: <500ms)
p99 latency:    ${p99.toFixed(0)}ms (budget: <1000ms)
Success rate:   ${okRate.toFixed(2)}% (budget: >99%)
Cache hit rate: ${cacheRate.toFixed(1)}%
================================
`,
    'tests/load/search-summary.json': JSON.stringify(data, null, 2),
  }
}
