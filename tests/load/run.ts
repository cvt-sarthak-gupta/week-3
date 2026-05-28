/**
 * tests/load/run.ts
 * Orchestrates k6 load tests. Requires k6: brew install k6
 *
 * Usage:
 *   npm run test:load                    — ingest + search (standard)
 *   npm run test:load -- --spike         — add spike test
 *   npm run test:load -- --soak          — add soak test (15 min)
 *   EXTENDED=1 npm run test:load         — all tests in extended mode
 *   SKIP_SEARCH=1 npm run test:load      — skip search test (auth not available)
 */

import { execSync } from 'node:child_process';
import { existsSync } from 'node:fs';
import { resolve, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = dirname(fileURLToPath(import.meta.url));

// ---------------------------------------------------------------------------
// Config from environment
// ---------------------------------------------------------------------------
const API_URL    = process.env['API_URL']    ?? 'http://localhost:3000';
const API_KEY    = process.env['API_KEY']    ?? 'a1b2c3d4-e5f6-7890-abcd-ef1234567890';
const PROJECT_ID = process.env['PROJECT_ID'] ?? '00000000-0000-0000-0000-000000000003';
const TENANT_ID  = process.env['TENANT_ID']  ?? '00000000-0000-0000-0000-000000000002';
const EXTENDED   = process.env['EXTENDED']   ?? '';
const BATCH_SIZE = process.env['BATCH_SIZE'] ?? '1';

const args = process.argv.slice(2);
const runSpike  = args.includes('--spike')  || process.env['RUN_SPIKE']  === '1';
const runSoak   = args.includes('--soak')   || process.env['RUN_SOAK']   === '1';
const skipSearch = process.env['SKIP_SEARCH'] === '1';

// ---------------------------------------------------------------------------
// Auto-fetch JWT for search test
// ---------------------------------------------------------------------------
let AUTH_TOKEN = process.env['AUTH_TOKEN'] ?? '';
if (!AUTH_TOKEN && !skipSearch) {
  try {
    const res = execSync(
      `curl -sf -X POST ${API_URL}/v1/auth/login \
        -H "Content-Type: application/json" \
        -d '{"email":"loadtest@example.com","password":"Password123!"}'`,
      { encoding: 'utf8' },
    );
    const parsed = JSON.parse(res) as { accessToken?: string };
    if (parsed.accessToken) {
      AUTH_TOKEN = parsed.accessToken;
      console.log('[load] Obtained AUTH_TOKEN via login.');
    }
  } catch {
    console.warn('[load] Could not obtain AUTH_TOKEN — set SKIP_SEARCH=1 to suppress this warning.');
    console.warn('[load] Search test will attempt to run without auth (expect 401 failures).');
  }
}

// ---------------------------------------------------------------------------
// Runner
// ---------------------------------------------------------------------------
interface RunResult {
  label: string;
  passed: boolean;
  durationMs: number;
}

function run(
  label: string,
  scriptPath: string,
  envVars: Record<string, string>,
): RunResult {
  if (!existsSync(scriptPath)) {
    console.error(`[load] Script not found: ${scriptPath}`);
    return { label, passed: false, durationMs: 0 };
  }

  const envArgs = Object.entries(envVars)
    .filter(([, v]) => v !== '')
    .map(([k, v]) => `--env ${k}=${v}`)
    .join(' ');

  const cmd = `k6 run ${envArgs} "${scriptPath}"`;
  const border = '═'.repeat(56);
  console.log(`\n${border}`);
  console.log(`  ${label}`);
  console.log(`${border}\n`);
  console.log(`  cmd: ${cmd}\n`);

  const start = Date.now();
  let passed  = true;

  try {
    execSync(cmd, { stdio: 'inherit' });
  } catch (e: unknown) {
    // k6 exits non-zero on threshold failures — capture but don't abort the suite
    const code = (e as { status?: number }).status ?? 1;
    if (code !== 0) {
      console.warn(`\n[load] "${label}" exited with code ${code} (threshold failures or k6 error)`);
      passed = false;
    }
  }

  return { label, passed, durationMs: Date.now() - start };
}

// ---------------------------------------------------------------------------
// Script paths
// ---------------------------------------------------------------------------
const ingestScript = resolve(__dirname, 'ingest.k6.js');
const searchScript = resolve(__dirname, 'search.k6.js');
const spikeScript  = resolve(__dirname, 'spike.k6.js');
const soakScript   = resolve(__dirname, 'soak.k6.js');

// ---------------------------------------------------------------------------
// Test suite
// ---------------------------------------------------------------------------
const results: RunResult[] = [];
const suiteStart = Date.now();

// 1. Ingest test (always)
results.push(run(
  `Ingest Load Test — batch=${BATCH_SIZE}, target 10k events/min, p95 < 100ms`,
  ingestScript,
  { API_URL, API_KEY, BATCH_SIZE, ...(EXTENDED ? { EXTENDED } : {}) },
));

// 2. Search test (always, unless skipped)
if (!skipSearch) {
  results.push(run(
    'Search Load Test — target p95 < 500ms, cache hit rate ≥ 80%',
    searchScript,
    {
      API_URL,
      AUTH_TOKEN,
      PROJECT_ID,
      TENANT_ID,
      ...(EXTENDED ? { EXTENDED } : {}),
    },
  ));
}

// 3. Spike test (opt-in)
if (runSpike) {
  results.push(run(
    'Spike Test — 500 RPS burst, verifies rate-limiter and recovery',
    spikeScript,
    { API_URL, API_KEY },
  ));
}

// 4. Soak test (opt-in)
if (runSoak) {
  results.push(run(
    `Soak Test — ${EXTENDED ? '30' : '15'} min stability run`,
    soakScript,
    {
      API_URL,
      API_KEY,
      AUTH_TOKEN,
      PROJECT_ID,
      TENANT_ID,
      ...(EXTENDED ? { EXTENDED } : {}),
    },
  ));
}

// ---------------------------------------------------------------------------
// Final report
// ---------------------------------------------------------------------------
const totalMs = Date.now() - suiteStart;
const border  = '═'.repeat(56);
console.log(`\n${border}`);
console.log('  Load Test Suite Results');
console.log(border);
for (const r of results) {
  const status  = r.passed ? '✓ PASS' : '✗ FAIL';
  const elapsed = (r.durationMs / 1000).toFixed(0);
  console.log(`  ${status}  ${r.label}  (${elapsed}s)`);
}
console.log(`${border}`);
console.log(`  Total: ${(totalMs / 1000).toFixed(0)}s`);
console.log(`  Summaries: tests/load/*-summary.json`);
console.log(`${border}\n`);

const anyFailed = results.some((r) => !r.passed);
if (anyFailed) {
  process.exit(1);
}
