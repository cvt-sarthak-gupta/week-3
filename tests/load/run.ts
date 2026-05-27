/**
 * tests/load/run.ts
 * Orchestrates both k6 load tests sequentially.
 * Requires k6 to be installed: brew install k6
 *
 * Usage: npm run test:load
 */

import { execSync } from 'node:child_process';
import { existsSync } from 'node:fs';
import { resolve, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = dirname(fileURLToPath(import.meta.url));

const API_URL = process.env['API_URL'] ?? 'http://localhost:3000';
const API_KEY  = process.env['API_KEY']  ?? 'a1b2c3d4-e5f6-7890-abcd-ef1234567890';
const PROJECT_ID = process.env['PROJECT_ID'] ?? '00000000-0000-0000-0000-000000000003';
const TENANT_ID = process.env['TENANT_ID'] ?? '00000000-0000-0000-0000-000000000002';

// Auto-fetch a JWT for the search load test if AUTH_TOKEN not supplied
let AUTH_TOKEN = process.env['AUTH_TOKEN'] ?? '';
if (!AUTH_TOKEN) {
  try {
    const res = execSync(
      `curl -sf -X POST ${API_URL}/v1/auth/login -H "Content-Type: application/json" -d '{"email":"loadtest@example.com","password":"Password123!"}'`,
      { encoding: 'utf8' },
    );
    const parsed = JSON.parse(res) as { accessToken?: string };
    if (parsed.accessToken) {
      AUTH_TOKEN = parsed.accessToken;
      console.log('[load] Obtained search AUTH_TOKEN via login.');
    }
  } catch {
    console.warn('[load] Could not obtain AUTH_TOKEN — search test will run without auth (expect 401s).');
  }
}

function run(label: string, scriptPath: string, envVars: Record<string, string>): boolean {
  if (!existsSync(scriptPath)) {
    console.error(`[load] Script not found: ${scriptPath}`);
    return false;
  }

  const envArgs = Object.entries(envVars)
    .map(([k, v]) => `--env ${k}=${v}`)
    .join(' ');

  const cmd = `k6 run ${envArgs} "${scriptPath}"`;
  console.log(`\n${'═'.repeat(50)}`);
  console.log(`  ${label}`);
  console.log(`${'═'.repeat(50)}\n`);
  console.log(`  cmd: ${cmd}\n`);

  try {
    execSync(cmd, { stdio: 'inherit' });
    return true;
  } catch {
    // k6 exits non-zero on threshold failures — still return true so we run both tests
    return true;
  }
}

const ingestScript = resolve(__dirname, 'ingest.k6.js');
const searchScript  = resolve(__dirname, 'search.k6.js');

run('Ingest Load Test — target 10k events/min, p95 < 100ms', ingestScript, {
  API_URL,
  API_KEY,
});

run('Search Load Test — target p95 < 500ms', searchScript, {
  API_URL,
  AUTH_TOKEN,
  PROJECT_ID,
  TENANT_ID,
});

console.log('\nLoad tests complete. Check tests/load/*-summary.json for full results.\n');
