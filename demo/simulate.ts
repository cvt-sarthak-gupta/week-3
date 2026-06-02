/**
 * ShopFast demo simulation.
 *
 * What it does:
 *   1. Registers a new "ShopFast Demo" tenant on PulseBoard
 *   2. Starts a local ShopFast app instrumented with the PulseBoard SDK
 *   3. Drives 60 seconds of realistic user traffic
 *   4. Queries PulseBoard APIs and renders a full run report + saves JSON
 *
 * Usage:
 *   cd demo && npx tsx simulate.ts
 */

import { PulseBoard } from './pulseboard.js';
import { createShopFastApp } from './shopfast-app.js';
import { printReport, saveReport } from './report.js';

const PULSEBOARD_HOST = 'http://localhost:3000';
const SHOPFAST_PORT   = 4000;

// LoadTest plan UUID — hardcoded in seed/seed-postgres.ts with fixed ID.
// 432M events/month → ~10k/min rate limit. Safe to hardcode; it's a seed fixture.
const LOADTEST_PLAN_ID = '00000000-0000-0000-0000-000000000001';

// ── Helpers ───────────────────────────────────────────────────────────────────

async function pb(path: string, method = 'GET', body?: unknown, token?: string) {
  const res = await fetch(`${PULSEBOARD_HOST}/v1${path}`, {
    method,
    headers: {
      'Content-Type': 'application/json',
      ...(token ? { Authorization: `Bearer ${token}` } : {}),
    },
    body: body !== undefined ? JSON.stringify(body) : undefined,
  });
  const text = await res.text();
  try { return { status: res.status, body: JSON.parse(text) }; }
  catch { return { status: res.status, body: text }; }
}

async function shopfast(path: string, method = 'GET', body?: unknown) {
  const res = await fetch(`http://127.0.0.1:${SHOPFAST_PORT}${path}`, {
    method,
    headers: { 'Content-Type': 'application/json' },
    body: body !== undefined ? JSON.stringify(body) : undefined,
  });
  const text = await res.text();
  try { return { status: res.status, body: JSON.parse(text) as unknown }; }
  catch { return { status: res.status, body: text as unknown }; }
}

function sleep(ms: number) { return new Promise(resolve => setTimeout(resolve, ms)); }
function pick<T>(arr: T[]): T { return arr[Math.floor(Math.random() * arr.length)]!; }

const USERS = [
  { userId: 'usr_001', email: 'alice@example.com' },
  { userId: 'usr_002', email: 'bob@example.com' },
  { userId: 'usr_003', email: 'carol@example.com' },
  { userId: 'usr_004', email: 'dave@example.com' },
  { userId: 'usr_005', email: 'eve@example.com' },
];

// ── Main ──────────────────────────────────────────────────────────────────────

async function main() {
  console.log('\n╔══════════════════════════════════════════════════════╗');
  console.log('║         ShopFast × PulseBoard — Live Demo            ║');
  console.log('╚══════════════════════════════════════════════════════╝\n');

  // ── Step 1: Register & login ───────────────────────────────────────────────
  console.log('▶  Step 1: Setting up PulseBoard account...');
  const email    = `shopfast-demo-${Date.now()}@example.com`;
  const password = 'Demo1234!';

  await pb('/auth/register', 'POST', { email, password, fullName: 'ShopFast Demo' });
  const loginRes = await pb('/auth/login', 'POST', { email, password });
  const token    = (loginRes.body as Record<string, string>).accessToken;
  console.log('   ✓ Registered and logged in');

  // ── Step 2: Onboard tenant ─────────────────────────────────────────────────
  console.log('\n▶  Step 2: Onboarding ShopFast tenant on PulseBoard...');
  const onboardRes = await pb('/tenants', 'POST', {
    tenantName: 'ShopFast Inc',
    tenantSlug: `shopfast-${Date.now()}`,
    planId: LOADTEST_PLAN_ID,
    projectName: 'ShopFast Production',
    projectSlug: 'shopfast-prod',
  }, token);

  if (onboardRes.status !== 201) {
    console.error('   ✗ Onboarding failed:', onboardRes.body);
    process.exit(1);
  }

  const { tenantId, projectId, apiKey } =
    onboardRes.body as { tenantId: string; projectId: string; apiKey: string };

  console.log(`   ✓ Tenant:  ${tenantId}`);
  console.log(`   ✓ Project: ${projectId}`);
  console.log(`   ✓ API Key: ${apiKey}`);

  // Re-login so JWT includes the new tenantId
  const authToken = ((await pb('/auth/login', 'POST', { email, password }))
    .body as Record<string, string>).accessToken;

  // ── Step 3: Start ShopFast ─────────────────────────────────────────────────
  console.log('\n▶  Step 3: Starting ShopFast app (port 4000)...');
  const sdk = new PulseBoard({ apiKey, host: PULSEBOARD_HOST });
  const closeShopFast = await createShopFastApp(sdk, SHOPFAST_PORT);
  const sfHealth = await shopfast('/health');
  console.log('   ✓ ShopFast health:', (sfHealth.body as Record<string, string>).status);

  // ── Step 4: Traffic simulation ─────────────────────────────────────────────
  console.log('\n▶  Step 4: Simulating 60 seconds of user traffic...\n   ');

  const PRODUCT_IDS = ['p1', 'p2', 'p3', 'p4', 'p5'];
  const simStart    = Date.now();
  let httpRequests  = 0;
  let checkouts     = 0;
  let successOrders = 0;
  let httpErrors    = 0;

  while (Date.now() - simStart < 60_000) {
    const user      = pick(USERS);
    const productId = pick(PRODUCT_IDS);

    await shopfast('/products');                           httpRequests++;
    const detail = await shopfast(`/products/${productId}`); httpRequests++;

    const prod = detail.body as Record<string, unknown>;
    if (detail.status === 200 && (prod.stock as number) > 0) {
      const addRes = await shopfast('/cart/add', 'POST', { productId, qty: 1, userId: user.userId });
      httpRequests++;
      if (addRes.status !== 200) httpErrors++;

      if (Math.random() < 0.6) {
        const checkoutRes = await shopfast('/checkout', 'POST', {
          userId: user.userId, email: user.email, items: [{ productId, qty: 1 }],
        });
        httpRequests++;
        checkouts++;
        if (checkoutRes.status === 200) successOrders++;
        else httpErrors++;
        process.stdout.write(checkoutRes.status === 200 ? '✓' : '✗');
      }
    } else {
      if (detail.status !== 200) httpErrors++;
      process.stdout.write('·');
    }

    await sleep(500 + Math.random() * 500);
  }

  const simDurationSec = Math.round((Date.now() - simStart) / 1000);
  console.log(`\n\n   ${httpRequests} requests  ·  ${checkouts} checkouts  ·  ${httpErrors} non-200 responses`);

  // ── Step 5: Wait for pipeline flush ───────────────────────────────────────
  console.log('\n▶  Step 5: Waiting for PulseBoard pipeline to flush (5s)...');
  await sleep(5000);

  // ── Step 6: Collect PulseBoard data ───────────────────────────────────────
  console.log('\n▶  Step 6: Collecting PulseBoard data...');

  const [errRes, fatalRes, warnRes, dashRes, intelRes, quotaRes] = await Promise.all([
    pb(`/tenants/${tenantId}/projects/${projectId}/logs/search?severity=error&limit=1`,  'GET', undefined, authToken),
    pb(`/tenants/${tenantId}/projects/${projectId}/logs/search?severity=fatal&limit=1`,  'GET', undefined, authToken),
    pb(`/tenants/${tenantId}/projects/${projectId}/logs/search?severity=warn&limit=1`,   'GET', undefined, authToken),
    pb(`/tenants/${tenantId}/projects/${projectId}/reports/dashboard`,                    'GET', undefined, authToken),
    pb(`/tenants/${tenantId}/projects/${projectId}/reports/error-intelligence?days=1`,   'GET', undefined, authToken),
    pb(`/tenants/${tenantId}/quota`,                                                      'GET', undefined, authToken),
  ]);

  const totalSearch = (await pb(
    `/tenants/${tenantId}/projects/${projectId}/logs/search`, 'GET', undefined, authToken,
  )).body as { total: number };

  const searchCounts = {
    error: (errRes.body   as { total: number }).total ?? 0,
    fatal: (fatalRes.body as { total: number }).total ?? 0,
    warn:  (warnRes.body  as { total: number }).total ?? 0,
    total: totalSearch.total ?? 0,
  };

  // ── Step 7: Render report ──────────────────────────────────────────────────
  console.log('\n▶  Step 7: Generating report...');

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  printReport({
    tenantId,
    projectId,
    simStats: {
      durationSec:      simDurationSec,
      httpRequests,
      checkoutAttempts: checkouts,
      userCount:        USERS.length,
      successCheckouts: successOrders,
      failedCheckouts:  checkouts - successOrders,
    },
    dashboard:        dashRes.body  as Parameters<typeof printReport>[0]['dashboard'],
    errorIntelligence: intelRes.body as Parameters<typeof printReport>[0]['errorIntelligence'],
    quota:            quotaRes.body  as Parameters<typeof printReport>[0]['quota'],
    searchCounts,
  });

  const reportPath = saveReport({
    tenantId,
    projectId,
    simStats: {
      durationSec:      simDurationSec,
      httpRequests,
      checkoutAttempts: checkouts,
      userCount:        USERS.length,
      successCheckouts: successOrders,
      failedCheckouts:  checkouts - successOrders,
    },
    dashboard:         dashRes.body  as Parameters<typeof saveReport>[0]['dashboard'],
    errorIntelligence: intelRes.body as Parameters<typeof saveReport>[0]['errorIntelligence'],
    quota:             quotaRes.body  as Parameters<typeof saveReport>[0]['quota'],
    searchCounts,
  });

  console.log(`   Report saved → ${reportPath}\n`);

  await closeShopFast();
}

main().catch((err) => {
  console.error('Demo failed:', err);
  process.exit(1);
});
