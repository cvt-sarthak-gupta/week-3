/**
 * ShopFast × PulseBoard — Report Generator
 *
 * Builds a formatted console report and saves a JSON file from the raw
 * PulseBoard API responses collected at the end of a simulation run.
 */

import { writeFileSync, mkdirSync } from 'node:fs';
import { join, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';

const __dir = dirname(fileURLToPath(import.meta.url));

// ── Types mirroring the PulseBoard API response shapes ───────────────────────

interface SeverityBucket {
  key: string;
  doc_count: number;
  top_events: {
    hits: {
      total: { value: number };
      hits: Array<{ _source: { message: string; occurredAt: string; fingerprint: string } }>;
    };
  };
}

interface DashboardReport {
  eventsOverTime: Array<{ key_as_string: string; doc_count: number }>;
  bySeverity: SeverityBucket[];
  responseTimePercentiles: Record<string, number | null>;
  uniqueErrorCount: number;
  significantErrorTerms: unknown[];
  errorCount: number;
  totalCount: { value: number };
}

interface ErrorIntelligenceEntry {
  message: string;
  count: number;
  firstSeen: string;
  lastSeen: string;
  fingerprint: string;
  affectedUsers: string[];
}

interface ErrorIntelligenceReport {
  topErrors: ErrorIntelligenceEntry[];
}

interface QuotaReport {
  planName: string;
  eventsThisMonth: number;
  eventQuotaPerMonth: number;
  percentUsed: number;
}

export interface SimStats {
  durationSec: number;
  httpRequests: number;
  checkoutAttempts: number;
  userCount: number;
  successCheckouts: number;
  failedCheckouts: number;
}

export interface ReportInput {
  tenantId: string;
  projectId: string;
  simStats: SimStats;
  dashboard: DashboardReport;
  errorIntelligence: ErrorIntelligenceReport;
  quota: QuotaReport;
  searchCounts: { error: number; fatal: number; warn: number; total: number };
}

// ── Helpers ──────────────────────────────────────────────────────────────────

const SEV_ORDER = ['fatal', 'error', 'warn', 'info', 'debug'];
const SEV_COLOR: Record<string, string> = {
  fatal: '\x1b[35m', // magenta
  error: '\x1b[31m', // red
  warn:  '\x1b[33m', // yellow
  info:  '\x1b[36m', // cyan
  debug: '\x1b[90m', // grey
};
const RESET = '\x1b[0m';
const BOLD  = '\x1b[1m';
const DIM   = '\x1b[2m';

function color(sev: string, text: string): string {
  return `${SEV_COLOR[sev] ?? ''}${text}${RESET}`;
}

function bar(value: number, max: number, width = 36): string {
  const filled = max === 0 ? 0 : Math.round((value / max) * width);
  return '█'.repeat(filled) + '░'.repeat(width - filled);
}

function pct(n: number, total: number): string {
  if (total === 0) return '  0.0%';
  return `${((n / total) * 100).toFixed(1).padStart(5)}%`;
}

function fmt(iso: string): string {
  return iso.replace('T', ' ').replace(/\.\d+Z$/, ' UTC');
}

function pad(s: string | number, w: number): string {
  return String(s).padStart(w);
}

function section(title: string): void {
  console.log(`\n${BOLD}${title}${RESET}`);
  console.log('─'.repeat(title.length));
}

// ── Main print function ───────────────────────────────────────────────────────

export function printReport(input: ReportInput): void {
  const { simStats, dashboard, errorIntelligence, quota, searchCounts } = input;

  const sevMap: Record<string, number> = {};
  for (const b of dashboard.bySeverity) sevMap[b.key] = b.doc_count;
  const maxSevCount = Math.max(...Object.values(sevMap), 1);
  const total = dashboard.totalCount?.value ?? searchCounts.total;

  const now = new Date().toISOString().replace('T', ' ').slice(0, 19);

  console.log('\n╔══════════════════════════════════════════════════════════════╗');
  console.log(`║${BOLD}         ShopFast × PulseBoard — Run Report               ${RESET}║`);
  console.log(`║${DIM}         Generated: ${now} UTC${' '.repeat(Math.max(0, 22 - now.length))}${RESET}║`);
  console.log('╚══════════════════════════════════════════════════════════════╝');

  // ── Simulation summary ────────────────────────────────────────────────────
  section('SIMULATION SUMMARY');
  console.log(`  Duration:           ${simStats.durationSec}s`);
  console.log(`  HTTP requests:      ${simStats.httpRequests}`);
  console.log(`  Checkout attempts:  ${simStats.checkoutAttempts}`);
  console.log(`  Users simulated:    ${simStats.userCount}`);

  // ── Event capture ─────────────────────────────────────────────────────────
  section('EVENT CAPTURE');
  console.log(`  Total indexed:  ${BOLD}${total}${RESET} events\n`);

  console.log('  SEVERITY BREAKDOWN');
  console.log('  ┌─────────┬────────┬' + '─'.repeat(38) + '┐');
  for (const sev of SEV_ORDER) {
    const n = sevMap[sev] ?? 0;
    if (n === 0 && !['fatal','error'].includes(sev)) continue;
    const c   = color(sev, sev.padEnd(7));
    const num = pad(n, 6);
    const b   = bar(n, maxSevCount);
    console.log(`  │ ${c} │ ${num} │ ${b} │`);
  }
  console.log('  └─────────┴────────┴' + '─'.repeat(38) + '┘');

  // ── Activity timeline (sparkline of last 24 active hours) ─────────────────
  const activeSlots = dashboard.eventsOverTime.filter((s) => s.doc_count > 0);
  if (activeSlots.length > 0) {
    section('ACTIVITY TIMELINE (active hours only)');
    const slotMax = Math.max(...activeSlots.map((s) => s.doc_count), 1);
    const SPARKS  = ' ▁▂▃▄▅▆▇█';
    const spark   = activeSlots
      .map((s) => SPARKS[Math.min(8, Math.round((s.doc_count / slotMax) * 8))] ?? ' ')
      .join('');
    const startHr = activeSlots[0]!.key_as_string.slice(11, 16);
    const endHr   = activeSlots[activeSlots.length - 1]!.key_as_string.slice(11, 16);
    console.log(`  ${startHr} ${spark} ${endHr} UTC`);
    console.log(`  Peak: ${slotMax} events/hr  ·  Active hours: ${activeSlots.length}`);
  }

  // ── Top events ─────────────────────────────────────────────────────────────
  section('TOP EVENTS BY FREQUENCY');

  const topN = errorIntelligence.topErrors?.slice(0, 10) ?? [];
  if (topN.length === 0) {
    console.log('  (no data)');
  } else {
    const maxCount = topN[0]!.count;
    for (let i = 0; i < topN.length; i++) {
      const e       = topN[i]!;
      const fp      = e.fingerprint ?? '';
      const sev     = fp.startsWith('Error:') ? 'error'
                    : fp.startsWith('log:')   ? (
                        e.message.toLowerCase().includes('out-of-stock') ? 'warn' :
                        e.message.toLowerCase().includes('timeout') ? 'fatal' : 'info'
                      )
                    : 'info';
      const sevLabel = color(sev, sev.toUpperCase().padEnd(5));
      const countStr = `[${pad(e.count, 3)}×]`;
      const msg      = e.message.slice(0, 60).padEnd(60);
      const users    = e.affectedUsers.length > 0
        ? `  users: ${e.affectedUsers.slice(0, 3).join(', ')}${e.affectedUsers.length > 3 ? ` +${e.affectedUsers.length - 3}` : ''}`
        : '';
      console.log(`  #${String(i + 1).padEnd(2)} ${countStr}  ${msg}  ${sevLabel}${DIM}${users}${RESET}`);
    }
  }

  // ── Checkout funnel ────────────────────────────────────────────────────────
  section('CHECKOUT FUNNEL');
  const attempts  = simStats.checkoutAttempts;
  const successes = simStats.successCheckouts;
  const errors    = searchCounts.error;
  const fatals    = searchCounts.fatal;
  const other     = attempts - successes - errors - fatals;

  console.log(`  Attempts:          ${pad(attempts, 4)}`);
  console.log(`  ├─ Successful:     ${pad(successes, 4)}  ${pct(successes, attempts)}  ${bar(successes, attempts, 20)}`);
  console.log(`  ├─ Payment failed: ${pad(errors, 4)}  ${pct(errors, attempts)}  ${color('error', bar(errors, attempts, 20))}  (expected ~20%)`);
  console.log(`  ├─ DB timeout:     ${pad(fatals, 4)}  ${pct(fatals, attempts)}  ${color('fatal', bar(fatals, attempts, 20))}  (expected ~5%)`);
  if (other > 0) {
    console.log(`  └─ Other:          ${pad(other, 4)}  ${pct(other, attempts)}`);
  }

  // ── Error spotlight ────────────────────────────────────────────────────────
  const errorEntry = topN.find((e) => e.fingerprint?.startsWith('Error:') && e.message.includes('Payment'));
  const fatalEntry = topN.find((e) => e.message.includes('Timeout') || e.message.includes('timeout'));

  if (errorEntry ?? fatalEntry) {
    section('ERROR SPOTLIGHT');

    if (errorEntry) {
      console.log(`  ${color('error', '● PAYMENT FAILURE')}  (${errorEntry.count} occurrences)`);
      console.log(`    Message:        ${errorEntry.message.slice(0, 70)}`);
      console.log(`    First seen:     ${fmt(errorEntry.firstSeen)}`);
      console.log(`    Last seen:      ${fmt(errorEntry.lastSeen)}`);
      if (errorEntry.affectedUsers.length > 0) {
        console.log(`    Affected users: ${errorEntry.affectedUsers.join(', ')}`);
      }
    }

    if (fatalEntry) {
      console.log(`\n  ${color('fatal', '● DATABASE TIMEOUT')}  (${fatalEntry.count} occurrence${fatalEntry.count > 1 ? 's' : ''})`);
      console.log(`    Message:    ${fatalEntry.message.slice(0, 70)}`);
      console.log(`    First seen: ${fmt(fatalEntry.firstSeen)}`);
      console.log(`    Last seen:  ${fmt(fatalEntry.lastSeen)}`);
    }
  }

  // ── Response time percentiles ──────────────────────────────────────────────
  const p = dashboard.responseTimePercentiles ?? {};
  const hasP = Object.values(p).some((v) => v !== null);
  if (hasP) {
    section('RESPONSE TIME PERCENTILES  (payload.responseTimeMs)');
    console.log(`  p50:  ${p['50.0'] !== null ? `${p['50.0']}ms` : 'n/a'}`);
    console.log(`  p75:  ${p['75.0'] !== null ? `${p['75.0']}ms` : 'n/a'}`);
    console.log(`  p95:  ${p['95.0'] !== null ? `${p['95.0']}ms` : 'n/a'}`);
    console.log(`  p99:  ${p['99.0'] !== null ? `${p['99.0']}ms` : 'n/a'}`);
  }

  // ── Unique fingerprints ────────────────────────────────────────────────────
  section('DEDUPLICATION');
  console.log(`  Unique event fingerprints:  ${dashboard.uniqueErrorCount}`);
  console.log(`  Total raw events:           ${total}`);
  const ratio = total > 0 ? (dashboard.uniqueErrorCount / total * 100).toFixed(1) : '0.0';
  console.log(`  Noise ratio (unique/total): ${ratio}%`);

  // ── Quota ──────────────────────────────────────────────────────────────────
  section('QUOTA USAGE');
  const used     = quota.eventsThisMonth ?? 0;
  const limit    = Number(quota.eventQuotaPerMonth ?? 0);
  const pctUsed  = limit > 0 ? ((used / limit) * 100).toFixed(4) : '0.0000';
  console.log(`  Plan:         ${quota.planName ?? 'unknown'}`);
  console.log(`  This month:   ${used.toLocaleString()} / ${limit.toLocaleString()} events`);
  console.log(`  Used:         ${pctUsed}%  ${bar(used, limit, 30)}`);

  console.log('\n' + '═'.repeat(64) + '\n');
}

// ── JSON report save ──────────────────────────────────────────────────────────

export function saveReport(input: ReportInput): string {
  const reportsDir = join(__dir, 'reports');
  mkdirSync(reportsDir, { recursive: true });

  const ts       = new Date().toISOString().replace(/[:.]/g, '-').slice(0, 19);
  const filename = `shopfast-${ts}.json`;
  const filepath = join(reportsDir, filename);

  const { simStats, searchCounts, quota } = input;
  const sevMap: Record<string, number> = {};
  for (const b of input.dashboard.bySeverity) sevMap[b.key] = b.doc_count;

  const report = {
    generatedAt:    new Date().toISOString(),
    tenantId:       input.tenantId,
    projectId:      input.projectId,
    simulation: {
      durationSec:       simStats.durationSec,
      httpRequests:      simStats.httpRequests,
      checkoutAttempts:  simStats.checkoutAttempts,
      usersSimulated:    simStats.userCount,
    },
    eventCapture: {
      total:             input.dashboard.totalCount?.value ?? searchCounts.total,
      bySeverity:        sevMap,
      uniqueFingerprints: input.dashboard.uniqueErrorCount,
    },
    checkoutFunnel: {
      attempts:   simStats.checkoutAttempts,
      successful: simStats.successCheckouts,
      errors:     searchCounts.error,
      fatals:     searchCounts.fatal,
      successRate: simStats.checkoutAttempts > 0
        ? `${((simStats.successCheckouts / simStats.checkoutAttempts) * 100).toFixed(1)}%`
        : '0%',
    },
    topEvents: input.errorIntelligence.topErrors?.slice(0, 10) ?? [],
    responseTimePercentiles: input.dashboard.responseTimePercentiles,
    quota: {
      plan:              quota.planName,
      eventsThisMonth:   quota.eventsThisMonth,
      quotaLimit:        quota.eventQuotaPerMonth,
      percentUsed:       quota.percentUsed,
    },
  };

  writeFileSync(filepath, JSON.stringify(report, null, 2));
  return filepath;
}
