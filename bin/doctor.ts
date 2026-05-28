/**
 * PulseBoard doctor — pre-flight environment and connectivity check.
 * Intentionally self-contained: reads .env manually so it can report
 * missing vars before config.ts's Zod parse would throw.
 *
 * Usage: npm run doctor
 */
import { execSync } from 'node:child_process';
import { existsSync, readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const ROOT = join(dirname(fileURLToPath(import.meta.url)), '..');

// ---------------------------------------------------------------------------
// Output helpers
// ---------------------------------------------------------------------------

const c = {
  pass:  (s: string) => `\x1b[32m✓\x1b[0m ${s}`,
  fail:  (s: string) => `\x1b[31m✗\x1b[0m ${s}`,
  warn:  (s: string) => `\x1b[33m⚠\x1b[0m ${s}`,
  bold:  (s: string) => `\x1b[1m${s}\x1b[0m`,
  dim:   (s: string) => `\x1b[2m${s}\x1b[0m`,
  cyan:  (s: string) => `\x1b[36m${s}\x1b[0m`,
};

let errorCount = 0;
let warnCount  = 0;

function pass(msg: string, detail = '')  { console.log(`  ${c.pass(msg)}${detail ? c.dim('  ' + detail) : ''}`); }
function fail(msg: string, detail = '')  { console.log(`  ${c.fail(msg)}${detail ? '  ' + detail : ''}`); errorCount++; }
function warn(msg: string, detail = '')  { console.log(`  ${c.warn(msg)}${detail ? c.dim('  ' + detail) : ''}`); warnCount++; }
function sub (msg: string, detail = '')  { console.log(`     ${c.dim('·')} ${msg}${detail ? c.dim('  ' + detail) : ''}`); }
function section(title: string)          { console.log(`\n${c.bold(title)}`); }

// ---------------------------------------------------------------------------
// .env parser — no dotenv dep needed
// ---------------------------------------------------------------------------

function parseEnvFile(path: string): Record<string, string> {
  if (!existsSync(path)) return {};
  const out: Record<string, string> = {};
  for (const raw of readFileSync(path, 'utf8').split('\n')) {
    const line = raw.trim();
    if (!line || line.startsWith('#')) continue;
    const eq = line.indexOf('=');
    if (eq === -1) continue;
    out[line.slice(0, eq).trim()] = line.slice(eq + 1).trim();
  }
  return out;
}

// ---------------------------------------------------------------------------
// Required + optional vars (mirrors config.ts)
// ---------------------------------------------------------------------------

const REQUIRED_VARS: string[] = [
  'JWT_SECRET',
  'PG_HOST', 'PG_DATABASE', 'PG_USER', 'PG_PASSWORD',
  'MONGO_URL', 'MONGO_DB_NAME',
];

const OPTIONAL_VARS: Array<{ key: string; default: string }> = [
  { key: 'NODE_ENV',                   default: 'development' },
  { key: 'LOG_LEVEL',                  default: 'info'        },
  { key: 'API_PORT',                   default: '3000'        },
  { key: 'JWT_EXPIRY',                 default: '15m'         },
  { key: 'JWT_REFRESH_EXPIRY',         default: '7d'          },
  { key: 'PG_PORT',                    default: '5432'        },
  { key: 'PG_POOL_MIN',               default: '2'           },
  { key: 'PG_POOL_MAX',               default: '20'          },
  { key: 'ES_URL',                     default: 'http://localhost:9200' },
  { key: 'REDIS_HOST',                 default: 'localhost'   },
  { key: 'REDIS_PORT',                 default: '6379'        },
  { key: 'INGEST_WORKER_CONCURRENCY',  default: '3'           },
  { key: 'INGEST_XPENDING_RECLAIM_MS', default: '30000'       },
  { key: 'INGEST_STREAM_MAX_LEN',      default: '1000000'     },
  { key: 'CACHE_DEFAULT_TTL_SECONDS',  default: '300'         },
];

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main(): Promise<void> {
  console.log(`\n${c.bold(c.cyan('PulseBoard — Doctor'))}`);
  console.log(c.dim('Checking Node.js, Docker, environment, and datastore connectivity…'));

  // -------------------------------------------------------------------------
  // 1. Node.js version
  // -------------------------------------------------------------------------
  section('Node.js');
  const [major = 0] = process.versions.node.split('.').map(Number);
  if (major >= 20) {
    pass(`Node.js ${process.versions.node}`);
  } else {
    fail(`Node.js ${process.versions.node}`, 'requires ≥ 20  →  https://nodejs.org');
  }

  // -------------------------------------------------------------------------
  // 2. Docker
  // -------------------------------------------------------------------------
  section('Docker');
  try {
    const ver = execSync('docker --version', { encoding: 'utf8', stdio: 'pipe' }).trim();
    pass(ver);
    try {
      execSync('docker info', { encoding: 'utf8', stdio: 'pipe' });
      pass('Docker daemon is running');
    } catch {
      fail('Docker daemon is not running', 'start Docker Desktop, or: sudo systemctl start docker');
    }
  } catch {
    fail('Docker not found', 'install Docker Desktop  →  https://docs.docker.com/get-docker/');
  }

  // -------------------------------------------------------------------------
  // 3. node_modules
  // -------------------------------------------------------------------------
  section('Dependencies');
  if (existsSync(join(ROOT, 'node_modules', 'fastify'))) {
    pass('node_modules installed');
  } else {
    fail('node_modules missing', 'run: npm install');
  }

  // -------------------------------------------------------------------------
  // 4. .env and environment variables
  // -------------------------------------------------------------------------
  section('Environment (.env)');
  const envPath = join(ROOT, '.env');
  const fileEnv = parseEnvFile(envPath);

  if (!existsSync(envPath)) {
    fail('.env not found', 'run: cp .env.example .env  then fill in values');
  } else {
    pass('.env file found');
  }

  // Merge file vars into process.env so connectivity checks pick them up
  for (const [k, v] of Object.entries(fileEnv)) {
    process.env[k] ??= v;
  }

  const resolvedEnv = (key: string): string | undefined =>
    process.env[key] ?? fileEnv[key];

  // Required vars
  let envOk = true;
  for (const key of REQUIRED_VARS) {
    const val = resolvedEnv(key);
    if (!val) {
      fail(key, 'missing — required');
      envOk = false;
    } else if (key === 'JWT_SECRET' && val.length < 32) {
      fail(key, `too short (${val.length} chars, need ≥ 32)`);
      envOk = false;
    } else {
      const display = (key.includes('PASSWORD') || key.includes('SECRET')) ? '[redacted]' : val;
      pass(key, display);
    }
  }

  // Optional vars — warn if absent (they have defaults so the app still starts)
  for (const { key, default: def } of OPTIONAL_VARS) {
    const val = resolvedEnv(key);
    if (!val) {
      warn(key, `not set — will default to "${def}"`);
    }
  }

  if (!envOk) {
    console.log(`\n  ${c.fail('Skipping datastore checks')} — fix env vars above first`);
  } else {
    // -----------------------------------------------------------------------
    // 5. Datastore connectivity
    // -----------------------------------------------------------------------
    section('Datastores');

    // ── PostgreSQL ──────────────────────────────────────────────────────────
    {
      const host = resolvedEnv('PG_HOST') ?? 'localhost';
      const port = parseInt(resolvedEnv('PG_PORT') ?? '5432', 10);
      const database = resolvedEnv('PG_DATABASE') ?? 'pulseboard';
      const label = `PostgreSQL  ${host}:${port}/${database}`;
      try {
        const { default: pg } = await import('pg');
        const client = new pg.Client({
          host, port, database,
          user:     resolvedEnv('PG_USER')     ?? 'postgres',
          password: resolvedEnv('PG_PASSWORD') ?? '',
          connectionTimeoutMillis: 4_000,
        });
        const t = Date.now();
        await client.connect();
        await client.query('SELECT 1');
        pass(label, `${Date.now() - t}ms`);

        // Migration status
        try {
          const { rows } = await client.query<{ count: string; last: string }>(
            `SELECT COUNT(*) AS count, MAX(filename) AS last FROM schema_migrations`,
          );
          const n    = parseInt(rows[0]?.count ?? '0', 10);
          const last = rows[0]?.last ?? '—';
          if (n > 0) {
            sub(`${n} migration${n !== 1 ? 's' : ''} applied`, `latest: ${last}`);
          } else {
            warn('No migrations applied', 'run: npm run migrate');
          }
        } catch {
          warn('schema_migrations table not found', 'run: npm run migrate');
        }

        await client.end();
      } catch (err) {
        fail(label, err instanceof Error ? err.message : String(err));
      }
    }

    // ── MongoDB ─────────────────────────────────────────────────────────────
    {
      const url = resolvedEnv('MONGO_URL') ?? 'mongodb://localhost:27017';
      const label = `MongoDB  ${url}`;
      try {
        const { MongoClient, ServerApiVersion } = await import('mongodb');
        const client = new MongoClient(url, {
          serverApi: { version: ServerApiVersion.v1, strict: true, deprecationErrors: true },
          serverSelectionTimeoutMS: 4_000,
          connectTimeoutMS:         4_000,
        });
        const t = Date.now();
        await client.connect();
        await client.db('admin').command({ ping: 1 });
        pass(label, `${Date.now() - t}ms`);

        // Replica set status — skip check when directConnection=true because
        // that mode intentionally bypasses topology discovery (the RS still exists).
        const isDirectConn = url.includes('directConnection=true') || url.includes('directConnection=1');
        if (!isDirectConn) {
          try {
            const status = await client.db('admin').command({ replSetGetStatus: 1 }) as {
              ok: number; set: string; members: unknown[];
            };
            if (status.ok === 1) {
              sub(`Replica set "${status.set}"`, `${status.members.length} member(s)`);
            }
          } catch {
            warn('Replica set not detected', 'change streams need ?replicaSet=rs0 in MONGO_URL');
          }
        } else {
          sub('directConnection=true', 'bypasses RS topology discovery — fine for local dev');
        }

        await client.close();
      } catch (err) {
        fail(label, err instanceof Error ? err.message : String(err));
      }
    }

    // ── Elasticsearch ───────────────────────────────────────────────────────
    {
      const url = resolvedEnv('ES_URL') ?? 'http://localhost:9200';
      const label = `Elasticsearch  ${url}`;
      try {
        const t = Date.now();
        const res = await fetch(`${url}/_cluster/health`, { signal: AbortSignal.timeout(4_000) });
        const latency = Date.now() - t;
        if (res.ok) {
          const body = await res.json() as { status: string; number_of_nodes: number };
          const status = body.status;
          if (status === 'green' || status === 'yellow') {
            pass(label, `${latency}ms`);
            sub(`Cluster status: ${status}`, `${body.number_of_nodes} node(s)`);
          } else {
            // Red is normal for ~60s after first start while shards allocate
            warn(label, `cluster status: ${status} — if just started, wait 60s and retry`);
          }
        } else {
          fail(label, `HTTP ${res.status}`);
        }
      } catch (err) {
        fail(label, err instanceof Error ? err.message : String(err));
      }
    }

    // ── Redis ───────────────────────────────────────────────────────────────
    {
      const host = resolvedEnv('REDIS_HOST') ?? 'localhost';
      const port = parseInt(resolvedEnv('REDIS_PORT') ?? '6379', 10);
      const label = `Redis  ${host}:${port}`;
      try {
        const { Redis } = await import('ioredis');
        const redis = new Redis({
          host, port,
          lazyConnect:          true,
          connectTimeout:       4_000,
          maxRetriesPerRequest: 0,
          retryStrategy:        () => null,
        });
        const t = Date.now();
        await redis.connect();
        const pong = await redis.ping();
        const latency = Date.now() - t;
        await redis.quit();
        if (pong === 'PONG') {
          pass(label, `${latency}ms`);
        } else {
          warn(label, `unexpected PING response: ${pong}`);
        }
      } catch (err) {
        fail(label, err instanceof Error ? err.message : String(err));
      }
    }
  }

  // -------------------------------------------------------------------------
  // 6. Summary
  // -------------------------------------------------------------------------
  section('Summary');
  if (errorCount === 0 && warnCount === 0) {
    console.log(`  ${c.pass('All checks passed')}  — stack is ready\n`);
    process.exit(0);
  }
  if (errorCount > 0) console.log(`  ${c.fail(`${errorCount} error${errorCount !== 1 ? 's' : ''} found — fix above before starting`)}`);
  if (warnCount  > 0) console.log(`  ${c.warn(`${warnCount} warning${warnCount  !== 1 ? 's' : ''} — non-blocking, but worth reviewing`)}`);
  console.log();
  process.exit(errorCount > 0 ? 1 : 0);
}

main().catch((err: unknown) => {
  console.error('\nDoctor crashed unexpectedly:', err);
  process.exit(1);
});
