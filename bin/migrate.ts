/**
 * PulseBoard migration runner
 *
 * - Reads all .sql files from migrations/ in sorted numerical order
 * - Tracks applied migrations in a schema_migrations table
 * - Connects as the postgres superuser (PG_ADMIN_USER/PG_ADMIN_PASSWORD or
 *   falls back to PG_USER/PG_PASSWORD) — app_user lacks DDL permissions
 * - Each unapplied migration runs in its own transaction; on failure the
 *   runner exits with code 1 so CI catches it immediately
 *
 * Usage:
 *   tsx bin/migrate.ts
 *   node dist/bin/migrate.js
 */

import { createRequire } from 'module';
import { fileURLToPath } from 'url';
import path from 'path';
import fs from 'fs';
import pg from 'pg';

// CommonJS compat shim (required for any CJS-only packages if added later)
const require = createRequire(import.meta.url);

// __dirname equivalent for ESM
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// ---------------------------------------------------------------------------
// Config resolution
// ---------------------------------------------------------------------------

function env(key: string, fallback?: string): string {
  const val = process.env[key] ?? fallback;
  if (val === undefined) {
    console.error(`[migrate] Missing required env var: ${key}`);
    process.exit(1);
  }
  return val;
}

const host     = env('PG_HOST', 'localhost');
const port     = parseInt(env('PG_PORT', '5432'), 10);
const database = env('PG_DATABASE', 'pulseboard');
const user     = env('PG_ADMIN_USER',     process.env['PG_USER']     ?? 'postgres');
const password = env('PG_ADMIN_PASSWORD', process.env['PG_PASSWORD'] ?? '');

// ---------------------------------------------------------------------------
// Migration runner
// ---------------------------------------------------------------------------

const MIGRATIONS_DIR = path.resolve(__dirname, '../migrations');
const SCHEMA_MIGRATIONS_DDL = `
  CREATE TABLE IF NOT EXISTS schema_migrations (
    filename   TEXT        PRIMARY KEY,
    applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
  )
`;

async function run(): Promise<void> {
  const client = new pg.Client({ host, port, database, user, password });

  try {
    await client.connect();
    console.log(`[migrate] Connected to ${database} at ${host}:${port} as ${user}`);
  } catch (err) {
    console.error('[migrate] Failed to connect to PostgreSQL:', err);
    process.exit(1);
  }

  try {
    // Ensure tracking table exists (outside any transaction — DDL is auto-committed)
    await client.query(SCHEMA_MIGRATIONS_DDL);
    console.log('[migrate] schema_migrations table ready');

    // Fetch already-applied migrations
    const { rows } = await client.query<{ filename: string }>(
      'SELECT filename FROM schema_migrations ORDER BY filename',
    );
    const applied = new Set(rows.map((r) => r.filename));

    // Collect .sql files in sorted order
    let files: string[];
    try {
      files = fs
        .readdirSync(MIGRATIONS_DIR)
        .filter((f) => f.endsWith('.sql'))
        .sort(); // lexicographic sort keeps 0001, 0002, … in order
    } catch (err) {
      console.error(`[migrate] Cannot read migrations directory (${MIGRATIONS_DIR}):`, err);
      process.exit(1);
    }

    if (files.length === 0) {
      console.log('[migrate] No migration files found — nothing to do');
      return;
    }

    let ranCount = 0;

    for (const filename of files) {
      if (applied.has(filename)) {
        console.log(`[migrate] Skipping (already applied): ${filename}`);
        continue;
      }

      const filepath = path.join(MIGRATIONS_DIR, filename);
      const sql = fs.readFileSync(filepath, 'utf8');

      console.log(`[migrate] Applying: ${filename} …`);

      try {
        await client.query('BEGIN');
        await client.query(sql);
        await client.query(
          'INSERT INTO schema_migrations (filename) VALUES ($1)',
          [filename],
        );
        await client.query('COMMIT');
        console.log(`[migrate] Applied:  ${filename}`);
        ranCount++;
      } catch (err) {
        await client.query('ROLLBACK').catch(() => {
          // ignore rollback error — connection may be in a bad state
        });
        console.error(`[migrate] Failed on migration: ${filename}`, err);
        process.exit(1);
      }
    }

    if (ranCount === 0) {
      console.log('[migrate] All migrations already applied — database is up to date');
    } else {
      console.log(`[migrate] Done — ${ranCount} migration(s) applied`);
    }
  } finally {
    await client.end();
  }
}

run();
