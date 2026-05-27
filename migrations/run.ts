import { readFileSync, readdirSync } from 'node:fs';
import { join, resolve, dirname } from 'node:path';
import { fileURLToPath } from 'node:url';
import pg from 'pg';

// Resolve the migrations directory relative to the project root so this works
// both when run directly via tsx (src at project root) and when compiled to
// dist/ inside Docker (where SQL files live at /app/migrations/, not /app/dist/migrations/).
const _thisDir = dirname(fileURLToPath(import.meta.url));
const MIGRATIONS_DIR = _thisDir.endsWith('dist/migrations')
  ? resolve(_thisDir, '../../migrations')
  : _thisDir;

const pool = new pg.Pool({
  host: process.env['PG_HOST'] ?? 'localhost',
  port: Number(process.env['PG_PORT'] ?? 5432),
  database: process.env['PG_DATABASE'] ?? 'pulseboard',
  user: process.env['PG_USER'] ?? 'postgres',
  password: process.env['PG_PASSWORD'] ?? 'postgres',
});

async function run(): Promise<void> {
  const client = await pool.connect();
  try {
    // Ensure tracking table exists
    await client.query(`
      CREATE TABLE IF NOT EXISTS schema_migrations (
        filename TEXT PRIMARY KEY,
        applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
      )
    `);

    // Find and sort all SQL migration files
    const files = readdirSync(MIGRATIONS_DIR)
      .filter((f) => f.endsWith('.sql'))
      .sort();

    for (const file of files) {
      const { rows } = await client.query(
        'SELECT 1 FROM schema_migrations WHERE filename = $1',
        [file],
      );
      if (rows.length > 0) {
        console.log(`  skip  ${file}`);
        continue;
      }

      console.log(`  apply ${file}`);
      const sql = readFileSync(join(MIGRATIONS_DIR, file), 'utf8');
      await client.query('BEGIN');
      try {
        await client.query(sql);
        await client.query(
          'INSERT INTO schema_migrations (filename) VALUES ($1)',
          [file],
        );
        await client.query('COMMIT');
      } catch (err) {
        await client.query('ROLLBACK');
        throw err;
      }
    }

    console.log('Migrations complete.');
  } finally {
    client.release();
    await pool.end();
  }
}

run().catch((err) => {
  console.error('Migration failed:', err);
  process.exit(1);
});
