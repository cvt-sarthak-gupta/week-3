/**
 * seed/index.ts
 * Orchestrator: runs all seeders sequentially with timing.
 *
 * Run with: tsx seed/index.ts
 * Or via npm:  npm run seed   (mapped to tsx seed/index.ts in package.json)
 */

import { seedPostgres } from './seed-postgres.js';
import { seedMongo }    from './seed-mongo.js';
import { seedElastic }  from './seed-elastic.js';
import { seedRedis }    from './seed-redis.js';

async function main(): Promise<void> {
  console.time('total-seed');

  // ── 1. PostgreSQL ──────────────────────────────────────────────────────────
  console.log('\n══════════════════════════════════════════');
  console.log('  Stage 1: PostgreSQL');
  console.log('══════════════════════════════════════════\n');
  console.time('postgres');
  await seedPostgres();
  console.timeEnd('postgres');

  // ── 2. MongoDB ─────────────────────────────────────────────────────────────
  console.log('\n══════════════════════════════════════════');
  console.log('  Stage 2: MongoDB');
  console.log('══════════════════════════════════════════\n');
  console.time('mongo');
  await seedMongo();
  console.timeEnd('mongo');

  // ── 3. Elasticsearch ───────────────────────────────────────────────────────
  console.log('\n══════════════════════════════════════════');
  console.log('  Stage 3: Elasticsearch');
  console.log('══════════════════════════════════════════\n');
  console.time('elastic');
  await seedElastic();
  console.timeEnd('elastic');

  // ── 4. Redis ───────────────────────────────────────────────────────────────
  console.log('\n══════════════════════════════════════════');
  console.log('  Stage 4: Redis');
  console.log('══════════════════════════════════════════\n');
  console.time('redis');
  await seedRedis();
  console.timeEnd('redis');

  // ── Done ───────────────────────────────────────────────────────────────────
  console.log('\n══════════════════════════════════════════');
  console.log('  All stages complete');
  console.log('══════════════════════════════════════════\n');
  console.timeEnd('total-seed');

  process.exit(0);
}

main().catch((err) => {
  console.error('[seed] FATAL:', err);
  process.exit(1);
});
