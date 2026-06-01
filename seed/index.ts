/**
 * seed/index.ts
 * Orchestrator: runs all seeders sequentially with timing.
 *
 * Run with: tsx seed/index.ts
 * Or via npm:  npm run seed   (mapped to tsx seed/index.ts in package.json)
 */

import pg from 'pg';
import { MongoClient } from 'mongodb';
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

  // ── 2.5. MongoDB project_configs — sync with PG project IDs ────────────────
  // project_configs documents are keyed by the same ID as PG projects. Without
  // this step, a consistency audit (X3) would report every PG project as
  // missing_mongo_config because seed-mongo.ts uses its own random project IDs.
  console.log('\n[seed] Seeding project_configs from PG project IDs…');
  console.time('project_configs');
  await seedProjectConfigs();
  console.timeEnd('project_configs');

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

async function seedProjectConfigs(): Promise<void> {
  const PG_HOST     = process.env['PG_HOST']     ?? 'localhost';
  const PG_PORT     = Number(process.env['PG_PORT']     ?? 5432);
  const PG_DATABASE = process.env['PG_DATABASE'] ?? 'pulseboard';
  const PG_USER     = process.env['PG_USER']     ?? 'postgres';
  const PG_PASSWORD = process.env['PG_PASSWORD'] ?? 'postgres';
  const MONGO_URL   = process.env['MONGO_URL']   ?? 'mongodb://localhost:27017/?replicaSet=rs0';
  const MONGO_DB    = process.env['MONGO_DB_NAME'] ?? 'pulseboard';

  const pool = new pg.Pool({ host: PG_HOST, port: PG_PORT, database: PG_DATABASE, user: PG_USER, password: PG_PASSWORD, max: 3 });
  const mongoClient = new MongoClient(MONGO_URL, { serverSelectionTimeoutMS: 15_000 });

  try {
    await mongoClient.connect();
    const db = mongoClient.db(MONGO_DB);
    const col = db.collection('project_configs');

    // Read all PG projects in batches
    let offset = 0;
    const BATCH = 2_000;
    let total = 0;

    while (true) {
      const { rows } = await pool.query<{ id: string; tenant_id: string; name: string; retention_days: number }>(
        `SELECT p.id, p.tenant_id, p.name, pl.retention_days
         FROM projects p
         JOIN tenants t ON t.id = p.tenant_id
         JOIN plans pl ON pl.id = t.plan_id
         WHERE NOT p.is_archived
         ORDER BY p.id
         LIMIT $1 OFFSET $2`,
        [BATCH, offset],
      );
      if (rows.length === 0) break;

      const docs = rows.map((r) => ({
        _id: r.id,
        tenantId: r.tenant_id,
        name: r.name,
        retentionDays: r.retention_days,
        alertsEnabled: true,
        createdAt: new Date(),
        updatedAt: new Date(),
        settings: { samplingRate: 1.0, ignoredErrors: [] as string[], retentionDays: r.retention_days },
      }));

      // Upsert to be idempotent (safe to re-run)
      const ops = docs.map((d) => ({
        updateOne: {
          filter: { _id: d._id },
          update: { $setOnInsert: d },
          upsert: true,
        },
      }));
      await col.bulkWrite(ops as any, { ordered: false });
      total += docs.length;
      offset += rows.length;
      if (offset % 10_000 === 0) console.log(`  [project_configs] upserted ${total.toLocaleString()} docs`);
    }

    console.log(`[project_configs] Done — ${total.toLocaleString()} docs upserted`);
  } finally {
    await mongoClient.close();
    await pool.end();
  }
}

main().catch((err) => {
  console.error('[seed] FATAL:', err);
  process.exit(1);
});
