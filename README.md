# PulseBoard

Multi-tenant application monitoring and log analytics platform. Sentry × Datadog × Mixpanel, condensed into a single production-grade backend.

## Stack

| Layer | Technology |
|---|---|
| API | Fastify + TypeScript (strict ESM) |
| PostgreSQL | `pg` — tenants, billing, RLS, partitioning |
| MongoDB | `mongodb` — raw events, logs, config, change streams |
| Elasticsearch | `@elastic/elasticsearch` — log search, alerting, dashboards |
| Redis | `ioredis` — rate limiting, cache, streams, pub/sub, leaderboard |

## Prerequisites

- Docker + Docker Compose
- Node.js ≥ 20
- k6 (for load tests): `brew install k6`

## Quick Start

```bash
# 1. Start all datastores
docker compose up -d postgres mongo elasticsearch redis

# 2. Install dependencies
npm install

# 3. Verify everything is wired up correctly
npm run doctor

# 4. Run migrations
npm run migrate

# 5. Seed all four stores (~5–10 min)
npm run seed

# 6. Start all services
docker compose up -d api ingest-worker change-stream-worker alert-worker jobs

# API is available at http://localhost:3000
# Health: http://localhost:3000/health
# Docs:   http://localhost:3000/docs
```

## Development (individual services)

The `.env` file is pre-configured for the **test stack** (isolated ports that don't conflict with a locally-installed Postgres or the production Docker stack).

```bash
# Start the test-stack datastores
docker compose -f docker-compose.test.yml up -d

# Verify everything before you start coding
npm run doctor

npm run dev:api               # Fastify API server (port 3000)
npm run dev:ingest-worker     # XREADGROUP consumer
npm run dev:change-stream     # Mongo change stream → Redis pub/sub
npm run dev:alert-worker      # PSUBSCRIBE alerts:fatal:* + dedup
npm run dev:jobs              # Cron dispatcher
```

> **Why test-stack ports?**  A locally-installed Postgres binds `127.0.0.1:5432` and shadows Docker's port-mapped container. The test stack uses `5433/27018/9201/6380` to sidestep all conflicts. The `docker-compose.yml` services always override host/port with Docker service names, so the `.env` values only matter when running services locally.

## Running Tests

### Integration tests (require running datastores)

```bash
# Start isolated test datastores (separate ports from dev)
docker compose -f docker-compose.test.yml up -d

# Run all integration tests
npm run test:integration

# Run a specific test file
npx vitest run tests/integration/r5-leaderboard.test.ts
```

Test ports (test compose):
- PostgreSQL: 5433
- MongoDB: 27018
- Elasticsearch: 9201
- Redis: 6380

### Load tests (require running API + seed data)

```bash
# Ingest throughput — target 10k events/min, p95 < 100ms
npm run test:load
# or directly:
k6 run tests/load/ingest.k6.js

# Search latency — target p95 < 500ms
k6 run tests/load/search.k6.js

# With custom target:
k6 run --env API_URL=http://localhost:3000 --env API_KEY=<key> tests/load/ingest.k6.js
```

## Project Structure

```
src/
  config.ts          # Zod-validated env; fail-fast on startup
  logger.ts          # pino structured logging with redaction
  errors.ts          # AppError hierarchy + Fastify error handler
  db/
    postgres.ts      # Pool + withTenant() RLS helper
    mongo.ts         # MongoClient singleton + typed collections
    elastic.ts       # ES client, ILM policies, index templates
    redis.ts         # ioredis + Lua script loader
  domain/
    ingestion.ts     # 7-stage event pipeline with traceStage()
    tenants.ts       # Onboarding saga with inline compensations
    alerts.ts        # Alert rules + percolator sync + dedup fire
    reports.ts       # Error intelligence ($facet) + quota report
    consistency.ts   # Cross-DB audit (X3)
    retention.ts     # Per-project batched Mongo deleteMany
  lib/
    circuit-breaker.ts  # closed/open/half-open per dependency
    cache.ts            # cache-aside + stampede lock + SCAN+UNLINK
    rate-limit.ts       # Sliding window via Lua
    auth.ts             # JWT (users) + API key (SDK ingest)
    lua/
      sliding-window.lua
      dedup-fire.lua
  workers/
    ingest-consumer.ts    # XREADGROUP + XPENDING reclaim + DLQ
    change-stream.ts      # Leader-elected Mongo CS → pub/sub
    alert-subscriber.ts   # PSUBSCRIBE + dedup-fire
  routes/
    ingest.ts, auth.ts, tenants.ts, projects.ts, alerts.ts,
    search.ts, reports.ts, leaderboard.ts, health.ts, metrics.ts
migrations/
  0001_init.sql           # plans, tenants, users, members
  0002_projects.sql       # projects, monthly_usage, usage_dedup
  0003_alert_rules.sql
  0004_billing_events_partitioned.sql
  0005_rls_policies.sql   # app_user role + RLS policies
  0006_audit_log.sql      # audit trigger + pl/pgsql fn
  0007_indexes.sql        # GIN, composite, partial indexes
tests/
  integration/           # 27 real-DB test files (one per task)
  load/                  # k6 scripts for ingest + search
```

## Architecture Decisions

See [DECISIONS.md](./DECISIONS.md) for the full reasoning behind every non-obvious choice. Key decisions:

- **At-least-once + deterministic `eventId`** — uuidv7 at the HTTP edge; used as Mongo `_id`, ES `_id`, usage dedup key, alert fire-lock suffix. Makes every consumer safely idempotent.
- **`SET LOCAL` inside transactions** — tenant context never leaks across pooled connections.
- **ES retention tiers** (30d/90d/365d) vs per-project ILM — keeps ES cluster state O(tiers) not O(projects).
- **Mongo as canonical, ES as projection** — ES can be rebuilt from Mongo by `eventId`-idempotent reindex.
- **Inline saga compensations** — no generic framework; one concrete `onboardTenant()` function with explicit rollbacks.

## Environment Variables

Copy `.env.example` and fill in values. All vars are validated at startup via Zod — the process aborts if any required var is missing or malformed.

```bash
cp .env.example .env
```

## Metrics

`GET /metrics` returns JSON with Redis-backed counters:

- `ingest.lag` — stream depth (events not yet consumed)
- `ingest.dlq.size` — dead-letter queue depth
- `cache.hit` / `cache.miss` — per-project
- `breaker.<dep>.state` — 0=closed / 1=half-open / 2=open
- `alert.fanout.failures` — dedup-fire failures
