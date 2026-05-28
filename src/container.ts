import { PostgresPool } from './db/postgres/index.js';
import { MongoDatabase } from './db/mongo/index.js';
import { ElasticClient } from './db/elastic/index.js';
import { RedisClient } from './db/redis/index.js';
import { createBreakers, type CircuitBreakers } from './lib/circuit-breaker/index.js';
import { CacheService } from './lib/cache/cache.service.js';
import { RateLimitService } from './lib/rate-limit/rate-limit.service.js';
import { ApiKeyService } from './lib/auth/api-key.service.js';
import { AuthService } from './modules/auth/auth.service.js';
import { TenantService } from './modules/tenants/tenants.service.js';
import { ProjectService } from './modules/projects/projects.service.js';
import { IngestService } from './modules/ingest/ingest.service.js';
import { PipelineService } from './modules/pipeline/pipeline.service.js';
import { AlertsService } from './modules/alerts/alerts.service.js';
import { SearchService } from './modules/search/search.service.js';
import { ReportsService } from './modules/reports/reports.service.js';
import { LeaderboardService } from './modules/leaderboard/leaderboard.service.js';
import { HealthService } from './modules/health/health.service.js';
import { MetricsService } from './modules/metrics/metrics.service.js';
import { ConsistencyService } from './modules/consistency/consistency.service.js';
import { RetentionService } from './modules/retention/retention.service.js';
import { IngestWorker } from './workers/ingest/ingest.worker.js';
import { ChangeStreamWorker } from './workers/change-stream/change-stream.worker.js';
import { AlertWorker } from './workers/alert/alert.worker.js';
import { JobScheduler } from './jobs/scheduler.js';
import { logger } from './utils/logger.js';

// ---------------------------------------------------------------------------
// AppContainer interface
// ---------------------------------------------------------------------------

export interface AppContainer {
  // DB
  pool: PostgresPool;
  mongo: MongoDatabase;
  es: ElasticClient;
  redis: RedisClient;
  // Lib
  breakers: CircuitBreakers;
  cache: CacheService;
  rateLimit: RateLimitService;
  apiKey: ApiKeyService;
  // Services
  auth: AuthService;
  tenants: TenantService;
  projects: ProjectService;
  ingest: IngestService;
  pipeline: PipelineService;
  alerts: AlertsService;
  search: SearchService;
  reports: ReportsService;
  leaderboard: LeaderboardService;
  health: HealthService;
  metrics: MetricsService;
  consistency: ConsistencyService;
  retention: RetentionService;
  // Workers
  ingestWorker: IngestWorker;
  changeStreamWorker: ChangeStreamWorker;
  alertWorker: AlertWorker;
  // Scheduler
  scheduler: JobScheduler;
}

// ---------------------------------------------------------------------------
// createContainer — wires all dependencies in the correct order
// ---------------------------------------------------------------------------

export async function createContainer(): Promise<AppContainer> {
  // 1. Create DB clients
  const pool = new PostgresPool();
  const mongo = new MongoDatabase();
  const es = new ElasticClient();
  const redis = new RedisClient();

  // 2. Connect all
  logger.info('Connecting to all datastores...');
  await Promise.all([
    pool.healthCheck().then(() => logger.info('PostgreSQL pool ready')),
    mongo.connect().then(() => logger.info('MongoDB connected')),
    es.healthCheck().then(() => logger.info('Elasticsearch ready')),
    redis.connect().then(() => logger.info('Redis connected')),
  ]);

  // 3. Create lib layer
  const breakers = createBreakers();
  const cache = new CacheService(redis);
  const rateLimit = new RateLimitService(redis, pool);
  const apiKey = new ApiKeyService(redis, pool);

  // 4. Create services
  const auth = new AuthService(pool);
  const tenants = new TenantService(pool, mongo, es, redis);
  const projects = new ProjectService(pool, es, redis);
  const ingest = new IngestService(pool, redis, rateLimit);
  const pipeline = new PipelineService(pool, mongo, es, redis, breakers);
  const alerts = new AlertsService(pool, es, redis);
  const search = new SearchService(es, redis, breakers);
  const reports = new ReportsService(pool, mongo);
  const leaderboard = new LeaderboardService(redis, pool);
  const health = new HealthService(pool, mongo, es, redis);
  const metrics = new MetricsService(redis, breakers);
  const consistency = new ConsistencyService(pool, mongo, es);
  const retention = new RetentionService(pool, mongo);

  // 5. Create workers
  const ingestWorker = new IngestWorker(redis, mongo, es, pool, pipeline);
  const changeStreamWorker = new ChangeStreamWorker(mongo, redis);
  const alertWorker = new AlertWorker(redis, pool, es);

  // 6. Create scheduler
  const scheduler = new JobScheduler(pool, mongo, es);

  logger.info('Container wired successfully');

  return {
    pool,
    mongo,
    es,
    redis,
    breakers,
    cache,
    rateLimit,
    apiKey,
    auth,
    tenants,
    projects,
    ingest,
    pipeline,
    alerts,
    search,
    reports,
    leaderboard,
    health,
    metrics,
    consistency,
    retention,
    ingestWorker,
    changeStreamWorker,
    alertWorker,
    scheduler,
  };
}

// ---------------------------------------------------------------------------
// closeContainer — graceful shutdown of all connections
// ---------------------------------------------------------------------------

export async function closeContainer(container: AppContainer): Promise<void> {
  logger.info('Closing container connections...');
  await Promise.allSettled([
    container.pool.close().catch((err) => logger.error({ err }, 'Error closing PG pool')),
    container.mongo.close().catch((err) => logger.error({ err }, 'Error closing MongoDB')),
    container.es.close().catch((err) => logger.error({ err }, 'Error closing Elasticsearch')),
    container.redis.close().catch((err) => logger.error({ err }, 'Error closing Redis')),
  ]);
  logger.info('All container connections closed');
}
