import { PostgresDatabase } from './db/postgres.js';
import { MongoDatabase } from './db/mongo.js';
import { RedisDatabase, STREAM_KEY, CONSUMER_GROUP } from './db/redis.js';
import { ElasticsearchDatabase } from './db/elastic.js';
import { CircuitBreakerRegistry } from './lib/circuit-breaker.js';
import { AuthService } from './lib/auth.js';
import { CacheService } from './lib/cache.js';
import { RateLimitService } from './lib/rate-limit.js';
import { IngestionService } from './domain/ingestion.js';
import { AlertService } from './domain/alerts.js';
import { TenantService } from './domain/tenants.js';
import { ProjectService } from './domain/projects.js';
import { ReportService } from './domain/reports.js';
import { ConsistencyService } from './domain/consistency.js';
import { RetentionService } from './domain/retention.js';
import { DashboardService } from './domain/dashboards.js';
import { config } from './config.js';

export class AppContainer {
  readonly pg: PostgresDatabase;
  readonly mongo: MongoDatabase;
  readonly redis: RedisDatabase;
  readonly es: ElasticsearchDatabase;
  readonly breakers: CircuitBreakerRegistry;
  readonly auth: AuthService;
  readonly cache: CacheService;
  readonly rateLimit: RateLimitService;
  readonly ingestion: IngestionService;
  readonly alerts: AlertService;
  readonly tenants: TenantService;
  readonly projects: ProjectService;
  readonly reports: ReportService;
  readonly consistency: ConsistencyService;
  readonly retention: RetentionService;
  readonly dashboards: DashboardService;

  constructor(cfg: typeof config) {
    this.pg = new PostgresDatabase({
      host: cfg.pg.host,
      port: cfg.pg.port,
      database: cfg.pg.database,
      user: cfg.pg.user,
      password: cfg.pg.password,
      min: cfg.pg.poolMin,
      max: cfg.pg.poolMax,
    });
    this.mongo = new MongoDatabase(cfg.mongo.url, cfg.mongo.dbName);
    this.redis = new RedisDatabase({ host: cfg.redis.host, port: cfg.redis.port });
    this.es = new ElasticsearchDatabase(cfg.es.url);
    this.breakers = new CircuitBreakerRegistry();
    this.auth = new AuthService(this.pg, this.redis);
    this.cache = new CacheService(this.redis);
    this.rateLimit = new RateLimitService(this.redis, this.pg);
    this.ingestion = new IngestionService(this.pg, this.mongo, this.redis, this.es, this.breakers);
    this.alerts = new AlertService(this.pg, this.es, this.redis);
    this.tenants = new TenantService(this.pg, this.mongo, this.es, this.redis);
    this.projects = new ProjectService(this.pg, this.redis);
    this.reports = new ReportService(this.mongo, this.es, this.redis, this.cache);
    this.consistency = new ConsistencyService(this.pg, this.mongo, this.es);
    this.retention = new RetentionService(this.pg, this.mongo);
    this.dashboards = new DashboardService(this.mongo);
  }

  async initialize(): Promise<void> {
    await Promise.all([
      this.pg.healthCheck(),
      this.mongo.connect(),
      this.redis.connect(),
    ]);

    // Apply MongoDB schema validation + compound indexes to the events and logs
    // collections.  Idempotent — safe to call on every startup.
    await this.mongo.ensureEventsCollection();

    // Ensure ES index template, ILM policies, and percolator index exist.
    await this.es.ensureSetup();

    // Load Lua scripts into Redis and get their SHAs cached.
    await this.redis.loadLua('sliding-window');
    await this.redis.loadLua('dedup-fire');

    await this.redis.ensureConsumerGroup(STREAM_KEY, CONSUMER_GROUP);
  }

  async close(): Promise<void> {
    await Promise.all([
      this.pg.close(),
      this.mongo.close(),
      this.redis.close(),
      this.es.close(),
    ]);
  }
}
