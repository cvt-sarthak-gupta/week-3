import type { PostgresPool } from '../../db/postgres/index.js';
import type { MongoDatabase } from '../../db/mongo/index.js';
import type { ElasticClient } from '../../db/elastic/index.js';
import type { RedisClient } from '../../db/redis/index.js';

export interface DatastoreChecks {
  postgres: boolean;
  mongo: boolean;
  elasticsearch: boolean;
  redis: boolean;
}

export interface HealthResult {
  status: 'ok' | 'degraded';
  checks: DatastoreChecks;
  timestamp: string;
}

const HEALTH_TIMEOUT_MS = 200;

export class HealthService {
  constructor(
    private readonly pool: PostgresPool,
    private readonly mongo: MongoDatabase,
    private readonly es: ElasticClient,
    private readonly redis: RedisClient,
  ) {}

  async check(): Promise<HealthResult> {
    const fallback = { ok: false, latencyMs: HEALTH_TIMEOUT_MS };

    function withTimeout<T>(promise: Promise<T>, fallbackValue: T): Promise<T> {
      return Promise.race([
        promise,
        new Promise<T>((resolve) =>
          setTimeout(() => resolve(fallbackValue), HEALTH_TIMEOUT_MS),
        ),
      ]);
    }

    const [pgResult, mongoResult, esResult, redisResult] = await Promise.allSettled([
      withTimeout(this.pool.healthCheck(), fallback),
      withTimeout(this.mongo.healthCheck(), fallback),
      withTimeout(this.es.healthCheck(), fallback),
      withTimeout(this.redis.healthCheck(), fallback),
    ]);

    const pg = pgResult.status === 'fulfilled' ? pgResult.value.ok : false;
    const mongo = mongoResult.status === 'fulfilled' ? mongoResult.value.ok : false;
    const es = esResult.status === 'fulfilled' ? esResult.value.ok : false;
    const redis = redisResult.status === 'fulfilled' ? redisResult.value.ok : false;

    const allOk = pg && mongo && es && redis;

    return {
      status: allOk ? 'ok' : 'degraded',
      checks: {
        postgres: pg,
        mongo,
        elasticsearch: es,
        redis,
      },
      timestamp: new Date().toISOString(),
    };
  }
}
