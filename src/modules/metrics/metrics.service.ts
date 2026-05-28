import type { RedisClient } from '../../db/redis/index.js';
import type { CircuitBreakers } from '../../lib/circuit-breaker/index.js';

// ---------------------------------------------------------------------------
// Metric key inventory (matches the original route)
// ---------------------------------------------------------------------------

const METRIC_KEYS = [
  'metrics:events:ingested:total',
  'metrics:events:ingested:errors',
  'metrics:events:stream:published',
  'metrics:events:worker:processed',
  'metrics:events:worker:failed',
  'metrics:events:worker:dlq',
  'metrics:alerts:triggered:total',
  'metrics:alerts:percolator:matched',
  'metrics:cache:hits:global',
  'metrics:cache:misses:global',
  'metrics:ratelimit:rejected:total',
  'metrics:api:requests:total',
  'metrics:api:requests:5xx',
  'metrics:api:requests:4xx',
] as const;

type MetricKey = (typeof METRIC_KEYS)[number];

// ---------------------------------------------------------------------------
// MetricsResult
// ---------------------------------------------------------------------------

export interface MetricsResult {
  counters: Record<string, number>;
  stream: {
    ingestLag: number;
    dlqSize: number;
  };
  circuitBreakers: {
    postgres: string;
    mongo: string;
    elasticsearch: string;
    redis: string;
  };
  alertFanoutFailures: number;
  collectedAt: string;
}

// ---------------------------------------------------------------------------
// MetricsService
// ---------------------------------------------------------------------------

export class MetricsService {
  constructor(
    private readonly redis: RedisClient,
    private readonly breakers: CircuitBreakers,
  ) {}

  async getMetrics(): Promise<MetricsResult> {
    // 1. Read all counter keys via getCounter (sequential, small set)
    const counterValues = await Promise.all(
      METRIC_KEYS.map(async (key) => {
        const value = await this.redis.getCounter(key);
        return [key, value] as [MetricKey, number];
      }),
    );

    const counters: Record<string, number> = {};
    for (const [key, value] of counterValues) {
      counters[key] = value;
    }

    // 2. Stream lag: XLEN on the events stream
    const ingestLag = await this.redis.client
      .xlen(this.redis.STREAM_KEY)
      .catch(() => 0);

    // 3. DLQ size: LLEN on the DLQ key
    const dlqSize = await this.redis.client
      .llen(this.redis.STREAM_DLQ)
      .catch(() => 0);

    // 4. Alert fanout failures counter
    const alertFanoutFailures = await this.redis.getCounter('alert.fanout.failures');

    // 5. Circuit breaker states
    const circuitBreakers = {
      postgres: this.breakers.postgres.getState(),
      mongo: this.breakers.mongo.getState(),
      elasticsearch: this.breakers.elasticsearch.getState(),
      redis: this.breakers.redis.getState(),
    };

    return {
      counters,
      stream: {
        ingestLag,
        dlqSize,
      },
      circuitBreakers,
      alertFanoutFailures,
      collectedAt: new Date().toISOString(),
    };
  }
}
