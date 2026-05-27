import { z } from 'zod';

const coercePort = z.coerce.number().int().positive();
const coerceInt = (def: number) => z.coerce.number().int().nonnegative().default(def);

const schema = z.object({
  NODE_ENV: z.enum(['development', 'production', 'test']).default('development'),
  LOG_LEVEL: z.string().default('info'),
  API_PORT: coercePort.default(3000),

  JWT_SECRET: z.string().min(32),
  JWT_EXPIRY: z.string().default('15m'),
  JWT_REFRESH_EXPIRY: z.string().default('7d'),

  PG_HOST: z.string().min(1),
  PG_PORT: coercePort.default(5432),
  PG_DATABASE: z.string().min(1),
  PG_USER: z.string().min(1),
  PG_PASSWORD: z.string().min(1),
  PG_POOL_MIN: coerceInt(2),
  PG_POOL_MAX: coerceInt(20),

  MONGO_URL: z.string().url(),
  MONGO_DB_NAME: z.string().min(1),

  ES_URL: z.string().url().default('http://localhost:9200'),

  REDIS_HOST: z.string().default('localhost'),
  REDIS_PORT: coercePort.default(6379),

  INGEST_WORKER_CONCURRENCY: coerceInt(3),
  INGEST_XPENDING_RECLAIM_MS: coerceInt(30000),
  INGEST_STREAM_MAX_LEN: coerceInt(1000000),

  CACHE_DEFAULT_TTL_SECONDS: coerceInt(300),
});

const parsed = schema.parse(process.env);

export const config = Object.freeze({
  node: parsed.NODE_ENV,
  log: parsed.LOG_LEVEL,
  api: Object.freeze({
    port: parsed.API_PORT,
  }),
  jwt: Object.freeze({
    secret: parsed.JWT_SECRET,
    expiry: parsed.JWT_EXPIRY,
    refreshExpiry: parsed.JWT_REFRESH_EXPIRY,
  }),
  pg: Object.freeze({
    host: parsed.PG_HOST,
    port: parsed.PG_PORT,
    database: parsed.PG_DATABASE,
    user: parsed.PG_USER,
    password: parsed.PG_PASSWORD,
    poolMin: parsed.PG_POOL_MIN,
    poolMax: parsed.PG_POOL_MAX,
  }),
  mongo: Object.freeze({
    url: parsed.MONGO_URL,
    dbName: parsed.MONGO_DB_NAME,
  }),
  es: Object.freeze({
    url: parsed.ES_URL,
  }),
  redis: Object.freeze({
    host: parsed.REDIS_HOST,
    port: parsed.REDIS_PORT,
  }),
  ingest: Object.freeze({
    workerConcurrency: parsed.INGEST_WORKER_CONCURRENCY,
    xpendingReclaimMs: parsed.INGEST_XPENDING_RECLAIM_MS,
    streamMaxLen: parsed.INGEST_STREAM_MAX_LEN,
  }),
  cache: Object.freeze({
    defaultTtlSeconds: parsed.CACHE_DEFAULT_TTL_SECONDS,
  }),
} as const);

export type Config = typeof config;
