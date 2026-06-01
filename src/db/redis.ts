import { readFile } from 'node:fs/promises';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { Redis } from 'ioredis';
import { config } from '../config.js';
import { logger } from '../logger.js';

export const redis = new Redis({
  host: config.redis.host,
  port: config.redis.port,
  lazyConnect: true,            // connect() is called explicitly
  enableReadyCheck: true,
  maxRetriesPerRequest: 3,
  retryStrategy(times: number): number | null {
    if (times > 10) return null; // stop retrying after 10 attempts
    return Math.min(times * 100, 3_000);
  },
});

redis.on('error', (err: Error) => {
  logger.error({ err }, 'Redis client error');
});

redis.on('reconnecting', () => {
  logger.warn('Redis client reconnecting…');
});

export async function connect(): Promise<void> {
  await redis.connect();
  logger.info({ host: config.redis.host, port: config.redis.port }, 'Redis ready');
}

export async function close(): Promise<void> {
  await redis.quit();
  logger.info('Redis client closed');
}

export async function healthCheck(): Promise<{ ok: boolean; latencyMs: number }> {
  const start = Date.now();
  try {
    await redis.ping();
    return { ok: true, latencyMs: Date.now() - start };
  } catch (err) {
    logger.error({ err }, 'Redis healthCheck failed');
    return { ok: false, latencyMs: Date.now() - start };
  }
}

// Resolve the lua directory relative to this source file so the path is stable
// regardless of cwd.
const __dirname = fileURLToPath(new URL('.', import.meta.url));
const LUA_DIR = join(__dirname, '..', 'lib', 'lua');

interface LuaScript {
  evalsha(keys: string[], args: (string | number)[]): Promise<unknown>;
}

// SHA cache: scriptName → SHA1 returned by SCRIPT LOAD
const shaCache = new Map<string, string>();

/**
 * Loads a Lua script from `src/lib/lua/<name>.lua`, registers it with Redis
 * via SCRIPT LOAD, and returns an object whose `evalsha` method calls EVALSHA
 * with an automatic fallback to EVAL on NOSCRIPT errors.
 */
export async function loadLua(name: string): Promise<LuaScript> {
  // Load and cache SHA on first call.
  if (!shaCache.has(name)) {
    const filePath = join(LUA_DIR, `${name}.lua`);
    const src = await readFile(filePath, 'utf8');
    const sha: string = await redis.script('LOAD', src) as string;
    shaCache.set(name, sha);
    logger.debug({ script: name, sha }, 'Lua script loaded');
  }

  const getSha = (): string => {
    const sha = shaCache.get(name);
    if (sha === undefined) throw new Error(`No SHA cached for Lua script "${name}"`);
    return sha;
  };

  return {
    async evalsha(keys: string[], args: (string | number)[]): Promise<unknown> {
      const sha = getSha();
      try {
        return await redis.evalsha(sha, keys.length, ...keys, ...args);
      } catch (err: unknown) {
        // On NOSCRIPT the script was flushed — fall back to EVAL and re-cache.
        if (err instanceof Error && err.message.startsWith('NOSCRIPT')) {
          logger.warn({ script: name }, 'NOSCRIPT: falling back to EVAL and re-loading');
          const filePath = join(LUA_DIR, `${name}.lua`);
          const src = await readFile(filePath, 'utf8');
          const newSha: string = await redis.script('LOAD', src) as string;
          shaCache.set(name, newSha);
          return redis.evalsha(newSha, keys.length, ...keys, ...args);
        }
        throw err;
      }
    },
  };
}

export const STREAM_KEY = 'events-stream';
export const STREAM_DLQ = 'events-stream-dlq';
export const CONSUMER_GROUP = 'ingesters';

/**
 * Creates the consumer group for STREAM_KEY if it doesn't already exist.
 * Uses the MKSTREAM flag so the stream is also created if absent.
 * Safely ignores BUSYGROUP errors (group already exists).
 */
export async function ensureConsumerGroup(): Promise<void> {
  try {
    await redis.xgroup('CREATE', STREAM_KEY, CONSUMER_GROUP, '$', 'MKSTREAM');
    logger.info({ stream: STREAM_KEY, group: CONSUMER_GROUP }, 'Redis consumer group created');
  } catch (err: unknown) {
    if (err instanceof Error && err.message.includes('BUSYGROUP')) {
      logger.debug({ stream: STREAM_KEY, group: CONSUMER_GROUP }, 'Redis consumer group already exists');
      return;
    }
    throw err;
  }
}

/**
 * Atomically increments a Redis counter by `amount` (default 1).
 */
export async function incrCounter(key: string, amount = 1): Promise<void> {
  await redis.incrby(key, amount);
}

/**
 * Returns the current value of a counter key. Returns 0 if the key does not
 * exist (Redis GET returns null for missing keys).
 */
export async function getCounter(key: string): Promise<number> {
  const val = await redis.get(key);
  if (val === null) return 0;
  return parseInt(val, 10);
}

/**
 * Sets a gauge value. Optionally applies a TTL in seconds via EXPIRE.
 */
export async function setGauge(
  key: string,
  value: number,
  ttlSeconds?: number,
): Promise<void> {
  await redis.set(key, value);
  if (ttlSeconds !== undefined && ttlSeconds > 0) {
    await redis.expire(key, ttlSeconds);
  }
}
