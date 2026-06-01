import { readFile } from 'node:fs/promises';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { Redis } from 'ioredis';
import { config } from '../../config.js';
import { logger } from '../../utils/logger.js';
import type { LuaScript } from './types.js';

// Resolve the lua directory relative to this source file so the path is stable
// regardless of cwd.
const __dirname = fileURLToPath(new URL('.', import.meta.url));
const LUA_DIR = join(__dirname, '..', '..', '..', 'db', 'redis', 'lua');

// SHA cache: scriptName → SHA1 returned by SCRIPT LOAD
const shaCache = new Map<string, string>();

export class RedisClient {
  readonly client: Redis;

  readonly STREAM_KEY = 'events-stream';
  readonly STREAM_DLQ = 'events-stream-dlq';
  readonly CONSUMER_GROUP = 'ingesters';

  constructor() {
    this.client = new Redis({
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

    this.client.on('error', (err: Error) => {
      logger.error({ err }, 'Redis client error');
    });

    this.client.on('reconnecting', () => {
      logger.warn('Redis client reconnecting…');
    });
  }

  async connect(): Promise<void> {
    await this.client.connect();
    logger.info({ host: config.redis.host, port: config.redis.port }, 'Redis ready');
  }

  async close(): Promise<void> {
    await this.client.quit();
    logger.info('Redis client closed');
  }

  async healthCheck(): Promise<{ ok: boolean; latencyMs: number }> {
    const start = Date.now();
    try {
      await this.client.ping();
      return { ok: true, latencyMs: Date.now() - start };
    } catch (err) {
      logger.error({ err }, 'Redis healthCheck failed');
      return { ok: false, latencyMs: Date.now() - start };
    }
  }

  /**
   * Loads a Lua script from `src/db/redis/lua/<name>.lua`, registers it with Redis
   * via SCRIPT LOAD, and returns an object whose `evalsha` method calls EVALSHA
   * with an automatic fallback to EVAL on NOSCRIPT errors.
   */
  async loadLua(name: string): Promise<LuaScript> {
    // Load and cache SHA on first call.
    if (!shaCache.has(name)) {
      const filePath = join(LUA_DIR, `${name}.lua`);
      const src = await readFile(filePath, 'utf8');
      const sha: string = await this.client.script('LOAD', src) as string;
      shaCache.set(name, sha);
      logger.debug({ script: name, sha }, 'Lua script loaded');
    }

    const redis = this.client;
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

  /**
   * Creates the consumer group for STREAM_KEY if it doesn't already exist.
   * Uses the MKSTREAM flag so the stream is also created if absent.
   * Safely ignores BUSYGROUP errors (group already exists).
   */
  async ensureConsumerGroup(): Promise<void> {
    try {
      await this.client.xgroup('CREATE', this.STREAM_KEY, this.CONSUMER_GROUP, '$', 'MKSTREAM');
      logger.info({ stream: this.STREAM_KEY, group: this.CONSUMER_GROUP }, 'Redis consumer group created');
    } catch (err: unknown) {
      if (err instanceof Error && err.message.includes('BUSYGROUP')) {
        logger.debug({ stream: this.STREAM_KEY, group: this.CONSUMER_GROUP }, 'Redis consumer group already exists');
        return;
      }
      throw err;
    }
  }

  /**
   * Atomically increments a Redis counter by `amount` (default 1).
   */
  async incrCounter(key: string, amount = 1): Promise<void> {
    await this.client.incrby(key, amount);
  }

  /**
   * Returns the current value of a counter key. Returns 0 if the key does not
   * exist (Redis GET returns null for missing keys).
   */
  async getCounter(key: string): Promise<number> {
    const val = await this.client.get(key);
    if (val === null) return 0;
    return parseInt(val, 10);
  }

  /**
   * Sets a gauge value. Optionally applies a TTL in seconds via EXPIRE.
   */
  async setGauge(
    key: string,
    value: number,
    ttlSeconds?: number,
  ): Promise<void> {
    await this.client.set(key, value);
    if (ttlSeconds !== undefined && ttlSeconds > 0) {
      await this.client.expire(key, ttlSeconds);
    }
  }
}
