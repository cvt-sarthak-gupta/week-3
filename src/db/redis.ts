import { readFile } from 'node:fs/promises';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { Redis } from 'ioredis';
import { logger } from '../logger.js';

export const STREAM_KEY = 'events-stream';
export const DLQ_KEY = 'events-stream-dlq';
export const CONSUMER_GROUP = 'ingesters';

// Resolve the lua directory relative to this source file so the path is stable
// regardless of cwd.
const __dirname = fileURLToPath(new URL('.', import.meta.url));
const LUA_DIR = join(__dirname, '..', 'lib', 'lua');

export class RedisDatabase {
  readonly #client: Redis;
  // SHA cache: scriptName → SHA1 returned by SCRIPT LOAD
  readonly #shaCache = new Map<string, string>();

  constructor({ host, port }: { host: string; port: number }) {
    this.#client = new Redis({
      host,
      port,
      lazyConnect: true,            // connect() is called explicitly
      enableReadyCheck: true,
      maxRetriesPerRequest: 3,
      retryStrategy(times: number): number | null {
        if (times > 10) return null; // stop retrying after 10 attempts
        return Math.min(times * 100, 3_000);
      },
    });

    this.#client.on('error', (err: Error) => {
      logger.error({ err }, 'Redis client error');
    });

    this.#client.on('reconnecting', () => {
      logger.warn('Redis client reconnecting…');
    });
  }

  get client(): Redis {
    return this.#client;
  }

  async connect(): Promise<void> {
    await this.#client.connect();
    logger.info(
      { host: this.#client.options.host, port: this.#client.options.port },
      'Redis ready',
    );
  }

  /**
   * Loads a Lua script from `src/lib/lua/<name>.lua`, registers it with Redis
   * via SCRIPT LOAD, and returns the SHA along with the script name.
   * Caches the SHA so subsequent calls skip the file read.
   */
  async loadLua(name: string): Promise<{ sha: string; name: string }> {
    // Load and cache SHA on first call.
    if (!this.#shaCache.has(name)) {
      const filePath = join(LUA_DIR, `${name}.lua`);
      const src = await readFile(filePath, 'utf8');
      const sha: string = (await this.#client.script('LOAD', src)) as string;
      this.#shaCache.set(name, sha);
      logger.debug({ script: name, sha }, 'Lua script loaded');
    }

    const sha = this.#shaCache.get(name)!;
    return { sha, name };
  }

  /**
   * Creates the consumer group for the given stream if it doesn't already exist.
   * Uses the MKSTREAM flag so the stream is also created if absent.
   * Safely ignores BUSYGROUP errors (group already exists).
   */
  async ensureConsumerGroup(stream: string, group: string): Promise<void> {
    try {
      await this.#client.xgroup('CREATE', stream, group, '$', 'MKSTREAM');
      logger.info({ stream, group }, 'Redis consumer group created');
    } catch (err: unknown) {
      if (err instanceof Error && err.message.includes('BUSYGROUP')) {
        logger.debug({ stream, group }, 'Redis consumer group already exists');
        return;
      }
      throw err;
    }
  }

  async healthCheck(): Promise<{ ok: boolean; latencyMs: number }> {
    const start = Date.now();
    try {
      await this.#client.ping();
      return { ok: true, latencyMs: Date.now() - start };
    } catch (err) {
      logger.error({ err }, 'Redis healthCheck failed');
      return { ok: false, latencyMs: Date.now() - start };
    }
  }

  async close(): Promise<void> {
    await this.#client.quit();
    logger.info('Redis client closed');
  }
}
