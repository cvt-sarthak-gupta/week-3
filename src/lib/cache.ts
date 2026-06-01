// - On miss: one holder gets the lock, runs the fetcher, caches result
// - Others poll briefly waiting for the result
// - Invalidation: SCAN + UNLINK (never KEYS)
// - Hit/miss metrics tracked per project in Redis counters

import type { RedisDatabase } from '../db/redis.js';
import { logger } from '../logger.js';

export class CacheService {
  private readonly redis: RedisDatabase;

  private readonly LOCK_PREFIX = 'cache-lock:';
  private readonly LOCK_TTL_SECONDS = 10;
  private readonly POLL_INTERVAL_MS = 50;
  private readonly POLL_TIMEOUT_MS = 5_000;

  constructor(redis: RedisDatabase) {
    this.redis = redis;
  }

  // -------------------------------------------------------------------------
  // Private helpers
  // -------------------------------------------------------------------------

  private async acquireLock(key: string, ownerId: string): Promise<boolean> {
    // ioredis overload: set(key, value, expiryMode, time, setMode)
    const result = await this.redis.client.set(
      `${this.LOCK_PREFIX}${key}`,
      ownerId,
      'EX',
      this.LOCK_TTL_SECONDS,
      'NX',
    );
    return result === 'OK';
  }

  private async releaseLock(key: string, ownerId: string): Promise<void> {
    // Only release if we still own the lock (Lua for atomicity)
    const script = `
      if redis.call('GET', KEYS[1]) == ARGV[1] then
        return redis.call('DEL', KEYS[1])
      else
        return 0
      end
    `;
    await this.redis.client.eval(script, 1, `${this.LOCK_PREFIX}${key}`, ownerId);
  }

  private generateOwnerId(): string {
    return `${process.pid}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
  }

  // -------------------------------------------------------------------------
  // Public API
  // -------------------------------------------------------------------------

  async getOrFill<T>(
    key: string,
    ttlSeconds: number,
    fetcher: () => Promise<T>,
    projectId?: string,
  ): Promise<T> {
    // 1. Try cache hit — if Redis is down, fall through directly to the fetcher
    //    (X5: "Cache misses must go directly to source databases" when Redis is unavailable)
    let cached: string | null = null;
    try {
      cached = await this.redis.client.get(key);
    } catch {
      logger.warn({ key }, 'cache: Redis GET failed (Redis down?) — fetching directly from source');
      return fetcher();
    }

    if (cached !== null) {
      if (projectId) this.incrHit(projectId);
      return JSON.parse(cached) as T;
    }

    // 2. Try to acquire fill lock (stampede prevention)
    const ownerId = this.generateOwnerId();
    let locked = false;
    try {
      locked = await this.acquireLock(key, ownerId);
    } catch {
      // Redis down — fall through to direct fetch
      logger.warn({ key }, 'cache: Redis lock failed (Redis down?) — fetching directly from source');
      if (projectId) this.incrMiss(projectId);
      return fetcher();
    }

    if (locked) {
      // We won the lock — fetch and populate.
      // Count exactly one miss here: only the holder that actually goes to the
      // source database is a true cache miss.  Concurrent waiters that poll and
      // get the value from the filled cache are hits, not misses.
      try {
        // Double-check: another holder may have filled the cache just before
        // we acquired the lock.
        const rechecked = await this.redis.client.get(key).catch(() => null);
        if (rechecked !== null) {
          if (projectId) this.incrHit(projectId);
          return JSON.parse(rechecked) as T;
        }

        if (projectId) this.incrMiss(projectId);
        const value = await fetcher();
        await this.redis.client.setex(key, ttlSeconds, JSON.stringify(value)).catch(() => undefined);
        logger.debug({ key, ttlSeconds }, 'cache filled');
        return value;
      } finally {
        await this.releaseLock(key, ownerId).catch(() => undefined);
      }
    }

    // 3. Someone else holds the lock — poll until the value appears.
    //    Waiters that get the value from cache are counted as hits, not misses.
    const deadline = Date.now() + this.POLL_TIMEOUT_MS;
    while (Date.now() < deadline) {
      await new Promise<void>((resolve) => setTimeout(resolve, this.POLL_INTERVAL_MS));

      const polled = await this.redis.client.get(key).catch(() => null);
      if (polled !== null) {
        if (projectId) this.incrHit(projectId);
        return JSON.parse(polled) as T;
      }

      // If the lock was released but cache is still empty, try fetching directly
      const lockExists = await this.redis.client.exists(`${this.LOCK_PREFIX}${key}`).catch(() => -1);
      if (lockExists === 0) {
        // Lock gone but no cache — try to become the new filler
        const retryLocked = await this.acquireLock(key, ownerId).catch(() => false);
        if (retryLocked) {
          try {
            if (projectId) this.incrMiss(projectId);
            const value = await fetcher();
            await this.redis.client.setex(key, ttlSeconds, JSON.stringify(value)).catch(() => undefined);
            return value;
          } finally {
            await this.releaseLock(key, ownerId).catch(() => undefined);
          }
        }
      }
    }

    // 4. Timeout — fall through to direct fetch to avoid hanging the caller
    logger.warn(
      { key },
      'cache stampede poll timed out, fetching directly',
    );
    if (projectId) this.incrMiss(projectId);
    return fetcher();
  }

  async invalidatePattern(pattern: string): Promise<number> {
    let cursor = '0';
    const keysToDelete: string[] = [];

    do {
      const [nextCursor, keys] = await this.redis.client.scan(
        cursor,
        'MATCH',
        pattern,
        'COUNT',
        200,
      );
      cursor = nextCursor;
      for (const k of keys) {
        keysToDelete.push(k);
      }
    } while (cursor !== '0');

    if (keysToDelete.length === 0) return 0;

    // Pipeline UNLINK in chunks of 200
    const CHUNK = 200;
    for (let i = 0; i < keysToDelete.length; i += CHUNK) {
      const chunk = keysToDelete.slice(i, i + CHUNK);
      const pipeline = this.redis.client.pipeline();
      for (const k of chunk) {
        pipeline.unlink(k);
      }
      await pipeline.exec();
    }

    logger.debug({ pattern, deleted: keysToDelete.length }, 'cache invalidated by pattern');
    return keysToDelete.length;
  }

  incrHit(projectId: string): void {
    this.redis.client.incr(`cache:hits:${projectId}`).catch(() => undefined);
  }

  incrMiss(projectId: string): void {
    this.redis.client.incr(`cache:misses:${projectId}`).catch(() => undefined);
  }

  async getMetrics(
    projectIds: string[],
  ): Promise<Record<string, { hits: number; misses: number }>> {
    if (projectIds.length === 0) return {};

    const pipeline = this.redis.client.pipeline();
    for (const id of projectIds) {
      pipeline.get(`cache:hits:${id}`);
      pipeline.get(`cache:misses:${id}`);
    }

    const results = await pipeline.exec();
    const metrics: Record<string, { hits: number; misses: number }> = {};

    projectIds.forEach((id, idx) => {
      const hitsRaw = results?.[idx * 2]?.[1] as string | null;
      const missesRaw = results?.[idx * 2 + 1]?.[1] as string | null;
      metrics[id] = {
        hits: hitsRaw !== null && hitsRaw !== undefined ? parseInt(hitsRaw, 10) : 0,
        misses: missesRaw !== null && missesRaw !== undefined ? parseInt(missesRaw, 10) : 0,
      };
    });

    return metrics;
  }
}
