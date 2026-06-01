// - On miss: one holder gets the lock, runs the fetcher, caches result
// - Others poll briefly waiting for the result
// - Invalidation: SCAN + UNLINK (never KEYS)
// - Hit/miss metrics tracked per project in Redis counters

import { redis, incrCounter } from '../db/redis.js';
import { logger } from '../logger.js';

const LOCK_PREFIX = 'cache-lock:';
const LOCK_TTL_SECONDS = 10;
const POLL_INTERVAL_MS = 50;
const POLL_TIMEOUT_MS = 5_000;

async function acquireLock(key: string, ownerId: string): Promise<boolean> {
  // ioredis overload: set(key, value, expiryMode, time, setMode)
  const result = await redis.set(
    `${LOCK_PREFIX}${key}`,
    ownerId,
    'EX',
    LOCK_TTL_SECONDS,
    'NX',
  );
  return result === 'OK';
}

async function releaseLock(key: string, ownerId: string): Promise<void> {
  // Only release if we still own the lock (Lua for atomicity)
  const script = `
    if redis.call('GET', KEYS[1]) == ARGV[1] then
      return redis.call('DEL', KEYS[1])
    else
      return 0
    end
  `;
  await redis.eval(script, 1, `${LOCK_PREFIX}${key}`, ownerId);
}

function generateOwnerId(): string {
  return `${process.pid}-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;
}

export async function getOrFill<T>(
  key: string,
  ttlSeconds: number,
  fetcher: () => Promise<T>,
  projectId?: string,
): Promise<T> {
  // 1. Try cache hit
  const cached = await redis.get(key);
  if (cached !== null) {
    if (projectId !== undefined) {
      await incrCounter(`cache:hits:${projectId}`).catch(() => undefined);
    }
    return JSON.parse(cached) as T;
  }

  if (projectId !== undefined) {
    await incrCounter(`cache:misses:${projectId}`).catch(() => undefined);
  }

  // 2. Try to acquire fill lock (stampede prevention)
  const ownerId = generateOwnerId();
  const locked = await acquireLock(key, ownerId);

  if (locked) {
    // We won the lock — fetch and populate
    try {
      // Double-check: another holder may have filled the cache just before
      // we acquired the lock.
      const rechecked = await redis.get(key);
      if (rechecked !== null) {
        return JSON.parse(rechecked) as T;
      }

      const value = await fetcher();
      await redis.setex(key, ttlSeconds, JSON.stringify(value));
      logger.debug({ key, ttlSeconds }, 'cache filled');
      return value;
    } finally {
      await releaseLock(key, ownerId).catch(() => undefined);
    }
  }

  // 3. Someone else holds the lock — poll until the value appears
  const deadline = Date.now() + POLL_TIMEOUT_MS;
  while (Date.now() < deadline) {
    await new Promise<void>((resolve) => setTimeout(resolve, POLL_INTERVAL_MS));

    const polled = await redis.get(key);
    if (polled !== null) {
      return JSON.parse(polled) as T;
    }

    // If the lock was released but cache is still empty, try fetching directly
    const lockExists = await redis.exists(`${LOCK_PREFIX}${key}`);
    if (lockExists === 0) {
      // Lock gone but no cache — try to become the new filler
      const retryLocked = await acquireLock(key, ownerId);
      if (retryLocked) {
        try {
          const value = await fetcher();
          await redis.setex(key, ttlSeconds, JSON.stringify(value));
          return value;
        } finally {
          await releaseLock(key, ownerId).catch(() => undefined);
        }
      }
    }
  }

  // 4. Timeout — fall through to direct fetch to avoid hanging the caller
  logger.warn(
    { key },
    'cache stampede poll timed out, fetching directly',
  );
  return fetcher();
}

export async function invalidatePattern(pattern: string): Promise<number> {
  let cursor = '0';
  const keysToDelete: string[] = [];

  do {
    const [nextCursor, keys] = await redis.scan(
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
    const pipeline = redis.pipeline();
    for (const k of chunk) {
      pipeline.unlink(k);
    }
    await pipeline.exec();
  }

  logger.debug({ pattern, deleted: keysToDelete.length }, 'cache invalidated by pattern');
  return keysToDelete.length;
}

export async function invalidateProject(projectId: string): Promise<void> {
  const count = await invalidatePattern(`report:${projectId}:*`);
  logger.info({ projectId, deleted: count }, 'project cache invalidated');
}
