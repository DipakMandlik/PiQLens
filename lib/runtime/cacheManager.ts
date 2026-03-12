/**
 * Production-Grade In-Memory Cache Manager
 *
 * Features:
 * - TTL-based expiry
 * - Promise deduplication (concurrency-safe — only ONE Snowflake query per cache key)
 * - Error-safe: failed fetches clean up both cache and inflight entries
 * - Memory guard: MAX_ENTRIES cap with oldest-first eviction
 * - Periodic cleanup: sweeps expired entries every 5 minutes
 * - Metrics: hit/miss/refresh counters for observability
 * - Structured logging via existing logger
 *
 * ⚠️  SERVERLESS WARNING: In-memory cache resets on every cold start.
 *     This is acceptable for Vercel/Lambda — the cache simply rebuilds on demand.
 *     For persistent Node servers, the cache lives for the lifetime of the process.
 */

import { logger } from '@/lib/logger';

interface CacheEntry<T> {
  data: T;
  expiresAt: number;
}

type CacheEvent = 'HIT' | 'MISS' | 'REFRESH' | 'INVALIDATE';

class CacheManager {
  private store = new Map<string, CacheEntry<unknown>>();
  private inflight = new Map<string, Promise<unknown>>();

  /** Maximum cached entries — prevents unbounded memory growth. */
  private readonly MAX_ENTRIES = 100;

  /** Cleanup interval reference for teardown. */
  private cleanupTimer: ReturnType<typeof setInterval> | null = null;

  private stats = { hits: 0, misses: 0, refreshes: 0, evictions: 0 };

  constructor() {
    this.startPeriodicCleanup();
  }

  // ─── Logging ─────────────────────────────────────────────────────────

  private log(event: CacheEvent, key: string): void {
    logger.info(`[Cache] ${event} - ${key}`);
  }

  // ─── Memory Guard ────────────────────────────────────────────────────

  /**
   * Evict oldest entries when the cache exceeds MAX_ENTRIES.
   * Map insertion order guarantees FIFO — first key is oldest.
   */
  private enforceLimit(): void {
    while (this.store.size > this.MAX_ENTRIES) {
      const oldestKey = this.store.keys().next().value;
      if (oldestKey !== undefined) {
        this.store.delete(oldestKey);
        this.inflight.delete(oldestKey);
        this.stats.evictions++;
        logger.debug(`[Cache] EVICT - ${oldestKey} (limit: ${this.MAX_ENTRIES})`);
      } else {
        break;
      }
    }
  }

  // ─── Periodic Cleanup ────────────────────────────────────────────────

  /**
   * Sweep expired entries every 5 minutes to prevent stale data
   * from consuming memory in long-running server processes.
   */
  private startPeriodicCleanup(): void {
    // Guard: don't start if not in a Node server context
    if (typeof setInterval === 'undefined') return;

    this.cleanupTimer = setInterval(() => {
      const now = Date.now();
      let cleaned = 0;

      for (const [key, entry] of this.store.entries()) {
        // Only remove if expired AND not currently being refetched
        if (entry.expiresAt < now && !this.inflight.has(key)) {
          this.store.delete(key);
          cleaned++;
        }
      }

      if (cleaned > 0) {
        logger.info(`[Cache] CLEANUP - removed ${cleaned} expired entries`);
      }
    }, 5 * 60 * 1000); // every 5 minutes

    // Allow Node process to exit without waiting for the timer
    if (this.cleanupTimer && typeof this.cleanupTimer === 'object' && 'unref' in this.cleanupTimer) {
      this.cleanupTimer.unref();
    }
  }

  // ─── Core API ────────────────────────────────────────────────────────

  /**
   * Core cache method with TTL + promise deduplication + memory guard.
   *
   * @param key     Unique cache key (e.g. "snowflake:metadata")
   * @param ttl     Time-to-live in seconds
   * @param fetcher Async function that produces the value on cache miss
   */
  async getOrSet<T>(
    key: string,
    ttl: number,
    fetcher: () => Promise<T>,
  ): Promise<T> {
    // 1. Check for valid cached value
    const entry = this.store.get(key) as CacheEntry<T> | undefined;
    if (entry && Date.now() < entry.expiresAt) {
      this.stats.hits++;
      this.log('HIT', key);
      return entry.data;
    }

    // 2. If another request is already fetching this key, await the same promise
    const existing = this.inflight.get(key) as Promise<T> | undefined;
    if (existing) {
      this.stats.hits++;
      this.log('HIT', key); // dedup hit — waiting on inflight
      return existing;
    }

    // 3. Track as miss or refresh
    if (entry) {
      this.stats.refreshes++;
      this.log('REFRESH', key);
    } else {
      this.stats.misses++;
      this.log('MISS', key);
    }

    // 4. Execute fetcher, store the promise for dedup
    const promise = fetcher()
      .then((data) => {
        this.store.set(key, {
          data,
          expiresAt: Date.now() + ttl * 1000,
        });
        this.inflight.delete(key);
        this.enforceLimit();
        return data;
      })
      .catch((error) => {
        // Clean up on failure so the next request retries
        this.store.delete(key);
        this.inflight.delete(key);
        throw error;
      });

    this.inflight.set(key, promise);
    return promise;
  }

  /**
   * Invalidate a specific cache key.
   */
  invalidate(key: string): void {
    this.store.delete(key);
    this.inflight.delete(key);
    this.log('INVALIDATE', key);
  }

  /**
   * Invalidate all keys that start with the given prefix.
   */
  invalidateByPrefix(prefix: string): void {
    for (const key of this.store.keys()) {
      if (key.startsWith(prefix)) {
        this.store.delete(key);
        this.inflight.delete(key);
      }
    }
    this.log('INVALIDATE', `${prefix}*`);
  }

  /**
   * Clear the entire cache and reset stats.
   */
  clearAll(): void {
    this.store.clear();
    this.inflight.clear();
    this.stats = { hits: 0, misses: 0, refreshes: 0, evictions: 0 };
    logger.info('[Cache] CLEARED — all entries removed');
  }

  /**
   * Get cache statistics for observability and production monitoring.
   */
  getStats(): {
    size: number;
    keys: string[];
    hits: number;
    misses: number;
    refreshes: number;
    evictions: number;
    hitRate: string;
  } {
    const total = this.stats.hits + this.stats.misses + this.stats.refreshes;
    return {
      size: this.store.size,
      keys: Array.from(this.store.keys()),
      hits: this.stats.hits,
      misses: this.stats.misses,
      refreshes: this.stats.refreshes,
      evictions: this.stats.evictions,
      hitRate: total > 0 ? `${((this.stats.hits / total) * 100).toFixed(1)}%` : '0%',
    };
  }

  /**
   * Stop periodic cleanup (for graceful shutdown / testing).
   */
  destroy(): void {
    if (this.cleanupTimer) {
      clearInterval(this.cleanupTimer);
      this.cleanupTimer = null;
    }
  }
}

// Singleton instance — survives across requests in persistent Node server
export const cacheManager = new CacheManager();
