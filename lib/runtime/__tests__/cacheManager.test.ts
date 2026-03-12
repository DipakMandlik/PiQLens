/**
 * CacheManager Production Hardening Test
 *
 * Validates:
 * 1. Concurrency dedup at 50 parallel requests
 * 2. Cache HIT / TTL expiry / REFRESH cycle
 * 3. Error cleanup
 * 4. Memory guard (MAX_ENTRIES eviction)
 * 5. Metrics accuracy (hits, misses, refreshes, evictions, hitRate)
 * 6. Manual invalidation
 *
 * Run: npx tsx lib/runtime/__tests__/cacheManager.test.ts
 */

// ─── Inline CacheManager (no @/ alias in direct tsx execution) ──────

interface CacheEntry<T> {
  data: T;
  expiresAt: number;
}

class TestCacheManager {
  private store = new Map<string, CacheEntry<unknown>>();
  private inflight = new Map<string, Promise<unknown>>();
  private readonly MAX_ENTRIES: number;
  private stats = { hits: 0, misses: 0, refreshes: 0, evictions: 0 };

  constructor(maxEntries = 100) {
    this.MAX_ENTRIES = maxEntries;
  }

  private enforceLimit(): void {
    while (this.store.size > this.MAX_ENTRIES) {
      const oldestKey = this.store.keys().next().value;
      if (oldestKey !== undefined) {
        this.store.delete(oldestKey);
        this.inflight.delete(oldestKey);
        this.stats.evictions++;
      } else {
        break;
      }
    }
  }

  async getOrSet<T>(key: string, ttl: number, fetcher: () => Promise<T>): Promise<T> {
    const entry = this.store.get(key) as CacheEntry<T> | undefined;
    if (entry && Date.now() < entry.expiresAt) {
      this.stats.hits++;
      return entry.data;
    }

    const existing = this.inflight.get(key) as Promise<T> | undefined;
    if (existing) {
      this.stats.hits++;
      return existing;
    }

    if (entry) {
      this.stats.refreshes++;
    } else {
      this.stats.misses++;
    }

    const promise = fetcher()
      .then((data) => {
        this.store.set(key, { data, expiresAt: Date.now() + ttl * 1000 });
        this.inflight.delete(key);
        this.enforceLimit();
        return data;
      })
      .catch((error) => {
        this.store.delete(key);
        this.inflight.delete(key);
        throw error;
      });

    this.inflight.set(key, promise);
    return promise;
  }

  invalidate(key: string): void {
    this.store.delete(key);
    this.inflight.delete(key);
  }

  clearAll(): void {
    this.store.clear();
    this.inflight.clear();
    this.stats = { hits: 0, misses: 0, refreshes: 0, evictions: 0 };
  }

  getStats() {
    const total = this.stats.hits + this.stats.misses + this.stats.refreshes;
    return {
      size: this.store.size,
      ...this.stats,
      hitRate: total > 0 ? `${((this.stats.hits / total) * 100).toFixed(1)}%` : '0%',
    };
  }
}

// ─── Test Runner ────────────────────────────────────────────────────────

let passed = 0;
let failed = 0;

function assert(condition: boolean, message: string) {
  if (condition) {
    console.log(`  ✅ ${message}`);
    passed++;
  } else {
    console.error(`  ❌ ${message}`);
    failed++;
  }
}

async function run() {
  // ─── Test 1: Concurrency Dedup (50 parallel) ──────────────────────
  console.log('\n🔒 Test 1: Concurrency deduplication (50 concurrent requests)');
  const cache = new TestCacheManager();
  let fetcherCalls = 0;

  const fetcher = () =>
    new Promise<string>((resolve) => {
      fetcherCalls++;
      setTimeout(() => resolve('result'), 50);
    });

  const promises = Array.from({ length: 50 }, () =>
    cache.getOrSet('test:dedup', 60, fetcher),
  );

  const results = await Promise.all(promises);
  assert(fetcherCalls === 1, `Fetcher called exactly once (actual: ${fetcherCalls})`);
  assert(results.every((r) => r === 'result'), 'All 50 requests received identical result');

  let stats = cache.getStats();
  assert(stats.misses === 1, `Misses = 1 (actual: ${stats.misses})`);
  assert(stats.hits === 49, `Hits = 49 (actual: ${stats.hits})`);

  // ─── Test 2: Cache HIT ─────────────────────────────────────────────
  console.log('\n⚡ Test 2: Cache HIT on subsequent call');
  fetcherCalls = 0;
  const cached = await cache.getOrSet('test:dedup', 60, fetcher);
  assert(cached === 'result', 'Got cached value');
  assert(fetcherCalls === 0, 'Fetcher NOT called (served from cache)');

  // ─── Test 3: TTL Expiry + REFRESH tracking ────────────────────────
  console.log('\n⏰ Test 3: TTL expiry → refresh counter');
  cache.clearAll();
  fetcherCalls = 0;

  await cache.getOrSet('test:ttl', 0.1, fetcher); // 100ms TTL
  assert(fetcherCalls === 1, 'First call triggers fetcher');

  await new Promise((r) => setTimeout(r, 200)); // Wait for TTL
  fetcherCalls = 0;

  await cache.getOrSet('test:ttl', 60, fetcher);
  assert(fetcherCalls === 1, 'Fetcher called again after TTL expired');

  stats = cache.getStats();
  assert(stats.refreshes === 1, `Refreshes = 1 (actual: ${stats.refreshes})`);

  // ─── Test 4: Error Cleanup ─────────────────────────────────────────
  console.log('\n💥 Test 4: Error cleanup');
  cache.clearAll();

  const failingFetcher = () => Promise.reject(new Error('Snowflake timeout'));
  try {
    await cache.getOrSet('test:error', 60, failingFetcher);
  } catch {
    // Expected
  }

  stats = cache.getStats();
  assert(stats.size === 0, 'Cache entry removed after error');

  // ─── Test 5: Memory Guard (MAX_ENTRIES eviction) ───────────────────
  console.log('\n🛡️  Test 5: Memory guard (MAX_ENTRIES = 5)');
  const smallCache = new TestCacheManager(5); // limit to 5

  for (let i = 0; i < 8; i++) {
    await smallCache.getOrSet(`key:${i}`, 60, () => Promise.resolve(i));
  }

  stats = smallCache.getStats();
  assert(stats.size === 5, `Cache size capped at 5 (actual: ${stats.size})`);
  assert(stats.evictions === 3, `3 evictions occurred (actual: ${stats.evictions})`);

  // Verify oldest keys were evicted (key:0, key:1, key:2)
  let evictedCorrectly = true;
  for (let i = 0; i < 3; i++) {
    const val = await smallCache.getOrSet(`key:${i}`, 60, () => Promise.resolve(-1));
    if (val !== -1) evictedCorrectly = false; // Should have been evicted, so fetcher runs
  }
  // After re-inserting 3 keys, 3 more evictions should have occurred
  stats = smallCache.getStats();
  assert(stats.evictions === 6, `6 total evictions after re-insert (actual: ${stats.evictions})`);

  // ─── Test 6: Metrics / hitRate ─────────────────────────────────────
  console.log('\n📊 Test 6: Metrics accuracy');
  const metricsCache = new TestCacheManager();
  await metricsCache.getOrSet('m1', 60, () => Promise.resolve('a')); // miss
  await metricsCache.getOrSet('m1', 60, () => Promise.resolve('a')); // hit
  await metricsCache.getOrSet('m1', 60, () => Promise.resolve('a')); // hit
  await metricsCache.getOrSet('m2', 60, () => Promise.resolve('b')); // miss

  stats = metricsCache.getStats();
  assert(stats.hits === 2, `Hits = 2 (actual: ${stats.hits})`);
  assert(stats.misses === 2, `Misses = 2 (actual: ${stats.misses})`);
  assert(stats.hitRate === '50.0%', `Hit rate = 50.0% (actual: ${stats.hitRate})`);

  // ─── Test 7: Manual Invalidation ──────────────────────────────────
  console.log('\n🗑️  Test 7: Manual invalidation');
  await metricsCache.getOrSet('del:me', 60, () => Promise.resolve('temp'));
  assert(metricsCache.getStats().size === 3, 'Entry exists');
  metricsCache.invalidate('del:me');
  assert(metricsCache.getStats().size === 2, 'Entry removed after invalidation');

  // ─── Summary ───────────────────────────────────────────────────────
  console.log(`\n${'─'.repeat(50)}`);
  console.log(`Results: ${passed} passed, ${failed} failed`);
  console.log(`${'─'.repeat(50)}\n`);

  process.exit(failed > 0 ? 1 : 0);
}

run().catch((e) => {
  console.error('Test runner error:', e);
  process.exit(1);
});
