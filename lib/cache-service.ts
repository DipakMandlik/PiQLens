import { getRedisClient, safeRedisOperation } from './redis';

/**
 * Cache key prefixes for different data types
 */
export const CachePrefix = {
    DATASET: 'piqlens:dataset:',
    TABLE_METADATA: 'piqlens:table:',
    USER_SESSION: 'piqlens:session:',
    PAGE_STATE: 'piqlens:page:',
    FILTERS: 'piqlens:filters:',
    QUALITY_SCORE: 'piqlens:quality:',
    GOVERNANCE: 'piqlens:governance:',
    CATALOG: 'piqlens:catalog:',
} as const;

/**
 * TTL (Time To Live) in seconds for different cache types
 */
export const CacheTTL = {
    DATASET: 3600, // 1 hour
    TABLE_METADATA: 3600, // 1 hour
    USER_SESSION: 86400, // 24 hours
    PAGE_STATE: 1800, // 30 minutes
    FILTERS: 1800, // 30 minutes
    QUALITY_SCORE: 1800, // 30 minutes
    GOVERNANCE: 3600, // 1 hour
    SHORT: 300, // 5 minutes
    LONG: 7200, // 2 hours
} as const;

/**
 * Generate a cache key with prefix
 */
export function generateCacheKey(prefix: string, ...parts: (string | number)[]): string {
    return `${prefix}${parts.join(':')}`;
}

/**
 * Get data from cache
 */
export async function getCache<T>(key: string): Promise<T | null> {
    return safeRedisOperation(async (client) => {
        const data = await client.get(key);
        if (!data) return null;

        try {
            return JSON.parse(data) as T;
        } catch (error) {
            console.error('Failed to parse cached data:', error);
            return null;
        }
    }, null);
}

/**
 * Set data in cache with TTL
 */
export async function setCache<T>(
    key: string,
    value: T,
    ttl: number = CacheTTL.SHORT
): Promise<boolean> {
    const result = await safeRedisOperation(async (client) => {
        const serialized = JSON.stringify(value);
        await client.setex(key, ttl, serialized);
        return true;
    }, false);
    return result ?? false;
}

/**
 * Delete data from cache
 */
export async function deleteCache(key: string): Promise<boolean> {
    const result = await safeRedisOperation(async (client) => {
        await client.del(key);
        return true;
    }, false);
    return result ?? false;
}

/**
 * Delete multiple keys matching a pattern
 */
export async function deleteCachePattern(pattern: string): Promise<number> {
    const result = await safeRedisOperation(async (client) => {
        const keys = await client.keys(pattern);
        if (keys.length === 0) return 0;

        await client.del(...keys);
        return keys.length;
    }, 0);
    return result ?? 0;
}

/**
 * Check if a key exists in cache
 */
export async function cacheExists(key: string): Promise<boolean> {
    const result = await safeRedisOperation(async (client) => {
        const exists = await client.exists(key);
        return exists === 1;
    }, false);
    return result ?? false;
}

/**
 * Get remaining TTL for a key
 */
export async function getCacheTTL(key: string): Promise<number> {
    const result = await safeRedisOperation(async (client) => {
        return await client.ttl(key);
    }, -1);
    return result ?? -1;
}

/**
 * Extend TTL for an existing key
 */
export async function extendCacheTTL(key: string, additionalSeconds: number): Promise<boolean> {
    const result = await safeRedisOperation(async (client) => {
        const currentTTL = await client.ttl(key);
        if (currentTTL > 0) {
            await client.expire(key, currentTTL + additionalSeconds);
            return true;
        }
        return false;
    }, false);
    return result ?? false;
}

/**
 * Get or set cache (cache-aside pattern)
 * If data exists in cache, return it. Otherwise, fetch from source and cache it.
 */
export async function getOrSetCache<T>(
    key: string,
    fetchFn: () => Promise<T>,
    ttl: number = CacheTTL.SHORT
): Promise<T | null> {
    // Try to get from cache first
    const cached = await getCache<T>(key);
    if (cached !== null) {
        return cached;
    }

    // Fetch from source
    try {
        const data = await fetchFn();

        // Cache the result
        await setCache(key, data, ttl);

        return data;
    } catch (error) {
        console.error('Failed to fetch and cache data:', error);
        return null;
    }
}

/**
 * Batch get multiple keys
 */
export async function batchGetCache<T>(keys: string[]): Promise<Map<string, T>> {
    const result = new Map<string, T>();

    if (keys.length === 0) return result;

    await safeRedisOperation(async (client) => {
        const values = await client.mget(...keys);

        values.forEach((value, index) => {
            if (value) {
                try {
                    result.set(keys[index], JSON.parse(value) as T);
                } catch (error) {
                    console.error(`Failed to parse cached data for key ${keys[index]}:`, error);
                }
            }
        });

        return result;
    }, null);

    return result;
}

/**
 * Batch set multiple keys
 */
export async function batchSetCache<T>(
    entries: Array<{ key: string; value: T; ttl?: number }>,
    defaultTTL: number = CacheTTL.SHORT
): Promise<boolean> {
    if (entries.length === 0) return true;

    const result = await safeRedisOperation(async (client) => {
        const pipeline = client.pipeline();

        entries.forEach(({ key, value, ttl }) => {
            const serialized = JSON.stringify(value);
            pipeline.setex(key, ttl ?? defaultTTL, serialized);
        });

        await pipeline.exec();
        return true;
    }, false);
    return result ?? false;
}

/**
 * Invalidate cache by prefix
 * Useful for invalidating all related cache entries
 */
export async function invalidateCacheByPrefix(prefix: string): Promise<number> {
    return deleteCachePattern(`${prefix}*`);
}

/**
 * Get cache statistics
 */
export async function getCacheStats(): Promise<{
    totalKeys: number;
    memoryUsed: string;
    hitRate?: number;
} | null> {
    return safeRedisOperation(async (client) => {
        const info = await client.info('stats');
        const dbsize = await client.dbsize();

        // Parse memory usage
        const memoryMatch = info.match(/used_memory_human:([^\r\n]+)/);
        const memoryUsed = memoryMatch ? memoryMatch[1] : 'unknown';

        // Parse hit rate if available
        const hitsMatch = info.match(/keyspace_hits:(\d+)/);
        const missesMatch = info.match(/keyspace_misses:(\d+)/);

        let hitRate: number | undefined;
        if (hitsMatch && missesMatch) {
            const hits = parseInt(hitsMatch[1]);
            const misses = parseInt(missesMatch[1]);
            const total = hits + misses;
            hitRate = total > 0 ? (hits / total) * 100 : 0;
        }

        return {
            totalKeys: dbsize,
            memoryUsed,
            hitRate,
        };
    }, null);
}
