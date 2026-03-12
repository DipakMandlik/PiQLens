/**
 * Caching Layer for Pi-Qualytics
 * In-memory cache with TTL support
 */

import { logger } from './logger';

interface CacheEntry<T> {
    data: T;
    expiresAt: number;
}

class Cache {
    private store: Map<string, CacheEntry<any>> = new Map();

    /**
     * Get value from cache
     */
    get<T>(key: string): T | null {
        const entry = this.store.get(key);

        if (!entry) {
            logger.logCache(key, 'MISS');
            return null;
        }

        // Check if expired
        if (Date.now() > entry.expiresAt) {
            this.store.delete(key);
            logger.logCache(key, 'MISS');
            return null;
        }

        logger.logCache(key, 'HIT');
        return entry.data as T;
    }

    /**
     * Set value in cache with TTL (in seconds)
     */
    set<T>(key: string, data: T, ttlSeconds: number): void {
        const expiresAt = Date.now() + (ttlSeconds * 1000);
        this.store.set(key, { data, expiresAt });
        logger.debug(`Cache SET: ${key}`, { ttl: `${ttlSeconds}s` });
    }

    /**
     * Delete value from cache
     */
    delete(key: string): void {
        this.store.delete(key);
        logger.debug(`Cache DELETE: ${key}`);
    }

    /**
     * Clear all cache entries
     */
    clear(): void {
        this.store.clear();
        logger.info('Cache cleared');
    }

    /**
     * Get or set pattern - fetch if not in cache
     */
    async getOrSet<T>(
        key: string,
        fetchFn: () => Promise<T>,
        ttlSeconds: number
    ): Promise<T> {
        // Try to get from cache
        const cached = this.get<T>(key);
        if (cached !== null) {
            return cached;
        }

        // Fetch fresh data
        const data = await fetchFn();
        this.set(key, data, ttlSeconds);
        return data;
    }

    /**
     * Get cache statistics
     */
    getStats() {
        return {
            size: this.store.size,
            keys: Array.from(this.store.keys()),
        };
    }
}

// Singleton instance
export const cache = new Cache();

/**
 * Cache TTL constants (in seconds)
 */
export const CacheTTL = {
    KPI_METRICS: 60, // 1 minute for KPIs
    REFERENCE_DATA: 300, // 5 minutes for reference data
    QUICK_METRICS: 30, // 30 seconds for frequently changing data
    STATIC_DATA: 3600, // 1 hour for static data
};

/**
 * Generate cache key for API endpoints
 */
export function generateCacheKey(endpoint: string, params?: Record<string, any>): string {
    if (!params || Object.keys(params).length === 0) {
        return endpoint;
    }

    const sortedParams = Object.keys(params)
        .sort()
        .map(key => `${key}=${params[key]}`)
        .join('&');

    return `${endpoint}?${sortedParams}`;
}
