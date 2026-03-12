import Redis from 'ioredis';
import { logger } from './logger';

// Singleton instance
let valkeyClient: Redis | null = null;

const ENV = process.env.NODE_ENV || 'development';

export function getValkeyClient(): Redis | null {
    if (process.env.ENABLE_VALKEY_CACHE === 'false') return null;

    const url = process.env.VALKEY_URL || process.env.REDIS_URL || process.env.KV_URL;
    if (!url) return null;

    if (valkeyClient && valkeyClient.status === 'ready') {
        return valkeyClient;
    }

    try {
        valkeyClient = new Redis(url, {
            maxRetriesPerRequest: 3,
            retryStrategy: (times: number) => Math.min(times * 50, 2000),
            enableReadyCheck: true,
            lazyConnect: true,
            family: 0, // Automatically prefers IPv4/IPv6 depending on OS capabilities (helps with Upstash endpoints)
            tls: url.startsWith('rediss://') ? { rejectUnauthorized: false } : undefined,
        });

        valkeyClient.on('error', (err) => console.error('Valkey connection error:', err.message));

        return valkeyClient;
    } catch (error) {
        console.error('Failed to initialize Valkey client:', error);
        return null;
    }
}

/**
 * Generates a strictly namespaced cache key.
 * Expected format: piqlens:{env}:{module}:{resource}:{identifier}
 */
export function buildCacheKey(moduleName: string, resource: string, identifier: string = 'all'): string {
    return `piqlens:${ENV}:${moduleName}:${resource}:${identifier}`;
}

/**
 * Primary read-through cache wrapper for PI_QLens
 * @param key The strict namespaced cache key
 * @param ttlSeconds Time-to-live in seconds
 * @param fetchFn The heavy database/API function to execute on a cache miss
 */
export async function getOrSetCache<T>(
    key: string,
    ttlSeconds: number,
    fetchFn: () => Promise<T>
): Promise<T> {
    const client = getValkeyClient();

    // If cache disabled/unavailable, jump straight to the source
    if (!client) {
        return await fetchFn();
    }

    try {
        const cachedValue = await client.get(key);
        if (cachedValue) {
            return JSON.parse(cachedValue) as T;
        }
    } catch (error) {
        console.warn(`Valkey read error for ${key}:`, error);
        // On read error, gracefully fall back to source
    }

    // Cache miss or read error
    const data = await fetchFn();

    try {
        if (data !== undefined && data !== null) {
            await client.setex(key, ttlSeconds, JSON.stringify(data));
        }
    } catch (error) {
        console.warn(`Valkey write error for ${key}:`, error);
    }

    return data;
}

/**
 * Invalidates specific cache keys. Supports exact matches or safe glob pattern invalidation.
 */
export async function invalidateCache(pattern: string): Promise<void> {
    const client = getValkeyClient();
    if (!client) return;

    try {
        if (pattern.includes('*')) {
            // Use SCAN to find keys instead of blocking KEYS for production safety
            let cursor = '0';
            const keysToDelete: string[] = [];

            do {
                const [nextCursor, keys] = await client.scan(cursor, 'MATCH', pattern, 'COUNT', 100);
                cursor = nextCursor;
                if (keys.length > 0) {
                    keysToDelete.push(...keys);
                }
            } while (cursor !== '0');

            if (keysToDelete.length > 0) {
                await client.del(...keysToDelete);
                logger.info(`Invalidated ${keysToDelete.length} Valkey keys matching: ${pattern}`);
            }
        } else {
            const count = await client.del(pattern);
            if (count > 0) {
                logger.info(`Invalidated Valkey key: ${pattern}`);
            }
        }
    } catch (error) {
        console.error(`Failed to invalidate Valkey cache for pattern ${pattern}:`, error);
    }
}
