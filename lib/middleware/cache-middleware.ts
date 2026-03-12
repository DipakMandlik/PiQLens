import { NextRequest, NextResponse } from 'next/server';
import { getCache, setCache, CacheTTL } from '../cache-service';
import { createHash } from 'crypto';

/**
 * Cache middleware configuration
 */
export interface CacheConfig {
    /** Cache TTL in seconds */
    ttl?: number;
    /** Cache key prefix */
    prefix?: string;
    /** Whether to include query parameters in cache key */
    includeQuery?: boolean;
    /** Whether to include request body in cache key */
    includeBody?: boolean;
    /** Custom cache key generator */
    keyGenerator?: (req: NextRequest) => string | Promise<string>;
    /** Whether to bypass cache (useful for debugging) */
    bypassCache?: boolean;
}

/**
 * Generate a cache key from request
 */
async function generateCacheKey(
    req: NextRequest,
    config: CacheConfig
): Promise<string> {
    // Use custom key generator if provided
    if (config.keyGenerator) {
        return config.keyGenerator(req);
    }

    const parts: string[] = [config.prefix || 'api'];

    // Add pathname
    parts.push(req.nextUrl.pathname);

    // Add query parameters if configured
    if (config.includeQuery && req.nextUrl.search) {
        const searchParams = new URLSearchParams(req.nextUrl.search);
        const sortedParams = Array.from(searchParams.entries())
            .sort(([a], [b]) => a.localeCompare(b))
            .map(([key, value]) => `${key}=${value}`)
            .join('&');
        parts.push(searchParams.toString());
    }

    // Add request body hash if configured
    if (config.includeBody && req.method === 'POST') {
        try {
            const body = await req.clone().text();
            if (body) {
                const hash = createHash('md5').update(body).digest('hex');
                parts.push(hash);
            }
        } catch (error) {
            console.warn('Failed to hash request body:', error);
        }
    }

    return parts.join(':');
}

/**
 * Wrap an API handler with caching
 */
export function withCache<T = unknown>(
    handler: (req: NextRequest) => Promise<NextResponse<T>>,
    config: CacheConfig = {}
) {
    return async (req: NextRequest): Promise<NextResponse<T>> => {
        // Check if caching is enabled
        const cacheEnabled = process.env.ENABLE_REDIS_CACHE !== 'false';

        if (!cacheEnabled || config.bypassCache) {
            return handler(req);
        }

        // Only cache GET requests by default
        if (req.method !== 'GET') {
            return handler(req);
        }

        try {
            // Generate cache key
            const cacheKey = await generateCacheKey(req, config);

            // Try to get from cache
            const cached = await getCache<{
                data: T;
                headers: Record<string, string>;
                status: number;
            }>(cacheKey);

            if (cached) {
                // Return cached response
                const response = NextResponse.json(cached.data, {
                    status: cached.status,
                    headers: {
                        ...cached.headers,
                        'X-Cache': 'HIT',
                        'X-Cache-Key': cacheKey,
                    },
                });

                return response as NextResponse<T>;
            }

            // Cache miss - call handler
            const response = await handler(req);

            // Only cache successful responses
            if (response.ok) {
                const data = await response.clone().json();

                // Extract headers
                const headers: Record<string, string> = {};
                response.headers.forEach((value, key) => {
                    headers[key] = value;
                });

                // Store in cache
                await setCache(
                    cacheKey,
                    {
                        data,
                        headers,
                        status: response.status,
                    },
                    config.ttl || CacheTTL.SHORT
                );

                // Add cache headers
                response.headers.set('X-Cache', 'MISS');
                response.headers.set('X-Cache-Key', cacheKey);
            }

            return response;
        } catch (error) {
            console.error('Cache middleware error:', error);
            // On error, bypass cache and call handler directly
            return handler(req);
        }
    };
}

/**
 * Cache invalidation helper
 * Use this to invalidate cache when data changes
 */
export async function invalidateCache(pattern: string): Promise<void> {
    const { deleteCachePattern } = await import('../cache-service');
    await deleteCachePattern(pattern);
}

/**
 * Preset cache configurations
 */
export const CachePresets = {
    /** Short-lived cache (5 minutes) */
    SHORT: {
        ttl: CacheTTL.SHORT,
        includeQuery: true,
    } as CacheConfig,

    /** Medium-lived cache (30 minutes) */
    MEDIUM: {
        ttl: CacheTTL.PAGE_STATE,
        includeQuery: true,
    } as CacheConfig,

    /** Long-lived cache (1 hour) */
    LONG: {
        ttl: CacheTTL.DATASET,
        includeQuery: true,
    } as CacheConfig,

    /** Dataset cache (1 hour, includes query params) */
    DATASET: {
        ttl: CacheTTL.DATASET,
        prefix: 'dataset',
        includeQuery: true,
    } as CacheConfig,

    /** Table metadata cache (1 hour) */
    TABLE_METADATA: {
        ttl: CacheTTL.TABLE_METADATA,
        prefix: 'table',
        includeQuery: true,
    } as CacheConfig,

    /** Quality score cache (30 minutes) */
    QUALITY_SCORE: {
        ttl: CacheTTL.QUALITY_SCORE,
        prefix: 'quality',
        includeQuery: true,
    } as CacheConfig,
};
