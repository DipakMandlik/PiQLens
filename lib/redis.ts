import Redis from 'ioredis';

let redis: Redis | null = null;

/**
 * Get or create Redis client singleton
 * Optimized for Vercel serverless environment
 * Returns null if caching is disabled or Redis is unavailable
 */
export function getRedisClient(): Redis | null {
    // Check if caching is explicitly disabled
    if (
        !process.env.ENABLE_REDIS_CACHE ||
        process.env.ENABLE_REDIS_CACHE === 'false'
    ) {
        return null;
    }

    // Check if Redis URL is provided
    const redisUrl = process.env.REDIS_URL || process.env.KV_URL;
    if (!redisUrl) {
        console.warn('Redis URL not configured. Caching disabled.');
        return null;
    }

    // Return existing client if already initialized and ready
    if (redis && redis.status === 'ready') {
        return redis;
    }

    try {
        // Create new Redis client with Vercel-optimized settings
        redis = new Redis(redisUrl, {
            maxRetriesPerRequest: 3,
            retryStrategy: (times: number) => {
                // Exponential backoff with max 2 seconds
                const delay = Math.min(times * 50, 2000);
                return delay;
            },
            enableReadyCheck: true,
            lazyConnect: true, // Critical for serverless cold starts
            tls: {
                // Enable TLS for Upstash and other cloud Redis providers
                rejectUnauthorized: false, // Accept self-signed certificates
            },
        });

        // Error handler - log but don't crash
        redis.on('error', (err) => {
            console.error('Redis error:', err.message);
        });

        redis.on('connect', () => {
            console.log('Redis connected');
        });

        redis.on('ready', () => {
            console.log('Redis ready');
        });

        return redis;
    } catch (error) {
        console.error('Failed to initialize Redis:', error);
        return null;
    }
}

/**
 * Close Redis connection
 * Should be called on application shutdown (not needed for serverless)
 */
export async function closeRedis(): Promise<void> {
    if (redis) {
        await redis.quit();
        redis = null;
    }
}

/**
 * Check if Redis is available and connected
 */
export async function isRedisAvailable(): Promise<boolean> {
    try {
        const client = getRedisClient();
        if (!client) return false;

        await client.ping();
        return true;
    } catch (error) {
        console.warn('Redis not available:', error);
        return false;
    }
}

/**
 * Gracefully handle Redis operations with fallback
 * Returns null if Redis is unavailable instead of throwing
 * 
 * @example
 * ```typescript
 * const result = await safeRedisOperation(
 *   async (client) => await client.get('key'),
 *   null
 * );
 * ```
 */
export async function safeRedisOperation<T>(
    operation: (client: Redis) => Promise<T>,
    fallbackValue: T | null = null
): Promise<T | null> {
    try {
        const client = getRedisClient();
        if (!client) return fallbackValue;

        return await operation(client);
    } catch (error) {
        console.warn('Redis operation failed, using fallback:', error);
        return fallbackValue;
    }
}
