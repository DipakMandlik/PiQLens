import { getValkeyClient } from './valkey';

/**
 * Basic rate limiting using Valkey.
 * Uses a token bucket or fixed window approach. 
 * For simplicity, we implement a fixed window counter.
 *
 * @param identifier Unique ID for the client (e.g., IP address or User ID)
 * @param action The action being rate-limited (e.g., 'run-scan', 'login')
 * @param limit Maximum number of requests allowed in the window
 * @param windowSec The time window in seconds
 * @returns Object indicating whether the request is allowed and remaining quota
 */
export async function rateLimit(
    identifier: string,
    action: string,
    limit: number,
    windowSec: number
): Promise<{ allowed: boolean; remaining: number; resetTime: number }> {
    const redis = getValkeyClient();

    // If cache is disabled, bypass rate limiting
    if (!redis) {
        return { allowed: true, remaining: limit, resetTime: 0 };
    }

    const key = `piqlens:${process.env.NODE_ENV || 'development'}:ratelimit:${action}:${identifier}`;

    try {
        const currentCountStr = await redis.get(key);
        let currentCount = currentCountStr ? parseInt(currentCountStr, 10) : 0;

        if (currentCount >= limit) {
            const ttl = await redis.pttl(key);
            return {
                allowed: false,
                remaining: 0,
                resetTime: Date.now() + (ttl > 0 ? ttl : windowSec * 1000)
            };
        }

        // Increment logically
        const multi = redis.multi();
        multi.incr(key);
        if (currentCount === 0) {
            multi.expire(key, windowSec);
        }

        const results = await multi.exec();

        // results is an array of [error, result]
        if (results && results[0] && !results[0][0]) {
            currentCount = results[0][1] as number;
        }

        return {
            allowed: true,
            remaining: Math.max(0, limit - currentCount),
            resetTime: Date.now() + (windowSec * 1000)
        };
    } catch (e) {
        console.error('Rate limit error:', e);
        // Fail open if Valkey is down
        return { allowed: true, remaining: 1, resetTime: 0 };
    }
}
