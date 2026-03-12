/**
 * Retry Logic for Pi-Qualytics
 * Implements exponential backoff for transient failures
 */

import { isRetryableError } from './errors';
import { logger } from './logger';

export interface RetryOptions {
    maxAttempts?: number;
    initialDelayMs?: number;
    maxDelayMs?: number;
    backoffMultiplier?: number;
}

const DEFAULT_RETRY_OPTIONS: Required<RetryOptions> = {
    maxAttempts: 3,
    initialDelayMs: 1000, // 1 second
    maxDelayMs: 10000, // 10 seconds
    backoffMultiplier: 2,
};

/**
 * Sleep for specified milliseconds
 */
function sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * Calculate delay with exponential backoff
 */
function calculateDelay(attempt: number, options: Required<RetryOptions>): number {
    const delay = options.initialDelayMs * Math.pow(options.backoffMultiplier, attempt - 1);
    return Math.min(delay, options.maxDelayMs);
}

/**
 * Retry a function with exponential backoff
 */
export async function retryWithBackoff<T>(
    fn: () => Promise<T>,
    options: RetryOptions = {},
    context?: string
): Promise<T> {
    const opts = { ...DEFAULT_RETRY_OPTIONS, ...options };
    let lastError: any;

    for (let attempt = 1; attempt <= opts.maxAttempts; attempt++) {
        try {
            const result = await fn();

            if (attempt > 1) {
                logger.info(`Retry succeeded on attempt ${attempt}`, { context });
            }

            return result;
        } catch (error) {
            lastError = error;

            // Check if error is retryable
            if (!isRetryableError(error)) {
                logger.warn(`Non-retryable error encountered`, { context, error });
                throw error;
            }

            // Don't retry if this was the last attempt
            if (attempt === opts.maxAttempts) {
                logger.error(`All ${opts.maxAttempts} retry attempts failed`, error, { context });
                throw error;
            }

            // Calculate delay and wait
            const delay = calculateDelay(attempt, opts);
            logger.warn(
                `Attempt ${attempt}/${opts.maxAttempts} failed, retrying in ${delay}ms`,
                { context, error: (error as any)?.message }
            );

            await sleep(delay);
        }
    }

    throw lastError;
}

/**
 * Retry a Snowflake query with backoff
 */
export async function retryQuery<T>(
    queryFn: () => Promise<T>,
    queryName: string,
    options?: RetryOptions
): Promise<T> {
    return retryWithBackoff(queryFn, options, `Query: ${queryName}`);
}

/**
 * Retry a connection attempt with backoff
 */
export async function retryConnection<T>(
    connectFn: () => Promise<T>,
    options?: RetryOptions
): Promise<T> {
    return retryWithBackoff(connectFn, options, 'Snowflake Connection');
}
