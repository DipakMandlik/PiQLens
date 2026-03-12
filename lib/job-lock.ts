import { getValkeyClient } from './valkey';

/**
 * Basic job locking using Valkey to prevent multiple simultaneous executions
 * of the same job across instances (e.g., Snowflake Data Quality Scans).
 */
export class JobLock {
    private redis = getValkeyClient();
    private lockKey: string;
    private lockTtlSec: number;
    private locked: boolean = false;

    constructor(jobName: string, identifier: string, maxDurationSec: number = 300) {
        this.lockKey = `piqlens:${process.env.NODE_ENV || 'development'}:locks:${jobName}:${identifier}`;
        this.lockTtlSec = maxDurationSec;
    }

    /**
     * Attempts to acquire the lock. Returns true if successful.
     */
    async acquire(): Promise<boolean> {
        if (!this.redis) return true; // Fail open if no valkey

        try {
            // SETNX (set if not exists)
            const result = await this.redis.set(this.lockKey, 'locked', 'EX', this.lockTtlSec, 'NX');
            if (result === 'OK') {
                this.locked = true;
                return true;
            }
            return false;
        } catch (error) {
            console.error('Job lock acquisition error:', error);
            return true; // fail open
        }
    }

    /**
     * Releases the lock.
     */
    async release(): Promise<void> {
        if (!this.redis || !this.locked) return;

        try {
            await this.redis.del(this.lockKey);
            this.locked = false;
        } catch (error) {
            console.error('Job lock release error:', error);
        }
    }

    /**
     * Refreshes the lock to prevent it from expiring if the job takes too long.
     */
    async refresh(): Promise<boolean> {
        if (!this.redis || !this.locked) return false;

        try {
            const result = await this.redis.expire(this.lockKey, this.lockTtlSec);
            return result === 1;
        } catch (error) {
            console.error('Job lock refresh error:', error);
            return false;
        }
    }
}
