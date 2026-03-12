/**
 * Job Queue Implementation
 * In-memory job queue with priority and retry support
 */

import { logger } from './logger';

export enum JobPriority {
    HIGH = 'HIGH',
    NORMAL = 'NORMAL',
    LOW = 'LOW',
}

export enum JobStatus {
    PENDING = 'pending',
    RUNNING = 'running',
    COMPLETED = 'completed',
    FAILED = 'failed',
    RETRYING = 'retrying',
}

export interface Job {
    id: string;
    scheduleId: string;
    scanType: string;
    database: string;
    schema: string;
    table: string;
    priority: JobPriority;
    status: JobStatus;
    retryCount: number;
    maxRetries: number;
    createdAt: Date;
    startedAt?: Date;
    completedAt?: Date;
    error?: string;
    result?: any;
}

class JobQueue {
    private queue: Job[] = [];
    private runningJobs: Map<string, Job> = new Map();
    private maxConcurrent: number = 5;
    private isProcessing: boolean = false;

    /**
     * Add job to queue
     */
    addJob(job: Omit<Job, 'id' | 'status' | 'createdAt'>): string {
        const jobId = crypto.randomUUID();
        const newJob: Job = {
            ...job,
            id: jobId,
            status: JobStatus.PENDING,
            createdAt: new Date(),
        };

        // Insert based on priority
        const priorityOrder = { HIGH: 0, NORMAL: 1, LOW: 2 };
        const insertIndex = this.queue.findIndex(
            (j) => priorityOrder[j.priority] > priorityOrder[newJob.priority]
        );

        if (insertIndex === -1) {
            this.queue.push(newJob);
        } else {
            this.queue.splice(insertIndex, 0, newJob);
        }

        logger.info(`Job added to queue: ${jobId}`, {
            scanType: job.scanType,
            table: `${job.database}.${job.schema}.${job.table}`,
            priority: job.priority,
            queueSize: this.queue.length,
        });

        // Start processing if not already running
        if (!this.isProcessing) {
            this.processQueue();
        }

        return jobId;
    }

    /**
     * Process jobs from queue
     */
    private async processQueue() {
        if (this.isProcessing) return;
        this.isProcessing = true;

        while (this.queue.length > 0 || this.runningJobs.size > 0) {
            // Start new jobs if under concurrent limit
            while (
                this.queue.length > 0 &&
                this.runningJobs.size < this.maxConcurrent
            ) {
                const job = this.queue.shift();
                if (job) {
                    this.executeJob(job);
                }
            }

            // Wait a bit before checking again
            await new Promise((resolve) => setTimeout(resolve, 100));
        }

        this.isProcessing = false;
    }

    /**
     * Execute a single job
     */
    private async executeJob(job: Job) {
        job.status = JobStatus.RUNNING;
        job.startedAt = new Date();
        this.runningJobs.set(job.id, job);

        logger.info(`Job started: ${job.id}`, {
            scanType: job.scanType,
            table: `${job.database}.${job.schema}.${job.table}`,
        });

        try {
            // Call the appropriate scan API based on scan type
            const result = await this.executeScan(job);

            job.status = JobStatus.COMPLETED;
            job.completedAt = new Date();
            job.result = result;

            logger.info(`Job completed: ${job.id}`, {
                duration: job.completedAt.getTime() - job.startedAt!.getTime(),
            });
        } catch (error: any) {
            job.error = error.message;

            // Retry logic
            if (job.retryCount < job.maxRetries) {
                job.retryCount++;
                job.status = JobStatus.RETRYING;

                logger.warn(`Job failed, retrying (${job.retryCount}/${job.maxRetries}): ${job.id}`, {
                    error: error.message,
                });

                // Re-add to queue with delay
                setTimeout(() => {
                    job.status = JobStatus.PENDING;
                    this.queue.unshift(job); // Add to front for retry
                }, 1000 * job.retryCount); // Exponential backoff
            } else {
                job.status = JobStatus.FAILED;
                job.completedAt = new Date();

                logger.error(`Job failed after ${job.maxRetries} retries: ${job.id}`, error);
            }
        } finally {
            this.runningJobs.delete(job.id);
        }
    }

    /**
     * Execute scan based on type
     */
    private async executeScan(job: Job): Promise<any> {
        const baseUrl = process.env.NEXT_PUBLIC_APP_URL || 'http://localhost:3000';

        if (job.scanType === 'profiling' || job.scanType === 'full') {
            const response = await fetch(`${baseUrl}/api/dq/run-profiling`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    database: job.database,
                    schema: job.schema,
                    table: job.table,
                    profile_level: 'BASIC',
                    triggered_by: 'scheduled',
                }),
            });
            return await response.json();
        }

        if (job.scanType === 'checks' || job.scanType === 'full') {
            const response = await fetch(`${baseUrl}/api/dq/run-custom-scan`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    database: job.database,
                    schema: job.schema,
                    table: job.table,
                    triggered_by: 'scheduled',
                }),
            });
            return await response.json();
        }

        throw new Error(`Unknown scan type: ${job.scanType}`);
    }

    /**
     * Get job status
     */
    getJob(jobId: string): Job | undefined {
        const running = this.runningJobs.get(jobId);
        if (running) return running;

        return this.queue.find((j) => j.id === jobId);
    }

    /**
     * Get queue statistics
     */
    getStats() {
        return {
            pending: this.queue.length,
            running: this.runningJobs.size,
            total: this.queue.length + this.runningJobs.size,
        };
    }

    /**
     * Set max concurrent jobs
     */
    setMaxConcurrent(max: number) {
        this.maxConcurrent = max;
    }
}

// Singleton instance
export const jobQueue = new JobQueue();
