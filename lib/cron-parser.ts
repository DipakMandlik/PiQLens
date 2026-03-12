/**
 * Cron Expression Parser and Utilities
 * Supports standard cron syntax and schedule type conversion
 */

import { CronExpressionParser } from 'cron-parser';

export interface CronSchedule {
    expression: string;
    timezone: string;
    nextRun: Date;
    description: string;
}

/**
 * Validate cron expression
 */
export function validateCronExpression(expression: string): boolean {
    try {
        CronExpressionParser.parse(expression);
        return true;
    } catch (error) {
        return false;
    }
}

/**
 * Get next run time from cron expression
 */
export function getNextRunTime(expression: string, timezone: string = 'UTC'): Date {
    try {
        const interval = CronExpressionParser.parse(expression, {
            currentDate: new Date(),
            tz: timezone,
        });
        return interval.next().toDate();
    } catch (error) {
        throw new Error(`Invalid cron expression: ${expression}`);
    }
}

/**
 * Get multiple next run times
 */
export function getNextRunTimes(
    expression: string,
    count: number = 5,
    timezone: string = 'UTC'
): Date[] {
    try {
        const interval = CronExpressionParser.parse(expression, {
            currentDate: new Date(),
            tz: timezone,
        });

        const dates: Date[] = [];
        for (let i = 0; i < count; i++) {
            dates.push(interval.next().toDate());
        }
        return dates;
    } catch (error) {
        throw new Error(`Invalid cron expression: ${expression}`);
    }
}

/**
 * Convert simple schedule type to cron expression
 */
export function scheduleTypeToCron(
    scheduleType: string,
    scheduleTime?: string,
    scheduleDays?: string[]
): string {
    if (scheduleType === 'hourly') {
        return '0 * * * *'; // Every hour at minute 0
    }

    if (scheduleType === 'daily' && scheduleTime) {
        const [hours, minutes] = scheduleTime.split(':').map(Number);
        return `${minutes} ${hours} * * *`; // Daily at specified time
    }

    if (scheduleType === 'weekly' && scheduleTime && scheduleDays) {
        const [hours, minutes] = scheduleTime.split(':').map(Number);
        const dayNumbers = scheduleDays.map(day => {
            const dayMap: Record<string, number> = {
                'Sunday': 0, 'Monday': 1, 'Tuesday': 2, 'Wednesday': 3,
                'Thursday': 4, 'Friday': 5, 'Saturday': 6
            };
            return dayMap[day] || 0;
        }).join(',');
        return `${minutes} ${hours} * * ${dayNumbers}`; // Weekly on specified days
    }

    if (scheduleType === 'monthly' && scheduleTime) {
        const [hours, minutes] = scheduleTime.split(':').map(Number);
        return `${minutes} ${hours} 1 * *`; // First day of every month
    }

    throw new Error(`Unsupported schedule type: ${scheduleType}`);
}

/**
 * Get human-readable description of cron expression
 */
export function describeCronExpression(expression: string): string {
    try {
        const parts = expression.split(' ');
        if (parts.length !== 5) {
            return expression;
        }

        const [minute, hour, dayOfMonth, month, dayOfWeek] = parts;

        // Hourly
        if (minute !== '*' && hour === '*' && dayOfMonth === '*' && month === '*' && dayOfWeek === '*') {
            return `Every hour at minute ${minute}`;
        }

        // Daily
        if (minute !== '*' && hour !== '*' && dayOfMonth === '*' && month === '*' && dayOfWeek === '*') {
            return `Daily at ${hour.padStart(2, '0')}:${minute.padStart(2, '0')}`;
        }

        // Weekly
        if (minute !== '*' && hour !== '*' && dayOfMonth === '*' && month === '*' && dayOfWeek !== '*') {
            const dayNames = ['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday'];
            const days = dayOfWeek.split(',').map(d => dayNames[parseInt(d)] || d).join(', ');
            return `Weekly on ${days} at ${hour.padStart(2, '0')}:${minute.padStart(2, '0')}`;
        }

        // Every N minutes
        if (minute.startsWith('*/') && hour === '*') {
            const interval = minute.substring(2);
            return `Every ${interval} minutes`;
        }

        // Every N hours
        if (hour.startsWith('*/') && minute !== '*') {
            const interval = hour.substring(2);
            return `Every ${interval} hours at minute ${minute}`;
        }

        return expression;
    } catch (error) {
        return expression;
    }
}

/**
 * Check if schedule is due to run
 */
export function isScheduleDue(nextRunAt: Date): boolean {
    return new Date() >= nextRunAt;
}

/**
 * Calculate next run time after current execution
 */
export function calculateNextRun(
    cronExpression: string,
    timezone: string = 'UTC'
): Date {
    return getNextRunTime(cronExpression, timezone);
}

/**
 * Common cron expression presets
 */
export const CronPresets = {
    EVERY_MINUTE: '* * * * *',
    EVERY_5_MINUTES: '*/5 * * * *',
    EVERY_15_MINUTES: '*/15 * * * *',
    EVERY_30_MINUTES: '*/30 * * * *',
    EVERY_HOUR: '0 * * * *',
    EVERY_2_HOURS: '0 */2 * * *',
    EVERY_6_HOURS: '0 */6 * * *',
    DAILY_MIDNIGHT: '0 0 * * *',
    DAILY_9AM: '0 9 * * *',
    DAILY_6PM: '0 18 * * *',
    WEEKDAYS_9AM: '0 9 * * 1-5',
    WEEKENDS_10AM: '0 10 * * 0,6',
    WEEKLY_MONDAY_9AM: '0 9 * * 1',
    MONTHLY_FIRST_DAY: '0 0 1 * *',
};
