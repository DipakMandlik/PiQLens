/**
 * Structured Logging for Pi-Qualytics
 * Provides consistent logging with levels and context
 */

export enum LogLevel {
    DEBUG = 'DEBUG',
    INFO = 'INFO',
    WARN = 'WARN',
    ERROR = 'ERROR',
}

export interface LogContext {
    endpoint?: string;
    userId?: string;
    queryTime?: number;
    database?: string;
    schema?: string;
    table?: string;
    [key: string]: any;
}

class Logger {
    private isDevelopment = process.env.NODE_ENV !== 'production';

    private formatMessage(level: LogLevel, message: string, context?: LogContext): string {
        const timestamp = new Date().toISOString();
        const contextStr = context ? ` | ${JSON.stringify(context)}` : '';
        return `[${timestamp}] [${level}] ${message}${contextStr}`;
    }

    debug(message: string, context?: LogContext) {
        if (this.isDevelopment) {
            console.log(this.formatMessage(LogLevel.DEBUG, message, context));
        }
    }

    info(message: string, context?: LogContext) {
        console.log(this.formatMessage(LogLevel.INFO, message, context));
    }

    warn(message: string, context?: LogContext) {
        console.warn(this.formatMessage(LogLevel.WARN, message, context));
    }

    error(message: string, error?: any, context?: LogContext) {
        const errorContext = {
            ...context,
            error: error?.message || error,
            stack: error?.stack,
        };
        console.error(this.formatMessage(LogLevel.ERROR, message, errorContext));
    }

    /**
     * Log API request
     */
    logApiRequest(endpoint: string, method: string, params?: any) {
        this.info(`API Request: ${method} ${endpoint}`, { endpoint, params });
    }

    /**
     * Log API response
     */
    logApiResponse(endpoint: string, success: boolean, duration: number) {
        this.info(`API Response: ${endpoint}`, {
            endpoint,
            success,
            duration: `${duration}ms`,
        });
    }

    /**
     * Log query execution
     */
    logQuery(query: string, duration?: number, rowCount?: number) {
        this.debug('Query executed', {
            query: query.substring(0, 200), // Truncate long queries
            duration: duration ? `${duration}ms` : undefined,
            rowCount,
        });
    }

    /**
     * Log cache hit/miss/refresh/invalidate
     */
    logCache(key: string, event: 'HIT' | 'MISS' | 'REFRESH' | 'INVALIDATE') {
        this.info(`[Cache] ${event} - ${key}`);
    }
}

// Singleton instance
export const logger = new Logger();
