/**
 * Structured Error Handling for Pi-Qualytics
 * Provides consistent error responses and categorization
 */

export enum ErrorCode {
    // Connection Errors
    CONNECTION_FAILED = 'CONNECTION_FAILED',
    CONNECTION_TIMEOUT = 'CONNECTION_TIMEOUT',
    CONNECTION_LOST = 'CONNECTION_LOST',

    // Authentication Errors
    AUTH_FAILED = 'AUTH_FAILED',
    AUTH_INVALID_CREDENTIALS = 'AUTH_INVALID_CREDENTIALS',
    AUTH_EXPIRED = 'AUTH_EXPIRED',

    // Query Errors
    QUERY_FAILED = 'QUERY_FAILED',
    QUERY_TIMEOUT = 'QUERY_TIMEOUT',
    QUERY_SYNTAX_ERROR = 'QUERY_SYNTAX_ERROR',
    QUERY_PERMISSION_DENIED = 'QUERY_PERMISSION_DENIED',

    // Data Errors
    DATA_NOT_FOUND = 'DATA_NOT_FOUND',
    DATA_INVALID = 'DATA_INVALID',

    // System Errors
    INTERNAL_ERROR = 'INTERNAL_ERROR',
    SERVICE_UNAVAILABLE = 'SERVICE_UNAVAILABLE',

    // Validation Errors
    VALIDATION_ERROR = 'VALIDATION_ERROR',
    MISSING_PARAMETER = 'MISSING_PARAMETER',
}

export interface ApiError {
    code: ErrorCode;
    message: string;
    userMessage: string;
    details?: any;
    retryable: boolean;
}

export class SnowflakeError extends Error {
    public code: ErrorCode;
    public userMessage: string;
    public details?: any;
    public retryable: boolean;

    constructor(error: ApiError) {
        super(error.message);
        this.name = 'SnowflakeError';
        this.code = error.code;
        this.userMessage = error.userMessage;
        this.details = error.details;
        this.retryable = error.retryable;
    }
}

/**
 * Categorizes Snowflake errors and returns structured error info
 */
export function categorizeSnowflakeError(error: any): ApiError {
    const errorMessage = error?.message || error?.toString() || 'Unknown error';
    const errorCode = error?.code;

    // Connection errors
    if (errorMessage.includes('ECONNREFUSED') || errorMessage.includes('ENOTFOUND')) {
        return {
            code: ErrorCode.CONNECTION_FAILED,
            message: errorMessage,
            userMessage: 'Unable to connect to Snowflake. Please check your connection settings.',
            details: { originalError: errorCode },
            retryable: true,
        };
    }

    if (errorMessage.includes('timeout') || errorMessage.includes('ETIMEDOUT')) {
        return {
            code: ErrorCode.CONNECTION_TIMEOUT,
            message: errorMessage,
            userMessage: 'Connection to Snowflake timed out. Please try again.',
            details: { originalError: errorCode },
            retryable: true,
        };
    }

    // Authentication errors
    if (errorMessage.includes('Incorrect username or password') ||
        errorMessage.includes('Authentication failed') ||
        errorCode === '390100') {
        return {
            code: ErrorCode.AUTH_INVALID_CREDENTIALS,
            message: errorMessage,
            userMessage: 'Invalid Snowflake credentials. Please check your username and password.',
            details: { originalError: errorCode },
            retryable: false,
        };
    }

    if (errorMessage.includes('expired') || errorCode === '390114') {
        return {
            code: ErrorCode.AUTH_EXPIRED,
            message: errorMessage,
            userMessage: 'Your Snowflake session has expired. Please reconnect.',
            details: { originalError: errorCode },
            retryable: false,
        };
    }

    // Query errors
    if (errorMessage.includes('SQL compilation error') ||
        errorMessage.includes('syntax error') ||
        errorCode === '001003') {
        return {
            code: ErrorCode.QUERY_SYNTAX_ERROR,
            message: errorMessage,
            userMessage: 'Invalid query syntax. Please contact support.',
            details: { originalError: errorCode },
            retryable: false,
        };
    }

    if (errorMessage.includes('does not exist') ||
        errorMessage.includes('Object') ||
        errorCode === '002003') {
        return {
            code: ErrorCode.DATA_NOT_FOUND,
            message: errorMessage,
            userMessage: 'Requested data not found in Snowflake.',
            details: { originalError: errorCode },
            retryable: false,
        };
    }

    if (errorMessage.includes('permission') ||
        errorMessage.includes('access denied') ||
        errorCode === '003001') {
        return {
            code: ErrorCode.QUERY_PERMISSION_DENIED,
            message: errorMessage,
            userMessage: 'Permission denied. Please check your Snowflake role permissions.',
            details: { originalError: errorCode },
            retryable: false,
        };
    }

    // Query timeout
    if (errorMessage.includes('Query execution time exceeded') ||
        errorMessage.includes('statement timeout')) {
        return {
            code: ErrorCode.QUERY_TIMEOUT,
            message: errorMessage,
            userMessage: 'Query took too long to execute. Please try again or contact support.',
            details: { originalError: errorCode },
            retryable: true,
        };
    }

    // Default to internal error
    return {
        code: ErrorCode.INTERNAL_ERROR,
        message: errorMessage,
        userMessage: 'An unexpected error occurred. Please try again later.',
        details: { originalError: errorCode, stack: error?.stack },
        retryable: true,
    };
}

/**
 * Creates a standardized error response for API endpoints
 */
export function createErrorResponse(error: any) {
    const categorizedError = categorizeSnowflakeError(error);

    return {
        success: false,
        error: {
            code: categorizedError.code,
            message: categorizedError.message,
            userMessage: categorizedError.userMessage,
        },
        metadata: {
            timestamp: new Date().toISOString(),
            retryable: categorizedError.retryable,
        },
    };
}

/**
 * Determines if an error is retryable
 */
export function isRetryableError(error: any): boolean {
    const categorized = categorizeSnowflakeError(error);
    return categorized.retryable;
}
