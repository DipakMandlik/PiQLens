import { NextRequest, NextResponse } from 'next/server';
import type { SnowflakeConfig } from '@/lib/snowflake';
import { setServerConfig } from '@/lib/server-config';
import { logger } from '@/lib/logger';

export const runtime = 'nodejs';

/**
 * Parse Snowflake SDK errors and return user-friendly error messages
 */
function parseSnowflakeError(error: unknown): { message: string; statusCode: number } {
  const typedError = error as { code?: number; message?: string };
  const errorCode = typedError?.code;
  const errorMessage = typedError?.message || '';

  // Authentication errors
  if (errorCode === 390100 || errorCode === 390144) {
    return {
      message: 'Invalid username or password. Please check your credentials and try again.',
      statusCode: 401,
    };
  }

  // Key-pair specific setup errors
  if (errorMessage.includes('A password must be specified')) {
    return {
      message: 'Key-pair authentication failed to initialize. Verify authenticator and private key settings.',
      statusCode: 400,
    };
  }

  if (errorMessage.includes('Private key file not found at path')) {
    return {
      message: errorMessage,
      statusCode: 400,
    };
  }

  // Invalid account identifier
  if (errorCode === 390201 || errorMessage.includes('account') || errorMessage.includes('Account')) {
    return {
      message: 'Invalid account identifier. Please verify your Snowflake account URL.',
      statusCode: 401,
    };
  }

  // Network/connection errors
  if (errorMessage.includes('ENOTFOUND') || errorMessage.includes('ETIMEDOUT') || errorMessage.includes('ECONNREFUSED')) {
    return {
      message: 'Unable to connect to Snowflake. Please check your network connection and account URL.',
      statusCode: 500,
    };
  }

  // Role/warehouse access errors
  if (errorCode === 2043 || errorMessage.includes('does not exist or not authorized')) {
    return {
      message: 'Access denied. Please verify your role and warehouse permissions.',
      statusCode: 403,
    };
  }

  if (errorMessage.includes('specified in the connect string is not granted to this user')) {
    return {
      message: 'The selected role is not granted to this user. Use PIQLENS_ENGINEER_ROLE or leave role blank to use the user default role.',
      statusCode: 403,
    };
  }

  // Incorrect username format
  if (errorMessage.includes('Incorrect username or password was specified')) {
    return {
      message: 'Invalid username or password. Please check your credentials and try again.',
      statusCode: 401,
    };
  }

  // Generic error
  return {
    message: errorMessage || 'Failed to connect to Snowflake. Please try again.',
    statusCode: 500,
  };
}

/**
 * POST /api/snowflake/connect
 * Tests the Snowflake connection with provided credentials
 * 
 * Request body:
 * {
 *   "accountUrl": "...",
 *   "username": "...",
 *   "password": "..." or "token": "..." or "privateKeyPath": "...",
 *   "privateKeyPassphrase": "...", // optional
 *   "role": "..." // optional
 * }
 * 
 * Note: Warehouse, database, and schema are optional and can be set later
 */
export async function POST(request: NextRequest) {
  const startTime = Date.now();

  try {
    const config: SnowflakeConfig = await request.json();

    // Log authentication attempt (without sensitive data)
    logger.info('Snowflake authentication attempt', {
      endpoint: '/api/snowflake/connect',
      accountUrl: config.accountUrl,
      username: config.username,
      hasPassword: !!config.password,
      hasToken: !!config.token,
      hasPrivateKeyPath: !!config.privateKeyPath,
      role: config.role,
    });

    // Validate required fields
    if (!config.accountUrl && !config.account) {
      logger.warn('Missing account URL or account identifier');
      return NextResponse.json(
        { success: false, error: 'Account URL or Account is required' },
        { status: 400 }
      );
    }

    const hasPasswordAuth = !!(config.password || config.token);
    const hasKeyPairAuth = !!config.privateKeyPath;

    if (!config.username || (!hasPasswordAuth && !hasKeyPairAuth)) {
      logger.warn('Missing username or authentication method');
      return NextResponse.json(
        { success: false, error: 'Missing required fields: username and (password/token or private key path)' },
        { status: 400 }
      );
    }

    // Import Snowflake helpers at runtime to avoid bundler/Turbopack issues
    const { snowflakePool, executeQuery } = await import('@/lib/snowflake');
    const { getMetadata } = await import('@/services/snowflake/metadataService');

    // Test connection by executing a simple query
    // Note: We don't require warehouse/database/schema for initial connection
    // These can be set later when user selects them from the sidebar
    const connection = await snowflakePool.getConnection(config);

    // Query to test connection and get account info (cached for 10 min)
    const result = await getMetadata(connection, executeQuery);

    // Store config server-side for future API calls
    setServerConfig(config);

    const duration = Date.now() - startTime;
    logger.info('Snowflake authentication successful', {
      endpoint: '/api/snowflake/connect',
      username: config.username,
      duration: `${duration}ms`,
    });

    return NextResponse.json({
      success: true,
      message: 'Connection successful! Select warehouse, database, and schema from the sidebar to start.',
      data: result,
    });
  } catch (error: unknown) {
    const duration = Date.now() - startTime;
    const typedError = error as { code?: number; sqlState?: string };

    // Parse the error to get user-friendly message and appropriate status code
    const { message, statusCode } = parseSnowflakeError(error);

    // Log the error with full details for debugging
    logger.error('Snowflake authentication failed', error, {
      endpoint: '/api/snowflake/connect',
      errorCode: typedError?.code,
      sqlState: typedError?.sqlState,
      duration: `${duration}ms`,
    });

    return NextResponse.json(
      {
        success: false,
        error: message,
      },
      { status: statusCode }
    );
  }
}

