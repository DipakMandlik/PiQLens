import { NextResponse } from 'next/server';
import { getServerConfig, hasServerConfig } from '@/lib/server-config';

/**
 * GET /api/snowflake/status
 * Checks if there's a stored Snowflake connection
 */
export async function GET() {
  try {
    const isConnected = hasServerConfig();
    const config = getServerConfig();

    return NextResponse.json({
      success: true,
      isConnected,
      hasConfig: isConnected,
      // Don't return sensitive data, just indicate if config exists
      config: config ? {
        accountUrl: config.accountUrl ? '***' : undefined,
        database: config.database,
        schema: config.schema,
        warehouse: config.warehouse,
        username: config.username ? '***' : undefined,
        authMethod: config.privateKeyPath ? 'key-pair' : (config.password || config.token) ? 'password-or-token' : 'unknown',
      } : null,
    });
  } catch (error: unknown) {
    return NextResponse.json(
      {
        success: false,
        error: error instanceof Error ? error.message : 'Failed to check connection status',
      },
      { status: 500 }
    );
  }
}

