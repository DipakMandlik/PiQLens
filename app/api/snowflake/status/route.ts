import { NextRequest, NextResponse } from 'next/server';
import { getServerConfig, hasServerConfig } from '@/lib/server-config';

/**
 * GET /api/snowflake/status
 * Checks if there's a stored Snowflake connection
 */
export async function GET(request: NextRequest) {
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
      } : null,
    });
  } catch (error: any) {
    return NextResponse.json(
      {
        success: false,
        error: error.message || 'Failed to check connection status',
      },
      { status: 500 }
    );
  }
}

