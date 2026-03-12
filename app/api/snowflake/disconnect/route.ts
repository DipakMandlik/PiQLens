import { NextRequest, NextResponse } from 'next/server';
import { setServerConfig } from '@/lib/server-config';
import { snowflakePool } from '@/lib/snowflake';

/**
 * POST /api/snowflake/disconnect
 * Clears the stored Snowflake connection
 */
export async function POST(request: NextRequest) {
  try {
    // Close existing connection
    try {
      await snowflakePool.closeConnection();
    } catch (error) {
      // Ignore errors when closing
    }

    // Clear server-side config
    setServerConfig(null);

    return NextResponse.json({
      success: true,
      message: 'Disconnected from Snowflake',
    });
  } catch (error: any) {
    return NextResponse.json(
      {
        success: false,
        error: error.message || 'Failed to disconnect',
      },
      { status: 500 }
    );
  }
}

