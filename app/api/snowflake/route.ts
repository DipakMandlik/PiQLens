import { NextRequest, NextResponse } from 'next/server';
import { snowflakePool, executeQuery, getSnowflakeConfigFromEnv, SnowflakeConfig } from '@/lib/snowflake';

/**
 * POST /api/snowflake
 * Executes a SQL query against Snowflake
 * 
 * Request body:
 * {
 *   "query": "SELECT * FROM table_name LIMIT 10",
 *   "config": { ... } // Optional: Snowflake config. If not provided, uses env vars
 * }
 */
export async function POST(request: NextRequest) {
  try {
    const body = await request.json();
    const { query, config } = body;

    if (!query || typeof query !== 'string') {
      return NextResponse.json(
        { error: 'Query is required and must be a string' },
        { status: 400 }
      );
    }

    // Get connection from pool with optional config
    const connection = await snowflakePool.getConnection(config || undefined);

    // Execute query
    const result = await executeQuery(connection, query);

    return NextResponse.json({
      success: true,
      data: result,
    });
  } catch (error: any) {
    console.error('Snowflake API error:', error);
    return NextResponse.json(
      {
        success: false,
        error: error.message || 'Failed to execute query',
      },
      { status: 500 }
    );
  }
}

/**
 * GET /api/snowflake/test
 * Tests the Snowflake connection
 */
export async function GET(request: NextRequest) {
  try {
    const url = new URL(request.url);
    if (url.pathname.endsWith('/test')) {
      const connection = await snowflakePool.getConnection();
      const result = await executeQuery(connection, 'SELECT CURRENT_VERSION() as version, CURRENT_DATABASE() as database, CURRENT_SCHEMA() as schema');
      
      return NextResponse.json({
        success: true,
        message: 'Connection successful',
        data: result,
      });
    }

    return NextResponse.json(
      { error: 'Invalid endpoint' },
      { status: 404 }
    );
  } catch (error: any) {
    console.error('Snowflake connection test error:', error);
    return NextResponse.json(
      {
        success: false,
        error: error.message || 'Failed to connect to Snowflake',
      },
      { status: 500 }
    );
  }
}

