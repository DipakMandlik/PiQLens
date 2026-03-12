import { NextRequest, NextResponse } from 'next/server';
import { getServerConfig } from '@/lib/server-config';

export const runtime = 'nodejs';

/**
 * GET /api/snowflake/databases
 * Fetches list of all databases accessible to the connected user
 */
export async function GET(request: NextRequest) {
  try {
    // Retrieve Snowflake connection configuration from server-side storage
    const config = getServerConfig();
    
    // Validate that we have a valid Snowflake connection configuration
    if (!config) {
      return NextResponse.json(
        {
          success: false,
          error: 'Not connected to Snowflake. Please connect first.',
        },
        { status: 401 }
      );
    }

    // Import Snowflake helpers
    const { snowflakePool, executeQuery } = await import('@/lib/snowflake');

    // Get connection from pool
    const connection = await snowflakePool.getConnection(config);
    
    // Query to get all databases
    const query = 'SHOW DATABASES';
    const result = await executeQuery(connection, query);

    // List of system/sample databases to exclude
    const excludedDatabases = [
      'SNOWFLAKE',
      'SNOWFLAKE_LEARNING_DB',
      'SNOWFLAKE_SAMPLE_DATA',
      'USER$CHETANT18'
    ];

    // Extract database names from the result and filter out excluded ones
    // SHOW DATABASES returns columns: created_on, name, is_default, is_current, origin, owner, comment, options, retention_time
    const databases = result.rows
      .map((row) => ({
        name: row[1], // name is the second column
        isDefault: row[2] === 'Y',
        isCurrent: row[3] === 'Y',
        owner: row[5],
        comment: row[6],
      }))
      .filter(db => !excludedDatabases.includes(db.name));

    return NextResponse.json({
      success: true,
      data: databases,
    });
  } catch (error: any) {
    console.error('Error fetching databases:', error);
    return NextResponse.json(
      {
        success: false,
        error: error.message || 'Failed to fetch databases',
      },
      { status: 500 }
    );
  }
}
