import { NextRequest, NextResponse } from 'next/server';
import { getServerConfig } from '@/lib/server-config';

export const runtime = 'nodejs';

/**
 * GET /api/snowflake/schemas?database=<database_name>
 * Fetches list of all schemas in the specified database
 */
export async function GET(request: NextRequest) {
  try {
    const searchParams = request.nextUrl.searchParams;
    const database = searchParams.get('database');

    if (!database) {
      return NextResponse.json(
        { success: false, error: 'Database parameter is required' },
        { status: 400 }
      );
    }

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
    
    // Query to get all schemas in the database
    const query = `SHOW SCHEMAS IN DATABASE ${database}`;
    const result = await executeQuery(connection, query);

    // List of system/default schemas to exclude
    const excludedSchemas = [
      'INFORMATION_SCHEMA',
      'PUBLIC'
    ];

    // Extract schema names from the result and filter out excluded ones
    // SHOW SCHEMAS returns columns: created_on, name, is_default, is_current, database_name, owner, comment, options, retention_time
    const schemas = result.rows
      .map((row) => ({
        name: row[1], // name is the second column
        isDefault: row[2] === 'Y',
        isCurrent: row[3] === 'Y',
        databaseName: row[4],
        owner: row[5],
        comment: row[6],
      }))
      .filter(schema => !excludedSchemas.includes(schema.name));

    return NextResponse.json({
      success: true,
      data: schemas,
    });
  } catch (error: any) {
    console.error('Error fetching schemas:', error);
    return NextResponse.json(
      {
        success: false,
        error: error.message || 'Failed to fetch schemas',
      },
      { status: 500 }
    );
  }
}
