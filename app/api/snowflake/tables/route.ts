import { NextRequest, NextResponse } from 'next/server';
import { getServerConfig } from '@/lib/server-config';

export const runtime = 'nodejs';

/**
 * GET /api/snowflake/tables?database=<database_name>&schema=<schema_name>
 * Fetches list of all tables in the specified database and schema
 */
export async function GET(request: NextRequest) {
  try {
    const searchParams = request.nextUrl.searchParams;
    const database = searchParams.get('database');
    const schema = searchParams.get('schema');

    if (!database || !schema) {
      return NextResponse.json(
        { success: false, error: 'Database and schema parameters are required' },
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
    
    // Query to get all tables in the schema
    const query = `SHOW TABLES IN SCHEMA ${database}.${schema}`;
    const result = await executeQuery(connection, query);

    // Extract table names from the result
    // SHOW TABLES returns columns: created_on, name, database_name, schema_name, kind, comment, cluster_by, rows, bytes, owner, retention_time, automatic_clustering, change_tracking, is_external, enable_schema_evolution, owner_role_type, is_event, is_hybrid, is_iceberg
    const tables = result.rows.map((row) => ({
      name: row[1], // name is the second column
      databaseName: row[2],
      schemaName: row[3],
      kind: row[4], // TABLE, VIEW, etc.
      rows: row[6],
      bytes: row[7],
      owner: row[9],
      comment: row[5],
    }));

    return NextResponse.json({
      success: true,
      data: tables,
    });
  } catch (error: any) {
    console.error('Error fetching tables:', error);
    return NextResponse.json(
      {
        success: false,
        error: error.message || 'Failed to fetch tables',
      },
      { status: 500 }
    );
  }
}
