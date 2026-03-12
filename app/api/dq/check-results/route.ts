import { NextRequest, NextResponse } from 'next/server';
import { snowflakePool, executeQuery, ensureConnectionContext } from '@/lib/snowflake';
import { getServerConfig } from '@/lib/server-config';

/**
 * GET /api/dq/check-results
 * Fetches check results data
 * 
 * Query params:
 * - limit: Number of records to fetch (default: 100)
 * - runId: Filter by run ID
 * - tableName: Filter by table name
 * - status: Filter by check status (PASS, FAIL, WARNING)
 * - days: Number of days to look back (default: 7)
 */
export async function GET(request: NextRequest) {
  try {
    const searchParams = request.nextUrl.searchParams;
    const limit = parseInt(searchParams.get('limit') || '100');
    const runId = searchParams.get('runId');
    const tableName = searchParams.get('tableName');
    const status = searchParams.get('status');
    const days = parseInt(searchParams.get('days') || '7');

    let query = `
      SELECT 
        CHECK_ID,
        RUN_ID,
        CHECK_TIMESTAMP,
        DATASET_ID,
        DATABASE_NAME,
        SCHEMA_NAME,
        TABLE_NAME,
        COLUMN_NAME,
        RULE_ID,
        RULE_NAME,
        RULE_TYPE,
        RULE_LEVEL,
        TOTAL_RECORDS,
        VALID_RECORDS,
        INVALID_RECORDS,
        NULL_RECORDS,
        DUPLICATE_RECORDS,
        PASS_RATE,
        THRESHOLD,
        CHECK_STATUS,
        EXECUTION_TIME_MS,
        EXECUTION_CREDITS,
        FAILURE_REASON,
        SAMPLE_INVALID_VALUES,
        CREATED_TS
      FROM DATA_QUALITY_DB.DQ_METRICS.DQ_CHECK_RESULTS
      WHERE CHECK_TIMESTAMP >= DATEADD(day, -${days}, CURRENT_TIMESTAMP())
    `;

    if (runId) {
      query += ` AND RUN_ID = '${runId.replace(/'/g, "''")}'`;
    }

    if (tableName) {
      query += ` AND TABLE_NAME = '${tableName.replace(/'/g, "''")}'`;
    }

    if (status) {
      query += ` AND CHECK_STATUS = '${status.replace(/'/g, "''")}'`;
    }

    query += ` ORDER BY CHECK_TIMESTAMP DESC LIMIT ${limit}`;

    // Get config from server-side storage
    const config = getServerConfig();
    if (!config) {
      return NextResponse.json(
        {
          success: false,
          error: 'Not connected to Snowflake. Please connect first.',
        },
        { status: 401 }
      );
    }

    const connection = await snowflakePool.getConnection(config);
    await ensureConnectionContext(connection, config);
    const result = await executeQuery(connection, query);

    return NextResponse.json({
      success: true,
      data: result.rows.map((row) => {
        const obj: any = {};
        result.columns.forEach((col, idx) => {
          obj[col] = row[idx];
        });
        return obj;
      }),
      rowCount: result.rowCount,
    });
  } catch (error: any) {
    console.error('Error fetching check results:', error);
    return NextResponse.json(
      {
        success: false,
        error: error.message || 'Failed to fetch check results',
      },
      { status: 500 }
    );
  }
}

