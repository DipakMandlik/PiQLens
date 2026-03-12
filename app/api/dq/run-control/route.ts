import { NextRequest, NextResponse } from 'next/server';
import { snowflakePool, executeQuery, ensureConnectionContext } from '@/lib/snowflake';
import { getServerConfig } from '@/lib/server-config';

/**
 * GET /api/dq/run-control
 * Fetches run control data
 * 
 * Query params:
 * - limit: Number of records to fetch (default: 50)
 * - status: Filter by run status
 * - days: Number of days to look back (default: 30)
 */
export async function GET(request: NextRequest) {
  try {
    const searchParams = request.nextUrl.searchParams;
    const limit = parseInt(searchParams.get('limit') || '50');
    const status = searchParams.get('status');
    const days = parseInt(searchParams.get('days') || '30');
    const runId = searchParams.get('runId'); // For polling specific run

    let query = `
      SELECT 
        RUN_ID,
        TRIGGERED_BY,
        START_TS,
        END_TS,
        DURATION_SECONDS,
        RUN_STATUS,
        RUN_TYPE,
        TOTAL_DATASETS,
        TOTAL_CHECKS,
        PASSED_CHECKS,
        FAILED_CHECKS,
        WARNING_CHECKS,
        SKIPPED_CHECKS,
        ROW_LEVEL_RECORDS_PROCESSED,
        TOTAL_ROWS_IN_SCOPE,
        ERROR_MESSAGE,
        CREATED_TS
      FROM DATA_QUALITY_DB.DQ_METRICS.DQ_RUN_CONTROL
    `;

    // If querying by specific runId, return just that row
    if (runId) {
      query += ` WHERE RUN_ID = '${runId.replace(/'/g, "''")}'`;
    } else {
      query += ` WHERE START_TS >= DATEADD(day, -${days}, CURRENT_TIMESTAMP())`;

      if (status) {
        query += ` AND RUN_STATUS = '${status.replace(/'/g, "''")}'`;
      }

      query += ` ORDER BY START_TS DESC LIMIT ${limit}`;
    }

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

    const rows = result.rows.map((row) => {
      const obj: any = {};
      result.columns.forEach((col, idx) => {
        obj[col] = row[idx];
      });
      return obj;
    });

    // If querying by runId, return single object
    if (runId) {
      return NextResponse.json({
        success: true,
        data: rows.length > 0 ? rows[0] : null,
      });
    }

    return NextResponse.json({
      success: true,
      data: rows,
      rowCount: result.rowCount,
    });
  } catch (error: any) {
    console.error('Error fetching run control:', error);
    return NextResponse.json(
      {
        success: false,
        error: error.message || 'Failed to fetch run control',
      },
      { status: 500 }
    );
  }
}

