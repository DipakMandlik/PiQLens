import { NextRequest, NextResponse } from 'next/server';
import { snowflakePool, executeQuery, ensureConnectionContext } from '@/lib/snowflake';
import { getServerConfig } from '@/lib/server-config';

/**
 * GET /api/dq/failed-records
 * Fetches failed records data
 * 
 * Query params:
 * - limit: Number of records to fetch (default: 100)
 * - checkId: Filter by check ID
 * - runId: Filter by run ID
 * - tableName: Filter by table name
 * - isCritical: Filter by critical failures only
 */
export async function GET(request: NextRequest) {
  try {
    const searchParams = request.nextUrl.searchParams;
    const limit = parseInt(searchParams.get('limit') || '100');
    const checkId = searchParams.get('checkId');
    const runId = searchParams.get('runId');
    const tableName = searchParams.get('tableName');
    const isCritical = searchParams.get('isCritical') === 'true';

    let query = `
      SELECT 
        FAILURE_ID,
        CHECK_ID,
        RUN_ID,
        DATASET_ID,
        TABLE_NAME,
        COLUMN_NAME,
        RULE_NAME,
        RULE_TYPE,
        FAILURE_TYPE,
        FAILED_RECORD_PK,
        FAILED_COLUMN_VALUE,
        EXPECTED_PATTERN,
        ACTUAL_VALUE_TYPE,
        RELATED_COLUMNS,
        ROW_CONTEXT,
        FAILURE_CATEGORY,
        IS_CRITICAL,
        CAN_AUTO_REMEDIATE,
        REMEDIATION_SUGGESTION,
        DEBUG_SQL,
        DETECTED_TS,
        CREATED_TS
      FROM DATA_QUALITY_DB.DQ_METRICS.DQ_FAILED_RECORDS
      WHERE 1=1
    `;

    if (checkId) {
      query += ` AND CHECK_ID = ${parseInt(checkId)}`;
    }

    if (runId) {
      query += ` AND RUN_ID = '${runId.replace(/'/g, "''")}'`;
    }

    if (tableName) {
      query += ` AND TABLE_NAME = '${tableName.replace(/'/g, "''")}'`;
    }

    if (isCritical) {
      query += ` AND IS_CRITICAL = TRUE`;
    }

    query += ` ORDER BY DETECTED_TS DESC LIMIT ${limit}`;

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
    console.error('Error fetching failed records:', error);
    return NextResponse.json(
      {
        success: false,
        error: error.message || 'Failed to fetch failed records',
      },
      { status: 500 }
    );
  }
}

