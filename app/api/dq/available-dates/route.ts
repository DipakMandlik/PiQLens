import { NextRequest, NextResponse } from 'next/server';
import { snowflakePool, executeQueryObjects, ensureConnectionContext } from '@/lib/snowflake';
import { getServerConfig } from '@/lib/server-config';
import { logger } from '@/lib/logger';
import { normalizeMode } from '@/lib/dq/data-view-mode';

function escapeSqlLiteral(value: string) {
  return value.replace(/'/g, "''");
}

/**
 * GET /api/dq/available-dates
 * Query params:
 * - table (optional)
 * - mode (optional, TABLE|TODAY)
 * - scope (legacy optional, FULL|INCREMENTAL)
 * - days (optional, default 90)
 */
export async function GET(request: NextRequest) {
  try {
    const { searchParams } = new URL(request.url);
    const tableName = searchParams.get('table');
    const modeRaw = searchParams.get('mode') || searchParams.get('scope');
    const mode = modeRaw ? normalizeMode(modeRaw) : null;
    const daysBack = Math.max(1, parseInt(searchParams.get('days') || '90', 10));

    const config = getServerConfig();
    if (!config) {
      return NextResponse.json({ success: false, error: 'Not connected' }, { status: 401 });
    }

    const connection = await snowflakePool.getConnection(config);
    await ensureConnectionContext(connection, config);

    if (!tableName) {
      const query = `
        SELECT DISTINCT TO_CHAR(SUMMARY_DATE, 'YYYY-MM-DD') AS DATE
        FROM DATA_QUALITY_DB.DQ_METRICS.DQ_DAILY_SUMMARY
        WHERE SUMMARY_DATE IS NOT NULL
          AND SUMMARY_DATE >= DATEADD(day, -${daysBack}, CURRENT_DATE())
        ORDER BY DATE DESC
      `;

      const rows = await executeQueryObjects(connection, query) as Array<{ DATE: string }>;
      const dates = rows.map((r) => r.DATE);

      return NextResponse.json({
        success: true,
        data: {
          dates,
          availableDates: dates,
          count: dates.length,
          mode: mode || 'ALL',
        },
      });
    }

    const safeTable = escapeSqlLiteral(tableName);

    let modeFilter = '';
    if (mode === 'TODAY') {
      // TODAY mode: only incremental/incremental runs
      modeFilter = " AND (rc.RUN_TYPE IS NULL OR UPPER(rc.RUN_TYPE) = 'INCREMENTAL')";
    } else if (mode === 'TABLE') {
      // TABLE mode: full/complete scans
      modeFilter = " AND (rc.RUN_TYPE IS NULL OR UPPER(rc.RUN_TYPE) = 'FULL')";
    }

    const query = `
      WITH run_dates AS (
        SELECT DATE_TRUNC('DAY', rc.START_TS) AS EXECUTION_DATE
        FROM DATA_QUALITY_DB.DQ_METRICS.DQ_RUN_CONTROL rc
        WHERE EXISTS (
          SELECT 1
          FROM DATA_QUALITY_DB.DQ_METRICS.DQ_CHECK_RESULTS cr
          WHERE cr.RUN_ID = rc.RUN_ID
            AND UPPER(cr.TABLE_NAME) = UPPER('${safeTable}')
        )
          AND rc.RUN_STATUS IN ('COMPLETED', 'COMPLETED_WITH_FAILURES', 'WARNING')
          AND DATE_TRUNC('DAY', rc.START_TS) >= DATEADD(day, -${daysBack}, CURRENT_DATE())
          ${modeFilter}
      ),
      summary_dates AS (
        SELECT DATE_TRUNC('DAY', ds.SUMMARY_DATE) AS EXECUTION_DATE
        FROM DATA_QUALITY_DB.DQ_METRICS.DQ_DAILY_SUMMARY ds
        WHERE UPPER(ds.TABLE_NAME) = UPPER('${safeTable}')
          AND ds.SUMMARY_DATE >= DATEADD(day, -${daysBack}, CURRENT_DATE())
      )
      SELECT DISTINCT TO_CHAR(EXECUTION_DATE, 'YYYY-MM-DD') AS EXECUTION_DATE
      FROM (
        SELECT EXECUTION_DATE FROM run_dates
        UNION
        SELECT EXECUTION_DATE FROM summary_dates
      ) all_dates
      WHERE EXECUTION_DATE IS NOT NULL
      ORDER BY EXECUTION_DATE DESC
    `;

    const rows = await executeQueryObjects(connection, query) as Array<{ EXECUTION_DATE: string }>;
    const dates = rows.map((r) => r.EXECUTION_DATE);

    return NextResponse.json({
      success: true,
      data: {
        dates,
        availableDates: dates,
        count: dates.length,
        table: tableName,
        mode: mode || 'ALL',
        lookbackDays: daysBack,
      },
    });
  } catch (error: unknown) {
    logger.error('Error fetching available dates', error);
    return NextResponse.json(
      { success: false, error: 'Failed to fetch available dates' },
      { status: 500 }
    );
  }
}
