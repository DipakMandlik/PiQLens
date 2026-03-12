import { NextRequest, NextResponse } from 'next/server';
import { snowflakePool, executeQueryObjects, ensureConnectionContext } from '@/lib/snowflake';
import { getServerConfig } from '@/lib/server-config';
import { createErrorResponse } from '@/lib/errors';
import { logger } from '@/lib/logger';
import { retryQuery } from '@/lib/retry';
import { cache, CacheTTL, generateCacheKey } from '@/lib/cache';

export async function GET(request: NextRequest) {
  const startTime = Date.now();
  const { searchParams } = new URL(request.url);
  const runId = (searchParams.get('run_id') || '').trim();
  const endpoint = `/api/metrics/run-summary?run_id=${runId || 'missing'}`;

  if (!runId) {
    return NextResponse.json(
      { success: false, error: 'Missing required parameter: run_id' },
      { status: 400 }
    );
  }

  try {
    logger.logApiRequest(endpoint, 'GET');

    const cacheKey = generateCacheKey(endpoint);
    const cached = cache.get(cacheKey);
    if (cached) {
      logger.logApiResponse(endpoint, true, Date.now() - startTime);
      return NextResponse.json({
        success: true,
        data: cached,
        metadata: { cached: true, timestamp: new Date().toISOString() },
      });
    }

    const config = getServerConfig();
    if (!config) {
      return NextResponse.json(
        { success: false, error: 'Not connected to Snowflake' },
        { status: 401 }
      );
    }

    const connection = await snowflakePool.getConnection(config);
    await ensureConnectionContext(connection, config);

    const payload = await retryQuery(async () => {
      const query = `
        WITH run_context AS (
          SELECT
            rc.RUN_ID,
            COALESCE(MAX(rc.RUN_TYPE), 'UNKNOWN') AS RUN_TYPE,
            MAX(rc.START_TS) AS EXECUTION_TIME,
            MAX(rc.DURATION_SECONDS) AS DURATION_SECONDS,
            MAX(cr.TABLE_NAME) AS TABLE_NAME,
            DATE_TRUNC('DAY', MAX(rc.START_TS)) AS RUN_DATE
          FROM DATA_QUALITY_DB.DQ_METRICS.DQ_RUN_CONTROL rc
          LEFT JOIN DATA_QUALITY_DB.DQ_METRICS.DQ_CHECK_RESULTS cr ON cr.RUN_ID = rc.RUN_ID
          WHERE rc.RUN_ID = ?
          GROUP BY rc.RUN_ID
        ),
        check_rollup AS (
          SELECT
            SUM(COALESCE(cr.TOTAL_RECORDS, 0)) AS ROWS_SCANNED,
            SUM(COALESCE(cr.INVALID_RECORDS, 0)) AS ROWS_FAILED,
            SUM(CASE WHEN UPPER(cr.CHECK_STATUS) IN ('PASS', 'PASSED') THEN 1 ELSE 0 END) AS CHECKS_PASSED,
            SUM(CASE WHEN UPPER(cr.CHECK_STATUS) IN ('FAIL', 'FAILED') THEN 1 ELSE 0 END) AS CHECKS_FAILED
          FROM DATA_QUALITY_DB.DQ_METRICS.DQ_CHECK_RESULTS cr
          WHERE cr.RUN_ID = ?
        ),
        impacted_columns AS (
          SELECT
            COUNT(*) AS COLUMNS_IMPACTED,
            COALESCE(LISTAGG(COLUMN_NAME, ', ') WITHIN GROUP (ORDER BY COLUMN_NAME), '') AS IMPACTED_COLUMNS
          FROM (
            SELECT DISTINCT COLUMN_NAME
            FROM DATA_QUALITY_DB.DQ_METRICS.DQ_CHECK_RESULTS
            WHERE RUN_ID = ?
              AND COLUMN_NAME IS NOT NULL
              AND COALESCE(INVALID_RECORDS, 0) > 0
          ) x
        ),
        most_failed_check AS (
          SELECT
            RULE_NAME,
            SUM(COALESCE(INVALID_RECORDS, 0)) AS FAILED_ROWS
          FROM DATA_QUALITY_DB.DQ_METRICS.DQ_CHECK_RESULTS
          WHERE RUN_ID = ?
          GROUP BY RULE_NAME
          ORDER BY FAILED_ROWS DESC, RULE_NAME
          LIMIT 1
        ),
        most_affected_column AS (
          SELECT
            COLUMN_NAME,
            SUM(COALESCE(INVALID_RECORDS, 0)) AS FAILED_ROWS
          FROM DATA_QUALITY_DB.DQ_METRICS.DQ_CHECK_RESULTS
          WHERE RUN_ID = ?
            AND COLUMN_NAME IS NOT NULL
          GROUP BY COLUMN_NAME
          ORDER BY FAILED_ROWS DESC, COLUMN_NAME
          LIMIT 1
        ),
        rows_added_today AS (
          SELECT COALESCE(SUM(run_rows), 0) AS ROWS_ADDED_TODAY
          FROM (
            SELECT
              rc.RUN_ID,
              MAX(COALESCE(cr.TOTAL_RECORDS, 0)) AS run_rows
            FROM DATA_QUALITY_DB.DQ_METRICS.DQ_RUN_CONTROL rc
            JOIN DATA_QUALITY_DB.DQ_METRICS.DQ_CHECK_RESULTS cr ON cr.RUN_ID = rc.RUN_ID
            JOIN run_context ctx ON 1 = 1
            WHERE ctx.RUN_DATE IS NOT NULL
              AND ctx.TABLE_NAME IS NOT NULL
              AND UPPER(cr.TABLE_NAME) = UPPER(ctx.TABLE_NAME)
              AND DATE_TRUNC('DAY', rc.START_TS) = ctx.RUN_DATE
              AND rc.RUN_STATUS IN ('COMPLETED', 'COMPLETED_WITH_FAILURES', 'WARNING')
            GROUP BY rc.RUN_ID
          ) r
        )
        SELECT
          ctx.RUN_ID,
          ctx.RUN_TYPE,
          ctx.EXECUTION_TIME,
          ctx.DURATION_SECONDS,
          COALESCE(cr.ROWS_SCANNED, 0) AS ROWS_SCANNED,
          COALESCE(cr.ROWS_FAILED, 0) AS ROWS_FAILED,
          COALESCE(cr.CHECKS_PASSED, 0) AS CHECKS_PASSED,
          COALESCE(cr.CHECKS_FAILED, 0) AS CHECKS_FAILED,
          COALESCE(ic.COLUMNS_IMPACTED, 0) AS COLUMNS_IMPACTED,
          COALESCE(ic.IMPACTED_COLUMNS, '') AS IMPACTED_COLUMNS,
          COALESCE(rad.ROWS_ADDED_TODAY, 0) AS ROWS_ADDED_TODAY,
          COALESCE(mfc.RULE_NAME, '-') AS MOST_FAILED_CHECK,
          COALESCE(mac.COLUMN_NAME, '-') AS MOST_AFFECTED_COLUMN
        FROM run_context ctx
        LEFT JOIN check_rollup cr ON 1 = 1
        LEFT JOIN impacted_columns ic ON 1 = 1
        LEFT JOIN rows_added_today rad ON 1 = 1
        LEFT JOIN most_failed_check mfc ON 1 = 1
        LEFT JOIN most_affected_column mac ON 1 = 1
      `;

      const rows = await executeQueryObjects(connection, query, [
        runId,
        runId,
        runId,
        runId,
        runId,
      ]) as Array<{
        RUN_ID?: string;
        RUN_TYPE?: string;
        EXECUTION_TIME?: string;
        DURATION_SECONDS?: number;
        ROWS_SCANNED?: number;
        ROWS_FAILED?: number;
        CHECKS_PASSED?: number;
        CHECKS_FAILED?: number;
        COLUMNS_IMPACTED?: number;
        IMPACTED_COLUMNS?: string;
        ROWS_ADDED_TODAY?: number;
        MOST_FAILED_CHECK?: string;
        MOST_AFFECTED_COLUMN?: string;
      }>;

      if (!rows || rows.length === 0 || !rows[0].RUN_ID) {
        return null;
      }

      const row = rows[0];
      const rowsScanned = Number(row.ROWS_SCANNED || 0);
      const rowsFailed = Number(row.ROWS_FAILED || 0);
      const failRate = rowsScanned > 0 ? Number(((rowsFailed / rowsScanned) * 100).toFixed(2)) : 0;

      return {
        run_id: row.RUN_ID,
        run_type: row.RUN_TYPE || 'UNKNOWN',
        execution_time: row.EXECUTION_TIME || null,
        execution_duration: Number(row.DURATION_SECONDS || 0),
        fail_rate: failRate,
        impacted_columns: row.IMPACTED_COLUMNS || '',
        failed_checks: Number(row.CHECKS_FAILED || 0),
        checks_passed: Number(row.CHECKS_PASSED || 0),
        checks_failed: Number(row.CHECKS_FAILED || 0),
        rows_added_today: Number(row.ROWS_ADDED_TODAY || 0),
        rows_failed: rowsFailed,
        columns_impacted: Number(row.COLUMNS_IMPACTED || 0),
        most_failed_check: row.MOST_FAILED_CHECK || '-',
        most_affected_column: row.MOST_AFFECTED_COLUMN || '-',
      };
    }, 'metrics-run-summary');

    if (!payload) {
      return NextResponse.json(
        { success: false, error: `No run found with run_id=${runId}` },
        { status: 404 }
      );
    }

    cache.set(cacheKey, payload, CacheTTL.KPI_METRICS);
    logger.logApiResponse(endpoint, true, Date.now() - startTime);

    return NextResponse.json({
      success: true,
      data: payload,
      metadata: { cached: false, timestamp: new Date().toISOString() },
    });
  } catch (error: unknown) {
    logger.error('Error in run-summary metrics API', error, { endpoint });
    return NextResponse.json(createErrorResponse(error), { status: 500 });
  }
}