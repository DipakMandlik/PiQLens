import { NextRequest, NextResponse } from 'next/server';
import { snowflakePool, executeQueryObjects, ensureConnectionContext } from '@/lib/snowflake';
import { getServerConfig } from '@/lib/server-config';
import { createErrorResponse } from '@/lib/errors';
import { logger } from '@/lib/logger';
import { cache, CacheTTL, generateCacheKey } from '@/lib/cache';

/**
 * GET /api/dq/critical-failed-records
 * Aggregates critical failed records by selected date with fallback to latest available date <= selected date.
 */
export async function GET(request: NextRequest) {
  const startTime = Date.now();
  const { searchParams } = new URL(request.url);
  const dateParam = searchParams.get('date');
  const endpoint = `/api/dq/critical-failed-records?date=${dateParam || 'default'}`;

  let dateFilter = 'CURRENT_DATE()';
  if (dateParam && /^\d{4}-\d{2}-\d{2}$/.test(dateParam)) {
    dateFilter = `'${dateParam}'::DATE`;
  }

  try {
    logger.logApiRequest(endpoint, 'GET');

    const cacheKey = generateCacheKey(endpoint);
    const cachedData = cache.get(cacheKey);
    if (cachedData) {
      logger.logApiResponse(endpoint, true, Date.now() - startTime);
      return NextResponse.json({
        success: true,
        data: cachedData,
        metadata: { cached: true, timestamp: new Date().toISOString() },
      });
    }

    const config = getServerConfig();
    if (!config) {
      return NextResponse.json(
        { success: false, error: 'Not connected to Snowflake. Please connect first.' },
        { status: 401 }
      );
    }

    const connection = await snowflakePool.getConnection(config);
    await ensureConnectionContext(connection, config);

    const query = `
      WITH effective_date AS (
        SELECT MAX(DATE_TRUNC('DAY', START_TS)) AS TARGET_DATE
        FROM DATA_QUALITY_DB.DQ_METRICS.DQ_RUN_CONTROL
        WHERE DATE_TRUNC('DAY', START_TS) <= ${dateFilter}
          AND RUN_STATUS IN ('COMPLETED', 'WARNING', 'COMPLETED_WITH_FAILURES')
      ),
      run_rollup AS (
        SELECT
          MAX(r.START_TS) AS LAST_SCAN_TS,
          MAX(r.RUN_ID) AS LATEST_RUN_ID
        FROM DATA_QUALITY_DB.DQ_METRICS.DQ_RUN_CONTROL r
        CROSS JOIN effective_date e
        WHERE e.TARGET_DATE IS NOT NULL
          AND DATE_TRUNC('DAY', r.START_TS) = e.TARGET_DATE
          AND r.RUN_STATUS IN ('COMPLETED', 'WARNING', 'COMPLETED_WITH_FAILURES')
      ),
      failed_rollup AS (
        SELECT
          COUNT(f.FAILURE_ID) AS CRITICAL_FAILED_RECORDS
        FROM DATA_QUALITY_DB.DQ_METRICS.DQ_FAILED_RECORDS f
        JOIN DATA_QUALITY_DB.DQ_METRICS.DQ_RUN_CONTROL r ON f.RUN_ID = r.RUN_ID
        CROSS JOIN effective_date e
        WHERE e.TARGET_DATE IS NOT NULL
          AND f.IS_CRITICAL = TRUE
          AND DATE_TRUNC('DAY', r.START_TS) = e.TARGET_DATE
          AND r.RUN_STATUS IN ('COMPLETED', 'WARNING', 'COMPLETED_WITH_FAILURES')
      )
      SELECT
        COALESCE(fr.CRITICAL_FAILED_RECORDS, 0) AS CRITICAL_FAILED_RECORDS,
        rr.LAST_SCAN_TS,
        rr.LATEST_RUN_ID,
        TO_CHAR(e.TARGET_DATE, 'YYYY-MM-DD') AS RESOLVED_DATE
      FROM effective_date e
      LEFT JOIN run_rollup rr ON 1 = 1
      LEFT JOIN failed_rollup fr ON 1 = 1
    `;

    const rows = await executeQueryObjects(connection, query) as Array<{
      CRITICAL_FAILED_RECORDS?: number;
      LAST_SCAN_TS?: string;
      LATEST_RUN_ID?: string;
      RESOLVED_DATE?: string;
    }>;

    const row = rows?.[0];
    const resolvedDate = row?.RESOLVED_DATE || null;

    const result = {
      hasData: Boolean(resolvedDate),
      criticalFailedRecords: Number(row?.CRITICAL_FAILED_RECORDS || 0),
      summaryDate: row?.LAST_SCAN_TS || null,
      runId: row?.LATEST_RUN_ID || null,
      resolvedDate,
    };

    cache.set(cacheKey, result, CacheTTL.QUICK_METRICS);
    logger.logApiResponse(endpoint, true, Date.now() - startTime);

    return NextResponse.json({
      success: true,
      data: result,
      metadata: { cached: false, timestamp: new Date().toISOString() },
    });
  } catch (error: unknown) {
    logger.error('Error fetching critical failed records', error, { endpoint });
    return NextResponse.json(createErrorResponse(error), { status: 500 });
  }
}