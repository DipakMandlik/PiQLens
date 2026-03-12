import { NextRequest, NextResponse } from 'next/server';
import { snowflakePool, executeQueryObjects, ensureConnectionContext } from '@/lib/snowflake';
import { getServerConfig } from '@/lib/server-config';
import { createErrorResponse } from '@/lib/errors';
import { logger } from '@/lib/logger';
import { retryQuery } from '@/lib/retry';
import { getOrSetCache, buildCacheKey } from '@/lib/valkey';

/**
 * GET /api/dq/failed-checks
 * Aggregates failed checks for selected date with fallback to latest available date <= selected date.
 */
export async function GET(request: NextRequest) {
  const startTime = Date.now();
  const { searchParams } = new URL(request.url);
  const dateParam = searchParams.get('date');
  const endpoint = `/api/dq/failed-checks?date=${dateParam || 'default'}`;

  let dateFilter = 'CURRENT_DATE()';
  if (dateParam && /^\d{4}-\d{2}-\d{2}$/.test(dateParam)) {
    dateFilter = `'${dateParam}'::DATE`;
  }

  try {
    logger.logApiRequest(endpoint, 'GET');

    const valkeyKey = buildCacheKey('dashboard', 'failed-checks', dateParam || 'default');

    const result = await getOrSetCache(valkeyKey, 300, async () => {
      const config = getServerConfig();
      if (!config) {
        throw new Error('AUTH_FAILED: Not connected to Snowflake');
      }

      const connection = await snowflakePool.getConnection(config);
      await ensureConnectionContext(connection, config);

      return await retryQuery(async () => {
        const query = `
          WITH effective_date AS (
          SELECT MAX(DATE_TRUNC('DAY', START_TS)) AS TARGET_DATE
          FROM DATA_QUALITY_DB.DQ_METRICS.DQ_RUN_CONTROL
          WHERE DATE_TRUNC('DAY', START_TS) <= ${dateFilter}
            AND RUN_STATUS IN ('COMPLETED', 'WARNING', 'COMPLETED_WITH_FAILURES')
        )
        SELECT
          COALESCE(SUM(r.FAILED_CHECKS), 0) AS DAILY_FAILED,
          MAX(r.START_TS) AS LAST_SCAN_TS,
          MAX(r.RUN_ID) AS LATEST_RUN_ID,
          TO_CHAR(MAX(e.TARGET_DATE), 'YYYY-MM-DD') AS RESOLVED_DATE
        FROM DATA_QUALITY_DB.DQ_METRICS.DQ_RUN_CONTROL r
        CROSS JOIN effective_date e
        WHERE e.TARGET_DATE IS NOT NULL
          AND DATE_TRUNC('DAY', r.START_TS) = e.TARGET_DATE
          AND r.RUN_STATUS IN ('COMPLETED', 'WARNING', 'COMPLETED_WITH_FAILURES')
      `;

        const rows = await executeQueryObjects(connection, query) as Array<{
          DAILY_FAILED?: number;
          LAST_SCAN_TS?: string;
          LATEST_RUN_ID?: string;
          RESOLVED_DATE?: string;
        }>;

        const row = rows?.[0];
        const resolvedDate = row?.RESOLVED_DATE || null;

        if (!resolvedDate) {
          return { hasData: false, totalFailedChecks: 0, failedChecksDifference: 0, resolvedDate: null };
        }

        return {
          hasData: true,
          totalFailedChecks: Number(row?.DAILY_FAILED || 0),
          failedChecksDifference: 0,
          summaryDate: row?.LAST_SCAN_TS || null,
          runId: row?.LATEST_RUN_ID || null,
          resolvedDate,
        };
      }, 'failed-checks');
    }); // End of getOrSetCache payload

    logger.logApiResponse(endpoint, true, Date.now() - startTime);

    return NextResponse.json({
      success: true,
      data: result,
      metadata: { cached: true, timestamp: new Date().toISOString() }, // Simplified cache metadata
    });
  } catch (error: any) {
    logger.error('Error fetching failed checks', error, { endpoint });

    if (error.message?.includes('AUTH_FAILED')) {
      return NextResponse.json(
        { success: false, error: { code: 'AUTH_FAILED', message: 'Not connected to Snowflake' } },
        { status: 401 }
      );
    }

    return NextResponse.json(createErrorResponse(error), { status: 500 });
  }
}