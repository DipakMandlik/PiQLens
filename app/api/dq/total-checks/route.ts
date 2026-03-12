import { NextRequest, NextResponse } from 'next/server';
import { snowflakePool, executeQueryObjects, ensureConnectionContext } from '@/lib/snowflake';
import { getServerConfig } from '@/lib/server-config';
import { createErrorResponse } from '@/lib/errors';
import { logger } from '@/lib/logger';
import { retryQuery } from '@/lib/retry';
import { getOrSetCache, buildCacheKey } from '@/lib/valkey';

/**
 * GET /api/dq/total-checks
 * Aggregates checks for the selected date; if absent, falls back to latest available date <= selected date.
 * `lastExecution` is sourced from latest run START_TS (not check timestamp).
 */
export async function GET(request: NextRequest) {
  const startTime = Date.now();
  const { searchParams } = new URL(request.url);
  const dateParam = searchParams.get('date');
  const endpoint = `/api/dq/total-checks?date=${dateParam || 'default'}`;

  let dateFilter = 'CURRENT_DATE()';
  if (dateParam && /^\d{4}-\d{2}-\d{2}$/.test(dateParam)) {
    dateFilter = `'${dateParam}'::DATE`;
  }

  try {
    logger.logApiRequest(endpoint, 'GET');

    const valkeyKey = buildCacheKey('dashboard', 'total-checks', dateParam || 'default');

    const result = await getOrSetCache(valkeyKey, 300, async () => {
      const config = getServerConfig();
      if (!config) {
        throw new Error('AUTH_FAILED: Not connected to Snowflake');
      }

      const connection = await snowflakePool.getConnection(config);
      await ensureConnectionContext(connection, config);

      return await retryQuery(async () => {
        const statsQuery = `
        WITH effective_date AS (
          SELECT MAX(DATE_TRUNC('DAY', r.START_TS)) AS TARGET_DATE
          FROM DATA_QUALITY_DB.DQ_METRICS.DQ_RUN_CONTROL r
          WHERE DATE_TRUNC('DAY', r.START_TS) <= ${dateFilter}
        ),
        run_scope AS (
          SELECT
            r.RUN_ID,
            r.START_TS,
            r.RUN_TYPE,
            COALESCE(r.ROW_LEVEL_RECORDS_PROCESSED, 0) AS ROWS_PROCESSED
          FROM DATA_QUALITY_DB.DQ_METRICS.DQ_RUN_CONTROL r
          CROSS JOIN effective_date e
          WHERE e.TARGET_DATE IS NOT NULL
            AND DATE_TRUNC('DAY', r.START_TS) = e.TARGET_DATE
        ),
        latest_run AS (
          SELECT RUN_ID, START_TS, RUN_TYPE
          FROM run_scope
          QUALIFY ROW_NUMBER() OVER (ORDER BY START_TS DESC, RUN_ID DESC) = 1
        ),
        checks_scope AS (
          SELECT c.*
          FROM DATA_QUALITY_DB.DQ_METRICS.DQ_CHECK_RESULTS c
          JOIN run_scope rs ON c.RUN_ID = rs.RUN_ID
        ),
        run_rows AS (
          SELECT SUM(ROWS_PROCESSED) AS TOTAL_ROWS
          FROM run_scope
        )
        SELECT
          COUNT(DISTINCT c.DATASET_ID) AS DATASETS_SCANNED,
          COUNT(DISTINCT c.DATASET_ID, c.RULE_ID) AS UNIQUE_CHECKS_DEFINED,
          MAX(c.CHECK_TIMESTAMP) AS LAST_CHECK_TS,
          MAX(lr.RUN_ID) AS LATEST_RUN_ID,
          MAX(lr.RUN_TYPE) AS RUN_TYPE,
          MAX(TO_VARCHAR(lr.START_TS, 'YYYY-MM-DD HH24:MI:SS.FF3')) AS LAST_EXECUTION_TS,
          MAX(rr.TOTAL_ROWS) AS TOTAL_ROWS,
          TO_CHAR(MAX(e.TARGET_DATE), 'YYYY-MM-DD') AS RESOLVED_DATE
        FROM effective_date e
        LEFT JOIN checks_scope c ON 1 = 1
        LEFT JOIN latest_run lr ON 1 = 1
        LEFT JOIN run_rows rr ON 1 = 1
        WHERE e.TARGET_DATE IS NOT NULL
      `;

        const rows = await executeQueryObjects(connection, statsQuery) as Array<{
          DATASETS_SCANNED?: number;
          UNIQUE_CHECKS_DEFINED?: number;
          LAST_CHECK_TS?: string;
          LAST_EXECUTION_TS?: string;
          LATEST_RUN_ID?: string;
          RUN_TYPE?: string;
          TOTAL_ROWS?: number;
          RESOLVED_DATE?: string;
        }>;

        const row = rows?.[0];
        const resolvedDate = row?.RESOLVED_DATE || null;
        const totalChecks = Number(row?.UNIQUE_CHECKS_DEFINED || 0);

        if (!resolvedDate || totalChecks === 0) {
          return {
            hasData: false,
            totalChecks: 0,
            datasetsProcessed: 0,
            runType: 'Unknown',
            rowsValidated: 0,
            resolvedDate,
          };
        }

        return {
          hasData: true,
          totalChecks,
          checkDate: row?.LAST_CHECK_TS || null,
          lastExecution: row?.LAST_EXECUTION_TS || row?.LAST_CHECK_TS || null,
          runId: row?.LATEST_RUN_ID || null,
          datasetsProcessed: Number(row?.DATASETS_SCANNED || 0),
          runType: row?.RUN_TYPE || 'Unknown',
          rowsValidated: Number(row?.TOTAL_ROWS || 0),
          resolvedDate,
        };
      }, 'total-checks');
    }); // End of getOrSetCache payload

    logger.logApiResponse(endpoint, true, Date.now() - startTime);

    return NextResponse.json({
      success: true,
      data: result,
      metadata: { cached: true, timestamp: new Date().toISOString() }, // Simplified cache metadata
    });
  } catch (error: any) {
    logger.error('Error fetching total checks', error, { endpoint });

    if (error.message?.includes('AUTH_FAILED')) {
      return NextResponse.json(
        { success: false, error: { code: 'AUTH_FAILED', message: 'Not connected to Snowflake' } },
        { status: 401 }
      );
    }

    return NextResponse.json(createErrorResponse(error), { status: 500 });
  }
}

