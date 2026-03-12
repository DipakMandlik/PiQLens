import { NextRequest, NextResponse } from 'next/server';
import { snowflakePool, executeQueryObjects, ensureConnectionContext } from '@/lib/snowflake';
import { getServerConfig } from '@/lib/server-config';
import { createErrorResponse } from '@/lib/errors';
import { logger } from '@/lib/logger';
import { retryQuery } from '@/lib/retry';
import { getOrSetCache, buildCacheKey } from '@/lib/valkey';

/**
 * GET /api/dq/sla-compliance
 * Aggregates SLA compliance by selected date with fallback to latest available summary date <= selected date.
 */
export async function GET(request: NextRequest) {
  const startTime = Date.now();
  const { searchParams } = new URL(request.url);
  const dateParam = searchParams.get('date');
  const endpoint = `/api/dq/sla-compliance?date=${dateParam || 'default'}`;

  let dateFilter = 'CURRENT_DATE()';
  if (dateParam && /^\d{4}-\d{2}-\d{2}$/.test(dateParam)) {
    dateFilter = `'${dateParam}'::DATE`;
  }

  try {
    logger.logApiRequest(endpoint, 'GET');

    const valkeyKey = buildCacheKey('dashboard', 'sla-compliance', dateParam || 'default');

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
            SELECT MAX(SUMMARY_DATE) AS TARGET_DATE
            FROM DATA_QUALITY_DB.DQ_METRICS.DQ_DAILY_SUMMARY
            WHERE SUMMARY_DATE <= ${dateFilter}
          )
          SELECT
            ROUND(
              (SUM(CASE WHEN s.IS_SLA_MET THEN 1 ELSE 0 END) * 100.0)
              / NULLIF(COUNT(*), 0),
              2
            ) AS SLA_COMPLIANCE_PCT,
            MAX(s.LAST_RUN_TS) AS LAST_SCAN_TS,
            MAX(s.LAST_RUN_ID) AS LAST_RUN_ID,
            TO_CHAR(MAX(e.TARGET_DATE), 'YYYY-MM-DD') AS RESOLVED_DATE
          FROM DATA_QUALITY_DB.DQ_METRICS.DQ_DAILY_SUMMARY s
          CROSS JOIN effective_date e
          WHERE e.TARGET_DATE IS NOT NULL
            AND s.SUMMARY_DATE = e.TARGET_DATE
        `;

        const rows = await executeQueryObjects(connection, query) as Array<{
          SLA_COMPLIANCE_PCT?: number;
          LAST_SCAN_TS?: string;
          LAST_RUN_ID?: string;
          RESOLVED_DATE?: string;
        }>;

        const row = rows?.[0];
        const resolvedDate = row?.RESOLVED_DATE || null;

        if (!resolvedDate) {
          return { hasData: false, slaCompliancePct: 0, summaryDate: null, runId: null, resolvedDate: null };
        }

        return {
          hasData: true,
          slaCompliancePct: Number(row?.SLA_COMPLIANCE_PCT || 0),
          summaryDate: row?.LAST_SCAN_TS || null,
          runId: row?.LAST_RUN_ID || null,
          resolvedDate,
        };
      }, 'sla-compliance');
    }); // End of getOrSetCache payload

    logger.logApiResponse(endpoint, true, Date.now() - startTime);

    return NextResponse.json({
      success: true,
      data: result,
      metadata: { cached: true, timestamp: new Date().toISOString() },
    });
  } catch (error: any) {
    logger.error('Error fetching SLA compliance', error, { endpoint });

    if (error.message?.includes('AUTH_FAILED')) {
      return NextResponse.json(
        { success: false, error: { code: 'AUTH_FAILED', message: 'Not connected to Snowflake' } },
        { status: 401 }
      );
    }

    return NextResponse.json(createErrorResponse(error), { status: 500 });
  }
}