import { NextRequest, NextResponse } from 'next/server';
import { snowflakePool, executeQuery, ensureConnectionContext } from '@/lib/snowflake';
import { getServerConfig } from '@/lib/server-config';
import { logger } from '@/lib/logger';
import { createErrorResponse } from '@/lib/errors';
import { getDailySummary } from '@/services/snowflake/dashboardService';

/**
 * GET /api/dq/daily-summary
 * Fetches aggregated DQ score trend data for the last 30 days
 *
 * Performance:
 * - Cached via CacheManager with 300s TTL and concurrency dedup.
 *
 * Returns the average DQ_SCORE for each day, grouped by SUMMARY_DATE
 * Used for displaying trend visualization in the dashboard
 */
export async function GET(request: NextRequest) {
  const startTime = Date.now();
  const endpoint = '/api/dq/daily-summary';

  try {
    logger.logApiRequest(endpoint, 'GET');

    const config = getServerConfig();
    if (!config) {
      return NextResponse.json(
        { success: false, error: 'Not connected to Snowflake. Please connect first.' },
        { status: 401 }
      );
    }

    const connection = await snowflakePool.getConnection(config);
    await ensureConnectionContext(connection, config);

    const result = await getDailySummary(connection, executeQuery);

    const duration = Date.now() - startTime;
    logger.logApiResponse(endpoint, true, duration);

    return NextResponse.json({
      success: true,
      data: result.data,
      rowCount: result.rowCount,
    });
  } catch (error: any) {
    logger.error('Error fetching daily summary', error, { endpoint });
    return NextResponse.json(createErrorResponse(error), { status: 500 });
  }
}
