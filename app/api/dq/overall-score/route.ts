import { NextRequest, NextResponse } from 'next/server';
import { snowflakePool, executeQueryObjects, ensureConnectionContext } from '@/lib/snowflake';
import { getServerConfig } from '@/lib/server-config';
import { createErrorResponse } from '@/lib/errors';
import { logger } from '@/lib/logger';
import { getOverallScore } from '@/services/snowflake/dashboardService';

/**
 * GET /api/dq/overall-score
 * Fetches overall DQ score calculated as average of all DQ_SCORE values for CURRENT_DATE
 *
 * Performance:
 * - Cached via CacheManager with 120s TTL and concurrency dedup.
 * - Uses JOIN with DQ_RUN_CONTROL to ensure consistent date filtering with other APIs.
 */
export async function GET(request: NextRequest) {
  const startTime = Date.now();
  const { searchParams } = new URL(request.url);
  const dateParam = searchParams.get('date');

  let targetDate = 'CURRENT_DATE()';
  if (dateParam && /^\d{4}-\d{2}-\d{2}$/.test(dateParam)) {
    targetDate = `'${dateParam}'::DATE`;
  }

  const endpoint = `/api/dq/overall-score?date=${dateParam || 'today'}`;

  try {
    logger.logApiRequest(endpoint, 'GET');

    const config = getServerConfig();
    if (!config) {
      return NextResponse.json(
        { success: false, error: { code: 'AUTH_FAILED', message: 'Not connected to Snowflake' } },
        { status: 401 }
      );
    }

    const connection = await snowflakePool.getConnection(config);
    await ensureConnectionContext(connection, config);

    const result = await getOverallScore(connection, executeQueryObjects, targetDate, dateParam);

    const duration = Date.now() - startTime;
    logger.logApiResponse(endpoint, true, duration);

    return NextResponse.json({
      success: true,
      data: result,
      metadata: { timestamp: new Date().toISOString(), queryTime: duration },
    });
  } catch (error: any) {
    logger.error('Error fetching overall DQ score', error, { endpoint });
    return NextResponse.json(createErrorResponse(error), { status: 500 });
  }
}
