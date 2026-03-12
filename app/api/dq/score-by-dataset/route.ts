import { NextRequest, NextResponse } from 'next/server';
import { snowflakePool, executeQuery, ensureConnectionContext } from '@/lib/snowflake';
import { getServerConfig } from '@/lib/server-config';
import { logger } from '@/lib/logger';
import { createErrorResponse } from '@/lib/errors';
import { getScoreByDataset } from '@/services/snowflake/dashboardService';

/**
 * GET /api/dq/score-by-dataset
 * Fetches average DQ scores by dataset for the most recent date
 *
 * Performance:
 * - Cached via CacheManager with 300s TTL and concurrency dedup.
 */
export async function GET(request: NextRequest) {
  const startTime = Date.now();
  const endpoint = '/api/dq/score-by-dataset';

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

    const result = await getScoreByDataset(connection, executeQuery);

    const duration = Date.now() - startTime;
    logger.logApiResponse(endpoint, true, duration);

    return NextResponse.json({
      success: true,
      data: result,
    });
  } catch (error: any) {
    logger.error('Error fetching score by dataset', error, { endpoint });
    return NextResponse.json(createErrorResponse(error), { status: 500 });
  }
}