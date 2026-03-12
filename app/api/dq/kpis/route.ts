import { NextRequest, NextResponse } from 'next/server';
import { snowflakePool, executeQuery, ensureConnectionContext } from '@/lib/snowflake';
import { getServerConfig } from '@/lib/server-config';
import { logger } from '@/lib/logger';
import { createErrorResponse } from '@/lib/errors';
import { getKpis } from '@/services/snowflake/dashboardService';

/**
 * GET /api/dq/kpis
 * Fetches aggregated KPI data for the dashboard
 *
 * Performance:
 * - Cached via CacheManager with 120s TTL and concurrency dedup.
 *
 * Query params:
 * - days: Number of days to aggregate (default: 1 for latest day)
 */
export async function GET(request: NextRequest) {
  const startTime = Date.now();
  const endpoint = '/api/dq/kpis';

  try {
    logger.logApiRequest(endpoint, 'GET');

    const searchParams = request.nextUrl.searchParams;
    const days = parseInt(searchParams.get('days') || '1');

    const config = getServerConfig();
    if (!config) {
      return NextResponse.json(
        { success: false, error: 'Not connected to Snowflake. Please connect first.' },
        { status: 401 }
      );
    }

    const connection = await snowflakePool.getConnection(config);
    await ensureConnectionContext(connection, config);

    const kpis = await getKpis(connection, executeQuery, days);

    const duration = Date.now() - startTime;
    logger.logApiResponse(endpoint, true, duration);

    return NextResponse.json({
      success: true,
      data: kpis,
    });
  } catch (error: any) {
    logger.error('Error fetching KPIs', error, { endpoint });
    return NextResponse.json(createErrorResponse(error), { status: 500 });
  }
}
