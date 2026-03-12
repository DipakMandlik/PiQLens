import { NextRequest, NextResponse } from 'next/server';
import { snowflakePool, ensureConnectionContext } from '@/lib/snowflake';
import { getServerConfig } from '@/lib/server-config';
import { getLast5QualityHistory } from '@/lib/services/qualityHistoryService';

export const dynamic = 'force-dynamic';
export const revalidate = 0;

/**
 * GET /api/dq/quality-score-history
 *
 * Implements Phase 3 architecture: wraps the service layer and returns exactly
 * { last_5_days: QualityHistoryCard[] }
 */
export async function GET(request: NextRequest) {
  try {
    const { searchParams } = new URL(request.url);
    const database = searchParams.get('database');
    const schema = searchParams.get('schema');
    const table = searchParams.get('table');

    if (!database || !schema || !table) {
      return NextResponse.json(
        { success: false, error: 'Missing required parameters: database, schema, table' },
        { status: 400, headers: { 'Cache-Control': 'no-store' } }
      );
    }

    const config = getServerConfig();
    if (!config) throw new Error('AUTH_FAILED: Not connected to Snowflake');

    const conn = await snowflakePool.getConnection(config);
    await ensureConnectionContext(conn, config);

    // Call phase 3 service layer with proper cache TTL integration
    const history = await getLast5QualityHistory(conn, database, schema, table);

    // Phase 1 response shape exactly
    return NextResponse.json(
      {
        last_5_days: history,
      },
      { headers: { 'Cache-Control': 'no-store, no-cache, must-revalidate' } }
    );
  } catch (error: any) {
    console.error('GET /api/dq/quality-score-history error:', error);

    if (error.message?.includes('AUTH_FAILED')) {
      return NextResponse.json(
        { success: false, error: 'Not connected to Snowflake. Please connect first.' },
        { status: 401, headers: { 'Cache-Control': 'no-store' } }
      );
    }

    return NextResponse.json(
      { success: false, error: error.message || 'Failed to fetch quality score history' },
      { status: 500, headers: { 'Cache-Control': 'no-store' } }
    );
  }
}
