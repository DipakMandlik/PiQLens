import { NextRequest, NextResponse } from 'next/server';
import { snowflakePool, executeQuery, ensureConnectionContext } from '@/lib/snowflake';
import { getServerConfig } from '@/lib/server-config';

/**
 * GET /api/dq/data-trust-level
 * Fetches the most common trust level for CURRENT_DATE
 * Falls back to most recent date if no data for today
 */
export async function GET(request: NextRequest) {
  try {
    // Get config from server-side storage
    const config = getServerConfig();
    if (!config) {
      return NextResponse.json(
        {
          success: false,
          error: 'Not connected to Snowflake. Please connect first.',
        },
        { status: 401 }
      );
    }

    const connection = await snowflakePool.getConnection(config);
    await ensureConnectionContext(connection, config);

    // Try to get trust level for current date first
    const todayTrustLevelQuery = `
      SELECT
        TRUST_LEVEL,
        COUNT(*) AS cnt
      FROM DATA_QUALITY_DB.DQ_METRICS.DQ_DAILY_SUMMARY
      WHERE SUMMARY_DATE = CURRENT_DATE
      GROUP BY TRUST_LEVEL
      ORDER BY cnt DESC
      LIMIT 1
    `;

    const todayResult = await executeQuery(connection, todayTrustLevelQuery);
    let trustLevel = null;

    if (todayResult.rows.length > 0 && todayResult.rows[0][0] !== null) {
      trustLevel = todayResult.rows[0][0];
    } else {
      // Fallback to most recent date
      const fallbackQuery = `
        SELECT
          TRUST_LEVEL,
          COUNT(*) AS cnt
        FROM DATA_QUALITY_DB.DQ_METRICS.DQ_DAILY_SUMMARY
        WHERE SUMMARY_DATE = (
          SELECT MAX(SUMMARY_DATE)
          FROM DATA_QUALITY_DB.DQ_METRICS.DQ_DAILY_SUMMARY
        )
        GROUP BY TRUST_LEVEL
        ORDER BY cnt DESC
        LIMIT 1
      `;

      const fallbackResult = await executeQuery(connection, fallbackQuery);
      if (fallbackResult.rows.length > 0 && fallbackResult.rows[0][0] !== null) {
        trustLevel = fallbackResult.rows[0][0];
      }
    }

    return NextResponse.json({
      success: true,
      data: {
        trustLevel: trustLevel || 'Unknown',
      },
    });
  } catch (error: any) {
    console.error('Error fetching data trust level:', error);
    return NextResponse.json(
      {
        success: false,
        error: error.message || 'Failed to fetch data trust level',
      },
      { status: 500 }
    );
  }
}