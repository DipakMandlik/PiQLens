import { NextRequest, NextResponse } from 'next/server';
import { snowflakePool, executeQuery, ensureConnectionContext } from '@/lib/snowflake';
import { getServerConfig } from '@/lib/server-config';

/**
 * GET /api/dq/sla-compliance-monitor
 * Fetches SLA compliance data for datasets for the most recent date
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

    // Get SLA compliance data for the most recent date
    const slaComplianceQuery = `
      SELECT
        DATASET_ID,
        ROUND(DQ_SCORE, 2) AS DQ_SCORE,
        90 AS SLA_TARGET,
        CASE
          WHEN DQ_SCORE >= 90 THEN 'Met'
          ELSE 'Breached'
        END AS SLA_STATUS
      FROM DATA_QUALITY_DB.DQ_METRICS.DQ_DAILY_SUMMARY
      WHERE SUMMARY_DATE = (
        SELECT MAX(SUMMARY_DATE)
        FROM DATA_QUALITY_DB.DQ_METRICS.DQ_DAILY_SUMMARY
      )
      ORDER BY DQ_SCORE DESC
    `;

    const result = await executeQuery(connection, slaComplianceQuery);
    const slaData = result.rows.map(row => ({
      name: row[0], // DATASET_ID
      score: row[1], // DQ_SCORE
      slaTarget: row[2], // SLA_TARGET
      status: row[3] // SLA_STATUS
    }));

    return NextResponse.json({
      success: true,
      data: {
        slaCompliance: slaData,
      },
    });
  } catch (error) {
    console.error('Error fetching SLA compliance data:', error);
    return NextResponse.json(
      {
        success: false,
        error: 'Failed to fetch SLA compliance data',
      },
      { status: 500 }
    );
  }
}