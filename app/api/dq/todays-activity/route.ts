import { NextRequest, NextResponse } from 'next/server';
import { snowflakePool, executeQuery, ensureConnectionContext } from '@/lib/snowflake';
import { getServerConfig } from '@/lib/server-config';
import { logger } from '@/lib/logger';
import { createErrorResponse } from '@/lib/errors';
import { retryQuery } from '@/lib/retry';

/**
 * GET /api/dq/todays-activity
 * Fetches recent runs for selected date with fallback to latest available date <= selected date.
 */
export async function GET(request: NextRequest) {
  const startTime = Date.now();
  const endpoint = '/api/dq/todays-activity';
  const { searchParams } = new URL(request.url);
  const dateParam = searchParams.get('date');

  let targetDate = 'CURRENT_DATE()';
  if (dateParam && /^\d{4}-\d{2}-\d{2}$/.test(dateParam)) {
    targetDate = `'${dateParam}'::DATE`;
  }

  try {
    const config = getServerConfig();
    if (!config) {
      return NextResponse.json({ success: false, error: 'Not connected' }, { status: 401 });
    }

    const connection = await snowflakePool.getConnection(config);
    await ensureConnectionContext(connection, config);

    const result = await retryQuery(async () => {
      const query = `
        WITH effective_date AS (
          SELECT MAX(DATE_TRUNC('DAY', START_TS)) AS TARGET_DATE
          FROM DATA_QUALITY_DB.DQ_METRICS.DQ_RUN_CONTROL
          WHERE DATE_TRUNC('DAY', START_TS) <= ${targetDate}
            AND RUN_STATUS IN ('COMPLETED', 'WARNING', 'COMPLETED_WITH_FAILURES')
        )
        SELECT
          r.RUN_ID,
          r.RUN_TYPE,
          MAX(c.CHECK_TIMESTAMP) AS RUN_TIME,
          COUNT(DISTINCT c.DATASET_ID) AS DATASETS_SCANNED,
          COUNT(DISTINCT c.DATASET_ID, c.RULE_ID) AS UNIQUE_CHECKS,
          COUNT(*) AS TOTAL_EXECUTIONS,
          AVG(c.PASS_RATE) AS OVERALL_SCORE,
          SUM(CASE WHEN c.CHECK_STATUS = 'PASSED' THEN 1 ELSE 0 END) AS PASSED_CHECKS,
          SUM(CASE WHEN c.CHECK_STATUS = 'FAILED' THEN 1 ELSE 0 END) AS FAILED_CHECKS,
          SUM(CASE WHEN c.CHECK_STATUS = 'WARNING' THEN 1 ELSE 0 END) AS WARNING_CHECKS,
          SUM(c.TOTAL_RECORDS) AS TOTAL_RECORDS,
          SUM(c.INVALID_RECORDS) AS FAILED_RECORDS_COUNT,
          AVG(CASE WHEN c.RULE_TYPE = 'COMPLETENESS' THEN c.PASS_RATE ELSE NULL END) AS COMPLETENESS_SCORE,
          AVG(CASE WHEN c.RULE_TYPE = 'UNIQUENESS' THEN c.PASS_RATE ELSE NULL END) AS UNIQUENESS_SCORE,
          AVG(CASE WHEN c.RULE_TYPE = 'VALIDITY' THEN c.PASS_RATE ELSE NULL END) AS VALIDITY_SCORE,
          AVG(CASE WHEN c.RULE_TYPE = 'CONSISTENCY' THEN c.PASS_RATE ELSE NULL END) AS CONSISTENCY_SCORE,
          AVG(CASE WHEN c.RULE_TYPE = 'FRESHNESS' THEN c.PASS_RATE ELSE NULL END) AS FRESHNESS_SCORE,
          AVG(CASE WHEN c.RULE_TYPE = 'VOLUME' THEN c.PASS_RATE ELSE NULL END) AS VOLUME_SCORE,
          TO_CHAR(MAX(e.TARGET_DATE), 'YYYY-MM-DD') AS RESOLVED_DATE
        FROM DATA_QUALITY_DB.DQ_METRICS.DQ_CHECK_RESULTS c
        JOIN DATA_QUALITY_DB.DQ_METRICS.DQ_RUN_CONTROL r ON c.RUN_ID = r.RUN_ID
        CROSS JOIN effective_date e
        WHERE e.TARGET_DATE IS NOT NULL
          AND DATE_TRUNC('DAY', r.START_TS) = e.TARGET_DATE
        GROUP BY r.RUN_ID, r.RUN_TYPE
        ORDER BY RUN_TIME DESC
        LIMIT 5
      `;

      return executeQuery(connection, query);
    }, 'todays-activity');

    const runs = result.rows.map((row) => {
      const avgScore = Number(row[6]);
      const finalScore = normalizeScore(avgScore);

      return {
        runId: row[0],
        runType: row[1],
        runTime: row[2],
        datasetsScanned: Number(row[3]),
        uniqueChecks: Number(row[4]),
        totalExecutions: Number(row[5]),
        overallScore: Math.round(finalScore),
        passedChecks: Number(row[7]),
        failedChecks: Number(row[8]),
        warningChecks: Number(row[9]),
        totalRecords: Number(row[10]),
        failedRecordsCount: Number(row[11]),
        slaMet: Math.round(finalScore) >= 90,
        dimensions: {
          completeness: Math.round(normalizeScore(row[12])),
          uniqueness: Math.round(normalizeScore(row[13])),
          validity: Math.round(normalizeScore(row[14])),
          consistency: Math.round(normalizeScore(row[15])),
          freshness: Math.round(normalizeScore(row[16])),
          volume: Math.round(normalizeScore(row[17])),
        },
        trustLevel: calculateTrustLevel(finalScore),
        qualityGrade: calculateQualityGrade(finalScore),
        failureRate: Number(row[10]) > 0 ? Number(((Number(row[11]) / Number(row[10])) * 100).toFixed(2)) : 0,
        resolvedDate: row[18] || null,
      };
    });

    const duration = Date.now() - startTime;
    logger.logApiResponse(endpoint, true, duration);

    return NextResponse.json({
      success: true,
      data: runs,
      metadata: {
        timestamp: new Date().toISOString(),
        resolvedDate: runs[0]?.resolvedDate || null,
      },
    });
  } catch (error: unknown) {
    logger.error('Error fetching timeline', error, { endpoint });
    return NextResponse.json(createErrorResponse(error), { status: 500 });
  }
}

function normalizeScore(val: unknown): number {
  if (val === null || val === undefined) return 0;
  const num = Number(val);
  return num <= 1 && num > -1 && num !== 0 ? num * 100 : num;
}

function calculateTrustLevel(score: number): 'HIGH' | 'MEDIUM' | 'LOW' {
  if (score >= 90) return 'HIGH';
  if (score >= 70) return 'MEDIUM';
  return 'LOW';
}

function calculateQualityGrade(score: number): string {
  if (score >= 97) return 'A+';
  if (score >= 93) return 'A';
  if (score >= 85) return 'B';
  if (score >= 70) return 'C';
  if (score >= 60) return 'D';
  return 'F';
}