import { NextRequest, NextResponse } from 'next/server';
import { snowflakePool, executeQueryObjects, ensureConnectionContext } from '@/lib/snowflake';
import { getServerConfig } from '@/lib/server-config';
import { createErrorResponse } from '@/lib/errors';
import { logger } from '@/lib/logger';
import { retryQuery } from '@/lib/retry';
import { getOrSetCache, buildCacheKey } from '@/lib/valkey';

type SummarySnapshot = {
  SUMMARY_DATE: string;
  LAST_RUN_TS: string;
  DATASET_ID: string;
  DQ_SCORE: number | null;
  COMPLETENESS_SCORE: number | null;
  UNIQUENESS_SCORE: number | null;
  VALIDITY_SCORE: number | null;
  CONSISTENCY_SCORE: number | null;
  FRESHNESS_SCORE: number | null;
  VOLUME_SCORE: number | null;
  TOTAL_CHECKS: number | null;
  FAILED_CHECKS: number | null;
  TRUST_LEVEL: string | null;
  QUALITY_GRADE: string | null;
  IS_SLA_MET: boolean | null;
};

const DIMENSION_KEYS = [
  'completeness',
  'uniqueness',
  'validity',
  'consistency',
  'freshness',
  'volume',
] as const;

type DimensionKey = (typeof DIMENSION_KEYS)[number];

function normalizeScore(value: unknown): number {
  if (value === null || value === undefined) return 0;
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) return 0;
  if (numeric !== 0 && numeric > -1 && numeric < 1) return numeric * 100;
  return numeric;
}

function toCardSnapshot(row: SummarySnapshot) {
  const completeness = normalizeScore(row.COMPLETENESS_SCORE);
  const uniqueness = normalizeScore(row.UNIQUENESS_SCORE);
  const validity = normalizeScore(row.VALIDITY_SCORE);
  const consistency = normalizeScore(row.CONSISTENCY_SCORE);
  const freshness = normalizeScore(row.FRESHNESS_SCORE);
  const volume = normalizeScore(row.VOLUME_SCORE);

  return {
    summaryDate: row.SUMMARY_DATE,
    lastRunTs: row.LAST_RUN_TS,
    datasetId: row.DATASET_ID,
    overallScore: normalizeScore(row.DQ_SCORE),
    coverageScore: (completeness + uniqueness) / 2,
    validityScore: validity,
    dimensions: {
      completeness,
      uniqueness,
      validity,
      consistency,
      freshness,
      volume,
    },
    totalChecks: Number(row.TOTAL_CHECKS || 0),
    failedChecks: Number(row.FAILED_CHECKS || 0),
    trustLevel: row.TRUST_LEVEL || 'LOW',
    qualityGrade: row.QUALITY_GRADE || 'F',
    isSlaMet: Boolean(row.IS_SLA_MET),
  };
}

function calculateDelta(today: number, yesterday: number | null): number | null {
  if (yesterday === null || yesterday === undefined) return null;
  return Number((today - yesterday).toFixed(2));
}

function getLargestNegativeDimension(
  todayDims: Record<DimensionKey, number>,
  yesterdayDims: Record<DimensionKey, number> | null
): { key: DimensionKey; delta: number } | null {
  if (!yesterdayDims) return null;

  const negatives = DIMENSION_KEYS.map((key) => ({
    key,
    delta: Number((todayDims[key] - yesterdayDims[key]).toFixed(2)),
  })).filter((entry) => entry.delta < 0);

  if (!negatives.length) return null;
  negatives.sort((a, b) => a.delta - b.delta);
  return negatives[0];
}

function buildMicroInsight(
  delta: number | null,
  largestNegative: { key: DimensionKey; delta: number } | null
): string {
  if (delta === null) {
    return 'No previous day data available.';
  }
  if (delta > 0) {
    return 'Quality improved compared to yesterday.';
  }
  if (delta < 0) {
    if (largestNegative?.key === 'validity') {
      return 'Drop primarily driven by validity checks.';
    }
    return 'Quality declined compared to yesterday.';
  }
  return 'No material change vs yesterday.';
}

/**
 * GET /api/dq/card-comparison
 * Aggregates all summary rows for resolved target date and previous date.
 */
export async function GET(request: NextRequest) {
  const startTime = Date.now();
  const { searchParams } = new URL(request.url);
  const dateParam = searchParams.get('date');
  const endpoint = `/api/dq/card-comparison?date=${dateParam || 'today'}`;

  let targetDateSql = 'CURRENT_DATE()';
  if (dateParam && /^\d{4}-\d{2}-\d{2}$/.test(dateParam)) {
    targetDateSql = `'${dateParam}'::DATE`;
  }

  try {
    logger.logApiRequest(endpoint, 'GET');

    const valkeyKey = buildCacheKey('dashboard', 'card-comparison', dateParam || 'today');

    // Using 5 minutes (300 seconds) TTL
    const payload = await getOrSetCache(valkeyKey, 300, async () => {
      const config = getServerConfig();
      if (!config) {
        throw new Error('AUTH_FAILED: Not connected to Snowflake');
      }

      const connection = await snowflakePool.getConnection(config);
      await ensureConnectionContext(connection, config);

      const result = await retryQuery(async () => {
        const query = `
        WITH resolved_date AS (
          SELECT MAX(SUMMARY_DATE) AS SUMMARY_DATE
          FROM DATA_QUALITY_DB.DQ_METRICS.DQ_DAILY_SUMMARY
          WHERE SUMMARY_DATE <= ${targetDateSql}
        ),
        previous_date AS (
          SELECT MAX(SUMMARY_DATE) AS SUMMARY_DATE
          FROM DATA_QUALITY_DB.DQ_METRICS.DQ_DAILY_SUMMARY
          WHERE SUMMARY_DATE < (SELECT SUMMARY_DATE FROM resolved_date)
        ),
        today_agg AS (
          SELECT
            d.SUMMARY_DATE,
            MAX(d.LAST_RUN_TS) AS LAST_RUN_TS,
            'ALL_DATASETS' AS DATASET_ID,
            CASE
              WHEN SUM(COALESCE(d.TOTAL_CHECKS, 0)) = 0 THEN AVG(COALESCE(d.DQ_SCORE, 0))
              ELSE SUM(COALESCE(d.DQ_SCORE, 0) * COALESCE(d.TOTAL_CHECKS, 0)) / NULLIF(SUM(COALESCE(d.TOTAL_CHECKS, 0)), 0)
            END AS DQ_SCORE,
            CASE
              WHEN SUM(COALESCE(d.TOTAL_CHECKS, 0)) = 0 THEN AVG(COALESCE(d.COMPLETENESS_SCORE, 0))
              ELSE SUM(COALESCE(d.COMPLETENESS_SCORE, 0) * COALESCE(d.TOTAL_CHECKS, 0)) / NULLIF(SUM(COALESCE(d.TOTAL_CHECKS, 0)), 0)
            END AS COMPLETENESS_SCORE,
            CASE
              WHEN SUM(COALESCE(d.TOTAL_CHECKS, 0)) = 0 THEN AVG(COALESCE(d.UNIQUENESS_SCORE, 0))
              ELSE SUM(COALESCE(d.UNIQUENESS_SCORE, 0) * COALESCE(d.TOTAL_CHECKS, 0)) / NULLIF(SUM(COALESCE(d.TOTAL_CHECKS, 0)), 0)
            END AS UNIQUENESS_SCORE,
            CASE
              WHEN SUM(COALESCE(d.TOTAL_CHECKS, 0)) = 0 THEN AVG(COALESCE(d.VALIDITY_SCORE, 0))
              ELSE SUM(COALESCE(d.VALIDITY_SCORE, 0) * COALESCE(d.TOTAL_CHECKS, 0)) / NULLIF(SUM(COALESCE(d.TOTAL_CHECKS, 0)), 0)
            END AS VALIDITY_SCORE,
            CASE
              WHEN SUM(COALESCE(d.TOTAL_CHECKS, 0)) = 0 THEN AVG(COALESCE(d.CONSISTENCY_SCORE, 0))
              ELSE SUM(COALESCE(d.CONSISTENCY_SCORE, 0) * COALESCE(d.TOTAL_CHECKS, 0)) / NULLIF(SUM(COALESCE(d.TOTAL_CHECKS, 0)), 0)
            END AS CONSISTENCY_SCORE,
            CASE
              WHEN SUM(COALESCE(d.TOTAL_CHECKS, 0)) = 0 THEN AVG(COALESCE(d.FRESHNESS_SCORE, 0))
              ELSE SUM(COALESCE(d.FRESHNESS_SCORE, 0) * COALESCE(d.TOTAL_CHECKS, 0)) / NULLIF(SUM(COALESCE(d.TOTAL_CHECKS, 0)), 0)
            END AS FRESHNESS_SCORE,
            CASE
              WHEN SUM(COALESCE(d.TOTAL_CHECKS, 0)) = 0 THEN AVG(COALESCE(d.VOLUME_SCORE, 0))
              ELSE SUM(COALESCE(d.VOLUME_SCORE, 0) * COALESCE(d.TOTAL_CHECKS, 0)) / NULLIF(SUM(COALESCE(d.TOTAL_CHECKS, 0)), 0)
            END AS VOLUME_SCORE,
            SUM(COALESCE(d.TOTAL_CHECKS, 0)) AS TOTAL_CHECKS,
            SUM(COALESCE(d.FAILED_CHECKS, 0)) AS FAILED_CHECKS,
            CASE
              WHEN SUM(COALESCE(d.FAILED_CHECKS, 0)) = 0 THEN 'HIGH'
              WHEN SUM(COALESCE(d.FAILED_CHECKS, 0)) < 10 THEN 'MEDIUM'
              ELSE 'LOW'
            END AS TRUST_LEVEL,
            CASE
              WHEN (
                CASE
                  WHEN SUM(COALESCE(d.TOTAL_CHECKS, 0)) = 0 THEN AVG(COALESCE(d.DQ_SCORE, 0))
                  ELSE SUM(COALESCE(d.DQ_SCORE, 0) * COALESCE(d.TOTAL_CHECKS, 0)) / NULLIF(SUM(COALESCE(d.TOTAL_CHECKS, 0)), 0)
                END
              ) >= 90 THEN 'A'
              WHEN (
                CASE
                  WHEN SUM(COALESCE(d.TOTAL_CHECKS, 0)) = 0 THEN AVG(COALESCE(d.DQ_SCORE, 0))
                  ELSE SUM(COALESCE(d.DQ_SCORE, 0) * COALESCE(d.TOTAL_CHECKS, 0)) / NULLIF(SUM(COALESCE(d.TOTAL_CHECKS, 0)), 0)
                END
              ) >= 75 THEN 'B'
              WHEN (
                CASE
                  WHEN SUM(COALESCE(d.TOTAL_CHECKS, 0)) = 0 THEN AVG(COALESCE(d.DQ_SCORE, 0))
                  ELSE SUM(COALESCE(d.DQ_SCORE, 0) * COALESCE(d.TOTAL_CHECKS, 0)) / NULLIF(SUM(COALESCE(d.TOTAL_CHECKS, 0)), 0)
                END
              ) >= 60 THEN 'C'
              ELSE 'D'
            END AS QUALITY_GRADE,
            IFF(MIN(IFF(COALESCE(d.IS_SLA_MET, FALSE), 1, 0)) = 1, TRUE, FALSE) AS IS_SLA_MET
          FROM DATA_QUALITY_DB.DQ_METRICS.DQ_DAILY_SUMMARY d
          JOIN resolved_date rd ON d.SUMMARY_DATE = rd.SUMMARY_DATE
          GROUP BY d.SUMMARY_DATE
        ),
        yesterday_agg AS (
          SELECT
            d.SUMMARY_DATE,
            MAX(d.LAST_RUN_TS) AS LAST_RUN_TS,
            'ALL_DATASETS' AS DATASET_ID,
            CASE
              WHEN SUM(COALESCE(d.TOTAL_CHECKS, 0)) = 0 THEN AVG(COALESCE(d.DQ_SCORE, 0))
              ELSE SUM(COALESCE(d.DQ_SCORE, 0) * COALESCE(d.TOTAL_CHECKS, 0)) / NULLIF(SUM(COALESCE(d.TOTAL_CHECKS, 0)), 0)
            END AS DQ_SCORE,
            CASE
              WHEN SUM(COALESCE(d.TOTAL_CHECKS, 0)) = 0 THEN AVG(COALESCE(d.COMPLETENESS_SCORE, 0))
              ELSE SUM(COALESCE(d.COMPLETENESS_SCORE, 0) * COALESCE(d.TOTAL_CHECKS, 0)) / NULLIF(SUM(COALESCE(d.TOTAL_CHECKS, 0)), 0)
            END AS COMPLETENESS_SCORE,
            CASE
              WHEN SUM(COALESCE(d.TOTAL_CHECKS, 0)) = 0 THEN AVG(COALESCE(d.UNIQUENESS_SCORE, 0))
              ELSE SUM(COALESCE(d.UNIQUENESS_SCORE, 0) * COALESCE(d.TOTAL_CHECKS, 0)) / NULLIF(SUM(COALESCE(d.TOTAL_CHECKS, 0)), 0)
            END AS UNIQUENESS_SCORE,
            CASE
              WHEN SUM(COALESCE(d.TOTAL_CHECKS, 0)) = 0 THEN AVG(COALESCE(d.VALIDITY_SCORE, 0))
              ELSE SUM(COALESCE(d.VALIDITY_SCORE, 0) * COALESCE(d.TOTAL_CHECKS, 0)) / NULLIF(SUM(COALESCE(d.TOTAL_CHECKS, 0)), 0)
            END AS VALIDITY_SCORE,
            CASE
              WHEN SUM(COALESCE(d.TOTAL_CHECKS, 0)) = 0 THEN AVG(COALESCE(d.CONSISTENCY_SCORE, 0))
              ELSE SUM(COALESCE(d.CONSISTENCY_SCORE, 0) * COALESCE(d.TOTAL_CHECKS, 0)) / NULLIF(SUM(COALESCE(d.TOTAL_CHECKS, 0)), 0)
            END AS CONSISTENCY_SCORE,
            CASE
              WHEN SUM(COALESCE(d.TOTAL_CHECKS, 0)) = 0 THEN AVG(COALESCE(d.FRESHNESS_SCORE, 0))
              ELSE SUM(COALESCE(d.FRESHNESS_SCORE, 0) * COALESCE(d.TOTAL_CHECKS, 0)) / NULLIF(SUM(COALESCE(d.TOTAL_CHECKS, 0)), 0)
            END AS FRESHNESS_SCORE,
            CASE
              WHEN SUM(COALESCE(d.TOTAL_CHECKS, 0)) = 0 THEN AVG(COALESCE(d.VOLUME_SCORE, 0))
              ELSE SUM(COALESCE(d.VOLUME_SCORE, 0) * COALESCE(d.TOTAL_CHECKS, 0)) / NULLIF(SUM(COALESCE(d.TOTAL_CHECKS, 0)), 0)
            END AS VOLUME_SCORE,
            SUM(COALESCE(d.TOTAL_CHECKS, 0)) AS TOTAL_CHECKS,
            SUM(COALESCE(d.FAILED_CHECKS, 0)) AS FAILED_CHECKS,
            CASE
              WHEN SUM(COALESCE(d.FAILED_CHECKS, 0)) = 0 THEN 'HIGH'
              WHEN SUM(COALESCE(d.FAILED_CHECKS, 0)) < 10 THEN 'MEDIUM'
              ELSE 'LOW'
            END AS TRUST_LEVEL,
            CASE
              WHEN (
                CASE
                  WHEN SUM(COALESCE(d.TOTAL_CHECKS, 0)) = 0 THEN AVG(COALESCE(d.DQ_SCORE, 0))
                  ELSE SUM(COALESCE(d.DQ_SCORE, 0) * COALESCE(d.TOTAL_CHECKS, 0)) / NULLIF(SUM(COALESCE(d.TOTAL_CHECKS, 0)), 0)
                END
              ) >= 90 THEN 'A'
              WHEN (
                CASE
                  WHEN SUM(COALESCE(d.TOTAL_CHECKS, 0)) = 0 THEN AVG(COALESCE(d.DQ_SCORE, 0))
                  ELSE SUM(COALESCE(d.DQ_SCORE, 0) * COALESCE(d.TOTAL_CHECKS, 0)) / NULLIF(SUM(COALESCE(d.TOTAL_CHECKS, 0)), 0)
                END
              ) >= 75 THEN 'B'
              WHEN (
                CASE
                  WHEN SUM(COALESCE(d.TOTAL_CHECKS, 0)) = 0 THEN AVG(COALESCE(d.DQ_SCORE, 0))
                  ELSE SUM(COALESCE(d.DQ_SCORE, 0) * COALESCE(d.TOTAL_CHECKS, 0)) / NULLIF(SUM(COALESCE(d.TOTAL_CHECKS, 0)), 0)
                END
              ) >= 60 THEN 'C'
              ELSE 'D'
            END AS QUALITY_GRADE,
            IFF(MIN(IFF(COALESCE(d.IS_SLA_MET, FALSE), 1, 0)) = 1, TRUE, FALSE) AS IS_SLA_MET
          FROM DATA_QUALITY_DB.DQ_METRICS.DQ_DAILY_SUMMARY d
          JOIN previous_date pd ON d.SUMMARY_DATE = pd.SUMMARY_DATE
          GROUP BY d.SUMMARY_DATE
        )
        SELECT
          t.SUMMARY_DATE AS TODAY_SUMMARY_DATE,
          t.LAST_RUN_TS AS TODAY_LAST_RUN_TS,
          t.DATASET_ID AS TODAY_DATASET_ID,
          t.DQ_SCORE AS TODAY_DQ_SCORE,
          t.COMPLETENESS_SCORE AS TODAY_COMPLETENESS_SCORE,
          t.UNIQUENESS_SCORE AS TODAY_UNIQUENESS_SCORE,
          t.VALIDITY_SCORE AS TODAY_VALIDITY_SCORE,
          t.CONSISTENCY_SCORE AS TODAY_CONSISTENCY_SCORE,
          t.FRESHNESS_SCORE AS TODAY_FRESHNESS_SCORE,
          t.VOLUME_SCORE AS TODAY_VOLUME_SCORE,
          t.TOTAL_CHECKS AS TODAY_TOTAL_CHECKS,
          t.FAILED_CHECKS AS TODAY_FAILED_CHECKS,
          t.TRUST_LEVEL AS TODAY_TRUST_LEVEL,
          t.QUALITY_GRADE AS TODAY_QUALITY_GRADE,
          t.IS_SLA_MET AS TODAY_IS_SLA_MET,
          y.SUMMARY_DATE AS YESTERDAY_SUMMARY_DATE,
          y.LAST_RUN_TS AS YESTERDAY_LAST_RUN_TS,
          y.DATASET_ID AS YESTERDAY_DATASET_ID,
          y.DQ_SCORE AS YESTERDAY_DQ_SCORE,
          y.COMPLETENESS_SCORE AS YESTERDAY_COMPLETENESS_SCORE,
          y.UNIQUENESS_SCORE AS YESTERDAY_UNIQUENESS_SCORE,
          y.VALIDITY_SCORE AS YESTERDAY_VALIDITY_SCORE,
          y.CONSISTENCY_SCORE AS YESTERDAY_CONSISTENCY_SCORE,
          y.FRESHNESS_SCORE AS YESTERDAY_FRESHNESS_SCORE,
          y.VOLUME_SCORE AS YESTERDAY_VOLUME_SCORE,
          y.TOTAL_CHECKS AS YESTERDAY_TOTAL_CHECKS,
          y.FAILED_CHECKS AS YESTERDAY_FAILED_CHECKS,
          y.TRUST_LEVEL AS YESTERDAY_TRUST_LEVEL,
          y.QUALITY_GRADE AS YESTERDAY_QUALITY_GRADE,
          y.IS_SLA_MET AS YESTERDAY_IS_SLA_MET
        FROM today_agg t
        LEFT JOIN yesterday_agg y ON 1 = 1
      `;

        return executeQueryObjects(connection, query);
      }, 'card-comparison');

      if (!result || result.length === 0) {
        return {
          hasData: false,
          today: null,
          yesterday: null,
          deltas: null,
          deltaAvailable: false,
          largestNegativeDimension: null,
          microInsights: {
            overall: 'No previous day data available.',
            coverage: 'No previous day data available.',
            validity: 'No previous day data available.',
          },
        };
      }

      const row = result[0];
      const today = toCardSnapshot({
        SUMMARY_DATE: row.TODAY_SUMMARY_DATE,
        LAST_RUN_TS: row.TODAY_LAST_RUN_TS,
        DATASET_ID: row.TODAY_DATASET_ID,
        DQ_SCORE: row.TODAY_DQ_SCORE,
        COMPLETENESS_SCORE: row.TODAY_COMPLETENESS_SCORE,
        UNIQUENESS_SCORE: row.TODAY_UNIQUENESS_SCORE,
        VALIDITY_SCORE: row.TODAY_VALIDITY_SCORE,
        CONSISTENCY_SCORE: row.TODAY_CONSISTENCY_SCORE,
        FRESHNESS_SCORE: row.TODAY_FRESHNESS_SCORE,
        VOLUME_SCORE: row.TODAY_VOLUME_SCORE,
        TOTAL_CHECKS: row.TODAY_TOTAL_CHECKS,
        FAILED_CHECKS: row.TODAY_FAILED_CHECKS,
        TRUST_LEVEL: row.TODAY_TRUST_LEVEL,
        QUALITY_GRADE: row.TODAY_QUALITY_GRADE,
        IS_SLA_MET: row.TODAY_IS_SLA_MET,
      });

      const hasYesterday = row.YESTERDAY_SUMMARY_DATE !== null && row.YESTERDAY_SUMMARY_DATE !== undefined;
      const yesterday = hasYesterday
        ? toCardSnapshot({
          SUMMARY_DATE: row.YESTERDAY_SUMMARY_DATE,
          LAST_RUN_TS: row.YESTERDAY_LAST_RUN_TS,
          DATASET_ID: row.YESTERDAY_DATASET_ID,
          DQ_SCORE: row.YESTERDAY_DQ_SCORE,
          COMPLETENESS_SCORE: row.YESTERDAY_COMPLETENESS_SCORE,
          UNIQUENESS_SCORE: row.YESTERDAY_UNIQUENESS_SCORE,
          VALIDITY_SCORE: row.YESTERDAY_VALIDITY_SCORE,
          CONSISTENCY_SCORE: row.YESTERDAY_CONSISTENCY_SCORE,
          FRESHNESS_SCORE: row.YESTERDAY_FRESHNESS_SCORE,
          VOLUME_SCORE: row.YESTERDAY_VOLUME_SCORE,
          TOTAL_CHECKS: row.YESTERDAY_TOTAL_CHECKS,
          FAILED_CHECKS: row.YESTERDAY_FAILED_CHECKS,
          TRUST_LEVEL: row.YESTERDAY_TRUST_LEVEL,
          QUALITY_GRADE: row.YESTERDAY_QUALITY_GRADE,
          IS_SLA_MET: row.YESTERDAY_IS_SLA_MET,
        })
        : null;

      const overallDelta = calculateDelta(today.overallScore, yesterday?.overallScore ?? null);
      const coverageDelta = calculateDelta(today.coverageScore, yesterday?.coverageScore ?? null);
      const validityDelta = calculateDelta(today.validityScore, yesterday?.validityScore ?? null);
      const completenessDelta = calculateDelta(today.dimensions.completeness, yesterday?.dimensions.completeness ?? null);
      const uniquenessDelta = calculateDelta(today.dimensions.uniqueness, yesterday?.dimensions.uniqueness ?? null);
      const consistencyDelta = calculateDelta(today.dimensions.consistency, yesterday?.dimensions.consistency ?? null);
      const freshnessDelta = calculateDelta(today.dimensions.freshness, yesterday?.dimensions.freshness ?? null);
      const volumeDelta = calculateDelta(today.dimensions.volume, yesterday?.dimensions.volume ?? null);

      const largestNegativeDimension = getLargestNegativeDimension(
        today.dimensions,
        yesterday?.dimensions ?? null
      );

      return {
        hasData: true,
        deltaAvailable: hasYesterday,
        today,
        yesterday,
        deltas: {
          overall: overallDelta,
          coverage: coverageDelta,
          validity: validityDelta,
          completeness: completenessDelta,
          uniqueness: uniquenessDelta,
          consistency: consistencyDelta,
          freshness: freshnessDelta,
          volume: volumeDelta,
        },
        largestNegativeDimension,
        microInsights: {
          overall: buildMicroInsight(overallDelta, largestNegativeDimension),
          coverage: buildMicroInsight(coverageDelta, largestNegativeDimension),
          validity: buildMicroInsight(validityDelta, largestNegativeDimension),
        },
      };
    }); // End of getOrSetCache callback

    logger.logApiResponse(endpoint, true, Date.now() - startTime);

    return NextResponse.json({
      success: true,
      data: payload,
      metadata: { cached: true, timestamp: new Date().toISOString() }, // Simplified cache metadata
    });
  } catch (error: any) {
    logger.error('Error fetching card comparison data', error, { endpoint });

    if (error.message?.includes('AUTH_FAILED')) {
      return NextResponse.json(
        { success: false, error: { code: 'AUTH_FAILED', message: 'Not connected to Snowflake' } },
        { status: 401 }
      );
    }

    return NextResponse.json(createErrorResponse(error), { status: 500 });
  }
}
