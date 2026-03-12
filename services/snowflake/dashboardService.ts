/**
 * Snowflake Dashboard Service
 *
 * Wraps frequently-hit dashboard queries behind the CacheManager.
 * Each method accepts a live Snowflake connection and returns the same
 * data shape the original route produced — zero API contract change.
 *
 * TTL Policy:
 *   Aggregation metrics (overall-score, KPIs): 120s
 *   Historical / static data (daily-summary, score-by-dataset): 300s
 */

import { cacheManager } from '@/lib/runtime/cacheManager';
import { retryQuery } from '@/lib/retry';

type ExecuteQueryFn = (conn: any, sql: string) => Promise<any>;
type ExecuteQueryObjectsFn = (conn: any, sql: string) => Promise<any[]>;

// ---------------------------------------------------------------------------
// Overall Score
// ---------------------------------------------------------------------------

export async function getOverallScore(
    connection: any,
    executeQueryObjects: ExecuteQueryObjectsFn,
    targetDate: string,
    dateParam: string | null,
): Promise<any> {
    const key = `snowflake:dashboard:overall-score:${dateParam || 'today'}`;

    return cacheManager.getOrSet(key, 120, () =>
        retryQuery(async () => {
            const query = `
        SELECT 
            AVG(c.PASS_RATE) as OVERALL_SCORE,
            COUNT(*) as TOTAL_EXECUTED,
            MAX(c.CHECK_TIMESTAMP) as LAST_SCAN_TS,
            AVG(CASE WHEN c.RULE_TYPE = 'COMPLETENESS' THEN c.PASS_RATE ELSE NULL END) as COMPLETENESS_SCORE,
            AVG(CASE WHEN c.RULE_TYPE = 'UNIQUENESS' THEN c.PASS_RATE ELSE NULL END) as UNIQUENESS_SCORE,
            AVG(CASE WHEN c.RULE_TYPE = 'VALIDITY' THEN c.PASS_RATE ELSE NULL END) as VALIDITY_SCORE,
            COUNT(CASE WHEN c.RULE_TYPE IN ('COMPLETENESS', 'UNIQUENESS') THEN 1 ELSE NULL END) as COVERAGE_EXECUTED,
            COUNT(CASE WHEN c.RULE_TYPE = 'VALIDITY' THEN 1 ELSE NULL END) as VALIDITY_EXECUTED
        FROM DATA_QUALITY_DB.DQ_METRICS.DQ_CHECK_RESULTS c
        JOIN DATA_QUALITY_DB.DQ_METRICS.DQ_RUN_CONTROL r ON c.RUN_ID = r.RUN_ID
        WHERE r.START_TS::DATE = ${targetDate}
          AND r.RUN_STATUS != 'FAILED'
      `;

            const rows = await executeQueryObjects(connection, query);

            if (!rows || rows.length === 0 || Number(rows[0].TOTAL_EXECUTED || 0) === 0) {
                return {
                    hasData: false,
                    overallScore: 0,
                    totalExecuted: 0,
                    coverageScore: 0,
                    coverageExecuted: 0,
                    validityScore: 0,
                    validityExecuted: 0,
                    summaryDate: null,
                };
            }

            const row = rows[0];
            const normalize = (val: any): number => {
                if (val === null || val === undefined) return 0;
                const num = Number(val);
                return num <= 1 && num > -1 && num !== 0 ? num * 100 : num;
            };

            const completeness = normalize(row.COMPLETENESS_SCORE);
            const uniqueness = normalize(row.UNIQUENESS_SCORE);

            return {
                hasData: true,
                overallScore: normalize(row.OVERALL_SCORE),
                totalExecuted: Number(row.TOTAL_EXECUTED || 0),
                coverageScore: (completeness + uniqueness) / 2,
                coverageExecuted: Number(row.COVERAGE_EXECUTED || 0),
                validityScore: normalize(row.VALIDITY_SCORE),
                validityExecuted: Number(row.VALIDITY_EXECUTED || 0),
                summaryDate: row.LAST_SCAN_TS,
                previousScore: null,
                scoreDifference: undefined,
            };
        }, 'overall-score'),
    );
}

// ---------------------------------------------------------------------------
// KPIs
// ---------------------------------------------------------------------------

export async function getKpis(
    connection: any,
    executeQuery: ExecuteQueryFn,
    days: number,
): Promise<any> {
    const key = `snowflake:dashboard:kpis:${days}`;

    return cacheManager.getOrSet(key, 120, async () => {
        const summaryQuery = `
      SELECT 
        SUM(TOTAL_CHECKS) as TOTAL_CHECKS,
        SUM(PASSED_CHECKS) as PASSED_CHECKS,
        SUM(FAILED_CHECKS) as FAILED_CHECKS,
        SUM(WARNING_CHECKS) as WARNING_CHECKS,
        AVG(DQ_SCORE) as AVG_DQ_SCORE,
        SUM(TOTAL_RECORDS) as TOTAL_RECORDS,
        SUM(FAILED_RECORDS_COUNT) as FAILED_RECORDS,
        COUNT(DISTINCT TABLE_NAME) as TOTAL_TABLES,
        SUM(CASE WHEN IS_SLA_MET = TRUE THEN 1 ELSE 0 END) as SLA_MET_COUNT,
        COUNT(*) as TOTAL_DATASETS
      FROM DATA_QUALITY_DB.DQ_METRICS.DQ_DAILY_SUMMARY
      WHERE SUMMARY_DATE >= DATEADD(day, -${days}, CURRENT_DATE())
        AND SUMMARY_DATE = (SELECT MAX(SUMMARY_DATE) FROM DATA_QUALITY_DB.DQ_METRICS.DQ_DAILY_SUMMARY)
    `;

        const runControlQuery = `
      SELECT 
        RUN_ID, RUN_STATUS, START_TS, END_TS,
        TOTAL_CHECKS, PASSED_CHECKS, FAILED_CHECKS, WARNING_CHECKS
      FROM DATA_QUALITY_DB.DQ_METRICS.DQ_RUN_CONTROL
      WHERE START_TS = (SELECT MAX(START_TS) FROM DATA_QUALITY_DB.DQ_METRICS.DQ_RUN_CONTROL)
      LIMIT 1
    `;

        const trendQuery = `
      SELECT 
        SUMMARY_DATE,
        AVG(DQ_SCORE) as AVG_SCORE,
        AVG(PREV_DAY_SCORE) as AVG_PREV_SCORE
      FROM DATA_QUALITY_DB.DQ_METRICS.DQ_DAILY_SUMMARY
      WHERE SUMMARY_DATE >= DATEADD(day, -${days}, CURRENT_DATE())
      GROUP BY SUMMARY_DATE
      ORDER BY SUMMARY_DATE DESC
      LIMIT 2
    `;

        const gradeQuery = `
      SELECT 
        QUALITY_GRADE, COUNT(*) as COUNT
      FROM DATA_QUALITY_DB.DQ_METRICS.DQ_DAILY_SUMMARY
      WHERE SUMMARY_DATE = (SELECT MAX(SUMMARY_DATE) FROM DATA_QUALITY_DB.DQ_METRICS.DQ_DAILY_SUMMARY)
      GROUP BY QUALITY_GRADE
    `;

        const [summaryResult, runControlResult, trendResult, gradeResult] = await Promise.all([
            executeQuery(connection, summaryQuery),
            executeQuery(connection, runControlQuery),
            executeQuery(connection, trendQuery),
            executeQuery(connection, gradeQuery),
        ]);

        const summary = summaryResult.rows[0] || [];
        const summaryObj: any = {};
        summaryResult.columns.forEach((col: string, idx: number) => {
            summaryObj[col] = summary[idx];
        });

        const runControl = runControlResult.rows[0] || [];
        const runControlObj: any = {};
        if (runControl.length > 0) {
            runControlResult.columns.forEach((col: string, idx: number) => {
                runControlObj[col] = runControl[idx];
            });
        }

        const slaCompliance = summaryObj.TOTAL_DATASETS > 0
            ? (summaryObj.SLA_MET_COUNT / summaryObj.TOTAL_DATASETS) * 100
            : 0;

        let scoreTrend = 'STABLE';
        if (trendResult.rows.length >= 2) {
            const current = trendResult.rows[0][1] || 0;
            const previous = trendResult.rows[1][1] || 0;
            if (current > previous) scoreTrend = 'IMPROVING';
            else if (current < previous) scoreTrend = 'DECLINING';
        }

        let qualityGrade = 'N/A';
        if (gradeResult.rows.length > 0) {
            const grades = gradeResult.rows.map((row: any[]) => ({
                grade: row[0],
                count: row[1],
            }));
            grades.sort((a: any, b: any) => (b.count || 0) - (a.count || 0));
            qualityGrade = grades[0].grade || 'N/A';
        }

        return {
            overallDQScore: summaryObj.AVG_DQ_SCORE || 0,
            totalChecks: summaryObj.TOTAL_CHECKS || 0,
            passedChecks: summaryObj.PASSED_CHECKS || 0,
            failedChecks: summaryObj.FAILED_CHECKS || 0,
            warningChecks: summaryObj.WARNING_CHECKS || 0,
            totalRecords: summaryObj.TOTAL_RECORDS || 0,
            failedRecords: summaryObj.FAILED_RECORDS || 0,
            totalTables: summaryObj.TOTAL_TABLES || 0,
            lastRunStatus: runControlObj.RUN_STATUS || 'UNKNOWN',
            lastRunTime: runControlObj.START_TS || null,
            lastRunId: runControlObj.RUN_ID || null,
            qualityGrade,
            slaCompliance: Math.round(slaCompliance * 100) / 100,
            scoreTrend,
            lastRunChecks: {
                total: runControlObj.TOTAL_CHECKS || 0,
                passed: runControlObj.PASSED_CHECKS || 0,
                failed: runControlObj.FAILED_CHECKS || 0,
                warning: runControlObj.WARNING_CHECKS || 0,
            },
        };
    });
}

// ---------------------------------------------------------------------------
// Daily Summary (30-day trend)
// ---------------------------------------------------------------------------

export async function getDailySummary(
    connection: any,
    executeQuery: ExecuteQueryFn,
): Promise<{ data: any[]; rowCount: number }> {
    const key = 'snowflake:dashboard:daily-summary';

    return cacheManager.getOrSet(key, 300, async () => {
        const query = `
      SELECT
        SUMMARY_DATE,
        ROUND(AVG(DQ_SCORE), 2) AS DQ_SCORE
      FROM DATA_QUALITY_DB.DQ_METRICS.DQ_DAILY_SUMMARY
      WHERE SUMMARY_DATE BETWEEN DATEADD(days, -30, CURRENT_DATE()) AND CURRENT_DATE()
      GROUP BY SUMMARY_DATE
      ORDER BY SUMMARY_DATE
    `;

        const result = await executeQuery(connection, query);

        return {
            data: result.rows.map((row: any[]) => {
                const obj: any = {};
                result.columns.forEach((col: string, idx: number) => {
                    obj[col] = row[idx];
                });
                return obj;
            }),
            rowCount: result.rowCount,
        };
    });
}

// ---------------------------------------------------------------------------
// Score by Dataset
// ---------------------------------------------------------------------------

export async function getScoreByDataset(
    connection: any,
    executeQuery: ExecuteQueryFn,
): Promise<{ datasets: Array<{ name: string; score: number }> }> {
    const key = 'snowflake:dashboard:score-by-dataset';

    return cacheManager.getOrSet(key, 300, async () => {
        const query = `
      SELECT
        DATASET_ID,
        ROUND(AVG(DQ_SCORE), 2) AS DQ_SCORE
      FROM DATA_QUALITY_DB.DQ_METRICS.DQ_DAILY_SUMMARY
      WHERE SUMMARY_DATE = (
        SELECT MAX(SUMMARY_DATE)
        FROM DATA_QUALITY_DB.DQ_METRICS.DQ_DAILY_SUMMARY
      )
      GROUP BY DATASET_ID
      ORDER BY DQ_SCORE DESC
    `;

        const result = await executeQuery(connection, query);
        return {
            datasets: result.rows.map((row: any[]) => ({
                name: row[0],
                score: row[1],
            })),
        };
    });
}

/**
 * Invalidate all dashboard cache entries.
 */
export function invalidateDashboardCache(): void {
    cacheManager.invalidateByPrefix('snowflake:dashboard:');
}
