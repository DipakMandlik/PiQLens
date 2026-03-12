import { executeQueryObjects, ensureConnectionContext, snowflakePool } from '@/lib/snowflake';
import { getServerConfig } from '@/lib/server-config';
import {
  AggregatedReportData,
  DailySummaryInsights,
  GenerateReportV2Request,
} from './types';
import {
  computeSuccessRate,
  ensureNonNegativeNumber,
  formatDateIST,
  formatTimestampIST,
  roundToTwo,
  sanitizeText,
} from './format-utils';

const COMPLETED_RUN_STATUSES = [
  'COMPLETED',
  'COMPLETED_WITH_FAILURES',
  'COMPLETED_WITH_ERRORS',
  'WARNING',
];

const FAILURE_ROW_LIMIT = 1000;

interface DatasetFilter {
  databaseName: string;
  schemaName: string;
  tableName: string;
}

function parseDatasetFilter(dataset?: string): DatasetFilter | null {
  if (!dataset || !dataset.trim()) return null;
  const parts = dataset.split('.').map((p) => p.trim()).filter(Boolean);
  if (parts.length !== 3) {
    throw new Error('Dataset must be in DATABASE.SCHEMA.TABLE format');
  }

  return {
    databaseName: parts[0].toUpperCase(),
    schemaName: parts[1].toUpperCase(),
    tableName: parts[2].toUpperCase(),
  };
}

function buildRunStatusPlaceholders(): string {
  return COMPLETED_RUN_STATUSES.map(() => '?').join(', ');
}

function buildRunPlaceholders(runIds: string[]): string {
  return runIds.map(() => '?').join(', ');
}

function buildDatasetFilterClause(alias: string, filter: DatasetFilter | null): { sql: string; binds: unknown[] } {
  if (!filter) {
    return { sql: '', binds: [] };
  }

  return {
    sql: `
      AND UPPER(${alias}.DATABASE_NAME) = ?
      AND UPPER(${alias}.SCHEMA_NAME) = ?
      AND UPPER(${alias}.TABLE_NAME) = ?
    `,
    binds: [filter.databaseName, filter.schemaName, filter.tableName],
  };
}

function normalizeScore(value: unknown): number {
  const numeric = Number(value ?? 0);
  if (!Number.isFinite(numeric)) return 0;
  const normalized = numeric !== 0 && numeric > -1 && numeric < 1 ? numeric * 100 : numeric;
  return roundToTwo(Math.max(0, normalized));
}

async function resolveRunIds(
  connection: unknown,
  request: GenerateReportV2Request,
  filter: DatasetFilter | null
): Promise<{ runIds: string[]; executionDate: string }> {
  if (request.mode === 'run') {
    if (!request.runId?.trim()) {
      throw new Error('runId is required when mode=run');
    }

    const datasetClause = filter
      ? `
        AND EXISTS (
          SELECT 1
          FROM DATA_QUALITY_DB.DQ_METRICS.DQ_CHECK_RESULTS c
          WHERE c.RUN_ID = rc.RUN_ID
            AND UPPER(c.DATABASE_NAME) = ?
            AND UPPER(c.SCHEMA_NAME) = ?
            AND UPPER(c.TABLE_NAME) = ?
        )
      `
      : '';

    const rows = await executeQueryObjects(
      connection,
      `
      SELECT rc.RUN_ID, rc.START_TS
      FROM DATA_QUALITY_DB.DQ_METRICS.DQ_RUN_CONTROL rc
      WHERE rc.RUN_ID = ?
        AND UPPER(COALESCE(rc.RUN_STATUS, '')) IN (${buildRunStatusPlaceholders()})
        ${datasetClause}
      LIMIT 1
      `,
      [
        request.runId.trim(),
        ...COMPLETED_RUN_STATUSES,
        ...(filter ? [filter.databaseName, filter.schemaName, filter.tableName] : []),
      ]
    );

    if (!rows.length) {
      throw new Error(`No completed run found for runId=${request.runId}`);
    }

    return {
      runIds: [rows[0].RUN_ID],
      executionDate: formatDateIST(rows[0].START_TS),
    };
  }

  if (!request.date) {
    throw new Error('date is required when mode=date_aggregate');
  }

  const datasetClause = filter
    ? `
      AND EXISTS (
        SELECT 1
        FROM DATA_QUALITY_DB.DQ_METRICS.DQ_CHECK_RESULTS c
        WHERE c.RUN_ID = rc.RUN_ID
          AND UPPER(c.DATABASE_NAME) = ?
          AND UPPER(c.SCHEMA_NAME) = ?
          AND UPPER(c.TABLE_NAME) = ?
      )
    `
    : '';

  const rows = await executeQueryObjects(
    connection,
    `
    SELECT rc.RUN_ID
    FROM DATA_QUALITY_DB.DQ_METRICS.DQ_RUN_CONTROL rc
    WHERE TO_CHAR(CONVERT_TIMEZONE('UTC', 'Asia/Kolkata', rc.START_TS), 'YYYY-MM-DD') = ?
      AND UPPER(COALESCE(rc.RUN_STATUS, '')) IN (${buildRunStatusPlaceholders()})
      ${datasetClause}
    ORDER BY rc.START_TS DESC
    `,
    [
      request.date,
      ...COMPLETED_RUN_STATUSES,
      ...(filter ? [filter.databaseName, filter.schemaName, filter.tableName] : []),
    ]
  );

  if (!rows.length) {
    throw new Error(`No completed runs found for date=${request.date}`);
  }

  return {
    runIds: rows.map((r: Record<string, unknown>) => sanitizeText(r.RUN_ID)).filter(Boolean),
    executionDate: request.date,
  };
}

async function fetchDailySummaryInsights(
  connection: unknown,
  executionDate: string,
  filter: DatasetFilter | null
): Promise<DailySummaryInsights | undefined> {
  const datasetWhere = buildDatasetFilterClause('d', filter);

  const todayRows = await executeQueryObjects(
    connection,
    `
    SELECT
      COALESCE(COUNT(DISTINCT COALESCE(d.DATASET_ID, UPPER(d.DATABASE_NAME) || '.' || UPPER(d.SCHEMA_NAME) || '.' || UPPER(d.TABLE_NAME))), 0) AS DATASET_COUNT,
      COALESCE(SUM(COALESCE(d.TOTAL_CHECKS, 0)), 0) AS TOTAL_CHECKS,
      COALESCE(SUM(COALESCE(d.FAILED_CHECKS, 0)), 0) AS FAILED_CHECKS,
      COALESCE(SUM(COALESCE(d.TOTAL_RECORDS, 0)), 0) AS TOTAL_RECORDS,
      COALESCE(SUM(COALESCE(d.FAILED_RECORDS_COUNT, 0)), 0) AS FAILED_RECORDS_COUNT,
      CASE
        WHEN COALESCE(SUM(COALESCE(d.TOTAL_CHECKS, 0)), 0) = 0 THEN COALESCE(AVG(COALESCE(d.DQ_SCORE, 0)), 0)
        ELSE SUM(COALESCE(d.DQ_SCORE, 0) * COALESCE(d.TOTAL_CHECKS, 0)) / NULLIF(SUM(COALESCE(d.TOTAL_CHECKS, 0)), 0)
      END AS DQ_SCORE,
      CASE
        WHEN COALESCE(SUM(COALESCE(d.TOTAL_CHECKS, 0)), 0) = 0 THEN COALESCE(AVG(COALESCE(d.COMPLETENESS_SCORE, 0)), 0)
        ELSE SUM(COALESCE(d.COMPLETENESS_SCORE, 0) * COALESCE(d.TOTAL_CHECKS, 0)) / NULLIF(SUM(COALESCE(d.TOTAL_CHECKS, 0)), 0)
      END AS COMPLETENESS_SCORE,
      CASE
        WHEN COALESCE(SUM(COALESCE(d.TOTAL_CHECKS, 0)), 0) = 0 THEN COALESCE(AVG(COALESCE(d.UNIQUENESS_SCORE, 0)), 0)
        ELSE SUM(COALESCE(d.UNIQUENESS_SCORE, 0) * COALESCE(d.TOTAL_CHECKS, 0)) / NULLIF(SUM(COALESCE(d.TOTAL_CHECKS, 0)), 0)
      END AS UNIQUENESS_SCORE,
      CASE
        WHEN COALESCE(SUM(COALESCE(d.TOTAL_CHECKS, 0)), 0) = 0 THEN COALESCE(AVG(COALESCE(d.VALIDITY_SCORE, 0)), 0)
        ELSE SUM(COALESCE(d.VALIDITY_SCORE, 0) * COALESCE(d.TOTAL_CHECKS, 0)) / NULLIF(SUM(COALESCE(d.TOTAL_CHECKS, 0)), 0)
      END AS VALIDITY_SCORE,
      CASE
        WHEN COALESCE(SUM(COALESCE(d.TOTAL_CHECKS, 0)), 0) = 0 THEN COALESCE(AVG(COALESCE(d.CONSISTENCY_SCORE, 0)), 0)
        ELSE SUM(COALESCE(d.CONSISTENCY_SCORE, 0) * COALESCE(d.TOTAL_CHECKS, 0)) / NULLIF(SUM(COALESCE(d.TOTAL_CHECKS, 0)), 0)
      END AS CONSISTENCY_SCORE,
      CASE
        WHEN COALESCE(SUM(COALESCE(d.TOTAL_CHECKS, 0)), 0) = 0 THEN COALESCE(AVG(COALESCE(d.FRESHNESS_SCORE, 0)), 0)
        ELSE SUM(COALESCE(d.FRESHNESS_SCORE, 0) * COALESCE(d.TOTAL_CHECKS, 0)) / NULLIF(SUM(COALESCE(d.TOTAL_CHECKS, 0)), 0)
      END AS FRESHNESS_SCORE,
      CASE
        WHEN COALESCE(SUM(COALESCE(d.TOTAL_CHECKS, 0)), 0) = 0 THEN COALESCE(AVG(COALESCE(d.VOLUME_SCORE, 0)), 0)
        ELSE SUM(COALESCE(d.VOLUME_SCORE, 0) * COALESCE(d.TOTAL_CHECKS, 0)) / NULLIF(SUM(COALESCE(d.TOTAL_CHECKS, 0)), 0)
      END AS VOLUME_SCORE,
      COALESCE(MAX(NULLIF(d.TRUST_LEVEL, '')), 'LOW') AS TRUST_LEVEL,
      COALESCE(MAX(NULLIF(d.QUALITY_GRADE, '')), 'F') AS QUALITY_GRADE,
      IFF(MIN(IFF(COALESCE(d.IS_SLA_MET, FALSE), 1, 0)) = 1, TRUE, FALSE) AS IS_SLA_MET,
      COALESCE(AVG(COALESCE(d.PREV_DAY_SCORE, 0)), 0) AS PREV_DAY_SCORE,
      COALESCE(MAX(NULLIF(d.SCORE_TREND, '')), 'STABLE') AS SCORE_TREND
    FROM DATA_QUALITY_DB.DQ_METRICS.DQ_DAILY_SUMMARY d
    WHERE d.SUMMARY_DATE = ?
      ${datasetWhere.sql}
    `,
    [executionDate, ...datasetWhere.binds]
  );

  if (!todayRows.length) {
    return undefined;
  }

  const today = todayRows[0] || {};
  const todayDQ = normalizeScore(today.DQ_SCORE);

  const prevRows = await executeQueryObjects(
    connection,
    `
    WITH prev_date AS (
      SELECT MAX(d.SUMMARY_DATE) AS SUMMARY_DATE
      FROM DATA_QUALITY_DB.DQ_METRICS.DQ_DAILY_SUMMARY d
      WHERE d.SUMMARY_DATE < ?
        ${datasetWhere.sql}
    )
    SELECT
      CASE
        WHEN COALESCE(SUM(COALESCE(d.TOTAL_CHECKS, 0)), 0) = 0 THEN COALESCE(AVG(COALESCE(d.DQ_SCORE, 0)), 0)
        ELSE SUM(COALESCE(d.DQ_SCORE, 0) * COALESCE(d.TOTAL_CHECKS, 0)) / NULLIF(SUM(COALESCE(d.TOTAL_CHECKS, 0)), 0)
      END AS PREV_DQ_SCORE
    FROM DATA_QUALITY_DB.DQ_METRICS.DQ_DAILY_SUMMARY d
    JOIN prev_date pd ON d.SUMMARY_DATE = pd.SUMMARY_DATE
    WHERE pd.SUMMARY_DATE IS NOT NULL
      ${datasetWhere.sql}
    `,
    [executionDate, ...datasetWhere.binds, ...datasetWhere.binds]
  );

  const fallbackPrev = normalizeScore(today.PREV_DAY_SCORE);
  const prevDQ = prevRows.length ? normalizeScore(prevRows[0]?.PREV_DQ_SCORE) : fallbackPrev;
  const totalRecords = ensureNonNegativeNumber(today.TOTAL_RECORDS);
  const failedRecords = ensureNonNegativeNumber(today.FAILED_RECORDS_COUNT);
  const failureRate = totalRecords > 0 ? roundToTwo((failedRecords / totalRecords) * 100) : 0;

  let trend = sanitizeText(today.SCORE_TREND).toUpperCase();
  if (!trend) {
    if (todayDQ > prevDQ) trend = 'IMPROVING';
    else if (todayDQ < prevDQ) trend = 'DEGRADING';
    else trend = 'STABLE';
  }

  return {
    datasetCount: ensureNonNegativeNumber(today.DATASET_COUNT),
    dqScore: todayDQ,
    completenessScore: normalizeScore(today.COMPLETENESS_SCORE),
    uniquenessScore: normalizeScore(today.UNIQUENESS_SCORE),
    validityScore: normalizeScore(today.VALIDITY_SCORE),
    consistencyScore: normalizeScore(today.CONSISTENCY_SCORE),
    freshnessScore: normalizeScore(today.FRESHNESS_SCORE),
    volumeScore: normalizeScore(today.VOLUME_SCORE),
    trustLevel: sanitizeText(today.TRUST_LEVEL) || 'LOW',
    qualityGrade: sanitizeText(today.QUALITY_GRADE) || 'F',
    isSlaMet: Boolean(today.IS_SLA_MET),
    totalRecords,
    failedRecordsCount: failedRecords,
    failureRate,
    prevDayScore: prevDQ,
    scoreDelta: roundToTwo(todayDQ - prevDQ),
    scoreTrend: trend,
  };
}

export async function aggregateReportData(request: GenerateReportV2Request): Promise<AggregatedReportData> {
  const config = getServerConfig();
  if (!config) {
    throw new Error('Database configuration not available');
  }

  const connection = await snowflakePool.getConnection(config);
  await ensureConnectionContext(connection, config);

  const datasetFilter = parseDatasetFilter(request.dataset);
  const { runIds, executionDate } = await resolveRunIds(connection, request, datasetFilter);

  const runPlaceholders = buildRunPlaceholders(runIds);
  const datasetWhere = buildDatasetFilterClause('c', datasetFilter);

  const summaryRows = await executeQueryObjects(
    connection,
    `
    SELECT
      COALESCE(COUNT(DISTINCT UPPER(c.DATABASE_NAME) || '.' || UPPER(c.SCHEMA_NAME) || '.' || UPPER(c.TABLE_NAME)), 0) AS TOTAL_DATASETS,
      COALESCE(COUNT(*), 0) AS TOTAL_CHECKS,
      COALESCE(SUM(CASE WHEN UPPER(c.CHECK_STATUS) IN ('PASS', 'PASSED') THEN 1 ELSE 0 END), 0) AS PASSED_CHECKS,
      COALESCE(SUM(CASE WHEN UPPER(c.CHECK_STATUS) IN ('FAIL', 'FAILED') THEN 1 ELSE 0 END), 0) AS FAILED_CHECKS
    FROM DATA_QUALITY_DB.DQ_METRICS.DQ_CHECK_RESULTS c
    WHERE c.RUN_ID IN (${runPlaceholders})
      ${datasetWhere.sql}
    `,
    [...runIds, ...datasetWhere.binds]
  );

  const summaryRow = summaryRows[0] || {};
  const totalChecks = ensureNonNegativeNumber(summaryRow.TOTAL_CHECKS);
  const passedChecks = ensureNonNegativeNumber(summaryRow.PASSED_CHECKS);
  const failedChecks = ensureNonNegativeNumber(summaryRow.FAILED_CHECKS);

  const summary = {
    totalDatasets: ensureNonNegativeNumber(summaryRow.TOTAL_DATASETS),
    totalChecks,
    passedChecks,
    failedChecks,
    successRate: computeSuccessRate(passedChecks, totalChecks),
  };

  const dailyInsights = await fetchDailySummaryInsights(connection, executionDate, datasetFilter);

  const datasetRows = await executeQueryObjects(
    connection,
    `
    SELECT
      COALESCE(c.DATABASE_NAME, '') AS DATABASE_NAME,
      COALESCE(c.SCHEMA_NAME, '') AS SCHEMA_NAME,
      COALESCE(c.TABLE_NAME, '') AS TABLE_NAME,
      COALESCE(COUNT(*), 0) AS TOTAL_CHECKS,
      COALESCE(SUM(CASE WHEN UPPER(c.CHECK_STATUS) IN ('PASS', 'PASSED') THEN 1 ELSE 0 END), 0) AS PASSED_CHECKS,
      COALESCE(SUM(CASE WHEN UPPER(c.CHECK_STATUS) IN ('FAIL', 'FAILED') THEN 1 ELSE 0 END), 0) AS FAILED_CHECKS,
      MAX(c.CHECK_TIMESTAMP) AS LAST_CHECK_TIMESTAMP
    FROM DATA_QUALITY_DB.DQ_METRICS.DQ_CHECK_RESULTS c
    WHERE c.RUN_ID IN (${runPlaceholders})
      ${datasetWhere.sql}
    GROUP BY c.DATABASE_NAME, c.SCHEMA_NAME, c.TABLE_NAME
    ORDER BY FAILED_CHECKS DESC, DATABASE_NAME, SCHEMA_NAME, TABLE_NAME
    `,
    [...runIds, ...datasetWhere.binds]
  );

  const datasets = datasetRows.map((row: Record<string, unknown>) => {
    const tableTotal = ensureNonNegativeNumber(row.TOTAL_CHECKS);
    const tablePassed = ensureNonNegativeNumber(row.PASSED_CHECKS);
    const tableFailed = ensureNonNegativeNumber(row.FAILED_CHECKS);

    return {
      databaseName: sanitizeText(row.DATABASE_NAME),
      schemaName: sanitizeText(row.SCHEMA_NAME),
      tableName: sanitizeText(row.TABLE_NAME),
      totalChecks: tableTotal,
      passedChecks: tablePassed,
      failedChecks: tableFailed,
      successRate: computeSuccessRate(tablePassed, tableTotal),
      lastCheckTimestamp: formatTimestampIST(row.LAST_CHECK_TIMESTAMP as string | Date | null),
    };
  });

  const failureCountRows = await executeQueryObjects(
    connection,
    `
    SELECT COALESCE(COUNT(*), 0) AS TOTAL_FAILURES
    FROM DATA_QUALITY_DB.DQ_METRICS.DQ_CHECK_RESULTS c
    WHERE c.RUN_ID IN (${runPlaceholders})
      AND UPPER(c.CHECK_STATUS) IN ('FAIL', 'FAILED')
      ${datasetWhere.sql}
    `,
    [...runIds, ...datasetWhere.binds]
  );

  const failureRowsTotal = ensureNonNegativeNumber(failureCountRows[0]?.TOTAL_FAILURES ?? 0);

  const failureRows = await executeQueryObjects(
    connection,
    `
    SELECT
      COALESCE(c.RUN_ID, '') AS RUN_ID,
      COALESCE(c.DATABASE_NAME, '') AS DATABASE_NAME,
      COALESCE(c.SCHEMA_NAME, '') AS SCHEMA_NAME,
      COALESCE(c.TABLE_NAME, '') AS TABLE_NAME,
      COALESCE(c.COLUMN_NAME, '') AS COLUMN_NAME,
      COALESCE(c.RULE_NAME, '') AS RULE_NAME,
      COALESCE(c.RULE_TYPE, '') AS RULE_TYPE,
      COALESCE(c.CHECK_STATUS, '') AS CHECK_STATUS,
      COALESCE(c.INVALID_RECORDS, 0) AS INVALID_RECORDS,
      COALESCE(c.TOTAL_RECORDS, 0) AS TOTAL_RECORDS,
      COALESCE(c.PASS_RATE, 0) AS PASS_RATE,
      COALESCE(c.THRESHOLD, 0) AS THRESHOLD,
      COALESCE(c.FAILURE_REASON, '') AS FAILURE_REASON,
      c.CHECK_TIMESTAMP AS CHECK_TIMESTAMP
    FROM DATA_QUALITY_DB.DQ_METRICS.DQ_CHECK_RESULTS c
    WHERE c.RUN_ID IN (${runPlaceholders})
      AND UPPER(c.CHECK_STATUS) IN ('FAIL', 'FAILED')
      ${datasetWhere.sql}
    ORDER BY COALESCE(c.INVALID_RECORDS, 0) DESC, c.CHECK_TIMESTAMP DESC
    LIMIT ${FAILURE_ROW_LIMIT}
    `,
    [...runIds, ...datasetWhere.binds]
  );

  const failures = failureRows.map((row: Record<string, unknown>) => ({
    runId: sanitizeText(row.RUN_ID),
    databaseName: sanitizeText(row.DATABASE_NAME),
    schemaName: sanitizeText(row.SCHEMA_NAME),
    tableName: sanitizeText(row.TABLE_NAME),
    columnName: sanitizeText(row.COLUMN_NAME),
    ruleName: sanitizeText(row.RULE_NAME),
    ruleType: sanitizeText(row.RULE_TYPE),
    checkStatus: sanitizeText(row.CHECK_STATUS),
    invalidRecords: ensureNonNegativeNumber(row.INVALID_RECORDS),
    totalRecords: ensureNonNegativeNumber(row.TOTAL_RECORDS),
    passRate: roundToTwo(ensureNonNegativeNumber(row.PASS_RATE)),
    threshold: roundToTwo(ensureNonNegativeNumber(row.THRESHOLD)),
    failureReason: sanitizeText(row.FAILURE_REASON),
    checkTimestamp: formatTimestampIST(row.CHECK_TIMESTAMP as string | Date | null),
  }));

  return {
    executionDate,
    runIds,
    summary,
    dailyInsights,
    datasets,
    failures,
    failureRowsTotal,
  };
}

export const REPORTING_CONSTANTS = {
  FAILURE_ROW_LIMIT,
};
