import { cache, CacheTTL, generateCacheKey } from "@/lib/cache";
import { getServerConfig } from "@/lib/server-config";
import { ensureConnectionContext, executeQueryObjects, snowflakePool } from "@/lib/snowflake";
import {
  computeDatasetScore,
  computeDimensionScores,
  getDimensionWeight,
  getFormulaMetadata,
  roundTo,
  scoreToImpactLevel,
} from "@/lib/overview/formulas";
import {
  DateMetricsPayload,
  DatasetMetricsPayload,
  DimensionScoreRow,
  FailureSummary,
  MetricEnvelope,
  OverviewRunType,
  OverviewScope,
  RunDeltaPayload,
  RunMetricsPayload,
} from "@/lib/overview/types";

interface RunContext {
  runId: string;
  datasetId: string;
  databaseName: string;
  schemaName: string;
  tableName: string;
  startTs: string | null;
  durationSeconds: number;
  runStatus: string;
  runType: OverviewRunType;
}

interface CheckRow {
  RULE_TYPE: string | null;
  TOTAL_RECORDS: number | null;
  VALID_RECORDS: number | null;
  INVALID_RECORDS: number | null;
  CHECK_STATUS: string | null;
  COLUMN_NAME: string | null;
  RULE_NAME: string | null;
  FAILURE_REASON: string | null;
}

const STATUS_FAILED = new Set(["FAIL", "FAILED", "ERROR"]);

function toIso(value: unknown): string | null {
  if (!value) return null;
  try {
    const dt = new Date(value as string | number | Date);
    if (Number.isNaN(dt.getTime())) return null;
    return dt.toISOString();
  } catch {
    return null;
  }
}

function asNumber(value: unknown): number {
  const parsed = Number(value ?? 0);
  return Number.isFinite(parsed) ? parsed : 0;
}

function asString(value: unknown, fallback = ""): string {
  if (value === null || value === undefined) return fallback;
  return String(value);
}

function deriveRunType(runId: string): OverviewRunType {
  const normalized = runId.toUpperCase();
  if (normalized.includes("PROFILE")) return "PROFILING";
  if (normalized.includes("CUSTOM")) return "CUSTOM_SCAN";
  if (normalized.includes("INCR") || normalized.includes("INCREMENTAL")) return "INCREMENTAL";
  if (normalized.includes("FULL") || normalized.includes("DQ_RUN") || normalized.includes("SCAN")) return "FULL";
  return "UNKNOWN";
}

function getConnectionKey(scope: OverviewScope, identity: string): string {
  return generateCacheKey(`/overview/${scope.toLowerCase()}`, { identity });
}

function buildEnvelope<T>(scope: OverviewScope, data: T): MetricEnvelope<T> {
  const formulas = getFormulaMetadata();
  return {
    computed_at: new Date().toISOString(),
    formula_version: formulas.formula_version,
    aggregation_version: formulas.aggregation_version,
    scope,
    formulas,
    data,
  };
}

async function getConnection() {
  const config = getServerConfig();
  if (!config) {
    throw new Error("Not connected to Snowflake");
  }
  const connection = await snowflakePool.getConnection(config);
  await ensureConnectionContext(connection, config);
  return connection;
}

async function queryRows<T = Record<string, unknown>>(connection: unknown, sqlText: string, binds: Array<string | number>): Promise<T[]> {
  return executeQueryObjects(connection, sqlText, binds) as Promise<T[]>;
}

async function fetchRunContext(connection: unknown, runId: string): Promise<RunContext | null> {
  const rows = await queryRows(
    connection,
    `
      SELECT
        cr.RUN_ID,
        COALESCE(MIN(cr.DATASET_ID), 'UNKNOWN') AS DATASET_ID,
        COALESCE(MIN(cr.DATABASE_NAME), 'UNKNOWN') AS DATABASE_NAME,
        COALESCE(MIN(cr.SCHEMA_NAME), 'UNKNOWN') AS SCHEMA_NAME,
        COALESCE(MIN(cr.TABLE_NAME), 'UNKNOWN') AS TABLE_NAME,
        MIN(rc.START_TS) AS START_TS,
        MAX(rc.DURATION_SECONDS) AS DURATION_SECONDS,
        COALESCE(MAX(rc.RUN_STATUS), 'UNKNOWN') AS RUN_STATUS
      FROM DATA_QUALITY_DB.DQ_METRICS.DQ_CHECK_RESULTS cr
      LEFT JOIN DATA_QUALITY_DB.DQ_METRICS.DQ_RUN_CONTROL rc
        ON rc.RUN_ID = cr.RUN_ID
      WHERE cr.RUN_ID = ?
      GROUP BY cr.RUN_ID
    `,
    [runId]
  );

  if (rows.length === 0) return null;
  const row = rows[0];
  return {
    runId: asString(row.RUN_ID),
    datasetId: asString(row.DATASET_ID, "UNKNOWN"),
    databaseName: asString(row.DATABASE_NAME, "UNKNOWN"),
    schemaName: asString(row.SCHEMA_NAME, "UNKNOWN"),
    tableName: asString(row.TABLE_NAME, "UNKNOWN"),
    startTs: toIso(row.START_TS),
    durationSeconds: asNumber(row.DURATION_SECONDS),
    runStatus: asString(row.RUN_STATUS, "UNKNOWN"),
    runType: deriveRunType(asString(row.RUN_ID)),
  };
}

async function fetchCheckRowsByRun(connection: unknown, runId: string): Promise<CheckRow[]> {
  return queryRows<CheckRow>(
    connection,
    `
      SELECT
        RULE_TYPE,
        TOTAL_RECORDS,
        VALID_RECORDS,
        INVALID_RECORDS,
        CHECK_STATUS,
        COLUMN_NAME,
        RULE_NAME,
        FAILURE_REASON
      FROM DATA_QUALITY_DB.DQ_METRICS.DQ_CHECK_RESULTS
      WHERE RUN_ID = ?
    `,
    [runId]
  );
}

async function fetchCheckRowsByDate(connection: unknown, datasetId: string, date: string): Promise<CheckRow[]> {
  return queryRows<CheckRow>(
    connection,
    `
      SELECT
        RULE_TYPE,
        TOTAL_RECORDS,
        VALID_RECORDS,
        INVALID_RECORDS,
        CHECK_STATUS,
        COLUMN_NAME,
        RULE_NAME,
        FAILURE_REASON
      FROM DATA_QUALITY_DB.DQ_METRICS.DQ_CHECK_RESULTS
      WHERE DATASET_ID = ?
        AND DATE_TRUNC('DAY', CHECK_TIMESTAMP) = ?::DATE
    `,
    [datasetId, date]
  );
}

async function fetchCheckRowsByDataset(connection: unknown, datasetId: string): Promise<CheckRow[]> {
  return queryRows<CheckRow>(
    connection,
    `
      SELECT
        RULE_TYPE,
        TOTAL_RECORDS,
        VALID_RECORDS,
        INVALID_RECORDS,
        CHECK_STATUS,
        COLUMN_NAME,
        RULE_NAME,
        FAILURE_REASON
      FROM DATA_QUALITY_DB.DQ_METRICS.DQ_CHECK_RESULTS
      WHERE DATASET_ID = ?
    `,
    [datasetId]
  );
}

function aggregateCheckStats(checkRows: CheckRow[]) {
  let recordsScanned = 0;
  let recordsPassed = 0;
  let recordsFailed = 0;
  let passedChecks = 0;
  let failedChecks = 0;
  let warningChecks = 0;
  let errorChecks = 0;
  let skippedChecks = 0;

  for (const row of checkRows) {
    const total = asNumber(row.TOTAL_RECORDS);
    const valid = asNumber(row.VALID_RECORDS);
    const invalid = asNumber(row.INVALID_RECORDS);
    const status = asString(row.CHECK_STATUS).toUpperCase();

    recordsScanned += total;
    recordsPassed += valid;
    recordsFailed += invalid;

    if (status === "PASS" || status === "PASSED") passedChecks += 1;
    else if (status === "WARNING") warningChecks += 1;
    else if (status === "SKIPPED") skippedChecks += 1;
    else if (status === "ERROR") errorChecks += 1;
    else if (status === "FAIL" || status === "FAILED") failedChecks += 1;
  }

  const checksExecuted = checkRows.length;
  const failureRate = recordsScanned > 0 ? roundTo((recordsFailed / recordsScanned) * 100) : 0;

  const dimensionScores = computeDimensionScores(
    checkRows.map((row) => ({
      ruleType: asString(row.RULE_TYPE, "UNKNOWN"),
      totalRecords: asNumber(row.TOTAL_RECORDS),
      invalidRecords: asNumber(row.INVALID_RECORDS),
      validRecords: asNumber(row.VALID_RECORDS),
      checkStatus: asString(row.CHECK_STATUS, "UNKNOWN"),
    }))
  );

  const qualityScore = computeDatasetScore(dimensionScores);

  return {
    recordsScanned,
    recordsPassed,
    recordsFailed,
    checksExecuted,
    passedChecks,
    failedChecks,
    warningChecks,
    errorChecks,
    skippedChecks,
    failureRate,
    qualityScore,
    dimensionScores,
  };
}

async function fetchSparkline(connection: unknown, datasetId: string, days: number): Promise<Array<{ date: string; score: number }>> {
  const rows = await queryRows(
    connection,
    `
      SELECT
        TO_CHAR(SUMMARY_DATE, 'YYYY-MM-DD') AS SUMMARY_DATE,
        ROUND(AVG(DQ_SCORE), 2) AS SCORE
      FROM DATA_QUALITY_DB.DQ_METRICS.DQ_DAILY_SUMMARY
      WHERE DATASET_ID = ?
        AND SUMMARY_DATE >= DATEADD(day, ?, CURRENT_DATE())
      GROUP BY SUMMARY_DATE
      ORDER BY SUMMARY_DATE ASC
    `,
    [datasetId, -Math.abs(days)]
  );

  return rows.map((row) => ({
    date: asString(row.SUMMARY_DATE),
    score: asNumber(row.SCORE),
  }));
}

async function fetchStabilityStats(connection: unknown, datasetId: string): Promise<{
  stabilityIndex: number;
  volatilityScore: number;
  anomalyCountTotal: number;
  anomalyFrequency: number;
}> {
  const [volatilityRows, anomalyRows] = await Promise.all([
    queryRows(
      connection,
      `
        SELECT
          COUNT(*) AS TOTAL_DAYS,
          COALESCE(STDDEV(DQ_SCORE), 0) AS VOLATILITY
        FROM DATA_QUALITY_DB.DQ_METRICS.DQ_DAILY_SUMMARY
        WHERE DATASET_ID = ?
      `,
      [datasetId]
    ),
    queryRows(
      connection,
      `
        WITH baseline AS (
          SELECT
            DQ_SCORE,
            AVG(DQ_SCORE) OVER() AS AVG_SCORE,
            STDDEV(DQ_SCORE) OVER() AS STDDEV_SCORE
          FROM DATA_QUALITY_DB.DQ_METRICS.DQ_DAILY_SUMMARY
          WHERE DATASET_ID = ?
        )
        SELECT
          COUNT(*) AS TOTAL_POINTS,
          COUNT_IF(ABS(DQ_SCORE - AVG_SCORE) > GREATEST(COALESCE(NULLIF(STDDEV_SCORE, 0) * 2, 0), 5)) AS ANOMALY_COUNT
        FROM baseline
      `,
      [datasetId]
    ),
  ]);

  const volatility = asNumber(volatilityRows[0]?.VOLATILITY);
  const stability = Math.max(0, 100 - volatility);
  const totalPoints = asNumber(anomalyRows[0]?.TOTAL_POINTS);
  const anomalyCount = asNumber(anomalyRows[0]?.ANOMALY_COUNT);
  const anomalyFrequency = totalPoints > 0 ? roundTo((anomalyCount / totalPoints) * 100) : 0;

  return {
    stabilityIndex: roundTo(stability),
    volatilityScore: roundTo(volatility),
    anomalyCountTotal: anomalyCount,
    anomalyFrequency,
  };
}

async function fetchPreviousRunId(connection: unknown, context: RunContext): Promise<string | null> {
  const rows = await queryRows(
    connection,
    `
      SELECT
        rc.RUN_ID
      FROM DATA_QUALITY_DB.DQ_METRICS.DQ_RUN_CONTROL rc
      JOIN DATA_QUALITY_DB.DQ_METRICS.DQ_CHECK_RESULTS cr
        ON rc.RUN_ID = cr.RUN_ID
      WHERE cr.DATASET_ID = ?
        AND cr.TABLE_NAME = ?
        AND rc.START_TS < ?::TIMESTAMP_NTZ
      GROUP BY rc.RUN_ID, rc.START_TS
      ORDER BY rc.START_TS DESC
      LIMIT 1
    `,
    [context.datasetId, context.tableName, context.startTs ?? "9999-12-31T00:00:00"]
  );

  return rows.length > 0 ? asString(rows[0].RUN_ID) : null;
}

function buildDimensionBreakdown(
  currentRows: CheckRow[],
  previousRows: CheckRow[]
): DimensionScoreRow[] {
  const currentScores = computeDimensionScores(
    currentRows.map((row) => ({
      ruleType: asString(row.RULE_TYPE, "UNKNOWN"),
      totalRecords: asNumber(row.TOTAL_RECORDS),
      invalidRecords: asNumber(row.INVALID_RECORDS),
      validRecords: asNumber(row.VALID_RECORDS),
      checkStatus: asString(row.CHECK_STATUS, "UNKNOWN"),
    }))
  );

  const previousScores = computeDimensionScores(
    previousRows.map((row) => ({
      ruleType: asString(row.RULE_TYPE, "UNKNOWN"),
      totalRecords: asNumber(row.TOTAL_RECORDS),
      invalidRecords: asNumber(row.INVALID_RECORDS),
      validRecords: asNumber(row.VALID_RECORDS),
      checkStatus: asString(row.CHECK_STATUS, "UNKNOWN"),
    }))
  );

  const dimensions = new Set<string>([
    ...Object.keys(currentScores),
    ...Object.keys(previousScores),
  ]);

  const breakdown: DimensionScoreRow[] = [];

  for (const dimension of dimensions) {
    const rowsForDimension = currentRows.filter(
      (row) => asString(row.RULE_TYPE, "UNKNOWN").toUpperCase() === dimension
    );

    const failedChecks = rowsForDimension.filter((row) =>
      STATUS_FAILED.has(asString(row.CHECK_STATUS).toUpperCase())
    ).length;

    const recordsFailed = rowsForDimension.reduce(
      (acc, row) => acc + asNumber(row.INVALID_RECORDS),
      0
    );

    const currentScore = currentScores[dimension] ?? 0;
    const previousScore = previousScores[dimension] ?? null;

    breakdown.push({
      dimension,
      score: roundTo(currentScore),
      failed_checks: failedChecks,
      total_checks: rowsForDimension.length,
      records_failed: recordsFailed,
      impact_level: scoreToImpactLevel(currentScore),
      trend_vs_previous:
        previousScore === null ? null : roundTo(currentScore - previousScore),
      weight: getDimensionWeight(dimension),
    });
  }

  return breakdown.sort((a, b) => a.score - b.score);
}

function detectRunAnomalies(
  delta: RunDeltaPayload,
  dimensionBreakdown: DimensionScoreRow[]
): Array<{ code: string; severity: "LOW" | "MEDIUM" | "HIGH" | "CRITICAL"; description: string; value: number }> {
  const anomalies: Array<{ code: string; severity: "LOW" | "MEDIUM" | "HIGH" | "CRITICAL"; description: string; value: number }> = [];

  if (delta.delta_records !== null && Math.abs(delta.delta_records) > 0) {
    const denominator = Math.max(1, delta.previous_records ?? 1);
    const pct = (delta.delta_records / denominator) * 100;
    if (Math.abs(pct) >= 25) {
      anomalies.push({
        code: "VOLUME_SHIFT",
        severity: Math.abs(pct) >= 50 ? "CRITICAL" : "HIGH",
        description: `Record volume changed by ${roundTo(pct)}% compared to previous run`,
        value: roundTo(pct),
      });
    }
  }

  if (delta.delta_fail_rate !== null && delta.delta_fail_rate >= 5) {
    anomalies.push({
      code: "FAILURE_RATE_SURGE",
      severity: delta.delta_fail_rate >= 10 ? "CRITICAL" : "HIGH",
      description: `Failure rate increased by ${roundTo(delta.delta_fail_rate)} percentage points`,
      value: roundTo(delta.delta_fail_rate),
    });
  }

  const regressedDimensions = dimensionBreakdown.filter(
    (row) => row.trend_vs_previous !== null && row.trend_vs_previous < -5
  );

  for (const row of regressedDimensions.slice(0, 2)) {
    anomalies.push({
      code: `DIMENSION_REGRESSION_${row.dimension}`,
      severity: row.trend_vs_previous !== null && row.trend_vs_previous < -15 ? "CRITICAL" : "MEDIUM",
      description: `${row.dimension} regressed by ${Math.abs(row.trend_vs_previous ?? 0)} points`,
      value: Math.abs(row.trend_vs_previous ?? 0),
    });
  }

  return anomalies;
}

function buildFailureSummaryFromRows(
  checkRows: CheckRow[],
  anomalies: Array<{ code: string; severity: "LOW" | "MEDIUM" | "HIGH" | "CRITICAL"; description: string; value: number }>
): FailureSummary {
  const failedOnly = checkRows.filter((row) =>
    STATUS_FAILED.has(asString(row.CHECK_STATUS).toUpperCase())
  );

  const byRule = new Map<string, { failures: number; failedRecords: number; totalRecords: number }>();
  const byColumn = new Map<string, { failures: number; failedRecords: number }>();
  const byPattern = new Map<string, number>();

  for (const row of failedOnly) {
    const rule = asString(row.RULE_NAME, "UNKNOWN_RULE");
    const column = asString(row.COLUMN_NAME, "<TABLE>");
    const pattern = asString(row.FAILURE_REASON, "CHECK_STATUS_FAILED");
    const invalid = asNumber(row.INVALID_RECORDS);
    const total = asNumber(row.TOTAL_RECORDS);

    const ruleEntry = byRule.get(rule) || { failures: 0, failedRecords: 0, totalRecords: 0 };
    ruleEntry.failures += 1;
    ruleEntry.failedRecords += invalid;
    ruleEntry.totalRecords += total;
    byRule.set(rule, ruleEntry);

    const columnEntry = byColumn.get(column) || { failures: 0, failedRecords: 0 };
    columnEntry.failures += 1;
    columnEntry.failedRecords += invalid;
    byColumn.set(column, columnEntry);

    byPattern.set(pattern, (byPattern.get(pattern) || 0) + 1);
  }

  const topFailedChecks = Array.from(byRule.entries())
    .map(([ruleName, value]) => ({
      rule_name: ruleName,
      failures: value.failures,
      failed_records: value.failedRecords,
      failure_rate: value.totalRecords > 0 ? roundTo((value.failedRecords / value.totalRecords) * 100) : 0,
    }))
    .sort((a, b) => b.failures - a.failures || b.failed_records - a.failed_records)
    .slice(0, 5);

  const mostImpactedColumns = Array.from(byColumn.entries())
    .map(([columnName, value]) => ({
      column_name: columnName,
      failures: value.failures,
      failed_records: value.failedRecords,
    }))
    .sort((a, b) => b.failed_records - a.failed_records || b.failures - a.failures)
    .slice(0, 5);

  const topFailurePatterns = Array.from(byPattern.entries())
    .map(([pattern, occurrences]) => ({ pattern, occurrences }))
    .sort((a, b) => b.occurrences - a.occurrences)
    .slice(0, 5);

  return {
    top_failed_checks: topFailedChecks,
    most_impacted_columns: mostImpactedColumns,
    top_failure_patterns: topFailurePatterns,
    anomalies_triggered: anomalies,
  };
}

async function fetchRunDeltaInternal(connection: unknown, runId: string): Promise<RunDeltaPayload> {
  const context = await fetchRunContext(connection, runId);
  if (!context) {
    throw new Error(`Run not found for run_id=${runId}`);
  }

  const currentRows = await fetchCheckRowsByRun(connection, runId);
  const currentStats = aggregateCheckStats(currentRows);

  const previousRunId = await fetchPreviousRunId(connection, context);

  if (!previousRunId) {
    return {
      run_id: runId,
      previous_run_id: null,
      delta_score: null,
      delta_fail_rate: null,
      delta_records: null,
      current_score: currentStats.qualityScore,
      previous_score: null,
      current_fail_rate: currentStats.failureRate,
      previous_fail_rate: null,
      current_records: currentStats.recordsScanned,
      previous_records: null,
    };
  }

  const previousRows = await fetchCheckRowsByRun(connection, previousRunId);
  const previousStats = aggregateCheckStats(previousRows);

  return {
    run_id: runId,
    previous_run_id: previousRunId,
    delta_score: roundTo(currentStats.qualityScore - previousStats.qualityScore),
    delta_fail_rate: roundTo(currentStats.failureRate - previousStats.failureRate),
    delta_records: currentStats.recordsScanned - previousStats.recordsScanned,
    current_score: currentStats.qualityScore,
    previous_score: previousStats.qualityScore,
    current_fail_rate: currentStats.failureRate,
    previous_fail_rate: previousStats.failureRate,
    current_records: currentStats.recordsScanned,
    previous_records: previousStats.recordsScanned,
  };
}

export async function getRunDelta(runId: string): Promise<MetricEnvelope<RunDeltaPayload>> {
  const cacheKey = getConnectionKey("RUN", `delta-${runId}`);
  const cached = cache.get<MetricEnvelope<RunDeltaPayload>>(cacheKey);
  if (cached) return cached;

  const connection = await getConnection();
  const delta = await fetchRunDeltaInternal(connection, runId);
  const response = buildEnvelope("RUN", delta);
  cache.set(cacheKey, response, CacheTTL.KPI_METRICS);
  return response;
}

export async function getRunDimensionBreakdown(runId: string): Promise<MetricEnvelope<{ run_id: string; previous_run_id: string | null; dimensions: DimensionScoreRow[] }>> {
  const cacheKey = getConnectionKey("RUN", `dimensions-${runId}`);
  const cached = cache.get<MetricEnvelope<{ run_id: string; previous_run_id: string | null; dimensions: DimensionScoreRow[] }>>(cacheKey);
  if (cached) return cached;

  const connection = await getConnection();
  const context = await fetchRunContext(connection, runId);
  if (!context) throw new Error(`Run not found for run_id=${runId}`);

  const currentRows = await fetchCheckRowsByRun(connection, runId);
  const previousRunId = await fetchPreviousRunId(connection, context);
  const previousRows = previousRunId ? await fetchCheckRowsByRun(connection, previousRunId) : [];

  const response = buildEnvelope("RUN", {
    run_id: runId,
    previous_run_id: previousRunId,
    dimensions: buildDimensionBreakdown(currentRows, previousRows),
  });

  cache.set(cacheKey, response, CacheTTL.KPI_METRICS);
  return response;
}

export async function getRunFailureSummary(runId: string): Promise<MetricEnvelope<{ run_id: string; summary: FailureSummary }>> {
  const cacheKey = getConnectionKey("RUN", `failures-${runId}`);
  const cached = cache.get<MetricEnvelope<{ run_id: string; summary: FailureSummary }>>(cacheKey);
  if (cached) return cached;

  const connection = await getConnection();
  const currentRows = await fetchCheckRowsByRun(connection, runId);
  const delta = await fetchRunDeltaInternal(connection, runId);

  const context = await fetchRunContext(connection, runId);
  if (!context) throw new Error(`Run not found for run_id=${runId}`);
  const previousRunId = await fetchPreviousRunId(connection, context);
  const previousRows = previousRunId ? await fetchCheckRowsByRun(connection, previousRunId) : [];
  const dimensions = buildDimensionBreakdown(currentRows, previousRows);
  const anomalies = detectRunAnomalies(delta, dimensions);

  const summary = buildFailureSummaryFromRows(currentRows, anomalies);
  const response = buildEnvelope("RUN", {
    run_id: runId,
    summary,
  });

  cache.set(cacheKey, response, CacheTTL.KPI_METRICS);
  return response;
}

export async function getRunMetrics(runId: string): Promise<MetricEnvelope<RunMetricsPayload>> {
  const cacheKey = getConnectionKey("RUN", `metrics-${runId}`);
  const cached = cache.get<MetricEnvelope<RunMetricsPayload>>(cacheKey);
  if (cached) return cached;

  const connection = await getConnection();

  const context = await fetchRunContext(connection, runId);
  if (!context) {
    throw new Error(`Run not found for run_id=${runId}`);
  }

  const checkRows = await fetchCheckRowsByRun(connection, runId);
  const stats = aggregateCheckStats(checkRows);
  const delta = await fetchRunDeltaInternal(connection, runId);
  const previousRows = delta.previous_run_id ? await fetchCheckRowsByRun(connection, delta.previous_run_id) : [];
  const dimensionBreakdown = buildDimensionBreakdown(checkRows, previousRows);
  const anomalies = detectRunAnomalies(delta, dimensionBreakdown);

  const [sparkline, stabilityStats] = await Promise.all([
    fetchSparkline(connection, context.datasetId, 7),
    fetchStabilityStats(connection, context.datasetId),
  ]);

  const payload: RunMetricsPayload = {
    run_id: context.runId,
    dataset_id: context.datasetId,
    run_type: context.runType,
    execution_timestamp: context.startTs,
    execution_duration: roundTo(context.durationSeconds),
    status: context.runStatus,
    records_scanned: stats.recordsScanned,
    records_passed: stats.recordsPassed,
    records_failed: stats.recordsFailed,
    failure_rate: stats.failureRate,
    checks_executed: stats.checksExecuted,
    quality_score: stats.qualityScore,
    dimension_scores: stats.dimensionScores,
    check_results: {
      passed: stats.passedChecks,
      failed: stats.failedChecks,
      warning: stats.warningChecks,
      error: stats.errorChecks,
      skipped: stats.skippedChecks,
    },
    anomaly_flags: anomalies.map((entry) => entry.code),
    sparkline_7d: sparkline,
    stability_index: stabilityStats.stabilityIndex,
    volatility_score: stabilityStats.volatilityScore,
    anomaly_frequency: stabilityStats.anomalyFrequency,
  };

  const response = buildEnvelope("RUN", payload);
  cache.set(cacheKey, response, CacheTTL.KPI_METRICS);
  return response;
}

async function fetchDateRunContext(connection: unknown, datasetId: string, date: string): Promise<{ runCount: number; latestRunId: string | null }> {
  const rows = await queryRows(
    connection,
    `
      SELECT
        COUNT(DISTINCT RUN_ID) AS RUN_COUNT,
        MAX_BY(RUN_ID, CHECK_TIMESTAMP) AS LATEST_RUN_ID
      FROM DATA_QUALITY_DB.DQ_METRICS.DQ_CHECK_RESULTS
      WHERE DATASET_ID = ?
        AND DATE_TRUNC('DAY', CHECK_TIMESTAMP) = ?::DATE
    `,
    [datasetId, date]
  );

  return {
    runCount: asNumber(rows[0]?.RUN_COUNT),
    latestRunId: rows[0]?.LATEST_RUN_ID ? asString(rows[0].LATEST_RUN_ID) : null,
  };
}

export async function getDateMetrics(datasetId: string, date: string): Promise<MetricEnvelope<DateMetricsPayload>> {
  const cacheKey = getConnectionKey("DATE", `${datasetId}-${date}`);
  const cached = cache.get<MetricEnvelope<DateMetricsPayload>>(cacheKey);
  if (cached) return cached;

  const connection = await getConnection();

  const [checkRows, runContext, sparkline, stabilityStats] = await Promise.all([
    fetchCheckRowsByDate(connection, datasetId, date),
    fetchDateRunContext(connection, datasetId, date),
    fetchSparkline(connection, datasetId, 7),
    fetchStabilityStats(connection, datasetId),
  ]);

  const stats = aggregateCheckStats(checkRows);
  const dimensionBreakdown = buildDimensionBreakdown(checkRows, []);
  const failureSummary = buildFailureSummaryFromRows(checkRows, []);

  const payload: DateMetricsPayload = {
    dataset_id: datasetId,
    date,
    run_count: runContext.runCount,
    latest_run_id: runContext.latestRunId,
    total_records_scanned_date: stats.recordsScanned,
    total_failed_date: stats.recordsFailed,
    avg_score_date: stats.qualityScore,
    failure_rate_date: stats.failureRate,
    checks_executed_date: stats.checksExecuted,
    sparkline_7d: sparkline,
    stability_index: stabilityStats.stabilityIndex,
    volatility_score: stabilityStats.volatilityScore,
    anomaly_frequency: stabilityStats.anomalyFrequency,
    dimension_breakdown: dimensionBreakdown,
    failure_summary: failureSummary,
  };

  const response = buildEnvelope("DATE", payload);
  cache.set(cacheKey, response, CacheTTL.KPI_METRICS);
  return response;
}

async function fetchDatasetRollup(connection: unknown, datasetId: string): Promise<{
  totalRuns: number;
  recordsScanned: number;
  recordsFailed: number;
  weightedScore: number;
  lastRunId: string | null;
  lastRunTs: string | null;
}> {
  const [summaryRows, latestRows, runCountRows] = await Promise.all([
    queryRows(
      connection,
      `
        SELECT
          COALESCE(SUM(TOTAL_RECORDS), 0) AS RECORDS_SCANNED,
          COALESCE(SUM(FAILED_RECORDS_COUNT), 0) AS RECORDS_FAILED,
          COALESCE(AVG(DQ_SCORE), 0) AS WEIGHTED_SCORE
        FROM DATA_QUALITY_DB.DQ_METRICS.DQ_DAILY_SUMMARY
        WHERE DATASET_ID = ?
      `,
      [datasetId]
    ),
    queryRows(
      connection,
      `
        SELECT
          rc.RUN_ID,
          rc.START_TS
        FROM DATA_QUALITY_DB.DQ_METRICS.DQ_RUN_CONTROL rc
        JOIN DATA_QUALITY_DB.DQ_METRICS.DQ_CHECK_RESULTS cr
          ON rc.RUN_ID = cr.RUN_ID
        WHERE cr.DATASET_ID = ?
        GROUP BY rc.RUN_ID, rc.START_TS
        ORDER BY rc.START_TS DESC
        LIMIT 1
      `,
      [datasetId]
    ),
    queryRows(
      connection,
      `
        SELECT COUNT(DISTINCT RUN_ID) AS TOTAL_RUNS
        FROM DATA_QUALITY_DB.DQ_METRICS.DQ_CHECK_RESULTS
        WHERE DATASET_ID = ?
      `,
      [datasetId]
    ),
  ]);

  return {
    totalRuns: asNumber(runCountRows[0]?.TOTAL_RUNS),
    recordsScanned: asNumber(summaryRows[0]?.RECORDS_SCANNED),
    recordsFailed: asNumber(summaryRows[0]?.RECORDS_FAILED),
    weightedScore: roundTo(asNumber(summaryRows[0]?.WEIGHTED_SCORE)),
    lastRunId: latestRows[0]?.RUN_ID ? asString(latestRows[0].RUN_ID) : null,
    lastRunTs: toIso(latestRows[0]?.START_TS),
  };
}

function buildDatasetDimensionBreakdown(checkRows: CheckRow[]): DimensionScoreRow[] {
  const breakdown = buildDimensionBreakdown(checkRows, []);
  return breakdown;
}

export async function getDatasetMetrics(datasetId: string): Promise<MetricEnvelope<DatasetMetricsPayload>> {
  const cacheKey = getConnectionKey("DATASET", datasetId);
  const cached = cache.get<MetricEnvelope<DatasetMetricsPayload>>(cacheKey);
  if (cached) return cached;

  const connection = await getConnection();

  const [rollup, checkRows, sparkline, stabilityStats] = await Promise.all([
    fetchDatasetRollup(connection, datasetId),
    fetchCheckRowsByDataset(connection, datasetId),
    fetchSparkline(connection, datasetId, 30),
    fetchStabilityStats(connection, datasetId),
  ]);

  const failureRate = rollup.recordsScanned > 0
    ? roundTo((rollup.recordsFailed / rollup.recordsScanned) * 100)
    : 0;

  const dimensionBreakdown = buildDatasetDimensionBreakdown(checkRows);
  const anomalies = stabilityStats.anomalyCountTotal > 0
    ? [{
      code: "LIFETIME_VOLATILITY",
      severity: stabilityStats.volatilityScore >= 10 ? "HIGH" as const : "MEDIUM" as const,
      description: `Dataset lifetime volatility score is ${stabilityStats.volatilityScore}`,
      value: stabilityStats.volatilityScore,
    }]
    : [];
  const failureSummary = buildFailureSummaryFromRows(checkRows, anomalies);

  const payload: DatasetMetricsPayload = {
    dataset_id: datasetId,
    total_runs: rollup.totalRuns,
    lifetime_records_scanned: rollup.recordsScanned,
    lifetime_records_failed: rollup.recordsFailed,
    lifetime_failure_rate: failureRate,
    lifetime_checks_executed: checkRows.length,
    stability_index: stabilityStats.stabilityIndex,
    volatility_score: stabilityStats.volatilityScore,
    anomaly_count_total: stabilityStats.anomalyCountTotal,
    anomaly_frequency: stabilityStats.anomalyFrequency,
    weighted_lifetime_score: rollup.weightedScore,
    last_run_id: rollup.lastRunId,
    last_run_timestamp: rollup.lastRunTs,
    sparkline_30d: sparkline,
    dimension_breakdown: dimensionBreakdown,
    failure_summary: failureSummary,
  };

  const response = buildEnvelope("DATASET", payload);
  cache.set(cacheKey, response, CacheTTL.REFERENCE_DATA);
  return response;
}








