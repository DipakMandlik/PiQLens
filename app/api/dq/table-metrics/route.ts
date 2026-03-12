import { NextRequest, NextResponse } from 'next/server';
import { snowflakePool, executeQueryObjects, ensureConnectionContext } from '@/lib/snowflake';
import { getServerConfig } from '@/lib/server-config';
import { logger } from '@/lib/logger';
import { createErrorResponse } from '@/lib/errors';
import { getOrSetCache, buildCacheKey } from '@/lib/valkey';
import { DataViewMode, normalizeMode } from '@/lib/dq/data-view-mode';

const STATUS_MESSAGES = {
  noTableRun: 'No run executed for selected table.',
  noDataToday: 'No data inserted on selected date.',
  noFullRunForDate: 'No full run on selected date. Displaying latest available full run.',
} as const;

function clampPercent(value: number): number {
  return Number(Math.max(0, Math.min(100, value)).toFixed(2));
}

function normalizePercent(value: number | null | undefined): number | null {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return null;
  const n = Number(value);
  return n >= 0 && n <= 1 ? clampPercent(n * 100) : clampPercent(n);
}

function formatScore(valid: number, total: number): number {
  if (!total) return 0;
  return clampPercent((valid / total) * 100);
}

function isValidDateLiteral(value: string) {
  return /^[0-9]{4}-[0-9]{2}-[0-9]{2}$/.test(value);
}

function escapeSqlLiteral(value: string) {
  return value.replace(/'/g, "''");
}

function toTimestampString(value: unknown): string | null {
  if (value === null || value === undefined) return null;
  if (typeof value === 'string') return value;
  const dt = new Date(value as string | number | Date);
  if (Number.isNaN(dt.getTime())) return String(value);
  return dt.toISOString();
}

function toDateOnly(value: unknown): string | null {
  if (value === null || value === undefined) return null;
  if (typeof value === 'string') {
    const matched = value.match(/^\d{4}-\d{2}-\d{2}/);
    if (matched) return matched[0];
  }
  const dt = new Date(value as string | number | Date);
  if (Number.isNaN(dt.getTime())) return null;
  return dt.toISOString().slice(0, 10);
}

type RunRef = {
  run_id: string;
  start_ts: string | null;
  run_type: string;
};

type AggregatedMetrics = {
  totalRecords: number;
  validRecords: number;
  invalidRecords: number;
  totalChecks: number;
  avgPassRate: number | null;
};

function buildNoDataPayload(table: string, requestedDate: string | null, mode: DataViewMode, statusMessage: string) {
  return {
    table,
    date: requestedDate,
    resolvedDate: requestedDate,
    mode,
    scope: mode,
    runType: null,
    runId: null,
    runTimestamp: null,
    recordsEvaluated: 0,
    validRecords: 0,
    invalidRecords: 0,
    failedRecords: 0,
    rulesExecuted: 0,
    qualityScore: 0,
    status: 'no_data',
    statusMessage,
    isFallbackRun: false,
  };
}

function buildReadyPayload(args: {
  table: string;
  mode: DataViewMode;
  requestedDate: string | null;
  resolvedDate: string;
  run: RunRef;
  metrics: AggregatedMetrics;
  isFallbackRun: boolean;
  dimensionScores?: { [key: string]: number } | null;
  qualityScoreOverride?: number | null;
}) {
  const { table, mode, requestedDate, resolvedDate, run, metrics, isFallbackRun, dimensionScores, qualityScoreOverride } = args;

  let qualityScore =
    typeof qualityScoreOverride === 'number' && !Number.isNaN(qualityScoreOverride)
      ? clampPercent(qualityScoreOverride)
      : (metrics.avgPassRate ?? formatScore(metrics.validRecords, metrics.totalRecords));

  if (dimensionScores && Object.keys(dimensionScores).length > 0) {
    const scores = Object.values(dimensionScores).map((v) => normalizePercent(v)).filter((v): v is number => typeof v === 'number');
    if (scores.length > 0) {
      qualityScore = clampPercent(scores.reduce((a, b) => a + b, 0) / scores.length);
    }
  }

  return {
    table,
    date: resolvedDate,
    requestedDate,
    resolvedDate,
    mode,
    scope: mode,
    runType: run.run_type,
    runId: run.run_id,
    runTimestamp: run.start_ts,
    recordsEvaluated: metrics.totalRecords,
    validRecords: metrics.validRecords,
    invalidRecords: metrics.invalidRecords,
    failedRecords: metrics.invalidRecords,
    rulesExecuted: metrics.totalChecks,
    qualityScore,
    status: 'ready',
    statusMessage: isFallbackRun ? STATUS_MESSAGES.noFullRunForDate : null,
    isFallbackRun,
  };
}

async function findLatestFullRun(
  connection: unknown,
  tableName: string,
  dateFilter: { date?: string; comparator: '<=' | '=' | 'none' }
): Promise<RunRef | null> {
  const safeTable = escapeSqlLiteral(tableName);
  const dateClause =
    dateFilter.comparator === 'none'
      ? ''
      : `AND DATE_TRUNC('DAY', rc.START_TS) ${dateFilter.comparator} '${dateFilter.date}'::DATE`;

  const query = `
    SELECT rc.RUN_ID, rc.START_TS, COALESCE(rc.RUN_TYPE, 'FULL') AS RUN_TYPE
    FROM DATA_QUALITY_DB.DQ_METRICS.DQ_RUN_CONTROL rc
    WHERE EXISTS (
      SELECT 1
      FROM DATA_QUALITY_DB.DQ_METRICS.DQ_CHECK_RESULTS cr
      WHERE cr.RUN_ID = rc.RUN_ID
        AND UPPER(cr.TABLE_NAME) = UPPER('${safeTable}')
    )
      ${dateClause}
      AND rc.RUN_STATUS IN ('COMPLETED', 'COMPLETED_WITH_FAILURES', 'WARNING')
      AND (rc.RUN_TYPE IS NULL OR UPPER(rc.RUN_TYPE) = 'FULL')
    ORDER BY DATE_TRUNC('DAY', rc.START_TS) DESC, rc.START_TS DESC
    LIMIT 1
  `;

  const rows = await executeQueryObjects(connection, query) as Array<{ RUN_ID: string; START_TS: unknown; RUN_TYPE: string }>;
  if (!rows.length) return null;

  return {
    run_id: rows[0].RUN_ID,
    start_ts: toTimestampString(rows[0].START_TS),
    run_type: rows[0].RUN_TYPE || 'FULL',
  };
}

async function findLatestRunOnDate(connection: unknown, tableName: string, date: string): Promise<RunRef | null> {
  const safeTable = escapeSqlLiteral(tableName);
  const query = `
    SELECT
      rc.RUN_ID,
      MAX(rc.START_TS) AS START_TS,
      COALESCE(MAX(rc.RUN_TYPE), 'UNKNOWN') AS RUN_TYPE
    FROM DATA_QUALITY_DB.DQ_METRICS.DQ_RUN_CONTROL rc
    JOIN DATA_QUALITY_DB.DQ_METRICS.DQ_CHECK_RESULTS cr ON cr.RUN_ID = rc.RUN_ID
    WHERE UPPER(cr.TABLE_NAME) = UPPER('${safeTable}')
      AND DATE_TRUNC('DAY', rc.START_TS) = '${date}'::DATE
      AND rc.RUN_STATUS IN ('COMPLETED', 'COMPLETED_WITH_FAILURES', 'WARNING')
    GROUP BY rc.RUN_ID
    ORDER BY MAX(rc.START_TS) DESC
    LIMIT 1
  `;

  const rows = await executeQueryObjects(connection, query) as Array<{ RUN_ID: string; START_TS: unknown; RUN_TYPE: string }>;
  if (!rows.length) return null;

  return {
    run_id: rows[0].RUN_ID,
    start_ts: toTimestampString(rows[0].START_TS),
    run_type: rows[0].RUN_TYPE || 'UNKNOWN',
  };
}

async function resolveLatestDateForMode(connection: unknown, tableName: string, mode: DataViewMode): Promise<string | null> {
  const safeTable = escapeSqlLiteral(tableName);

  const filter =
    mode === 'TABLE'
      ? "AND (rc.RUN_TYPE IS NULL OR UPPER(rc.RUN_TYPE) = 'FULL')"
      : '';

  const query = `
    SELECT TO_CHAR(MAX(DATE_TRUNC('DAY', rc.START_TS)), 'YYYY-MM-DD') AS TARGET_DATE
    FROM DATA_QUALITY_DB.DQ_METRICS.DQ_RUN_CONTROL rc
    WHERE EXISTS (
      SELECT 1
      FROM DATA_QUALITY_DB.DQ_METRICS.DQ_CHECK_RESULTS cr
      WHERE cr.RUN_ID = rc.RUN_ID
        AND UPPER(cr.TABLE_NAME) = UPPER('${safeTable}')
    )
      AND rc.RUN_STATUS IN ('COMPLETED', 'COMPLETED_WITH_FAILURES', 'WARNING')
      ${filter}
  `;

  const rows = await executeQueryObjects(connection, query) as Array<{ TARGET_DATE?: string }>;
  return rows[0]?.TARGET_DATE || null;
}

async function aggregateMetricsForRun(connection: unknown, runId: string, tableName: string): Promise<AggregatedMetrics> {
  const safeRunId = escapeSqlLiteral(runId);
  const safeTable = escapeSqlLiteral(tableName);

  const query = `
    SELECT
      COALESCE(MAX(TOTAL_RECORDS), 0) AS TOTAL_RECORDS,
      COALESCE(SUM(INVALID_RECORDS), 0) AS INVALID_RECORDS,
      COUNT(*) AS TOTAL_CHECKS,
      AVG(CASE WHEN PASS_RATE IS NOT NULL THEN PASS_RATE END) AS AVG_PASS_RATE
    FROM DATA_QUALITY_DB.DQ_METRICS.DQ_CHECK_RESULTS
    WHERE RUN_ID = '${safeRunId}'
      AND UPPER(TABLE_NAME) = UPPER('${safeTable}')
      AND UPPER(CHECK_STATUS) NOT IN ('SKIPPED', 'ERROR')
  `;

  const rows = await executeQueryObjects(connection, query) as Array<{
    TOTAL_RECORDS?: number;
    INVALID_RECORDS?: number;
    TOTAL_CHECKS?: number;
    AVG_PASS_RATE?: number;
  }>;

  const totalRecords = Number(rows[0]?.TOTAL_RECORDS || 0);
  const invalidRecords = Number(rows[0]?.INVALID_RECORDS || 0);

  return {
    totalRecords,
    validRecords: Math.max(0, totalRecords - invalidRecords),
    invalidRecords,
    totalChecks: Number(rows[0]?.TOTAL_CHECKS || 0),
    avgPassRate: normalizePercent(rows[0]?.AVG_PASS_RATE ?? null),
  };
}

async function aggregateMetricsForDate(connection: unknown, tableName: string, date: string): Promise<AggregatedMetrics> {
  const safeTable = escapeSqlLiteral(tableName);

  const query = `
    WITH run_rollup AS (
      SELECT
        rc.RUN_ID,
        MAX(COALESCE(cr.TOTAL_RECORDS, 0)) AS RUN_TOTAL_RECORDS,
        SUM(COALESCE(cr.INVALID_RECORDS, 0)) AS RUN_INVALID_RECORDS,
        COUNT(*) AS RUN_TOTAL_CHECKS,
        AVG(CASE WHEN cr.PASS_RATE IS NOT NULL THEN cr.PASS_RATE END) AS RUN_AVG_PASS_RATE
      FROM DATA_QUALITY_DB.DQ_METRICS.DQ_CHECK_RESULTS cr
      JOIN DATA_QUALITY_DB.DQ_METRICS.DQ_RUN_CONTROL rc ON cr.RUN_ID = rc.RUN_ID
      WHERE UPPER(cr.TABLE_NAME) = UPPER('${safeTable}')
        AND DATE_TRUNC('DAY', rc.START_TS) = '${date}'::DATE
        AND rc.RUN_STATUS IN ('COMPLETED', 'COMPLETED_WITH_FAILURES', 'WARNING')
        AND UPPER(COALESCE(cr.CHECK_STATUS, '')) NOT IN ('SKIPPED', 'ERROR')
      GROUP BY rc.RUN_ID
    )
    SELECT
      COALESCE(SUM(RUN_TOTAL_RECORDS), 0) AS TOTAL_RECORDS,
      COALESCE(SUM(RUN_INVALID_RECORDS), 0) AS INVALID_RECORDS,
      COALESCE(SUM(RUN_TOTAL_CHECKS), 0) AS TOTAL_CHECKS,
      CASE
        WHEN COALESCE(SUM(RUN_TOTAL_CHECKS), 0) = 0 THEN NULL
        ELSE SUM(COALESCE(RUN_AVG_PASS_RATE, 0) * RUN_TOTAL_CHECKS) / SUM(RUN_TOTAL_CHECKS)
      END AS AVG_PASS_RATE
    FROM run_rollup
  `;

  const rows = await executeQueryObjects(connection, query) as Array<{
    TOTAL_RECORDS?: number;
    INVALID_RECORDS?: number;
    TOTAL_CHECKS?: number;
    AVG_PASS_RATE?: number;
  }>;

  const totalRecords = Number(rows[0]?.TOTAL_RECORDS || 0);
  const invalidRecords = Number(rows[0]?.INVALID_RECORDS || 0);

  return {
    totalRecords,
    validRecords: Math.max(0, totalRecords - invalidRecords),
    invalidRecords,
    totalChecks: Number(rows[0]?.TOTAL_CHECKS || 0),
    avgPassRate: normalizePercent(rows[0]?.AVG_PASS_RATE ?? null),
  };
}

async function getDimensionScores(connection: unknown, runId: string): Promise<{ [key: string]: number } | null> {
  const safeRunId = escapeSqlLiteral(runId);

  const query = `
    SELECT
      DIMENSION_NAME,
      SCORE
    FROM DATA_QUALITY_DB.DQ_METRICS.DQ_DIMENSION_SCORES
    WHERE RUN_ID = '${safeRunId}'
  `;

  try {
    const rows = await executeQueryObjects(connection, query) as Array<{
      DIMENSION_NAME: string;
      SCORE: number;
    }>;

    if (!rows || rows.length === 0) {
      return null;
    }

    const scores: { [key: string]: number } = {};
    rows.forEach((row) => {
      scores[row.DIMENSION_NAME] = Number(row.SCORE) || 0;
    });

    return scores;
  } catch {
    return null;
  }
}

export async function GET(request: NextRequest) {
  const startTime = Date.now();
  const { searchParams } = new URL(request.url);
  const tableName = (searchParams.get('table') || '').trim();
  const mode = normalizeMode(searchParams.get('mode') || searchParams.get('scope') || 'TABLE');
  const dateParam = (searchParams.get('date') || '').trim() || null;

  try {
    logger.logApiRequest('/api/dq/table-metrics', 'GET');

    if (!tableName) {
      return NextResponse.json({ success: false, error: 'Missing table parameter' }, { status: 400 });
    }

    if (dateParam && !isValidDateLiteral(dateParam)) {
      return NextResponse.json({ success: false, error: 'Invalid date format. Use YYYY-MM-DD.' }, { status: 400 });
    }

    const valkeyKey = buildCacheKey('dq', 'table-metrics', `${tableName}:${mode}:${dateParam || 'latest'}`);

    const payload = await getOrSetCache(valkeyKey, 600, async () => {
      const config = getServerConfig();
      if (!config) {
        throw new Error('AUTH_FAILED: Not connected to Snowflake');
      }

      const connection = await snowflakePool.getConnection(config);
      await ensureConnectionContext(connection, config);

      let requestedDate = dateParam;
      if (!requestedDate) {
        requestedDate = await resolveLatestDateForMode(connection, tableName, mode);
      }

      if (!requestedDate) {
        return buildNoDataPayload(tableName, null, mode, STATUS_MESSAGES.noTableRun);
      }

      if (mode === 'TABLE') {
        const run = await findLatestFullRun(connection, tableName, {
          date: requestedDate,
          comparator: '<=',
        });

        if (!run) {
          return buildNoDataPayload(tableName, requestedDate, mode, STATUS_MESSAGES.noTableRun);
        }

        const resolvedDate = toDateOnly(run.start_ts) || requestedDate;
        const metrics = await aggregateMetricsForRun(connection, run.run_id, tableName);
        const dimensionScores = await getDimensionScores(connection, run.run_id);

        return buildReadyPayload({
          table: tableName,
          mode,
          requestedDate,
          resolvedDate,
          run,
          metrics,
          isFallbackRun: resolvedDate !== requestedDate,
          dimensionScores,
        });
      }

      if (mode === 'TODAY') {
        const run = await findLatestRunOnDate(connection, tableName, requestedDate);
        if (!run) {
          return buildNoDataPayload(tableName, requestedDate, mode, STATUS_MESSAGES.noDataToday);
        }

        const metrics = await aggregateMetricsForDate(connection, tableName, requestedDate);
        return buildReadyPayload({
          table: tableName,
          mode,
          requestedDate,
          resolvedDate: requestedDate,
          run,
          metrics,
          isFallbackRun: false,
          qualityScoreOverride: metrics.avgPassRate,
        });
      }

      throw new Error('UNSUPPORTED_MODE: Unsupported mode. Use TABLE or TODAY.');
    });

    logger.logApiResponse('/api/dq/table-metrics', true, Date.now() - startTime);
    return NextResponse.json({ success: true, data: payload, metadata: { cached: true, timestamp: new Date().toISOString() } });

  } catch (error: any) {
    logger.error('Error in table-metrics', error);

    if (error.message?.includes('AUTH_FAILED')) {
      return NextResponse.json(
        { success: false, error: 'Not connected to Snowflake' },
        { status: 401 }
      );
    }

    if (error.message?.includes('UNSUPPORTED_MODE')) {
      return NextResponse.json({ success: false, error: 'Unsupported mode. Use TABLE or TODAY.' }, { status: 400 });
    }

    return NextResponse.json(createErrorResponse(error), { status: 500 });
  }
}
