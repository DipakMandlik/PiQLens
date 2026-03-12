import { NextRequest, NextResponse } from 'next/server';
import { snowflakePool, executeQueryObjects, ensureConnectionContext } from '@/lib/snowflake';
import { getServerConfig } from '@/lib/server-config';
import { createErrorResponse } from '@/lib/errors';
import { logger } from '@/lib/logger';
import { retryQuery } from '@/lib/retry';
import { cache, CacheTTL, generateCacheKey } from '@/lib/cache';
import { DataViewMode, normalizeMode } from '@/lib/dq/data-view-mode';

function sanitizeIdentifier(value: string): string | null {
  const normalized = value.trim().toUpperCase();
  return /^[A-Z0-9_]+$/.test(normalized) ? normalized : null;
}

function isValidDateLiteral(value: string) {
  return /^\d{4}-\d{2}-\d{2}$/.test(value);
}

function escapeSqlLiteral(value: string) {
  return value.replace(/'/g, "''");
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

function toTimestampString(value: unknown): string | null {
  if (value === null || value === undefined) return null;
  if (typeof value === 'string') return value;
  const dt = new Date(value as string | number | Date);
  if (Number.isNaN(dt.getTime())) return String(value);
  return dt.toISOString();
}

type RunRef = {
  run_id: string;
  run_type: string;
  start_ts: string | null;
};

type Aggregate = {
  total_records: number;
  valid_records: number;
  invalid_records: number;
  total_checks: number;
  run_count: number;
};

function score(valid: number, total: number): number {
  if (!total) return 0;
  return Number(((valid / total) * 100).toFixed(2));
}

async function findLatestFullRunOnOrBefore(connection: unknown, table: string, date: string): Promise<RunRef | null> {
  const safeTable = escapeSqlLiteral(table);
  const query = `
    SELECT
      rc.RUN_ID,
      COALESCE(MAX(rc.RUN_TYPE), 'FULL') AS RUN_TYPE,
      MAX(rc.START_TS) AS START_TS
    FROM DATA_QUALITY_DB.DQ_METRICS.DQ_RUN_CONTROL rc
    JOIN DATA_QUALITY_DB.DQ_METRICS.DQ_CHECK_RESULTS cr ON cr.RUN_ID = rc.RUN_ID
    WHERE UPPER(cr.TABLE_NAME) = UPPER('${safeTable}')
      AND DATE_TRUNC('DAY', rc.START_TS) <= '${date}'::DATE
      AND rc.RUN_STATUS IN ('COMPLETED', 'COMPLETED_WITH_FAILURES', 'WARNING')
      AND (rc.RUN_TYPE IS NULL OR UPPER(rc.RUN_TYPE) = 'FULL')
    GROUP BY rc.RUN_ID
    ORDER BY MAX(rc.START_TS) DESC
    LIMIT 1
  `;

  const rows = await executeQueryObjects(connection, query) as Array<{ RUN_ID: string; RUN_TYPE: string; START_TS: unknown }>;
  if (!rows.length) return null;

  return {
    run_id: rows[0].RUN_ID,
    run_type: rows[0].RUN_TYPE || 'FULL',
    start_ts: toTimestampString(rows[0].START_TS),
  };
}

async function findLatestDateForTableMode(connection: unknown, table: string, date: string): Promise<string | null> {
  const safeTable = escapeSqlLiteral(table);

  const query = `
    SELECT TO_CHAR(MAX(DATE_TRUNC('DAY', rc.START_TS)), 'YYYY-MM-DD') AS TARGET_DATE
    FROM DATA_QUALITY_DB.DQ_METRICS.DQ_RUN_CONTROL rc
    JOIN DATA_QUALITY_DB.DQ_METRICS.DQ_CHECK_RESULTS cr ON cr.RUN_ID = rc.RUN_ID
    WHERE UPPER(cr.TABLE_NAME) = UPPER('${safeTable}')
      AND DATE_TRUNC('DAY', rc.START_TS) <= '${date}'::DATE
      AND rc.RUN_STATUS IN ('COMPLETED', 'COMPLETED_WITH_FAILURES', 'WARNING')
      AND (rc.RUN_TYPE IS NULL OR UPPER(rc.RUN_TYPE) = 'FULL')
  `;

  const rows = await executeQueryObjects(connection, query) as Array<{ TARGET_DATE?: string }>;
  return rows[0]?.TARGET_DATE || null;
}

async function aggregateForRun(connection: unknown, table: string, runId: string): Promise<Aggregate> {
  const safeTable = escapeSqlLiteral(table);
  const safeRunId = escapeSqlLiteral(runId);

  const query = `
    SELECT
      COALESCE(MAX(cr.TOTAL_RECORDS), 0) AS TOTAL_RECORDS,
      COALESCE(SUM(cr.INVALID_RECORDS), 0) AS INVALID_RECORDS,
      COUNT(*) AS TOTAL_CHECKS
    FROM DATA_QUALITY_DB.DQ_METRICS.DQ_CHECK_RESULTS cr
    WHERE cr.RUN_ID = '${safeRunId}'
      AND UPPER(cr.TABLE_NAME) = UPPER('${safeTable}')
      AND UPPER(COALESCE(cr.CHECK_STATUS, '')) NOT IN ('SKIPPED', 'ERROR')
  `;

  const rows = await executeQueryObjects(connection, query) as Array<{
    TOTAL_RECORDS?: number;
    INVALID_RECORDS?: number;
    TOTAL_CHECKS?: number;
  }>;

  const totalRecords = Number(rows[0]?.TOTAL_RECORDS || 0);
  const invalidRecords = Number(rows[0]?.INVALID_RECORDS || 0);

  return {
    total_records: totalRecords,
    valid_records: Math.max(0, totalRecords - invalidRecords),
    invalid_records: invalidRecords,
    total_checks: Number(rows[0]?.TOTAL_CHECKS || 0),
    run_count: rows.length > 0 ? 1 : 0,
  };
}

async function aggregateForDate(connection: unknown, table: string, date: string): Promise<Aggregate> {
  const safeTable = escapeSqlLiteral(table);
  const query = `
    WITH run_rollup AS (
      SELECT
        rc.RUN_ID,
        MAX(COALESCE(cr.TOTAL_RECORDS, 0)) AS RUN_TOTAL_RECORDS,
        SUM(COALESCE(cr.INVALID_RECORDS, 0)) AS RUN_INVALID_RECORDS,
        COUNT(*) AS RUN_TOTAL_CHECKS
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
      COUNT(*) AS RUN_COUNT
    FROM run_rollup
  `;

  const rows = await executeQueryObjects(connection, query) as Array<{
    TOTAL_RECORDS?: number;
    INVALID_RECORDS?: number;
    TOTAL_CHECKS?: number;
    RUN_COUNT?: number;
  }>;

  const totalRecords = Number(rows[0]?.TOTAL_RECORDS || 0);
  const invalidRecords = Number(rows[0]?.INVALID_RECORDS || 0);

  return {
    total_records: totalRecords,
    valid_records: Math.max(0, totalRecords - invalidRecords),
    invalid_records: invalidRecords,
    total_checks: Number(rows[0]?.TOTAL_CHECKS || 0),
    run_count: Number(rows[0]?.RUN_COUNT || 0),
  };
}

async function fetchTableRowCount(connection: unknown, safeDatabase: string, schema: string, table: string): Promise<number> {
  const query = `
    SELECT COALESCE(ROW_COUNT, 0) AS ROW_COUNT
    FROM ${safeDatabase}.INFORMATION_SCHEMA.TABLES
    WHERE TABLE_SCHEMA = ?
      AND TABLE_NAME = ?
    LIMIT 1
  `;

  const rows = await executeQueryObjects(connection, query, [schema.toUpperCase(), table.toUpperCase()]) as Array<{ ROW_COUNT?: number }>;
  return Number(rows[0]?.ROW_COUNT || 0);
}

export async function GET(request: NextRequest) {
  const startTime = Date.now();
  const { searchParams } = new URL(request.url);
  const database = searchParams.get('database');
  const schema = searchParams.get('schema');
  const table = searchParams.get('table');
  const dateParam = searchParams.get('date');
  const mode = normalizeMode(searchParams.get('mode') || searchParams.get('scope') || 'TABLE');

  const endpoint = `/api/metrics/table-summary?database=${database || ''}&schema=${schema || ''}&table=${table || ''}&date=${dateParam || 'default'}&mode=${mode}`;

  if (!database || !schema || !table) {
    return NextResponse.json(
      { success: false, error: 'Missing required parameters: database, schema, table' },
      { status: 400 }
    );
  }

  if (dateParam && !isValidDateLiteral(dateParam)) {
    return NextResponse.json(
      { success: false, error: 'Invalid date format. Use YYYY-MM-DD.' },
      { status: 400 }
    );
  }

  const safeDatabase = sanitizeIdentifier(database);
  if (!safeDatabase) {
    return NextResponse.json(
      { success: false, error: 'Invalid database identifier.' },
      { status: 400 }
    );
  }

  try {
    logger.logApiRequest(endpoint, 'GET');

    const cacheKey = generateCacheKey(endpoint);
    const cached = cache.get(cacheKey);
    if (cached) {
      logger.logApiResponse(endpoint, true, Date.now() - startTime);
      return NextResponse.json({
        success: true,
        data: cached,
        metadata: { cached: true, timestamp: new Date().toISOString() },
      });
    }

    const config = getServerConfig();
    if (!config) {
      return NextResponse.json(
        { success: false, error: 'Not connected to Snowflake' },
        { status: 401 }
      );
    }

    const connection = await snowflakePool.getConnection(config);
    await ensureConnectionContext(connection, config);

    const payload = await retryQuery(async () => {
      const todayDate = new Date().toISOString().slice(0, 10);
      const requestedDate = dateParam || todayDate;

      const totalTableRecords = await fetchTableRowCount(connection, safeDatabase, schema, table);
      const todayAggregate = await aggregateForDate(connection, table, requestedDate);


      const resolvedTableDate = await findLatestDateForTableMode(connection, table, requestedDate);
      const tableRun = resolvedTableDate
        ? await findLatestFullRunOnOrBefore(connection, table, requestedDate)
        : null;
      const tableAggregate = tableRun
        ? await aggregateForRun(connection, table, tableRun.run_id)
        : {
            total_records: 0,
            valid_records: 0,
            invalid_records: 0,
            total_checks: 0,
            run_count: 0,
          };

      const activeByMode: Record<DataViewMode, { hasData: boolean; aggregate: Aggregate; resolvedDate: string | null }> = {
        TABLE: {
          hasData: Boolean(tableRun),
          aggregate: tableAggregate,
          resolvedDate: toDateOnly(tableRun?.start_ts) || resolvedTableDate || null,
        },
        TODAY: {
          hasData: todayAggregate.run_count > 0,
          aggregate: todayAggregate,
          resolvedDate: requestedDate,
        },
      };

      const active = activeByMode[mode];

      const activeModeValue = mode === 'TABLE'
        ? totalTableRecords
        : todayAggregate.total_records;

      return {
        mode,
        scope: mode,
        has_data: active.hasData,
        resolved_date: active.resolvedDate,
        requested_date: requestedDate,
        total_table_records: totalTableRecords,
        total_today_records: todayAggregate.total_records,
        active_mode_value: activeModeValue,
        total_failed: active.aggregate.invalid_records,
        total_checks: active.aggregate.total_checks,
        aggregated_score: score(active.aggregate.valid_records, active.aggregate.total_records),
        run_count: active.aggregate.run_count,
      };
    }, 'metrics-table-summary');

    cache.set(cacheKey, payload, CacheTTL.KPI_METRICS);
    logger.logApiResponse(endpoint, true, Date.now() - startTime);

    return NextResponse.json({
      success: true,
      data: payload,
      metadata: { cached: false, timestamp: new Date().toISOString() },
    });
  } catch (error: unknown) {
    logger.error('Error in table-summary metrics API', error, { endpoint });
    return NextResponse.json(createErrorResponse(error), { status: 500 });
  }
}


