/* eslint-disable @typescript-eslint/no-explicit-any */
import { NextRequest, NextResponse } from "next/server";
import { getServerConfig } from "@/lib/server-config";
import { snowflakePool } from "@/lib/snowflake";
import { logger } from "@/lib/logger";

export const dynamic = "force-dynamic";
export const revalidate = 0;

type ExecutionTimestampColumnMap = {
  startedAt: string;
  finishedAt: string;
};

const executionTimestampColumnMapCache = new Map<string, ExecutionTimestampColumnMap>();

function resolveExecutionTimestampColumn(columns: Set<string>, modern: string, legacy: string): string {
  if (columns.has(modern)) return modern;
  if (columns.has(legacy)) return legacy;
  throw new Error(
    `DQ_SCHEDULE_EXECUTION missing required column ${modern} (or legacy ${legacy}). `
    + `Run sql/production/17_Native_Task_Scheduler_Cutover.sql to align execution journal schema.`
  );
}

async function getExecutionTimestampColumnMap(conn: any, dqDatabase: string): Promise<ExecutionTimestampColumnMap> {
  const cacheKey = dqDatabase.toUpperCase();
  const cached = executionTimestampColumnMapCache.get(cacheKey);
  if (cached) return cached;

  const rows = await new Promise<any[]>((resolve, reject) => {
    conn.execute({
      sqlText: `
        SELECT COLUMN_NAME
        FROM ${dqDatabase}.INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = 'DQ_CONFIG'
          AND TABLE_NAME = 'DQ_SCHEDULE_EXECUTION'
      `,
      complete: (err: any, _stmt: any, resultRows: any) => {
        if (err) reject(err);
        else resolve(resultRows || []);
      },
    });
  });

  const columns = new Set(rows.map((row: any) => String(row.COLUMN_NAME || "").toUpperCase()));
  const map: ExecutionTimestampColumnMap = {
    startedAt: resolveExecutionTimestampColumn(columns, "STARTED_AT", "STARTED_AT_UTC"),
    finishedAt: resolveExecutionTimestampColumn(columns, "FINISHED_AT", "FINISHED_AT_UTC"),
  };

  executionTimestampColumnMapCache.set(cacheKey, map);
  return map;
}

function normalizeExecutionModeFilter(value: string | null): "MANUAL" | "SCHEDULED" | "AUTO" | null {
  const normalized = String(value || "").trim().toUpperCase();
  if (["MANUAL", "SCHEDULED", "AUTO"].includes(normalized)) {
    return normalized as "MANUAL" | "SCHEDULED" | "AUTO";
  }
  return null;
}

function normalizeRunTypeFilter(value: string | null): "FULL_SCAN" | "INCREMENTAL_SCAN" | "CUSTOM_SCAN" | "PROFILING" | null {
  const normalized = String(value || "").trim().toUpperCase();
  if (["FULL", "FULL_SCAN"].includes(normalized)) return "FULL_SCAN";
  if (["INCREMENTAL", "INCREMENTAL_SCAN"].includes(normalized)) return "INCREMENTAL_SCAN";
  if (["CUSTOM", "CUSTOM_SCAN"].includes(normalized)) return "CUSTOM_SCAN";
  if (normalized === "PROFILING") return "PROFILING";
  return null;
}

// GET /api/dq/activity?database=BANKING_DW&schema=BRONZE&table=STG_CUSTOMER
// Returns recent DQ runs for the given table.
export async function GET(request: NextRequest) {
  try {
    const searchParams = request.nextUrl.searchParams;
    const database = (searchParams.get("database") || "").toUpperCase();
    const schema = (searchParams.get("schema") || "").toUpperCase();
    const table = (searchParams.get("table") || "").toUpperCase();
    const dqDatabase = (searchParams.get("dqDatabase") || "DATA_QUALITY_DB").toUpperCase();
    const dqMetricsSchema = (searchParams.get("dqMetricsSchema") || "DQ_METRICS").toUpperCase();
    const limit = parseInt(searchParams.get("limit") || "20", 10);
    const executionModeFilter = normalizeExecutionModeFilter(searchParams.get("execution_mode"));
    const runTypeFilter = normalizeRunTypeFilter(searchParams.get("run_type"));

    if (!database || !schema || !table) {
      return NextResponse.json(
        { success: false, error: "Missing required parameters: database, schema, table" },
        { status: 400, headers: { "Cache-Control": "no-store" } }
      );
    }

    let conn: any;
    try {
      const config = getServerConfig();
      conn = await snowflakePool.getConnection(config || undefined);
    } catch (e: any) {
      return NextResponse.json(
        { success: false, error: `Unable to establish Snowflake connection: ${e?.message || e}` },
        { status: 401, headers: { "Cache-Control": "no-store" } }
      );
    }

    const executionColumns = await getExecutionTimestampColumnMap(conn, dqDatabase);

    const sql = `
      WITH scan_runs AS (
        SELECT
          rc.run_id,
          CASE
            WHEN rc.run_type IS NOT NULL AND rc.run_type != '' THEN UPPER(rc.run_type)
            WHEN UPPER(COALESCE(rc.run_type, '')) IN ('INCREMENTAL', 'INCREMENTAL_SCAN') THEN 'INCREMENTAL_SCAN'
            WHEN UPPER(COALESCE(rc.run_type, '')) IN ('FULL', 'FULL_SCAN') THEN 'FULL_SCAN'
            WHEN rc.run_id LIKE 'DQ_INC%' OR UPPER(rc.run_id) LIKE '%INCR%' THEN 'INCREMENTAL_SCAN'
            WHEN rc.run_id LIKE 'DQ_PROFILE%' THEN 'PROFILING'
            WHEN rc.run_id LIKE 'DQ_CUSTOM%' THEN 'CUSTOM_SCAN'
            ELSE 'FULL_SCAN'
          END AS run_type,
          CASE
            WHEN rc.execution_mode IS NOT NULL AND rc.execution_mode != '' THEN UPPER(rc.execution_mode)
            WHEN UPPER(COALESCE(rc.triggered_by, '')) IN ('S', 'SCHEDULED', 'SCHEDULED_TASK', 'SYSTEM', 'SCHEDULER') THEN 'SCHEDULED'
            WHEN UPPER(COALESCE(rc.triggered_by, '')) IN ('A', 'AUTO', 'AUTOMATED', 'BOT', 'ETL_PIPELINE') THEN 'AUTO'
            ELSE 'MANUAL'
          END AS execution_mode,
          rc.triggered_by,
          TO_VARCHAR(rc.start_ts, 'YYYY-MM-DD HH24:MI:SS.FF3') AS started_at,
          TO_VARCHAR(rc.end_ts, 'YYYY-MM-DD HH24:MI:SS.FF3') AS completed_at,
          rc.duration_seconds,
          rc.run_status AS status,
          COALESCE(rc.total_checks, 0) AS checks_executed,
          COALESCE(rc.failed_checks, 0) AS failed_checks,
          COALESCE(rc.warning_checks, 0) AS warnings,
          COALESCE(rc.skipped_checks, 0) AS skipped_checks,
          MIN(cr.database_name) AS database_name,
          MIN(cr.schema_name) AS schema_name,
          MIN(cr.table_name) AS table_name
        FROM ${dqDatabase}.${dqMetricsSchema}.DQ_RUN_CONTROL rc
        JOIN ${dqDatabase}.${dqMetricsSchema}.DQ_CHECK_RESULTS cr
          ON rc.run_id = cr.run_id
        WHERE UPPER(cr.database_name) = ?
          AND UPPER(cr.schema_name) = ?
          AND UPPER(cr.table_name) = ?
        GROUP BY
          rc.run_id,
          rc.run_type,
          rc.execution_mode,
          rc.triggered_by,
          rc.start_ts,
          rc.end_ts,
          rc.duration_seconds,
          rc.run_status,
          rc.total_checks,
          rc.failed_checks,
          rc.warning_checks,
          rc.skipped_checks
      ),
      scheduled_custom_scans AS (
        -- Fetch scheduled custom scans that have check results
        SELECT DISTINCT
          se.RUN_ID AS run_id,
          'CUSTOM_SCAN' AS run_type,
          'SCHEDULED' AS execution_mode,
          'SCHEDULED_TASK' AS triggered_by,
          TO_VARCHAR(se.${executionColumns.startedAt}, 'YYYY-MM-DD HH24:MI:SS.FF3') AS started_at,
          TO_VARCHAR(se.${executionColumns.finishedAt}, 'YYYY-MM-DD HH24:MI:SS.FF3') AS completed_at,
          COALESCE(DATEDIFF(second, se.${executionColumns.startedAt}, se.${executionColumns.finishedAt}), 0) AS duration_seconds,
          CASE
            WHEN UPPER(se.STATUS) = 'SUCCEEDED' THEN 'HEALTHY'
            WHEN UPPER(se.STATUS) = 'FAILED' THEN 'FAILED'
            ELSE 'RUNNING'
          END AS status,
          COALESCE(rc.total_checks, 0) AS checks_executed,
          COALESCE(rc.failed_checks, 0) AS failed_checks,
          COALESCE(rc.warning_checks, 0) AS warnings,
          0 AS skipped_checks,
          cr.database_name,
          cr.schema_name,
          cr.table_name
        FROM ${dqDatabase}.DQ_CONFIG.DQ_SCHEDULE_EXECUTION se
        INNER JOIN ${dqDatabase}.${dqMetricsSchema}.DQ_CHECK_RESULTS cr ON se.RUN_ID = cr.run_id
        LEFT JOIN ${dqDatabase}.${dqMetricsSchema}.DQ_RUN_CONTROL rc ON se.RUN_ID = rc.run_id
        WHERE se.STATUS IN ('SUCCEEDED', 'FAILED', 'RUNNING')
          AND se.RUN_ID IS NOT NULL
          AND UPPER(cr.database_name) = ?
          AND UPPER(cr.schema_name) = ?
          AND UPPER(cr.table_name) = ?
      ),
      profile_runs AS (
        SELECT
          rc.run_id,
          'PROFILING' AS run_type,
          CASE
            WHEN UPPER(COALESCE(rc.triggered_by, '')) IN ('S', 'SCHEDULED', 'SCHEDULED_TASK', 'SYSTEM', 'SCHEDULER') THEN 'SCHEDULED'
            WHEN UPPER(COALESCE(rc.triggered_by, '')) IN ('A', 'AUTO', 'AUTOMATED', 'BOT', 'ETL_PIPELINE') THEN 'AUTO'
            ELSE 'MANUAL'
          END AS execution_mode,
          rc.triggered_by,
          TO_VARCHAR(rc.start_ts, 'YYYY-MM-DD HH24:MI:SS.FF3') AS started_at,
          TO_VARCHAR(rc.end_ts, 'YYYY-MM-DD HH24:MI:SS.FF3') AS completed_at,
          rc.duration_seconds,
          rc.run_status AS status,
          COALESCE(rc.total_checks, 0) AS checks_executed,
          COALESCE(rc.failed_checks, 0) AS failed_checks,
          COALESCE(rc.warning_checks, 0) AS warnings,
          0 AS skipped_checks,
          MIN(cp.database_name) AS database_name,
          MIN(cp.schema_name) AS schema_name,
          MIN(cp.table_name) AS table_name
        FROM ${dqDatabase}.${dqMetricsSchema}.DQ_RUN_CONTROL rc
        JOIN ${dqDatabase}.${dqMetricsSchema}.DQ_COLUMN_PROFILE cp
          ON rc.run_id = cp.run_id
        WHERE rc.run_id LIKE 'DQ_PROFILE%'
          AND rc.run_id NOT IN (SELECT run_id FROM scan_runs)
          AND UPPER(cp.database_name) = ?
          AND UPPER(cp.schema_name) = ?
          AND UPPER(cp.table_name) = ?
        GROUP BY
          rc.run_id,
          rc.triggered_by,
          rc.start_ts,
          rc.end_ts,
          rc.duration_seconds,
          rc.run_status,
          rc.total_checks,
          rc.failed_checks,
          rc.warning_checks
      ),
      all_runs AS (
        SELECT * FROM scan_runs
        UNION ALL
        SELECT * FROM profile_runs
        UNION ALL
        SELECT * FROM scheduled_custom_scans
      ),
      deduplicated_runs AS (
        -- Deduplicate by run_id, keeping the entry with the most check data
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY run_id ORDER BY checks_executed DESC, started_at DESC) AS rn
        FROM all_runs
      )
      SELECT 
        run_id, run_type, execution_mode, triggered_by,
        started_at, completed_at, duration_seconds, status,
        checks_executed, failed_checks, warnings, skipped_checks,
        database_name, schema_name, table_name
      FROM deduplicated_runs
      WHERE rn = 1
        AND (? IS NULL OR execution_mode = ?)
        AND (? IS NULL OR run_type = ?)
      ORDER BY started_at DESC
      LIMIT ?
    `;

    const result = await new Promise<any[]>((resolve, reject) => {
      conn.execute({
        sqlText: sql,
        binds: [
          database,
          schema,
          table,
          database, // for scheduled_custom_scans database_name check
          schema,   // for scheduled_custom_scans schema_name check
          table,    // for scheduled_custom_scans table_name check
          database, // for profile_runs
          schema,   // for profile_runs
          table,    // for profile_runs
          executionModeFilter,
          executionModeFilter,
          runTypeFilter,
          runTypeFilter,
          limit,
        ],
        complete: (err: any, _stmt: any, rows: any) => {
          if (err) reject(err);
          else {
            logger.debug(`Activity query executed for ${database}.${schema}.${table}`);
            logger.debug(`Activity total rows returned: ${rows?.length || 0}`);
            if (rows && rows.length > 0) {
              logger.debug(`Activity row types: ${JSON.stringify(rows.map((r: any) => ({ run_id: r.RUN_ID, run_type: r.RUN_TYPE, execution_mode: r.EXECUTION_MODE, status: r.STATUS })))}`);
            }
            resolve(rows || []);
          }
        },
      });
    });

    // Keep both canonical and legacy keys for backward compatibility.
    const data = result.map((row: any) => ({
      run_id: row.RUN_ID,
      run_type: row.RUN_TYPE,
      execution_mode: row.EXECUTION_MODE,
      triggered_by: row.TRIGGERED_BY,
      started_at: row.STARTED_AT,
      completed_at: row.COMPLETED_AT,
      duration_seconds: row.DURATION_SECONDS,
      status: row.STATUS,
      checks_executed: row.CHECKS_EXECUTED,
      failed_checks: row.FAILED_CHECKS,
      warnings: row.WARNINGS,

      // Legacy aliases currently used by existing screens
      start_ts: row.STARTED_AT,
      end_ts: row.COMPLETED_AT,
      run_status: row.STATUS,
      total_checks: row.CHECKS_EXECUTED,
      warning_checks: row.WARNINGS,
      skipped_checks: row.SKIPPED_CHECKS,
      database_name: row.DATABASE_NAME,
      schema_name: row.SCHEMA_NAME,
      table_name: row.TABLE_NAME,
    }));

    return NextResponse.json(
      { success: true, data },
      { headers: { "Cache-Control": "no-store, no-cache, must-revalidate" } }
    );
  } catch (error: any) {
    console.error("GET /api/dq/activity error:", error);
    return NextResponse.json(
      { success: false, error: error.message || "Failed to fetch activity" },
      { status: 500, headers: { "Cache-Control": "no-store" } }
    );
  }
}
