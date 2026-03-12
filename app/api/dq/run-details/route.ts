import { NextRequest, NextResponse } from "next/server";
import { getServerConfig } from "@/lib/server-config";
import { snowflakePool } from "@/lib/snowflake";

export const dynamic = "force-dynamic";
export const revalidate = 0;

// GET /api/dq/run-details?run_id=DQ_CUSTOM_20260108_...
// Returns run summary and check-level results for a specific run.
export async function GET(request: NextRequest) {
  try {
    const searchParams = request.nextUrl.searchParams;
    const runId = searchParams.get("run_id");
    const dqDatabase = (searchParams.get("dqDatabase") || "DATA_QUALITY_DB").toUpperCase();
    const dqMetricsSchema = (searchParams.get("dqMetricsSchema") || "DQ_METRICS").toUpperCase();

    if (!runId) {
      return NextResponse.json(
        { success: false, error: "Missing required parameter: run_id" },
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

    // A) Run summary (raw timestamp strings, no timezone conversion)
    const summarySQL = `
      SELECT
        run_id,
        CASE
          WHEN UPPER(COALESCE(run_type, '')) IN ('INCREMENTAL', 'INCREMENTAL_SCAN') THEN 'INCREMENTAL_SCAN'
          WHEN UPPER(COALESCE(run_type, '')) IN ('FULL', 'FULL_SCAN') THEN 'FULL_SCAN'
          WHEN run_id LIKE 'DQ_INC%' OR UPPER(run_id) LIKE '%INCR%' THEN 'INCREMENTAL_SCAN'
          WHEN run_id LIKE 'DQ_PROFILE%' THEN 'PROFILING'
          WHEN run_id LIKE 'DQ_CUSTOM%' THEN 'CUSTOM_SCAN'
          ELSE 'FULL_SCAN'
        END AS run_type,
        run_status AS status,
        TO_VARCHAR(start_ts, 'YYYY-MM-DD HH24:MI:SS.FF3') AS started_at,
        TO_VARCHAR(end_ts, 'YYYY-MM-DD HH24:MI:SS.FF3') AS completed_at,
        duration_seconds,
        COALESCE(total_checks, 0) AS checks_executed,
        COALESCE(passed_checks, 0) AS passed_checks,
        COALESCE(failed_checks, 0) AS failed_checks,
        COALESCE(warning_checks, 0) AS warnings,
        COALESCE(skipped_checks, 0) AS skipped_checks,
        triggered_by,
        CASE
          WHEN UPPER(COALESCE(triggered_by, '')) IN ('S', 'SCHEDULED', 'SCHEDULED_TASK', 'SYSTEM', 'SCHEDULER') THEN 'SCHEDULED'
          WHEN UPPER(COALESCE(triggered_by, '')) IN ('A', 'AUTO', 'AUTOMATED', 'BOT', 'ETL_PIPELINE') THEN 'AUTO'
          ELSE 'MANUAL'
        END AS execution_mode
      FROM ${dqDatabase}.${dqMetricsSchema}.DQ_RUN_CONTROL
      WHERE run_id = ?
    `;

    const summaryResult = await new Promise<any[]>((resolve, reject) => {
      conn.execute({
        sqlText: summarySQL,
        binds: [runId],
        complete: (err: any, _stmt: any, rows: any) => {
          if (err) reject(err);
          else resolve(rows || []);
        },
      });
    });

    if (summaryResult.length === 0) {
      return NextResponse.json(
        { success: false, error: `No run found with run_id = ${runId}` },
        { status: 404, headers: { "Cache-Control": "no-store" } }
      );
    }

    // B) Check-level results
    const checksSQL = `
      SELECT
        rule_name,
        rule_type,
        column_name,
        check_status,
        pass_rate,
        threshold,
        total_records,
        invalid_records,
        failure_reason,
        database_name,
        schema_name,
        table_name,
        dataset_id
      FROM ${dqDatabase}.${dqMetricsSchema}.DQ_CHECK_RESULTS
      WHERE run_id = ?
      ORDER BY check_status DESC, rule_type
    `;

    const checksResult = await new Promise<any[]>((resolve, reject) => {
      conn.execute({
        sqlText: checksSQL,
        binds: [runId],
        complete: (err: any, _stmt: any, rows: any) => {
          if (err) reject(err);
          else resolve(rows || []);
        },
      });
    });

    const summary = summaryResult[0];
    const runSummary = {
      run_id: summary.RUN_ID,
      run_type: summary.RUN_TYPE,
      execution_mode: summary.EXECUTION_MODE,
      triggered_by: summary.TRIGGERED_BY,
      status: summary.STATUS,
      started_at: summary.STARTED_AT,
      completed_at: summary.COMPLETED_AT,
      duration_seconds: summary.DURATION_SECONDS,
      checks_executed: summary.CHECKS_EXECUTED,
      failed_checks: summary.FAILED_CHECKS,
      warnings: summary.WARNINGS,

      // Legacy aliases used in existing modal
      run_status: summary.STATUS,
      start_ts: summary.STARTED_AT,
      end_ts: summary.COMPLETED_AT,
      total_checks: summary.CHECKS_EXECUTED,
      passed_checks: summary.PASSED_CHECKS,
      warning_checks: summary.WARNINGS,
      skipped_checks: summary.SKIPPED_CHECKS,
    };

    const checks = checksResult.map((row: any) => ({
      rule_name: row.RULE_NAME,
      rule_type: row.RULE_TYPE,
      column_name: row.COLUMN_NAME,
      check_status: row.CHECK_STATUS,
      pass_rate: row.PASS_RATE,
      threshold: row.THRESHOLD,
      total_records: row.TOTAL_RECORDS,
      invalid_records: row.INVALID_RECORDS,
      failure_reason: row.FAILURE_REASON,
      database_name: row.DATABASE_NAME,
      schema_name: row.SCHEMA_NAME,
      table_name: row.TABLE_NAME,
      dataset_id: row.DATASET_ID,
    }));

    return NextResponse.json(
      {
        success: true,
        data: {
          summary: runSummary,
          checks,
        },
      },
      { headers: { "Cache-Control": "no-store, no-cache, must-revalidate" } }
    );
  } catch (error: any) {
    console.error("GET /api/dq/run-details error:", error);
    return NextResponse.json(
      { success: false, error: error.message || "Failed to fetch run details" },
      { status: 500, headers: { "Cache-Control": "no-store" } }
    );
  }
}
