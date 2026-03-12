/* eslint-disable @typescript-eslint/no-explicit-any */
import { NextRequest, NextResponse } from "next/server";
import { getServerConfig } from "@/lib/server-config";
import { snowflakePool } from "@/lib/snowflake";

export const dynamic = "force-dynamic";
export const revalidate = 0;

/**
 * Debug endpoint to diagnose scheduled custom scan visibility issues
 * GET /api/dq/activity-debug?database=BANKING_DW&schema=TESTING&table=TESTING_ACCOUNT
 */
export async function GET(request: NextRequest) {
  try {
    const searchParams = request.nextUrl.searchParams;
    const database = (searchParams.get("database") || "").toUpperCase();
    const schema = (searchParams.get("schema") || "").toUpperCase();
    const table = (searchParams.get("table") || "").toUpperCase();
    const dqDatabase = (searchParams.get("dqDatabase") || "DATA_QUALITY_DB").toUpperCase();
    const dqMetricsSchema = (searchParams.get("dqMetricsSchema") || "DQ_METRICS").toUpperCase();

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

    // 1. Check DQ_SCHEDULE_EXECUTION for recent scheduled runs
    const executionData = await new Promise<any[]>((resolve, reject) => {
      conn.execute({
        sqlText: `
          SELECT 
            EXECUTION_ID, SCHEDULE_ID, RUN_ID, STATUS, 
            STARTED_AT, FINISHED_AT, CREATED_AT
          FROM ${dqDatabase}.DQ_CONFIG.DQ_SCHEDULE_EXECUTION
          WHERE STATUS IN ('SUCCEEDED', 'FAILED')
            AND RUN_ID IS NOT NULL
          ORDER BY CREATED_AT DESC
          LIMIT 20
        `,
        complete: (err: any, _stmt: any, rows: any) => {
          if (err) reject(err);
          else resolve(rows || []);
        },
      });
    });

    // 2. Check DQ_RUN_CONTROL for scheduled runs
    const runControlData = await new Promise<any[]>((resolve, reject) => {
      conn.execute({
        sqlText: `
          SELECT 
            RUN_ID, TRIGGERED_BY, RUN_STATUS, 
            START_TS, END_TS, TOTAL_CHECKS, FAILED_CHECKS
          FROM ${dqDatabase}.${dqMetricsSchema}.DQ_RUN_CONTROL
          WHERE TRIGGERED_BY = 'SCHEDULED_TASK'
          ORDER BY START_TS DESC
          LIMIT 20
        `,
        complete: (err: any, _stmt: any, rows: any) => {
          if (err) reject(err);
          else resolve(rows || []);
        },
      });
    });

    // 3. Check DQ_CHECK_RESULTS for scheduled runs
    const checkResultsData = await new Promise<any[]>((resolve, reject) => {
      conn.execute({
        sqlText: `
          SELECT 
            rc.RUN_ID, COUNT(*) as check_count,
            MIN(cr.DATABASE_NAME) as database_name,
            MIN(cr.SCHEMA_NAME) as schema_name,
            MIN(cr.TABLE_NAME) as table_name
          FROM ${dqDatabase}.${dqMetricsSchema}.DQ_RUN_CONTROL rc
          LEFT JOIN ${dqDatabase}.${dqMetricsSchema}.DQ_CHECK_RESULTS cr
            ON rc.RUN_ID = cr.RUN_ID
          WHERE rc.TRIGGERED_BY = 'SCHEDULED_TASK'
          GROUP BY rc.RUN_ID
          ORDER BY rc.START_TS DESC
          LIMIT 20
        `,
        complete: (err: any, _stmt: any, rows: any) => {
          if (err) reject(err);
          else resolve(rows || []);
        },
      });
    });

    // 4. Test the scheduled_custom_scans CTE directly
    const cteTestData = await new Promise<any[]>((resolve, reject) => {
      conn.execute({
        sqlText: `
          SELECT
            se.RUN_ID,
            cr.DATABASE_NAME,
            cr.SCHEMA_NAME,
            cr.TABLE_NAME,
            COUNT(*) as check_results_count
          FROM ${dqDatabase}.DQ_CONFIG.DQ_SCHEDULE_EXECUTION se
          INNER JOIN ${dqDatabase}.${dqMetricsSchema}.DQ_CHECK_RESULTS cr 
            ON se.RUN_ID = cr.RUN_ID
          WHERE se.STATUS IN ('SUCCEEDED', 'FAILED', 'RUNNING')
            AND se.RUN_ID IS NOT NULL
            AND UPPER(cr.DATABASE_NAME) = '${database}'
            AND UPPER(cr.SCHEMA_NAME) = '${schema}'
            AND UPPER(cr.TABLE_NAME) = '${table}'
          GROUP BY se.RUN_ID, cr.DATABASE_NAME, cr.SCHEMA_NAME, cr.TABLE_NAME
          ORDER BY se.CREATED_AT DESC
          LIMIT 20
        `,
        complete: (err: any, _stmt: any, rows: any) => {
          if (err) reject(err);
          else resolve(rows || []);
        },
      });
    });

    return NextResponse.json(
      {
        success: true,
        database,
        schema,
        table,
        diagnostics: {
          dq_schedule_execution: {
            description: "Recent scheduled execution records",
            count: executionData.length,
            data: executionData,
          },
          dq_run_control_scheduled: {
            description: "DQ_RUN_CONTROL entries marked as SCHEDULED_TASK",
            count: runControlData.length,
            data: runControlData,
          },
          check_results_for_scheduled: {
            description: "Check results associated with scheduled runs",
            count: checkResultsData.length,
            data: checkResultsData,
          },
          cte_test_for_table: {
            description: `CTE test: scheduled_custom_scans filtered for ${database}.${schema}.${table}`,
            count: cteTestData.length,
            data: cteTestData,
          },
        },
      },
      { headers: { "Cache-Control": "no-store, no-cache, must-revalidate" } }
    );
  } catch (error: any) {
    console.error("GET /api/dq/activity-debug error:", error);
    return NextResponse.json(
      { success: false, error: error.message || "Debug query failed" },
      { status: 500, headers: { "Cache-Control": "no-store" } }
    );
  }
}
