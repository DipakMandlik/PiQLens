import { NextRequest, NextResponse } from "next/server";
import { getServerConfig } from "@/lib/server-config";
import { snowflakePool } from "@/lib/snowflake";

// POST /api/dq/rerun
// Re-executes a scan based on run_id by fetching original parameters
export async function POST(request: NextRequest) {
  try {
    const body = await request.json();
    const { run_id } = body;
    const dqDatabase = (body.dqDatabase || "DATA_QUALITY_DB").toUpperCase();
    const dqMetricsSchema = (body.dqMetricsSchema || "DQ_METRICS").toUpperCase();
    const dqEngineSchema = (body.dqEngineSchema || "DQ_ENGINE").toUpperCase();

    if (!run_id) {
      return NextResponse.json(
        { success: false, error: "Missing required parameter: run_id" },
        { status: 400 }
      );
    }

    let conn: any;
    try {
      const config = getServerConfig();
      conn = await snowflakePool.getConnection(config || undefined);
    } catch (e: any) {
      return NextResponse.json(
        { success: false, error: `Unable to establish Snowflake connection: ${e?.message || e}` },
        { status: 401 }
      );
    }

    // Determine run type
    const isCustomScan = run_id.startsWith("DQ_CUSTOM");

    if (isCustomScan) {
      // Fetch original parameters for custom scan
      const paramsSQL = `
        SELECT DISTINCT
          dataset_id,
          rule_name,
          column_name,
          threshold
        FROM ${dqDatabase}.${dqMetricsSchema}.DQ_CHECK_RESULTS
        WHERE run_id = ?
      `;

      const paramsResult = await new Promise<any[]>((resolve, reject) => {
        conn.execute({
          sqlText: paramsSQL,
          binds: [run_id],
          complete: (err: any, _stmt: any, rows: any) => {
            if (err) reject(err);
            else resolve(rows || []);
          },
        });
      });

      if (paramsResult.length === 0) {
        return NextResponse.json(
          { success: false, error: `No check results found for run_id = ${run_id}` },
          { status: 404 }
        );
      }

      // Execute each rule from the original run
      const results: any[] = [];
      for (const row of paramsResult) {
        const datasetId = row.DATASET_ID;
        const ruleName = row.RULE_NAME;
        const columnName = row.COLUMN_NAME;
        const threshold = row.THRESHOLD;

        // Call stored procedure
        const spSQL = `CALL ${dqDatabase}.${dqEngineSchema}.sp_run_custom_rule(?, ?, ?, ?, ?)`;
        const binds = [datasetId, ruleName, columnName, threshold, "RERUN"];

        try {
          const spResult = await new Promise<any>((resolve, reject) => {
            conn.execute({
              sqlText: spSQL,
              binds,
              complete: (err: any, _stmt: any, rows: any) => {
                if (err) reject(err);
                else resolve(rows);
              },
            });
          });

          // Parse SP response
          let runMeta: any = {};
          if (spResult && spResult[0] && spResult[0].SP_RUN_CUSTOM_RULE) {
            try {
              runMeta = JSON.parse(spResult[0].SP_RUN_CUSTOM_RULE);
            } catch (parseErr) {
              console.warn("Failed to parse SP response", parseErr);
            }
          }

          results.push({
            dataset_id: datasetId,
            rule_name: ruleName,
            column_name: columnName,
            success: true,
            run_id: runMeta.run_id || runMeta.RUN_ID,
            status: runMeta.status || runMeta.STATUS,
            pass_rate: runMeta.pass_rate || runMeta.PASS_RATE,
          });
        } catch (spErr: any) {
          results.push({
            dataset_id: datasetId,
            rule_name: ruleName,
            column_name: columnName,
            success: false,
            error: spErr.message,
          });
        }
      }

      const successCount = results.filter(r => r.success).length;
      const failCount = results.filter(r => !r.success).length;

      return NextResponse.json({
        success: true,
        data: {
          run_type: "CUSTOM_SCAN",
          original_run_id: run_id,
          checks_executed: results.length,
          success_count: successCount,
          fail_count: failCount,
          results,
        },
      });
    } else {
      // Full Scan - fetch dataset_id and call full scan procedure
      const datasetSQL = `
        SELECT DISTINCT dataset_id
        FROM ${dqDatabase}.${dqMetricsSchema}.DQ_CHECK_RESULTS
        WHERE run_id = ?
        LIMIT 1
      `;

      const datasetResult = await new Promise<any[]>((resolve, reject) => {
        conn.execute({
          sqlText: datasetSQL,
          binds: [run_id],
          complete: (err: any, _stmt: any, rows: any) => {
            if (err) reject(err);
            else resolve(rows || []);
          },
        });
      });

      if (datasetResult.length === 0) {
        return NextResponse.json(
          { success: false, error: `No dataset found for run_id = ${run_id}` },
          { status: 404 }
        );
      }

      const datasetId = datasetResult[0].DATASET_ID;

      // Call full scan stored procedure
      const spSQL = `CALL ${dqDatabase}.${dqEngineSchema}.sp_run_full_scan(?, ?)`;
      const binds = [datasetId, "RERUN"];

      try {
        const spResult = await new Promise<any>((resolve, reject) => {
          conn.execute({
            sqlText: spSQL,
            binds,
            complete: (err: any, _stmt: any, rows: any) => {
              if (err) reject(err);
              else resolve(rows);
            },
          });
        });

        // Parse SP response
        let runMeta: any = {};
        if (spResult && spResult[0]) {
          const key = Object.keys(spResult[0])[0];
          if (key && spResult[0][key]) {
            try {
              runMeta = JSON.parse(spResult[0][key]);
            } catch (parseErr) {
              runMeta = spResult[0];
            }
          }
        }

        return NextResponse.json({
          success: true,
          data: {
            run_type: "FULL_SCAN",
            original_run_id: run_id,
            dataset_id: datasetId,
            new_run_id: runMeta.run_id || runMeta.RUN_ID,
            status: runMeta.status || runMeta.STATUS,
          },
        });
      } catch (spErr: any) {
        return NextResponse.json(
          { success: false, error: `Full scan failed: ${spErr.message}` },
          { status: 500 }
        );
      }
    }
  } catch (error: any) {
    console.error("POST /api/dq/rerun error:", error);
    return NextResponse.json(
      { success: false, error: error.message || "Failed to rerun scan" },
      { status: 500 }
    );
  }
}
