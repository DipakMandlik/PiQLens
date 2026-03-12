import { NextRequest, NextResponse } from "next/server";
import { getServerConfig } from "@/lib/server-config";
import { snowflakePool } from "@/lib/snowflake";

// GET /api/dq/latest-profile
// Fetches the latest profiling results for a dataset/table
export async function GET(request: NextRequest) {
  try {
    const { searchParams } = new URL(request.url);
    const datasetId = searchParams.get("dataset_id");
    const database = searchParams.get("database");
    const schema = searchParams.get("schema");
    const table = searchParams.get("table");
    const dqDatabase = (searchParams.get("dqDatabase") || "DATA_QUALITY_DB").toUpperCase();
    const dqMetricsSchema = (searchParams.get("dqMetricsSchema") || "DQ_METRICS").toUpperCase();

    if (!datasetId && (!database || !schema || !table)) {
      return NextResponse.json(
        { success: false, error: "Either dataset_id or database/schema/table is required" },
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

    // Get the latest COMPLETED profiling run_id from DQ_RUN_CONTROL (authoritative source)
    // Join with DQ_COLUMN_PROFILE to filter by table coordinates
    let latestRunSQL: string;
    let binds: (string | number)[];

    if (datasetId) {
      latestRunSQL = `
        SELECT rc.RUN_ID, rc.START_TS
        FROM ${dqDatabase}.${dqMetricsSchema}.DQ_RUN_CONTROL rc
        INNER JOIN ${dqDatabase}.${dqMetricsSchema}.DQ_COLUMN_PROFILE cp
          ON rc.RUN_ID = cp.RUN_ID
        WHERE rc.RUN_ID LIKE 'DQ_PROFILE_%'
          AND rc.RUN_STATUS = 'COMPLETED'
          AND cp.DATASET_ID = ?
        GROUP BY rc.RUN_ID, rc.START_TS
        ORDER BY rc.START_TS DESC
        LIMIT 1
      `;
      binds = [datasetId];
    } else {
      latestRunSQL = `
        SELECT rc.RUN_ID, rc.START_TS
        FROM ${dqDatabase}.${dqMetricsSchema}.DQ_RUN_CONTROL rc
        INNER JOIN ${dqDatabase}.${dqMetricsSchema}.DQ_COLUMN_PROFILE cp
          ON rc.RUN_ID = cp.RUN_ID
        WHERE rc.RUN_ID LIKE 'DQ_PROFILE_%'
          AND rc.RUN_STATUS = 'COMPLETED'
          AND UPPER(cp.DATABASE_NAME) = UPPER(?)
          AND UPPER(cp.SCHEMA_NAME) = UPPER(?)
          AND UPPER(cp.TABLE_NAME) = UPPER(?)
        GROUP BY rc.RUN_ID, rc.START_TS
        ORDER BY rc.START_TS DESC
        LIMIT 1
      `;
      binds = [database!, schema!, table!];
    }

    const latestRunResult = await new Promise<any[]>((resolve, reject) => {
      conn.execute({
        sqlText: latestRunSQL,
        binds,
        complete: (err: any, _stmt: any, rows: any) => {
          if (err) reject(err);
          else resolve(rows || []);
        },
      });
    });

    if (latestRunResult.length === 0) {
      return NextResponse.json({
        success: true,
        data: {
          run_id: null,
          profile_ts: null,
          columns: [],
          message: "No profiling data found. Run profiling first.",
        },
      });
    }

    const latestRunId = latestRunResult[0].RUN_ID;
    const latestTs = latestRunResult[0].START_TS;

    // Fetch all column profiles for this run
    const profileSQL = `
      SELECT
        PROFILE_ID,
        RUN_ID,
        DATASET_ID,
        DATABASE_NAME,
        SCHEMA_NAME,
        TABLE_NAME,
        COLUMN_NAME,
        DATA_TYPE,
        TOTAL_RECORDS,
        NULL_COUNT,
        DISTINCT_COUNT,
        MIN_VALUE,
        MAX_VALUE,
        AVG_VALUE,
        STDDEV_VALUE,
        MIN_LENGTH,
        MAX_LENGTH,
        AVG_LENGTH,
        FUTURE_DATE_COUNT,
        PROFILE_TS
      FROM ${dqDatabase}.${dqMetricsSchema}.DQ_COLUMN_PROFILE
      WHERE RUN_ID = ?
      ORDER BY PROFILE_ID
    `;

    const profileResult = await new Promise<any[]>((resolve, reject) => {
      conn.execute({
        sqlText: profileSQL,
        binds: [latestRunId],
        complete: (err: any, _stmt: any, rows: any) => {
          if (err) reject(err);
          else resolve(rows || []);
        },
      });
    });

    // Transform results and compute derived metrics
    const columns = profileResult.map((row: any) => {
      const totalRecords = row.TOTAL_RECORDS || 0;
      const nullCount = row.NULL_COUNT || 0;
      const distinctCount = row.DISTINCT_COUNT || 0;

      // Compute percentages
      const nullPercent = totalRecords > 0 ? (nullCount / totalRecords) * 100 : 0;
      const distinctPercent = totalRecords > 0 ? (distinctCount / totalRecords) * 100 : 0;

      // Compute flags
      const flags: string[] = [];
      if (nullPercent > 20) flags.push("HIGH_NULLS");
      if (distinctPercent < 10 && distinctCount > 0) flags.push("LOW_CARDINALITY");
      if ((row.FUTURE_DATE_COUNT || 0) > 0) flags.push("FUTURE_DATES");
      if (distinctCount === totalRecords && totalRecords > 0) flags.push("UNIQUE");
      if (nullPercent === 0) flags.push("NO_NULLS");

      return {
        profile_id: row.PROFILE_ID,
        column_name: row.COLUMN_NAME,
        data_type: row.DATA_TYPE,
        total_records: totalRecords,
        null_count: nullCount,
        null_percent: Math.round(nullPercent * 100) / 100,
        distinct_count: distinctCount,
        distinct_percent: Math.round(distinctPercent * 100) / 100,
        min_value: row.MIN_VALUE,
        max_value: row.MAX_VALUE,
        avg_value: row.AVG_VALUE !== null ? Math.round(row.AVG_VALUE * 100) / 100 : null,
        stddev_value: row.STDDEV_VALUE !== null ? Math.round(row.STDDEV_VALUE * 100) / 100 : null,
        min_length: row.MIN_LENGTH,
        max_length: row.MAX_LENGTH,
        avg_length: row.AVG_LENGTH !== null ? Math.round(row.AVG_LENGTH * 100) / 100 : null,
        future_date_count: row.FUTURE_DATE_COUNT,
        flags,
        profile_ts: row.PROFILE_TS,
      };
    });

    // Get run summary info
    const runInfoSQL = `
      SELECT RUN_STATUS, START_TS, END_TS, DURATION_SECONDS, TRIGGERED_BY
      FROM ${dqDatabase}.${dqMetricsSchema}.DQ_RUN_CONTROL
      WHERE RUN_ID = ?
    `;
    const runInfoResult = await new Promise<any[]>((resolve, reject) => {
      conn.execute({
        sqlText: runInfoSQL,
        binds: [latestRunId],
        complete: (err: any, _stmt: any, rows: any) => {
          if (err) reject(err);
          else resolve(rows || []);
        },
      });
    });

    const runInfo = runInfoResult[0] || {};

    return NextResponse.json({
      success: true,
      data: {
        run_id: latestRunId,
        profile_ts: latestTs,
        run_status: runInfo.RUN_STATUS,
        start_ts: runInfo.START_TS,
        duration_seconds: runInfo.DURATION_SECONDS,
        triggered_by: runInfo.TRIGGERED_BY,
        database: profileResult[0]?.DATABASE_NAME,
        schema: profileResult[0]?.SCHEMA_NAME,
        table: profileResult[0]?.TABLE_NAME,
        dataset_id: profileResult[0]?.DATASET_ID,
        total_columns: columns.length,
        columns,
      },
    });
  } catch (error: any) {
    console.error("GET /api/dq/latest-profile error:", error);
    return NextResponse.json(
      { success: false, error: error.message || "Failed to fetch profile data" },
      { status: 500 }
    );
  }
}
