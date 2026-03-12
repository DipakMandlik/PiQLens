import { NextRequest, NextResponse } from "next/server";
import { getServerConfig } from "@/lib/server-config";
import { snowflakePool } from "@/lib/snowflake";

// GET /api/dq/rules?dqDatabase=...&dqSchema=...
// Optional: &datasetId=...&column=... to filter rules assigned to a specific column
export async function GET(request: NextRequest) {
  try {
    const searchParams = request.nextUrl.searchParams;
    const dqDatabase = searchParams.get("dqDatabase") || "DATA_QUALITY_DB";
    const dqSchema = searchParams.get("dqSchema") || "DQ_CONFIG";
    const datasetId = searchParams.get("datasetId");
    const columnName = searchParams.get("column");

    const config = getServerConfig();
    if (!config) {
      return NextResponse.json(
        { success: false, error: "No Snowflake connection found. Please connect first." },
        { status: 401 }
      );
    }

    const conn = await snowflakePool.getConnection(config);

    let sql: string;
    let binds: string[] = [];

    if (datasetId && columnName) {
      // Fetch only rules assigned to this column for this dataset
      sql = `
        SELECT
          rm.RULE_ID,
          rm.RULE_NAME,
          rm.RULE_TYPE,
          rm.RULE_LEVEL,
          rm.DESCRIPTION,
          drc.THRESHOLD_VALUE
        FROM ${dqDatabase.toUpperCase()}.${dqSchema.toUpperCase()}.DATASET_RULE_CONFIG drc
        JOIN ${dqDatabase.toUpperCase()}.${dqSchema.toUpperCase()}.RULE_MASTER rm
          ON drc.RULE_ID = rm.RULE_ID
        WHERE drc.DATASET_ID = ?
          AND UPPER(drc.COLUMN_NAME) = ?
          AND drc.IS_ACTIVE = TRUE
          AND rm.IS_ACTIVE = TRUE
        ORDER BY rm.RULE_TYPE, rm.RULE_NAME
      `;
      binds = [datasetId, columnName.toUpperCase()];

    } else if (datasetId) {
      // Fetch all rules assigned to this dataset (any column)
      sql = `
        SELECT
          rm.RULE_ID,
          rm.RULE_NAME,
          rm.RULE_TYPE,
          rm.RULE_LEVEL,
          rm.DESCRIPTION,
          drc.THRESHOLD_VALUE,
          drc.COLUMN_NAME
        FROM ${dqDatabase.toUpperCase()}.${dqSchema.toUpperCase()}.DATASET_RULE_CONFIG drc
        JOIN ${dqDatabase.toUpperCase()}.${dqSchema.toUpperCase()}.RULE_MASTER rm
          ON drc.RULE_ID = rm.RULE_ID
        WHERE drc.DATASET_ID = ?
          AND drc.IS_ACTIVE = TRUE
          AND rm.IS_ACTIVE = TRUE
        ORDER BY rm.RULE_LEVEL, rm.RULE_TYPE, rm.RULE_NAME
      `;
      binds = [datasetId];

    } else {
      // Fallback: all active rules
      sql = `
        SELECT
          r.RULE_ID,
          r.RULE_NAME,
          r.RULE_TYPE,
          r.RULE_LEVEL,
          r.DESCRIPTION
        FROM ${dqDatabase.toUpperCase()}.${dqSchema.toUpperCase()}.RULE_MASTER r
        WHERE r.IS_ACTIVE = TRUE
        ORDER BY r.RULE_LEVEL, r.RULE_TYPE, r.RULE_NAME
      `;
    }

    const rows = await new Promise<any[]>((resolve, reject) => {
      conn.execute({
        sqlText: sql,
        binds: binds,
        complete: (err: any, _stmt: any, rows: any[]) => {
          if (err) reject(err);
          else resolve(rows || []);
        },
      });
    });

    const data = rows.map((r) => ({
      rule_id: r["RULE_ID"],
      rule_name: r["RULE_NAME"],
      rule_type: r["RULE_TYPE"],
      rule_level: r["RULE_LEVEL"],
      description: r["DESCRIPTION"],
      threshold_value: r["THRESHOLD_VALUE"] ?? null,
      column_name: r["COLUMN_NAME"] ?? null,
    }));

    return NextResponse.json({ success: true, data });
  } catch (error: any) {
    console.error("GET /api/dq/rules error:", error);
    return NextResponse.json(
      { success: false, error: error.message || "Failed to fetch rules" },
      { status: 500 }
    );
  }
}
