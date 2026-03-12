import { NextRequest, NextResponse } from "next/server";
import { getServerConfig } from "@/lib/server-config";
import { snowflakePool } from "@/lib/snowflake";

// GET /api/dq/dataset-by-table?database=BANKING_DW&schema=BRONZE&table=STG_CUSTOMER
// Returns the dataset_id from DQ_CONFIG.DATASETS table for the given table
export async function GET(request: NextRequest) {
  try {
    const searchParams = request.nextUrl.searchParams;
    const database = (searchParams.get("database") || "").toUpperCase();
    const schema = (searchParams.get("schema") || "").toUpperCase();
    const table = (searchParams.get("table") || "").toUpperCase();
    const dqDatabase = (searchParams.get("dqDatabase") || "DATA_QUALITY_DB").toUpperCase();
    const dqConfigSchema = (searchParams.get("dqConfigSchema") || "DQ_CONFIG").toUpperCase();

    if (!database || !schema || !table) {
      return NextResponse.json(
        { success: false, error: "Missing required parameters: database, schema, table" },
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

    // Query the DATASETS table to find the dataset_id for this table
    const sql = `
      SELECT
        DATASET_ID,
        SOURCE_DATABASE,
        SOURCE_SCHEMA,
        SOURCE_TABLE,
        IS_ACTIVE
      FROM ${dqDatabase}.${dqConfigSchema}.DATASET_CONFIG
      WHERE UPPER(SOURCE_DATABASE) = ?
        AND UPPER(SOURCE_SCHEMA) = ?
        AND UPPER(SOURCE_TABLE) = ?
        AND IS_ACTIVE = TRUE
      LIMIT 1
    `;

    const result = await new Promise<any[]>((resolve, reject) => {
      conn.execute({
        sqlText: sql,
        binds: [database, schema, table],
        complete: (err: any, _stmt: any, rows: any) => {
          if (err) reject(err);
          else resolve(rows || []);
        },
      });
    });

    if (result.length === 0) {
      return NextResponse.json(
        { success: false, error: `No active dataset found for ${database}.${schema}.${table}` },
        { status: 404 }
      );
    }

    const dataset = result[0];
    return NextResponse.json({
      success: true,
      data: {
        dataset_id: dataset.DATASET_ID,
        source_database: dataset.SOURCE_DATABASE,
        source_schema: dataset.SOURCE_SCHEMA,
        source_table: dataset.SOURCE_TABLE,
        is_active: dataset.IS_ACTIVE,
      },
    });
  } catch (error: any) {
    console.error("GET /api/dq/dataset-by-table error:", error);
    return NextResponse.json(
      { success: false, error: error.message || "Failed to resolve dataset" },
      { status: 500 }
    );
  }
}
