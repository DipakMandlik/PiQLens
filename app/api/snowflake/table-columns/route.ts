import { NextRequest, NextResponse } from "next/server";
import { getServerConfig } from "@/lib/server-config";
import { snowflakePool } from "@/lib/snowflake";

// GET /api/snowflake/table-columns?database=...&schema=...&table=...
// Returns columns from the table: [{ name, type }]
export async function GET(request: NextRequest) {
  try {
    const searchParams = request.nextUrl.searchParams;
    const database = searchParams.get("database");
    const schema = searchParams.get("schema");
    const table = searchParams.get("table");

    if (!database || !schema || !table) {
      return NextResponse.json(
        { success: false, error: "Missing required params: database, schema, table" },
        { status: 400 }
      );
    }

    const config = getServerConfig();
    if (!config) {
      return NextResponse.json(
        { success: false, error: "No Snowflake connection found. Please connect first." },
        { status: 401 }
      );
    }

    const conn = await snowflakePool.getConnection(config);

    // Query INFORMATION_SCHEMA.COLUMNS to list all columns for the table
    const sql = `
      SELECT
        COLUMN_NAME,
        DATA_TYPE,
        ORDINAL_POSITION
      FROM ${database.toUpperCase()}.INFORMATION_SCHEMA.COLUMNS
      WHERE TABLE_SCHEMA = '${schema.toUpperCase()}'
        AND TABLE_NAME = '${table.toUpperCase()}'
      ORDER BY ORDINAL_POSITION
    `;

    const rows = await new Promise<any[]>((resolve, reject) => {
      conn.execute({
        sqlText: sql,
        complete: (err: any, _stmt: any, rows: any[]) => {
          if (err) reject(err);
          else resolve(rows || []);
        },
      });
    });

    const columns = rows.map((r) => ({
      name: r["COLUMN_NAME"],
      type: r["DATA_TYPE"],
    }));

    return NextResponse.json({ success: true, data: columns });
  } catch (error: any) {
    console.error("GET /api/snowflake/table-columns error:", error);
    return NextResponse.json(
      { success: false, error: error.message || "Failed to fetch table columns" },
      { status: 500 }
    );
  }
}
