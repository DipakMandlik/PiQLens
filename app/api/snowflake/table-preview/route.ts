import { NextRequest, NextResponse } from "next/server";
import { getServerConfig } from "@/lib/server-config";
import { snowflakePool } from "@/lib/snowflake";

/**
 * GET /api/snowflake/table-preview
 * Fetches preview data (first 100 rows) from a specific Snowflake table
 * 
 * Query Parameters:
 * - database: Database name (required)
 * - schema: Schema name (required)
 * - table: Table name (required)
 */
export async function GET(request: NextRequest) {
  try {
    const searchParams = request.nextUrl.searchParams;
    const database = searchParams.get("database");
    const schema = searchParams.get("schema");
    const table = searchParams.get("table");

    // Validate required parameters
    if (!database || !schema || !table) {
      return NextResponse.json(
        {
          success: false,
          error: "Missing required parameters: database, schema, and table are required",
        },
        { status: 400 }
      );
    }

    // Get server configuration (credentials stored after connection)
    const config = getServerConfig();
    if (!config) {
      return NextResponse.json(
        {
          success: false,
          error: "No Snowflake connection found. Please connect first.",
        },
        { status: 401 }
      );
    }

    // Get connection from pool
    const connection = await snowflakePool.getConnection(config);

    // Build query to fetch first 100 rows from the table
    // Using fully qualified table name to avoid context issues
    const query = `SELECT * FROM ${database}.${schema}.${table} LIMIT 100`;

    // Execute query
    const result = await new Promise<any>((resolve, reject) => {
      connection.execute({
        sqlText: query,
        complete: (err: any, stmt: any, rows: any[]) => {
          if (err) {
            reject(err);
          } else {
            resolve({ rows, columns: stmt.getColumns() });
          }
        },
      });
    });

    // Transform columns metadata
    const columns = result.columns.map((col: any) => ({
      name: col.getName(),
      type: col.getType(),
      nullable: col.isNullable(),
      scale: col.getScale(),
      precision: col.getPrecision(),
    }));

    return NextResponse.json({
      success: true,
      data: {
        columns,
        rows: result.rows,
        rowCount: result.rows.length,
      },
    });
  } catch (error: any) {
    console.error("Error fetching table preview:", error);
    return NextResponse.json(
      {
        success: false,
        error: error.message || "Failed to fetch table preview",
      },
      { status: 500 }
    );
  }
}
