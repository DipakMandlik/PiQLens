import { NextRequest, NextResponse } from "next/server";
import { getServerConfig } from "@/lib/server-config";
import { snowflakePool } from "@/lib/snowflake";

/**
 * GET /api/snowflake/table-stats
 * Fetches table statistics including row count
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

    // Get server configuration
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

    // Build query to fetch row count
    // Note: Snowflake stores identifiers in uppercase in INFORMATION_SCHEMA by default
    // Use fully qualified path: DATABASE.INFORMATION_SCHEMA.TABLES
    const query = `SELECT ROW_COUNT FROM ${database.toUpperCase()}.INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '${schema.toUpperCase()}' AND TABLE_NAME = '${table.toUpperCase()}'`;

    // Execute query
    const result = await new Promise<any>((resolve, reject) => {
      connection.execute({
        sqlText: query,
        complete: (err: any, stmt: any, rows: any) => {
          if (err) {
            reject(err);
          } else {
            resolve(rows);
          }
        },
      });
    });

    // Extract row count from result
    let rowCount = 0;
    if (result && result.length > 0) {
      rowCount = result[0]["ROW_COUNT"] || 0;
    }

    return NextResponse.json({
      success: true,
      data: {
        rowCount: rowCount,
        tableName: table,
        schemaName: schema,
        databaseName: database,
      },
    });
  } catch (error: any) {
    console.error("Error fetching table stats:", error);
    return NextResponse.json(
      {
        success: false,
        error: error.message || "Failed to fetch table stats",
      },
      { status: 500 }
    );
  }
}
