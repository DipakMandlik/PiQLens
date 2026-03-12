import { NextRequest, NextResponse } from "next/server";
import { getServerConfig } from "@/lib/server-config";
import { snowflakePool } from "@/lib/snowflake";
/**
 * @deprecated Legacy endpoint retained for backward compatibility only.
 * New Failures UI uses /api/dq/failures/* endpoints.
 */


/**
 * GET /api/snowflake/failed-records
 * Fetches failed records for a specific table from DQ_FAILED_RECORDS
 * 
 * Query Parameters:
 * - database: Database name (required)
 * - schema: Schema name (required)
 * - table: Table name (required)
 * - limit: Number of records to fetch (optional, default 100)
 */
export async function GET(request: NextRequest) {
  try {
    const searchParams = request.nextUrl.searchParams;
    const database = searchParams.get("database");
    const schema = searchParams.get("schema");
    const table = searchParams.get("table");
    const limit = searchParams.get("limit") || "100";

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

    // Build query to fetch failed records
    // Note: Assuming DATASET_ID might contain "DATABASE.SCHEMA" format or we filter by table name
    const query = `
      SELECT
        FAILURE_ID,
        CHECK_ID,
        RUN_ID,
        TABLE_NAME,
        COLUMN_NAME,
        RULE_NAME,
        RULE_TYPE,
        FAILURE_TYPE,
        FAILED_RECORD_PK,
        FAILED_COLUMN_VALUE,
        EXPECTED_PATTERN,
        ACTUAL_VALUE_TYPE,
        FAILURE_CATEGORY,
        IS_CRITICAL,
        CAN_AUTO_REMEDIATE,
        REMEDIATION_SUGGESTION,
        DETECTED_TS,
        CREATED_TS
      FROM DATA_QUALITY_DB.DQ_METRICS.DQ_FAILED_RECORDS
      WHERE TABLE_NAME = '${table.toUpperCase()}'
      ORDER BY DETECTED_TS DESC
      LIMIT ${parseInt(limit)}
    `;

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

    // Get column metadata
    const columns = result && result.length > 0 
      ? Object.keys(result[0]).map(col => ({
          name: col,
          type: typeof result[0][col]
        }))
      : [];

    return NextResponse.json({
      success: true,
      data: {
        columns: columns,
        rows: result || [],
        rowCount: result ? result.length : 0,
      },
    });
  } catch (error: any) {
    console.error("Error fetching failed records:", error);
    return NextResponse.json(
      {
        success: false,
        error: error.message || "Failed to fetch failed records",
      },
      { status: 500 }
    );
  }
}
