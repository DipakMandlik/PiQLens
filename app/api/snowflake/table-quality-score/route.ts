import { NextRequest, NextResponse } from "next/server";
import { getServerConfig } from "@/lib/server-config";
import { snowflakePool } from "@/lib/snowflake";

/**
 * GET /api/snowflake/table-quality-score
 * Fetches quality score for a specific table from DQ_DAILY_SUMMARY
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

    // Build query to fetch quality score and completeness
    // Prefers today's data if available, otherwise uses latest available date
    const query = `
      SELECT
        DQ_SCORE,
        COMPLETENESS_SCORE,
        UNIQUENESS_SCORE,
        CONSISTENCY_SCORE,
        VALIDITY_SCORE,
        PASSED_CHECKS,
        FAILED_CHECKS,
        SUMMARY_DATE
      FROM DATA_QUALITY_DB.DQ_METRICS.DQ_DAILY_SUMMARY
      WHERE TABLE_NAME = '${table.toUpperCase()}'
        AND SCHEMA_NAME = '${schema.toUpperCase()}'
        AND DATABASE_NAME = '${database.toUpperCase()}'
        AND SUMMARY_DATE = COALESCE(
          (SELECT MAX(SUMMARY_DATE)
           FROM DATA_QUALITY_DB.DQ_METRICS.DQ_DAILY_SUMMARY
           WHERE TABLE_NAME = '${table.toUpperCase()}'
             AND SCHEMA_NAME = '${schema.toUpperCase()}'
             AND DATABASE_NAME = '${database.toUpperCase()}'
             AND SUMMARY_DATE = CURRENT_DATE),
          (SELECT MAX(SUMMARY_DATE)
           FROM DATA_QUALITY_DB.DQ_METRICS.DQ_DAILY_SUMMARY
           WHERE TABLE_NAME = '${table.toUpperCase()}'
             AND SCHEMA_NAME = '${schema.toUpperCase()}'
             AND DATABASE_NAME = '${database.toUpperCase()}')
        )
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

    // Extract quality score and completeness from result
    let qualityScore = null;
    let completeness = null;
    let uniqueness = null;
    let consistency = null;
    let validity = null;
    let passedChecks = null;
    let failedChecks = null;
    let summaryDate = null;
    if (result && result.length > 0) {
      qualityScore = result[0]["DQ_SCORE"];
      completeness = result[0]["COMPLETENESS_SCORE"];
      uniqueness = result[0]["UNIQUENESS_SCORE"];
      consistency = result[0]["CONSISTENCY_SCORE"];
      validity = result[0]["VALIDITY_SCORE"];
      passedChecks = result[0]["PASSED_CHECKS"];
      failedChecks = result[0]["FAILED_CHECKS"];
      summaryDate = result[0]["SUMMARY_DATE"];
    }

    return NextResponse.json({
      success: true,
      data: {
        qualityScore: qualityScore,
        completeness: completeness,
        uniqueness: uniqueness,
        consistency: consistency,
        validity: validity,
        passedChecks: passedChecks,
        failedChecks: failedChecks,
        summaryDate: summaryDate,
        tableName: table,
        schemaName: schema,
        databaseName: database,
      },
    });
  } catch (error: any) {
    console.error("Error fetching table quality score:", error);
    return NextResponse.json(
      {
        success: false,
        error: error.message || "Failed to fetch table quality score",
      },
      { status: 500 }
    );
  }
}
