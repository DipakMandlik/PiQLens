import { NextRequest, NextResponse } from "next/server";
import { getServerConfig } from "@/lib/server-config";
import { snowflakePool, executeQuery, ensureConnectionContext } from "@/lib/snowflake";

/**
 * GET /api/dq/checks/history
 * Returns execution history for a specific rule on a specific table/column
 *
 * Query Parameters:
 * - database: Database name (required)
 * - schema: Schema name (required)
 * - table: Table name (required)
 * - ruleName: Rule name (required)
 * - column: Column name (optional, for column-level rules)
 * - limit: Number of results (optional, default: 20)
 */
export async function GET(request: NextRequest) {
    try {
        const searchParams = request.nextUrl.searchParams;
        const database = searchParams.get("database");
        const schema = searchParams.get("schema");
        const table = searchParams.get("table");
        const ruleName = searchParams.get("ruleName");
        const column = searchParams.get("column");
        const limit = parseInt(searchParams.get("limit") || "20");

        if (!database || !schema || !table || !ruleName) {
            return NextResponse.json(
                { success: false, error: "Missing required parameters: database, schema, table, ruleName" },
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

        const connection = await snowflakePool.getConnection(config);
        await ensureConnectionContext(connection, config);

        const db = database.toUpperCase();
        const sch = schema.toUpperCase();
        const tbl = table.toUpperCase();

        const columnFilter = column
            ? `AND UPPER(COLUMN_NAME) = '${column.toUpperCase()}'`
            : `AND COLUMN_NAME IS NULL`;

        const historyQuery = `
            SELECT
                RUN_ID,
                CHECK_TIMESTAMP,
                CHECK_STATUS,
                PASS_RATE,
                TOTAL_RECORDS,
                VALID_RECORDS,
                INVALID_RECORDS,
                THRESHOLD,
                EXECUTION_TIME_MS,
                FAILURE_REASON,
                SCAN_SCOPE
            FROM DATA_QUALITY_DB.DQ_METRICS.DQ_CHECK_RESULTS
            WHERE DATABASE_NAME = '${db}'
              AND SCHEMA_NAME = '${sch}'
              AND TABLE_NAME = '${tbl}'
              AND RULE_NAME = '${ruleName.toUpperCase()}'
              ${columnFilter}
            ORDER BY CHECK_TIMESTAMP DESC
            LIMIT ${limit}
        `;

        const result = await executeQuery(connection, historyQuery);

        const data = result.rows.map((row: any) => {
            const obj: any = {};
            result.columns.forEach((col: string, idx: number) => {
                obj[col] = row[idx];
            });

            // Standardize status
            let status = (obj.CHECK_STATUS || "").toUpperCase();
            if (status === "PASS") status = "PASSED";
            if (status === "FAIL") status = "FAILED";
            if (!["PASSED", "FAILED", "WARNING", "SKIPPED", "ERROR"].includes(status)) {
                status = "OTHER";
            }

            return {
                runId: obj.RUN_ID,
                timestamp: obj.CHECK_TIMESTAMP ? new Date(obj.CHECK_TIMESTAMP).toISOString() : null,
                status,
                passRate: obj.PASS_RATE != null ? Number(obj.PASS_RATE) : null,
                totalRecords: obj.TOTAL_RECORDS || 0,
                validRecords: obj.VALID_RECORDS || 0,
                invalidRecords: obj.INVALID_RECORDS || 0,
                threshold: obj.THRESHOLD,
                executionTimeMs: obj.EXECUTION_TIME_MS || 0,
                failureReason: obj.FAILURE_REASON,
                scanScope: obj.SCAN_SCOPE || "FULL"
            };
        });

        return NextResponse.json({ success: true, data });

    } catch (error: any) {
        console.error("[Check History] Error:", error);
        return NextResponse.json(
            { success: false, error: error.message || "Failed to fetch check history" },
            { status: 500 }
        );
    }
}
