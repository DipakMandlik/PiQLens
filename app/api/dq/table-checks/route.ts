import { NextRequest, NextResponse } from "next/server";
import { getServerConfig } from "@/lib/server-config";
import { snowflakePool, executeQuery, ensureConnectionContext } from "@/lib/snowflake";

/**
 * GET /api/dq/table-checks
 * Returns enhanced check results with aggregation metrics for the Checks tab
 *
 * Query Parameters:
 * - database: Database name (required)
 * - schema: Schema name (required)
 * - table: Table name (required)
 * - mode: 'today' | 'all' (optional, default = 'all')
 * - date: Filter date YYYY-MM-DD (optional)
 */
export async function GET(request: NextRequest) {
    try {
        const searchParams = request.nextUrl.searchParams;
        const database = searchParams.get("database");
        const schema = searchParams.get("schema");
        const table = searchParams.get("table");
        const mode = searchParams.get("mode") || "all";
        const date = searchParams.get("date");

        if (!database || !schema || !table) {
            return NextResponse.json(
                {
                    success: false,
                    error: "Missing required parameters: database, schema, and table are required",
                },
                { status: 400 }
            );
        }

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

        const connection = await snowflakePool.getConnection(config);
        await ensureConnectionContext(connection, config);

        const db = database.toUpperCase();
        const sch = schema.toUpperCase();
        const tbl = table.toUpperCase();

        // Date filter based on mode
        let dateFilter = "";
        if (mode === "today") {
            dateFilter = date
                ? `AND CHECK_TIMESTAMP::DATE = '${date}'`
                : `AND CHECK_TIMESTAMP::DATE = CURRENT_DATE()`;
        }
        // 'all' mode = no date filter

        // Enhanced query: latest per rule + aggregation metrics
        // Uses window functions to compute per-rule stats in one query
        const checksQuery = `
            WITH AllChecks AS (
                SELECT
                    CHECK_ID,
                    RUN_ID,
                    CHECK_TIMESTAMP,
                    DATASET_ID,
                    DATABASE_NAME,
                    SCHEMA_NAME,
                    TABLE_NAME,
                    COLUMN_NAME,
                    RULE_ID,
                    RULE_NAME,
                    RULE_TYPE,
                    RULE_LEVEL,
                    TOTAL_RECORDS,
                    VALID_RECORDS,
                    INVALID_RECORDS,
                    PASS_RATE,
                    THRESHOLD,
                    CHECK_STATUS,
                    EXECUTION_TIME_MS,
                    FAILURE_REASON,
                    CREATED_TS,
                    ROW_NUMBER() OVER (PARTITION BY RULE_NAME, COALESCE(COLUMN_NAME, '__TABLE__') ORDER BY CHECK_TIMESTAMP DESC) AS RN
                FROM DATA_QUALITY_DB.DQ_METRICS.DQ_CHECK_RESULTS
                WHERE DATABASE_NAME = '${db}'
                    AND SCHEMA_NAME = '${sch}'
                    AND TABLE_NAME = '${tbl}'
                    ${dateFilter}
            ),
            Aggregated AS (
                SELECT
                    RULE_NAME,
                    COALESCE(COLUMN_NAME, '__TABLE__') AS COLUMN_KEY,
                    COUNT(*) AS TOTAL_RUNS,
                    COUNT(CASE WHEN CHECK_STATUS IN ('FAIL', 'FAILED') THEN 1 END) AS FAILURE_COUNT,
                    COUNT(CASE WHEN CHECK_STATUS IN ('PASS', 'PASSED') THEN 1 END) AS PASS_COUNT,
                    MIN(CHECK_TIMESTAMP) AS FIRST_RUN,
                    MAX(CHECK_TIMESTAMP) AS LAST_RUN
                FROM DATA_QUALITY_DB.DQ_METRICS.DQ_CHECK_RESULTS
                WHERE DATABASE_NAME = '${db}'
                    AND SCHEMA_NAME = '${sch}'
                    AND TABLE_NAME = '${tbl}'
                GROUP BY RULE_NAME, COALESCE(COLUMN_NAME, '__TABLE__')
            ),
            PrevStatus AS (
                SELECT
                    RULE_NAME,
                    COALESCE(COLUMN_NAME, '__TABLE__') AS COLUMN_KEY,
                    CHECK_STATUS AS PREV_STATUS
                FROM DATA_QUALITY_DB.DQ_METRICS.DQ_CHECK_RESULTS
                WHERE DATABASE_NAME = '${db}'
                    AND SCHEMA_NAME = '${sch}'
                    AND TABLE_NAME = '${tbl}'
                QUALIFY ROW_NUMBER() OVER (PARTITION BY RULE_NAME, COALESCE(COLUMN_NAME, '__TABLE__') ORDER BY CHECK_TIMESTAMP DESC) = 2
            )
            SELECT
                ac.CHECK_ID,
                ac.RUN_ID,
                ac.CHECK_TIMESTAMP,
                ac.DATASET_ID,
                ac.DATABASE_NAME,
                ac.SCHEMA_NAME,
                ac.TABLE_NAME,
                ac.COLUMN_NAME,
                ac.RULE_ID,
                ac.RULE_NAME,
                ac.RULE_TYPE,
                ac.RULE_LEVEL,
                ac.TOTAL_RECORDS,
                ac.VALID_RECORDS,
                ac.INVALID_RECORDS,
                ac.PASS_RATE,
                ac.THRESHOLD,
                ac.CHECK_STATUS,
                ac.EXECUTION_TIME_MS,
                ac.FAILURE_REASON,
                ac.CREATED_TS,
                agg.TOTAL_RUNS,
                agg.FAILURE_COUNT,
                agg.PASS_COUNT,
                agg.FIRST_RUN,
                agg.LAST_RUN,
                ps.PREV_STATUS
            FROM AllChecks ac
            JOIN Aggregated agg ON ac.RULE_NAME = agg.RULE_NAME
                AND COALESCE(ac.COLUMN_NAME, '__TABLE__') = agg.COLUMN_KEY
            LEFT JOIN PrevStatus ps ON ac.RULE_NAME = ps.RULE_NAME
                AND COALESCE(ac.COLUMN_NAME, '__TABLE__') = ps.COLUMN_KEY
            WHERE ac.RN = 1
            ORDER BY ac.CHECK_TIMESTAMP DESC
        `;

        // Summary query based on date mode
        const summaryQuery = `
            SELECT
                COUNT(*) AS TOTAL_CHECKS,
                COUNT(CASE WHEN CHECK_STATUS IN ('PASS', 'PASSED') THEN 1 END) AS PASSED_CHECKS,
                COUNT(CASE WHEN CHECK_STATUS IN ('FAIL', 'FAILED') THEN 1 END) AS FAILED_CHECKS,
                COUNT(CASE WHEN CHECK_STATUS = 'WARNING' THEN 1 END) AS WARNING_CHECKS,
                COUNT(CASE WHEN CHECK_STATUS = 'SKIPPED' THEN 1 END) AS SKIPPED_CHECKS,
                COUNT(CASE WHEN CHECK_STATUS = 'ERROR' THEN 1 END) AS ERROR_CHECKS,
                COUNT(CASE WHEN CHECK_STATUS NOT IN ('PASS', 'PASSED', 'FAIL', 'FAILED', 'WARNING', 'SKIPPED', 'ERROR') THEN 1 END) AS OTHER_CHECKS,
                MAX(CHECK_TIMESTAMP) AS LAST_RUN_TIME
            FROM DATA_QUALITY_DB.DQ_METRICS.DQ_CHECK_RESULTS
            WHERE DATABASE_NAME = '${db}'
                AND SCHEMA_NAME = '${sch}'
                AND TABLE_NAME = '${tbl}'
                ${dateFilter}
        `;

        let checks: any[] = [];
        let summary = {
            totalChecks: 0,
            passedChecks: 0,
            failedChecks: 0,
            warningChecks: 0,
            skippedChecks: 0,
            errorChecks: 0,
            otherChecks: 0,
            lastRunTime: null as string | null,
            lastRunTimeFormatted: null as string | null,
        };

        // Get checks
        try {
            const checksResult = await executeQuery(connection, checksQuery);
            checks = checksResult.rows.map((row: any) => {
                const obj: any = {};
                checksResult.columns.forEach((col: string, idx: number) => {
                    obj[col] = row[idx];
                });

                // Standardize status
                let status = (obj.CHECK_STATUS || "").toUpperCase();
                if (status === "PASS") status = "PASSED";
                if (status === "FAIL") status = "FAILED";
                if (!["PASSED", "FAILED", "WARNING", "SKIPPED", "ERROR"].includes(status)) {
                    status = "OTHER";
                }

                let prevStatus = (obj.PREV_STATUS || "").toUpperCase();
                if (prevStatus === "PASS") prevStatus = "PASSED";
                if (prevStatus === "FAIL") prevStatus = "FAILED";

                // Compute trend
                let trend: "improved" | "degraded" | "stable" | "new" = "new";
                if (prevStatus) {
                    if (status === "PASSED" && prevStatus !== "PASSED") trend = "improved";
                    else if (status !== "PASSED" && prevStatus === "PASSED") trend = "degraded";
                    else trend = "stable";
                }

                return {
                    checkId: obj.CHECK_ID,
                    runId: obj.RUN_ID,
                    checkTimestamp: obj.CHECK_TIMESTAMP ? new Date(obj.CHECK_TIMESTAMP).toISOString() : null,
                    datasetId: obj.DATASET_ID,
                    columnName: obj.COLUMN_NAME,
                    ruleName: obj.RULE_NAME,
                    ruleType: obj.RULE_TYPE,
                    ruleLevel: obj.RULE_LEVEL || "Medium",
                    totalRecords: obj.TOTAL_RECORDS || 0,
                    validRecords: obj.VALID_RECORDS || 0,
                    invalidRecords: obj.INVALID_RECORDS || 0,
                    passRate: obj.PASS_RATE != null ? Number(obj.PASS_RATE) : null,
                    threshold: obj.THRESHOLD,
                    checkStatus: status,
                    executionTimeMs: obj.EXECUTION_TIME_MS || 0,
                    failureReason: obj.FAILURE_REASON,
                    scope: obj.COLUMN_NAME ? "Column" : "Table",
                    target: obj.COLUMN_NAME || obj.TABLE_NAME,
                    // Aggregation metrics
                    totalRuns: obj.TOTAL_RUNS || 1,
                    failureCount: obj.FAILURE_COUNT || 0,
                    passCount: obj.PASS_COUNT || 0,
                    trend,
                    lastRunTimestamp: obj.LAST_RUN ? new Date(obj.LAST_RUN).toISOString() : null,
                };
            });
        } catch (e: any) {
            console.log("Checks query error:", e.message);
        }

        // Get summary
        try {
            const summaryResult = await executeQuery(connection, summaryQuery);
            if (summaryResult.rows.length > 0) {
                const row = summaryResult.rows[0];
                const cols = summaryResult.columns;
                const getVal = (name: string) => row[cols.indexOf(name)] || 0;

                summary.totalChecks = getVal("TOTAL_CHECKS");
                summary.passedChecks = getVal("PASSED_CHECKS");
                summary.failedChecks = getVal("FAILED_CHECKS");
                summary.warningChecks = getVal("WARNING_CHECKS");
                summary.skippedChecks = getVal("SKIPPED_CHECKS");
                summary.errorChecks = getVal("ERROR_CHECKS");
                summary.otherChecks = getVal("OTHER_CHECKS");

                const lastRun = row[cols.indexOf("LAST_RUN_TIME")];
                if (lastRun) {
                    summary.lastRunTime = new Date(lastRun).toISOString();
                    summary.lastRunTimeFormatted = new Date(lastRun).toLocaleString("en-US", {
                        dateStyle: "medium",
                        timeStyle: "short",
                    });
                }
            }
        } catch (e: any) {
            console.log("Summary query error:", e.message);
        }

        return NextResponse.json({
            success: true,
            data: { summary, checks },
        });
    } catch (error: any) {
        console.error("Error fetching table checks:", error);
        return NextResponse.json(
            {
                success: false,
                error: error.message || "Failed to fetch table checks",
            },
            { status: 500 }
        );
    }
}
