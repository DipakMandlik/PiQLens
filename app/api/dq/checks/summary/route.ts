import { NextRequest, NextResponse } from "next/server";
import { getServerConfig } from "@/lib/server-config";
import { snowflakePool, executeQuery, ensureConnectionContext } from "@/lib/snowflake";

/**
 * GET /api/dq/checks/summary
 * Returns dual-layer summary: inventory (static config) + execution (dynamic results)
 *
 * Query Parameters:
 * - database: Database name (required)
 * - schema: Schema name (required)
 * - table: Table name (required)
 * - date: Filter date YYYY-MM-DD (optional, default = today)
 * - mode: 'today' | 'all' (optional, default = 'today')
 */
export async function GET(request: NextRequest) {
    try {
        const searchParams = request.nextUrl.searchParams;
        const database = searchParams.get("database");
        const schema = searchParams.get("schema");
        const table = searchParams.get("table");
        const mode = searchParams.get("mode") || "today";
        const date = searchParams.get("date"); // YYYY-MM-DD

        if (!database || !schema || !table) {
            return NextResponse.json(
                { success: false, error: "Missing required parameters: database, schema, table" },
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

        // 1. INVENTORY: Static check definitions from DATASET_RULE_CONFIG + RULE_MASTER
        const inventoryQuery = `
            SELECT
                COUNT(*) AS TOTAL_ACTIVE,
                COUNT(CASE WHEN rm.RULE_LEVEL = 'COLUMN' THEN 1 END) AS COLUMN_LEVEL,
                COUNT(CASE WHEN rm.RULE_LEVEL = 'TABLE' THEN 1 END) AS TABLE_LEVEL,
                COUNT(CASE WHEN dc.CRITICALITY = 'CRITICAL' THEN 1 END) AS CRITICAL_DATASETS
            FROM DATA_QUALITY_DB.DQ_CONFIG.DATASET_RULE_CONFIG drc
            JOIN DATA_QUALITY_DB.DQ_CONFIG.DATASET_CONFIG dc ON drc.DATASET_ID = dc.DATASET_ID
            JOIN DATA_QUALITY_DB.DQ_CONFIG.RULE_MASTER rm ON drc.RULE_ID = rm.RULE_ID
            WHERE dc.SOURCE_DATABASE = '${db}'
              AND dc.SOURCE_SCHEMA = '${sch}'
              AND dc.SOURCE_TABLE = '${tbl}'
              AND drc.IS_ACTIVE = TRUE
              AND rm.IS_ACTIVE = TRUE
        `;

        // Inventory by rule type
        const inventoryByTypeQuery = `
            SELECT
                rm.RULE_TYPE,
                COUNT(*) AS COUNT
            FROM DATA_QUALITY_DB.DQ_CONFIG.DATASET_RULE_CONFIG drc
            JOIN DATA_QUALITY_DB.DQ_CONFIG.DATASET_CONFIG dc ON drc.DATASET_ID = dc.DATASET_ID
            JOIN DATA_QUALITY_DB.DQ_CONFIG.RULE_MASTER rm ON drc.RULE_ID = rm.RULE_ID
            WHERE dc.SOURCE_DATABASE = '${db}'
              AND dc.SOURCE_SCHEMA = '${sch}'
              AND dc.SOURCE_TABLE = '${tbl}'
              AND drc.IS_ACTIVE = TRUE
              AND rm.IS_ACTIVE = TRUE
            GROUP BY rm.RULE_TYPE
            ORDER BY rm.RULE_TYPE
        `;

        // 2. EXECUTION: Dynamic check results
        const dateFilter = mode === "today"
            ? date ? `AND CHECK_TIMESTAMP::DATE = '${date}'` : `AND CHECK_TIMESTAMP::DATE = CURRENT_DATE()`
            : ""; // all-time = no date filter

        const executionQuery = `
            SELECT
                COUNT(*) AS EXECUTED,
                COUNT(CASE WHEN CHECK_STATUS IN ('PASS', 'PASSED') THEN 1 END) AS PASSED,
                COUNT(CASE WHEN CHECK_STATUS IN ('FAIL', 'FAILED') THEN 1 END) AS FAILED,
                COUNT(CASE WHEN CHECK_STATUS = 'WARNING' THEN 1 END) AS WARNING,
                COUNT(CASE WHEN CHECK_STATUS = 'SKIPPED' THEN 1 END) AS SKIPPED,
                COUNT(CASE WHEN CHECK_STATUS = 'ERROR' THEN 1 END) AS ERROR_COUNT,
                COUNT(CASE WHEN CHECK_STATUS NOT IN ('PASS', 'PASSED', 'FAIL', 'FAILED', 'WARNING', 'SKIPPED', 'ERROR') THEN 1 END) AS OTHER,
                MAX(CHECK_TIMESTAMP) AS LAST_RUN_TIME
            FROM DATA_QUALITY_DB.DQ_METRICS.DQ_CHECK_RESULTS
            WHERE DATABASE_NAME = '${db}'
              AND SCHEMA_NAME = '${sch}'
              AND TABLE_NAME = '${tbl}'
              ${dateFilter}
        `;

        // 3. COMPARISON: Yesterday's execution for trend
        const comparisonDate = date
            ? `DATEADD(day, -1, '${date}'::DATE)`
            : `DATEADD(day, -1, CURRENT_DATE())`;

        const comparisonQuery = `
            SELECT
                COUNT(*) AS YESTERDAY_EXECUTED,
                COUNT(CASE WHEN CHECK_STATUS IN ('FAIL', 'FAILED') THEN 1 END) AS YESTERDAY_FAILED
            FROM DATA_QUALITY_DB.DQ_METRICS.DQ_CHECK_RESULTS
            WHERE DATABASE_NAME = '${db}'
              AND SCHEMA_NAME = '${sch}'
              AND TABLE_NAME = '${tbl}'
              AND CHECK_TIMESTAMP::DATE = ${comparisonDate}
        `;

        // Execute all queries in parallel
        let inventory = { totalActive: 0, columnLevel: 0, tableLevel: 0, critical: 0, byType: {} as Record<string, number> };
        let execution = { executed: 0, passed: 0, failed: 0, warning: 0, skipped: 0, error: 0, other: 0, lastRunTime: null as string | null };
        let comparison = { yesterdayExecuted: 0, yesterdayFailed: 0, trend: "stable" as string };

        const [invResult, invTypeResult, execResult, compResult] = await Promise.all([
            executeQuery(connection, inventoryQuery).catch((e: any) => {
                console.warn("[Checks Summary] Inventory query failed:", e.message);
                return { rows: [] as any[], columns: [] as string[] };
            }),
            executeQuery(connection, inventoryByTypeQuery).catch((e: any) => {
                console.warn("[Checks Summary] Inventory by type query failed:", e.message);
                return { rows: [] as any[], columns: [] as string[] };
            }),
            executeQuery(connection, executionQuery).catch((e: any) => {
                console.warn("[Checks Summary] Execution query failed:", e.message);
                return { rows: [] as any[], columns: [] as string[] };
            }),
            mode === "today"
                ? executeQuery(connection, comparisonQuery).catch((e: any) => {
                    console.warn("[Checks Summary] Comparison query failed:", e.message);
                    return { rows: [] as any[], columns: [] as string[] };
                })
                : Promise.resolve({ rows: [] as any[], columns: [] as string[] })
        ]);

        // Parse inventory
        if (invResult.rows.length > 0) {
            const row = invResult.rows[0];
            const cols = invResult.columns;
            const g = (name: string) => row[cols.indexOf(name)] || 0;
            inventory.totalActive = g("TOTAL_ACTIVE");
            inventory.columnLevel = g("COLUMN_LEVEL");
            inventory.tableLevel = g("TABLE_LEVEL");
            inventory.critical = g("CRITICAL_DATASETS");
        }

        // Parse inventory by type
        const byType: Record<string, number> = {};
        for (const row of invTypeResult.rows) {
            const ruleType = row[invTypeResult.columns.indexOf("RULE_TYPE")];
            const count = row[invTypeResult.columns.indexOf("COUNT")] || 0;
            if (ruleType) byType[ruleType] = count;
        }
        inventory.byType = byType;

        // Parse execution
        if (execResult.rows.length > 0) {
            const row = execResult.rows[0];
            const cols = execResult.columns;
            const g = (name: string) => row[cols.indexOf(name)] || 0;
            execution.executed = g("EXECUTED");
            execution.passed = g("PASSED");
            execution.failed = g("FAILED");
            execution.warning = g("WARNING");
            execution.skipped = g("SKIPPED");
            execution.error = g("ERROR_COUNT");
            execution.other = g("OTHER");
            const lastRun = row[cols.indexOf("LAST_RUN_TIME")];
            execution.lastRunTime = lastRun ? new Date(lastRun).toISOString() : null;
        }

        // Parse comparison
        if (compResult.rows.length > 0) {
            const row = compResult.rows[0];
            const cols = compResult.columns;
            const g = (name: string) => row[cols.indexOf(name)] || 0;
            comparison.yesterdayExecuted = g("YESTERDAY_EXECUTED");
            comparison.yesterdayFailed = g("YESTERDAY_FAILED");

            if (execution.failed > comparison.yesterdayFailed) comparison.trend = "worse";
            else if (execution.failed < comparison.yesterdayFailed) comparison.trend = "better";
            else comparison.trend = "stable";
        }

        return NextResponse.json({
            success: true,
            data: { inventory, execution, comparison }
        });

    } catch (error: any) {
        console.error("[Checks Summary] Error:", error);
        return NextResponse.json(
            { success: false, error: error.message || "Failed to fetch checks summary" },
            { status: 500 }
        );
    }
}
