import { NextRequest, NextResponse } from "next/server";
import { getServerConfig } from "@/lib/server-config";
import { snowflakePool } from "@/lib/snowflake";
import { formatDistanceToNow } from "date-fns";

/**
 * GET /api/snowflake/table-load-history
 * Fetches load reliability metrics for a specific table
 * 
 * Query Parameters:
 * - database: Database name (required)
 * - schema: Schema name (required)
 * - table: Table name (required)
 * - days: Number of days to look back (optional, default: 30)
 */
export async function GET(request: NextRequest) {
    try {
        const searchParams = request.nextUrl.searchParams;
        const database = searchParams.get("database");
        const schema = searchParams.get("schema");
        const table = searchParams.get("table");
        const days = parseInt(searchParams.get("days") || "30");

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

        // Try multiple approaches to get load history data
        let loadData = null;

        // Approach 1: Try custom TABLE_LOAD_HISTORY table
        try {
            const customTableQuery = `
                SELECT 
                    COUNT(*) AS TOTAL_LOADS,
                    SUM(CASE WHEN LOAD_STATUS = 'SUCCESS' THEN 1 ELSE 0 END) AS SUCCESSFUL_LOADS,
                    SUM(CASE WHEN LOAD_STATUS = 'FAILED' THEN 1 ELSE 0 END) AS FAILED_LOADS,
                    ROUND(
                        (SUM(CASE WHEN LOAD_STATUS = 'SUCCESS' THEN 1 ELSE 0 END) * 100.0) / 
                        NULLIF(COUNT(*), 0), 
                        2
                    ) AS SUCCESS_RATE,
                    MAX(LOAD_END_TIME) AS LAST_LOAD_TIME,
                    SUM(ROWS_LOADED) AS TOTAL_ROWS_LOADED,
                    SUM(BYTES_LOADED) AS TOTAL_BYTES_LOADED
                FROM DATA_QUALITY_DB.DQ_METRICS.TABLE_LOAD_HISTORY
                WHERE DATABASE_NAME = '${database.toUpperCase()}'
                  AND SCHEMA_NAME = '${schema.toUpperCase()}'
                  AND TABLE_NAME = '${table.toUpperCase()}'
                  AND LOAD_START_TIME >= DATEADD(day, -${days}, CURRENT_TIMESTAMP())
            `;

            const result = await new Promise<any>((resolve, reject) => {
                connection.execute({
                    sqlText: customTableQuery,
                    complete: (err: any, stmt: any, rows: any) => {
                        if (err) {
                            reject(err);
                        } else {
                            resolve(rows);
                        }
                    },
                });
            });

            if (result && result.length > 0 && result[0].TOTAL_LOADS > 0) {
                loadData = result[0];
            }
        } catch (error) {
            console.log("Custom load history table not available, trying alternative approaches...");
        }

        // Approach 2: Try ACCOUNT_USAGE.LOAD_HISTORY (if accessible)
        if (!loadData) {
            try {
                const accountUsageQuery = `
                    SELECT 
                        COUNT(*) AS TOTAL_LOADS,
                        SUM(CASE WHEN STATUS = 'LOADED' THEN 1 ELSE 0 END) AS SUCCESSFUL_LOADS,
                        SUM(CASE WHEN STATUS != 'LOADED' THEN 1 ELSE 0 END) AS FAILED_LOADS,
                        ROUND(
                            (SUM(CASE WHEN STATUS = 'LOADED' THEN 1 ELSE 0 END) * 100.0) / 
                            NULLIF(COUNT(*), 0), 
                            2
                        ) AS SUCCESS_RATE,
                        MAX(LAST_LOAD_TIME) AS LAST_LOAD_TIME,
                        SUM(ROW_COUNT) AS TOTAL_ROWS_LOADED,
                        SUM(FILE_SIZE) AS TOTAL_BYTES_LOADED
                    FROM SNOWFLAKE.ACCOUNT_USAGE.LOAD_HISTORY
                    WHERE TABLE_CATALOG_NAME = '${database.toUpperCase()}'
                      AND TABLE_SCHEMA_NAME = '${schema.toUpperCase()}'
                      AND TABLE_NAME = '${table.toUpperCase()}'
                      AND LAST_LOAD_TIME >= DATEADD(day, -${days}, CURRENT_TIMESTAMP())
                `;

                const result = await new Promise<any>((resolve, reject) => {
                    connection.execute({
                        sqlText: accountUsageQuery,
                        complete: (err: any, stmt: any, rows: any) => {
                            if (err) {
                                reject(err);
                            } else {
                                resolve(rows);
                            }
                        },
                    });
                });

                if (result && result.length > 0 && result[0].TOTAL_LOADS > 0) {
                    loadData = result[0];
                }
            } catch (error) {
                console.log("ACCOUNT_USAGE not accessible, load history unavailable");
            }
        }

        // If we have load data, format and return it
        if (loadData) {
            const totalLoads = loadData.TOTAL_LOADS || 0;
            const successfulLoads = loadData.SUCCESSFUL_LOADS || 0;
            const failedLoads = loadData.FAILED_LOADS || 0;
            const successRate = loadData.SUCCESS_RATE || 0;
            const lastLoadTime = loadData.LAST_LOAD_TIME;
            const totalRows = loadData.TOTAL_ROWS_LOADED || 0;
            const totalBytes = loadData.TOTAL_BYTES_LOADED || 0;

            return NextResponse.json({
                success: true,
                data: {
                    totalLoads,
                    successfulLoads,
                    failedLoads,
                    successRate,
                    lastLoadTime: lastLoadTime ? lastLoadTime : null,
                    lastLoadTimeFormatted: lastLoadTime
                        ? formatDistanceToNow(new Date(lastLoadTime), { addSuffix: true })
                        : null,
                    rowCount: totalRows,
                    bytes: totalBytes,
                    sizeFormatted: formatBytes(totalBytes),
                    loadHistoryAvailable: true,
                },
            });
        }

        // No load history available
        return NextResponse.json({
            success: true,
            data: {
                totalLoads: 0,
                successfulLoads: 0,
                failedLoads: 0,
                successRate: null,
                lastLoadTime: null,
                lastLoadTimeFormatted: null,
                rowCount: 0,
                bytes: 0,
                sizeFormatted: "0 B",
                loadHistoryAvailable: false,
            },
        });
    } catch (error: any) {
        console.error("Error fetching table load history:", error);
        return NextResponse.json(
            {
                success: false,
                error: error.message || "Failed to fetch table load history",
            },
            { status: 500 }
        );
    }
}

/**
 * Helper function to format bytes into human-readable format
 */
function formatBytes(bytes: number): string {
    if (bytes === 0) return "0 B";
    const k = 1024;
    const sizes = ["B", "KB", "MB", "GB", "TB"];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return Math.round((bytes / Math.pow(k, i)) * 100) / 100 + " " + sizes[i];
}
