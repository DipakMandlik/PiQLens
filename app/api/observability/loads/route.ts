import { NextRequest, NextResponse } from "next/server";
import { getServerConfig } from "@/lib/server-config";
import { snowflakePool } from "@/lib/snowflake";

/**
 * GET /api/observability/loads
 * Returns load history and reliability metrics derived from INFORMATION_SCHEMA.LOAD_HISTORY
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

        // Query INFORMATION_SCHEMA.LOAD_HISTORY for load metrics
        // Note: LOAD_HISTORY may not be available for all table types
        const loadHistoryQuery = `
            SELECT 
                TABLE_NAME,
                FILE_NAME,
                LAST_LOAD_TIME,
                ROW_COUNT,
                ROW_PARSED,
                STATUS,
                ERROR_COUNT,
                ERROR_LIMIT
            FROM ${database.toUpperCase()}.INFORMATION_SCHEMA.LOAD_HISTORY
            WHERE TABLE_SCHEMA = '${schema.toUpperCase()}'
                AND TABLE_NAME = '${table.toUpperCase()}'
            ORDER BY LAST_LOAD_TIME DESC
            LIMIT 10
        `;

        // Also get summary stats from table metadata as fallback
        const tableQuery = `
            SELECT 
                ROW_COUNT,
                BYTES,
                LAST_ALTERED,
                CREATED
            FROM ${database.toUpperCase()}.INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = '${schema.toUpperCase()}' 
                AND TABLE_NAME = '${table.toUpperCase()}'
        `;

        let loadHistory: any[] = [];
        let tableStats: any = null;

        // Try to get load history (may fail if not available for table type)
        try {
            loadHistory = await new Promise<any>((resolve, reject) => {
                connection.execute({
                    sqlText: loadHistoryQuery,
                    complete: (err: any, stmt: any, rows: any) => {
                        if (err) {
                            console.log("LOAD_HISTORY not available:", err.message);
                            resolve([]);
                        } else {
                            resolve(rows || []);
                        }
                    },
                });
            });
        } catch (e) {
            console.log("LOAD_HISTORY query failed, using fallback");
            loadHistory = [];
        }

        // Get table stats
        tableStats = await new Promise<any>((resolve, reject) => {
            connection.execute({
                sqlText: tableQuery,
                complete: (err: any, stmt: any, rows: any) => {
                    if (err) {
                        reject(err);
                    } else {
                        resolve(rows?.[0] || null);
                    }
                },
            });
        });

        // Calculate load reliability metrics
        const totalLoads = loadHistory.length;
        const successfulLoads = loadHistory.filter((l: any) =>
            l.STATUS === "LOADED" || l.STATUS === "PARTIALLY_LOADED"
        ).length;
        const failedLoads = loadHistory.filter((l: any) =>
            l.STATUS === "LOAD_FAILED"
        ).length;
        const successRate = totalLoads > 0
            ? Math.round((successfulLoads / totalLoads) * 100)
            : null;

        // Get last successful load
        const lastSuccessfulLoad = loadHistory.find((l: any) =>
            l.STATUS === "LOADED" || l.STATUS === "PARTIALLY_LOADED"
        );
        const lastLoadTime = lastSuccessfulLoad?.LAST_LOAD_TIME
            ? new Date(lastSuccessfulLoad.LAST_LOAD_TIME).toISOString()
            : null;
        const lastLoadTimeFormatted = lastSuccessfulLoad?.LAST_LOAD_TIME
            ? new Date(lastSuccessfulLoad.LAST_LOAD_TIME).toLocaleString("en-US", {
                dateStyle: "medium",
                timeStyle: "short"
            })
            : null;

        // Format load history for response
        const formattedHistory = loadHistory.map((load: any) => ({
            fileName: load.FILE_NAME,
            loadTime: load.LAST_LOAD_TIME
                ? new Date(load.LAST_LOAD_TIME).toISOString()
                : null,
            loadTimeFormatted: load.LAST_LOAD_TIME
                ? new Date(load.LAST_LOAD_TIME).toLocaleString("en-US", {
                    dateStyle: "medium",
                    timeStyle: "short"
                })
                : null,
            rowCount: load.ROW_COUNT,
            rowParsed: load.ROW_PARSED,
            status: load.STATUS,
            errorCount: load.ERROR_COUNT,
        }));

        // Size metrics
        const bytes = tableStats?.BYTES || 0;
        let sizeFormatted = "0 B";
        if (bytes >= 1073741824) {
            sizeFormatted = `${(bytes / 1073741824).toFixed(2)} GB`;
        } else if (bytes >= 1048576) {
            sizeFormatted = `${(bytes / 1048576).toFixed(2)} MB`;
        } else if (bytes >= 1024) {
            sizeFormatted = `${(bytes / 1024).toFixed(2)} KB`;
        } else {
            sizeFormatted = `${bytes} B`;
        }

        return NextResponse.json({
            success: true,
            data: {
                // Load reliability
                totalLoads,
                successfulLoads,
                failedLoads,
                successRate,
                lastLoadTime,
                lastLoadTimeFormatted,

                // Size & growth
                rowCount: tableStats?.ROW_COUNT || 0,
                bytes,
                sizeFormatted,

                // Load history
                loadHistory: formattedHistory,

                // Availability indicator
                loadHistoryAvailable: loadHistory.length > 0,
            },
        });
    } catch (error: any) {
        console.error("Error fetching load metrics:", error);
        return NextResponse.json(
            {
                success: false,
                error: error.message || "Failed to fetch load metrics",
            },
            { status: 500 }
        );
    }
}
