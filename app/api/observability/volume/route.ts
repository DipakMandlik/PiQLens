import { NextRequest, NextResponse } from "next/server";
import { getServerConfig } from "@/lib/server-config";
import { snowflakePool } from "@/lib/snowflake";
import { getOrSetCache, buildCacheKey } from "@/lib/valkey";

/**
 * GET /api/observability/volume
 * Returns table volume metrics derived from INFORMATION_SCHEMA
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

        const valkeyKey = buildCacheKey('observability', 'volume', `${database}:${schema}:${table}`);

        const responseData = await getOrSetCache(valkeyKey, 600, async () => {
            const config = getServerConfig();
            if (!config) {
                throw new Error("AUTH_FAILED: No Snowflake connection found. Please connect first.");
            }

            const connection = await snowflakePool.getConnection(config);

            // Query INFORMATION_SCHEMA.TABLES for volume metadata
            const query = `
              SELECT 
                ROW_COUNT,
                BYTES,
                CREATED,
                LAST_ALTERED
              FROM ${database.toUpperCase()}.INFORMATION_SCHEMA.TABLES 
              WHERE TABLE_SCHEMA = '${schema.toUpperCase()}' 
                AND TABLE_NAME = '${table.toUpperCase()}'
            `;

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

            if (!result || result.length === 0) {
                return {
                    rowCount: null,
                    bytes: null,
                    bytesFormatted: null,
                    createdAt: null,
                    lastAlteredAt: null,
                };
            }

            const row = result[0];
            const bytes = row["BYTES"] || 0;

            // Format bytes to human-readable
            let bytesFormatted = "0 B";
            if (bytes >= 1073741824) {
                bytesFormatted = `${(bytes / 1073741824).toFixed(2)} GB`;
            } else if (bytes >= 1048576) {
                bytesFormatted = `${(bytes / 1048576).toFixed(2)} MB`;
            } else if (bytes >= 1024) {
                bytesFormatted = `${(bytes / 1024).toFixed(2)} KB`;
            } else {
                bytesFormatted = `${bytes} B`;
            }

            return {
                rowCount: row["ROW_COUNT"] || 0,
                bytes,
                bytesFormatted,
                createdAt: row["CREATED"],
                lastAlteredAt: row["LAST_ALTERED"],
            };
        });

        return NextResponse.json({
            success: true,
            data: responseData,
            metadata: { cached: true, timestamp: new Date().toISOString() }
        });
    } catch (error: any) {
        console.error("Error fetching volume metrics:", error);

        if (error.message?.includes('AUTH_FAILED')) {
            return NextResponse.json(
                { success: false, error: "No Snowflake connection found. Please connect first." },
                { status: 401 }
            );
        }

        return NextResponse.json(
            {
                success: false,
                error: error.message || "Failed to fetch volume metrics",
            },
            { status: 500 }
        );
    }
}
