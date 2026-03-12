import { NextRequest, NextResponse } from "next/server";
import { getServerConfig } from "@/lib/server-config";
import { snowflakePool } from "@/lib/snowflake";
import { getOrSetCache, CacheTTL } from "@/lib/cache-service";
import { getTableMetadataCacheKey } from "@/lib/session-service";

export async function GET(request: NextRequest) {
    try {
        const searchParams = request.nextUrl.searchParams;
        const database = searchParams.get("database");
        const schema = searchParams.get("schema");
        const table = searchParams.get("table");

        if (!database || !schema || !table) {
            return NextResponse.json(
                { success: false, error: "Missing required parameters: database, schema, and table are required" },
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

        const cacheKey = getTableMetadataCacheKey(database, schema, table);

        const cachedData = await getOrSetCache(
            cacheKey,
            async () => {
                const connection = await snowflakePool.getConnection(config);

                // Removed USE DATABASE to prevent session context leakage

                // Query to fetch table metadata using fully qualified identifiers safely
                const query = `
                  SELECT 
                    t.ROW_COUNT,
                    t.BYTES,
                    t.CREATED,
                    t.LAST_ALTERED,
                    (SELECT COUNT(*) FROM ${database}.INFORMATION_SCHEMA.COLUMNS c 
                     WHERE c.TABLE_SCHEMA = t.TABLE_SCHEMA 
                       AND c.TABLE_NAME = t.TABLE_NAME
                       AND c.TABLE_CATALOG = t.TABLE_CATALOG) as COLUMN_COUNT,
                    (SELECT COUNT(*) FROM ${database}.INFORMATION_SCHEMA.COLUMNS c 
                     WHERE c.TABLE_SCHEMA = t.TABLE_SCHEMA 
                       AND c.TABLE_NAME = t.TABLE_NAME 
                       AND c.TABLE_CATALOG = t.TABLE_CATALOG
                       AND c.IS_NULLABLE = 'YES') as NULLABLE_COLUMN_COUNT
                  FROM ${database}.INFORMATION_SCHEMA.TABLES t
                  WHERE t.TABLE_SCHEMA = ?
                    AND t.TABLE_NAME = ?
                `;

                const result = await new Promise<any>((resolve, reject) => {
                    connection.execute({
                        sqlText: query,
                        binds: [schema.toUpperCase(), table.toUpperCase()],
                        complete: (err: any, stmt: any, rows: any) => {
                            if (err) {
                                reject(err);
                            } else {
                                resolve(rows);
                            }
                        },
                    });
                });

                if (result && result.length > 0) {
                    const row = result[0];
                    const bytes = row.BYTES || 0;
                    const bytesFormatted = formatBytes(bytes);

                    const columnsQuery = `
                        SELECT 
                            COLUMN_NAME, 
                            DATA_TYPE, 
                            IS_NULLABLE, 
                            ORDINAL_POSITION
                        FROM ${database}.INFORMATION_SCHEMA.COLUMNS
                        WHERE TABLE_SCHEMA = ?
                          AND TABLE_NAME = ?
                        ORDER BY ORDINAL_POSITION
                    `;

                    const columnsResult = await new Promise<any>((resolve, reject) => {
                        connection.execute({
                            sqlText: columnsQuery,
                            binds: [schema.toUpperCase(), table.toUpperCase()],
                            complete: (err: any, stmt: any, rows: any) => {
                                if (err) reject(err);
                                else resolve(rows);
                            }
                        });
                    });

                    const columns = columnsResult.map((col: any) => ({
                        name: col.COLUMN_NAME,
                        dataType: col.DATA_TYPE,
                        isNullable: col.IS_NULLABLE === 'YES',
                        ordinalPosition: col.ORDINAL_POSITION
                    }));

                    return {
                        success: true,
                        data: {
                            rowCount: row.ROW_COUNT || 0,
                            bytes: bytes,
                            bytesFormatted: bytesFormatted,
                            created: row.CREATED,
                            lastAltered: row.LAST_ALTERED,
                            columnCount: row.COLUMN_COUNT || 0,
                            nullableColumnCount: row.NULLABLE_COLUMN_COUNT || 0,
                            columns: columns,
                        },
                    };
                }
                return null;
            },
            CacheTTL.TABLE_METADATA
        );

        if (cachedData) {
            const response = NextResponse.json(cachedData);
            response.headers.set('X-Cache-Key', cacheKey);
            return response;
        }

        return NextResponse.json({ success: false, error: "Table not found" }, { status: 404 });
    } catch (error: any) {
        console.error("Error fetching table metadata:", error);
        return NextResponse.json({ success: false, error: error.message || "Failed to fetch table metadata" }, { status: 500 });
    }
}

function formatBytes(bytes: number): string {
    if (bytes === 0) return "0 Bytes";
    const k = 1024;
    const sizes = ["Bytes", "KB", "MB", "GB", "TB"];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return Math.round((bytes / Math.pow(k, i)) * 100) / 100 + " " + sizes[i];
}
