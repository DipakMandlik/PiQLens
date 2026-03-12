import { NextRequest, NextResponse } from "next/server";
import { getServerConfig } from "@/lib/server-config";
import { snowflakePool } from "@/lib/snowflake";
import { getOrSetCache, buildCacheKey } from "@/lib/valkey";

/**
 * GET /api/observability/health-score
 * Computes an overall observability health score based on metadata KPIs
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

        const valkeyKey = buildCacheKey('observability', 'health-score', `${database}:${schema}:${table}`);

        const resultData = await getOrSetCache(valkeyKey, 600, async () => {
            const config = getServerConfig();
            if (!config) {
                throw new Error("AUTH_FAILED: No Snowflake connection found. Please connect first.");
            }

            const connection = await snowflakePool.getConnection(config);

            // Query both table and column metadata for health score computation
            const tableQuery = `
              SELECT 
                ROW_COUNT,
                BYTES,
                TIMESTAMPDIFF(MINUTE, LAST_ALTERED, CURRENT_TIMESTAMP()) AS FRESHNESS_DELAY_MINUTES
              FROM ${database.toUpperCase()}.INFORMATION_SCHEMA.TABLES 
              WHERE TABLE_SCHEMA = '${schema.toUpperCase()}' 
                AND TABLE_NAME = '${table.toUpperCase()}'
            `;

            const columnQuery = `
              SELECT 
                COUNT(*) AS COLUMN_COUNT,
                SUM(CASE WHEN IS_NULLABLE = 'YES' THEN 1 ELSE 0 END) AS NULLABLE_COUNT
              FROM ${database.toUpperCase()}.INFORMATION_SCHEMA.COLUMNS 
              WHERE TABLE_SCHEMA = '${schema.toUpperCase()}' 
                AND TABLE_NAME = '${table.toUpperCase()}'
            `;

            const [tableResult, columnResult] = await Promise.all([
                new Promise<any>((resolve, reject) => {
                    connection.execute({
                        sqlText: tableQuery,
                        complete: (err: any, stmt: any, rows: any) => {
                            if (err) reject(err);
                            else resolve(rows);
                        },
                    });
                }),
                new Promise<any>((resolve, reject) => {
                    connection.execute({
                        sqlText: columnQuery,
                        complete: (err: any, stmt: any, rows: any) => {
                            if (err) reject(err);
                            else resolve(rows);
                        },
                    });
                }),
            ]);

            // Calculate component scores
            let freshnessScore = 100;
            let volumeScore = 100;
            let schemaScore = 100;

            if (tableResult && tableResult.length > 0) {
                const freshnessDelay = tableResult[0]["FRESHNESS_DELAY_MINUTES"] || 0;
                const rowCount = tableResult[0]["ROW_COUNT"] || 0;

                // Freshness score: 100 if < 60 min, decreases with delay
                if (freshnessDelay > 1440) {
                    freshnessScore = 50; // Stale
                } else if (freshnessDelay > 60) {
                    freshnessScore = 75; // Delayed
                } else {
                    freshnessScore = 100; // On-time
                }

                // Volume score: 100 if has data, 50 if empty
                volumeScore = rowCount > 0 ? 100 : 50;
            }

            if (columnResult && columnResult.length > 0) {
                const columnCount = columnResult[0]["COLUMN_COUNT"] || 0;
                const nullableCount = columnResult[0]["NULLABLE_COUNT"] || 0;

                // Schema score based on nullable ratio (higher nullable = slightly lower score)
                const nullableRatio = columnCount > 0 ? nullableCount / columnCount : 0;
                schemaScore = Math.round(100 - nullableRatio * 20); // Max 20% penalty for all nullable
            }

            // Weighted overall score
            const overallScore = Math.round(
                freshnessScore * 0.4 + volumeScore * 0.3 + schemaScore * 0.3
            );

            return {
                overallScore,
                freshnessScore,
                volumeScore,
                schemaScore,
            };
        });

        return NextResponse.json({
            success: true,
            data: resultData,
            metadata: { cached: true, timestamp: new Date().toISOString() }
        });
    } catch (error: any) {
        console.error("Error computing health score:", error);

        if (error.message?.includes('AUTH_FAILED')) {
            return NextResponse.json(
                { success: false, error: "No Snowflake connection found. Please connect first." },
                { status: 401 }
            );
        }

        return NextResponse.json(
            {
                success: false,
                error: error.message || "Failed to compute health score",
            },
            { status: 500 }
        );
    }
}
