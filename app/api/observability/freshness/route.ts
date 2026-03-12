import { NextRequest, NextResponse } from "next/server";
import { getServerConfig } from "@/lib/server-config";
import { snowflakePool } from "@/lib/snowflake";
import { getOrSetCache, buildCacheKey } from "@/lib/valkey";

/**
 * GET /api/observability/freshness
 * Returns table freshness metrics derived from INFORMATION_SCHEMA
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

        const valkeyKey = buildCacheKey('observability', 'freshness', `${database}:${schema}:${table}`);

        const responseData = await getOrSetCache(valkeyKey, 600, async () => {
            const config = getServerConfig();
            if (!config) {
                throw new Error("AUTH_FAILED: No Snowflake connection found. Please connect first.");
            }

            const connection = await snowflakePool.getConnection(config);

            // Query INFORMATION_SCHEMA.TABLES for freshness metadata
            const query = `
              SELECT 
                LAST_ALTERED,
                CREATED,
                ROW_COUNT,
                TIMESTAMPDIFF(MINUTE, LAST_ALTERED, CURRENT_TIMESTAMP()) AS FRESHNESS_DELAY_MINUTES
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
                    lastAlteredAt: null,
                    createdAt: null,
                    freshnessDelayMinutes: null,
                    slaStatus: "Unknown",
                };
            }

            const row = result[0];
            const lastAlteredRaw = row["LAST_ALTERED"];
            const createdRaw = row["CREATED"];
            const freshnessDelayMinutes = row["FRESHNESS_DELAY_MINUTES"] || 0;

            // Format dates as ISO strings to prevent "Invalid Date" on client
            const lastAlteredAt = lastAlteredRaw
                ? new Date(lastAlteredRaw).toISOString()
                : null;
            const createdAt = createdRaw
                ? new Date(createdRaw).toISOString()
                : null;

            // Also provide human-readable formatted versions
            const lastAlteredFormatted = lastAlteredRaw
                ? new Date(lastAlteredRaw).toLocaleString("en-US", {
                    dateStyle: "medium",
                    timeStyle: "short"
                })
                : null;
            const createdFormatted = createdRaw
                ? new Date(createdRaw).toLocaleString("en-US", {
                    dateStyle: "medium",
                    timeStyle: "short"
                })
                : null;

            // Determine SLA status based on delay thresholds
            // < 60 min = On-time, 60-1440 min (1 day) = Delayed, > 1440 min = Stale
            let slaStatus = "On-time";
            if (freshnessDelayMinutes > 1440) {
                slaStatus = "Stale";
            } else if (freshnessDelayMinutes > 60) {
                slaStatus = "Delayed";
            }

            // Human-readable freshness delay
            let freshnessDelayFormatted = "Just now";
            if (freshnessDelayMinutes >= 1440) {
                const days = Math.floor(freshnessDelayMinutes / 1440);
                freshnessDelayFormatted = `${days} day${days > 1 ? "s" : ""} ago`;
            } else if (freshnessDelayMinutes >= 60) {
                const hours = Math.floor(freshnessDelayMinutes / 60);
                freshnessDelayFormatted = `${hours} hour${hours > 1 ? "s" : ""} ago`;
            } else if (freshnessDelayMinutes > 0) {
                freshnessDelayFormatted = `${Math.round(freshnessDelayMinutes)} min ago`;
            }

            return {
                lastAlteredAt,
                lastAlteredFormatted,
                createdAt,
                createdFormatted,
                freshnessDelayMinutes,
                freshnessDelayFormatted,
                slaStatus,
            };
        });

        return NextResponse.json({
            success: true,
            data: responseData,
            metadata: { cached: true, timestamp: new Date().toISOString() }
        });
    } catch (error: any) {
        console.error("Error fetching freshness metrics:", error);

        if (error.message?.includes('AUTH_FAILED')) {
            return NextResponse.json(
                { success: false, error: "No Snowflake connection found. Please connect first." },
                { status: 401 }
            );
        }

        return NextResponse.json(
            {
                success: false,
                error: error.message || "Failed to fetch freshness metrics",
            },
            { status: 500 }
        );
    }
}
