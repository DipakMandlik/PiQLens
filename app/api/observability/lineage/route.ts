import { NextRequest, NextResponse } from "next/server";
import { getServerConfig } from "@/lib/server-config";
import { snowflakePool } from "@/lib/snowflake";

/**
 * GET /api/observability/lineage
 * Returns lineage and impact information for a table
 * Derives upstream/downstream dependencies from:
 * 1. Schema layer patterns (BRONZE → SILVER → GOLD)
 * 2. Table naming conventions (STG_, DIM_, FACT_)
 * 3. Tables with similar base names across schemas
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
        const tableFullName = `${database.toUpperCase()}.${schema.toUpperCase()}.${table.toUpperCase()}`;
        const tableUpper = table.toUpperCase();
        const schemaUpper = schema.toUpperCase();

        // Define schema layer hierarchy for inferring lineage
        const schemaLayers: Record<string, { upstream: string[], downstream: string[] }> = {
            "RAW": { upstream: [], downstream: ["BRONZE", "STAGING", "STG"] },
            "BRONZE": { upstream: ["RAW"], downstream: ["SILVER", "STAGING", "STG"] },
            "STAGING": { upstream: ["RAW", "BRONZE"], downstream: ["SILVER", "CORE", "CURATED"] },
            "STG": { upstream: ["RAW", "BRONZE"], downstream: ["SILVER", "CORE", "CURATED"] },
            "SILVER": { upstream: ["BRONZE", "STAGING", "STG"], downstream: ["GOLD", "MART", "ANALYTICS", "PRESENTATION"] },
            "CORE": { upstream: ["BRONZE", "STAGING"], downstream: ["GOLD", "MART", "ANALYTICS"] },
            "CURATED": { upstream: ["BRONZE", "STAGING"], downstream: ["GOLD", "MART", "ANALYTICS"] },
            "GOLD": { upstream: ["SILVER", "CORE", "CURATED"], downstream: ["MART", "ANALYTICS", "PRESENTATION"] },
            "MART": { upstream: ["SILVER", "GOLD", "CORE"], downstream: ["ANALYTICS", "PRESENTATION", "BI"] },
            "ANALYTICS": { upstream: ["SILVER", "GOLD", "MART"], downstream: ["PRESENTATION", "BI"] },
            "PRESENTATION": { upstream: ["GOLD", "MART", "ANALYTICS"], downstream: [] },
        };

        // Extract base table name (remove prefixes like STG_, DIM_, FACT_)
        const prefixPattern = /^(STG_|DIM_|FACT_|RAW_|SRC_|TMP_|VW_|V_)/i;
        const baseTableName = tableUpper.replace(prefixPattern, "");

        // Get all schemas in the database
        const schemasQuery = `
            SELECT SCHEMA_NAME 
            FROM ${database.toUpperCase()}.INFORMATION_SCHEMA.SCHEMATA
            WHERE SCHEMA_NAME NOT IN ('INFORMATION_SCHEMA', 'PUBLIC')
            ORDER BY SCHEMA_NAME
        `;

        // Get all tables that might be related (same base name or in related schemas)
        const relatedTablesQuery = `
            SELECT 
                TABLE_SCHEMA,
                TABLE_NAME,
                TABLE_TYPE,
                ROW_COUNT,
                LAST_ALTERED
            FROM ${database.toUpperCase()}.INFORMATION_SCHEMA.TABLES
            WHERE TABLE_NAME LIKE '%${baseTableName}%'
                OR TABLE_NAME LIKE '%${tableUpper.replace("STG_", "").replace("DIM_", "").replace("FACT_", "")}%'
            ORDER BY TABLE_SCHEMA, TABLE_NAME
        `;

        // Get health status for the focus node
        const healthQuery = `
            SELECT 
                ROW_COUNT,
                TIMESTAMPDIFF(MINUTE, LAST_ALTERED, CURRENT_TIMESTAMP()) AS FRESHNESS_DELAY_MINUTES
            FROM ${database.toUpperCase()}.INFORMATION_SCHEMA.TABLES 
            WHERE TABLE_SCHEMA = '${schemaUpper}' 
                AND TABLE_NAME = '${tableUpper}'
        `;

        let upstream: any[] = [];
        let downstream: any[] = [];
        let healthStatus = "unknown";
        let freshnessMinutes = 0;

        // Get related tables
        try {
            const relatedTables = await new Promise<any>((resolve, reject) => {
                connection.execute({
                    sqlText: relatedTablesQuery,
                    complete: (err: any, stmt: any, rows: any) => {
                        if (err) {
                            console.log("Related tables query failed:", err.message);
                            resolve([]);
                        } else {
                            resolve(rows || []);
                        }
                    },
                });
            });

            // Get layer info for current schema
            const currentLayer = schemaLayers[schemaUpper] || { upstream: [], downstream: [] };

            // Categorize related tables as upstream or downstream
            for (const t of relatedTables) {
                const tSchema = t.TABLE_SCHEMA;
                const tName = t.TABLE_NAME;
                const tFullName = `${database.toUpperCase()}.${tSchema}.${tName}`;

                // Skip if same table
                if (tSchema === schemaUpper && tName === tableUpper) continue;

                // Check if this schema is upstream
                if (currentLayer.upstream.includes(tSchema)) {
                    upstream.push({
                        name: tFullName,
                        shortName: tName,
                        type: t.TABLE_TYPE === "VIEW" ? "VIEW" : "TABLE",
                        schema: tSchema,
                    });
                }
                // Check if this schema is downstream
                else if (currentLayer.downstream.includes(tSchema)) {
                    downstream.push({
                        name: tFullName,
                        shortName: tName,
                        type: t.TABLE_TYPE === "VIEW" ? "VIEW" : "TABLE",
                        schema: tSchema,
                    });
                }
                // Check by naming convention
                else if (tSchema === schemaUpper) {
                    // Same schema - use prefix logic
                    const currentPrefix = tableUpper.match(prefixPattern)?.[1] || "";
                    const otherPrefix = tName.match(prefixPattern)?.[1] || "";

                    // STG → DIM/FACT direction
                    if (currentPrefix === "STG_" && (otherPrefix === "DIM_" || otherPrefix === "FACT_")) {
                        downstream.push({
                            name: tFullName,
                            shortName: tName,
                            type: "TABLE",
                            schema: tSchema,
                        });
                    } else if ((currentPrefix === "DIM_" || currentPrefix === "FACT_") && otherPrefix === "STG_") {
                        upstream.push({
                            name: tFullName,
                            shortName: tName,
                            type: "TABLE",
                            schema: tSchema,
                        });
                    }
                }
            }

            // Limit results
            upstream = upstream.slice(0, 5);
            downstream = downstream.slice(0, 5);

        } catch (e: any) {
            console.log("Error getting related tables:", e.message);
        }

        // Get health status
        try {
            const healthResult = await new Promise<any>((resolve, reject) => {
                connection.execute({
                    sqlText: healthQuery,
                    complete: (err: any, stmt: any, rows: any) => {
                        if (err) reject(err);
                        else resolve(rows?.[0] || null);
                    },
                });
            });

            if (healthResult) {
                freshnessMinutes = healthResult.FRESHNESS_DELAY_MINUTES || 0;
                if (freshnessMinutes > 1440) {
                    healthStatus = "stale";
                } else if (freshnessMinutes > 60) {
                    healthStatus = "delayed";
                } else {
                    healthStatus = "healthy";
                }
            }
        } catch (e) {
            console.log("Health query failed");
        }

        // Generate business impact summary
        const downstreamCount = downstream.length;
        const upstreamCount = upstream.length;

        // Create impact assessment
        let impactLevel = "low";
        let impactSummary = "";

        if (healthStatus === "stale") {
            if (downstreamCount > 2) {
                impactLevel = "high";
                impactSummary = `This table is stale and feeds ${downstreamCount} downstream assets. Data consumers may be viewing outdated information.`;
            } else if (downstreamCount > 0) {
                impactLevel = "medium";
                impactSummary = `This table is stale and feeds ${downstreamCount} downstream asset${downstreamCount > 1 ? "s" : ""}. Downstream data may be outdated.`;
            } else {
                impactLevel = "low";
                impactSummary = `This table is stale but has no detected downstream dependencies.`;
            }
        } else if (healthStatus === "delayed") {
            if (downstreamCount > 0) {
                impactSummary = `This table has a freshness delay. ${downstreamCount} downstream asset${downstreamCount > 1 ? "s" : ""} may have slightly outdated data.`;
                impactLevel = downstreamCount > 2 ? "medium" : "low";
            } else {
                impactSummary = "This table has a minor freshness delay. No downstream impact detected.";
            }
        } else {
            if (downstreamCount > 0) {
                impactSummary = `This table is healthy. ${downstreamCount} downstream asset${downstreamCount > 1 ? "s" : ""} ${downstreamCount > 1 ? "are" : "is"} receiving fresh data.`;
            } else {
                impactSummary = "This table is healthy with no detected downstream consumers.";
            }
        }

        // Add context about upstream
        if (upstreamCount > 0) {
            impactSummary += ` Data flows from ${upstreamCount} upstream source${upstreamCount > 1 ? "s" : ""}.`;
        }

        return NextResponse.json({
            success: true,
            data: {
                // Focus node
                node: {
                    name: tableFullName,
                    shortName: tableUpper,
                    database: database.toUpperCase(),
                    schema: schemaUpper,
                    table: tableUpper,
                    healthStatus,
                    freshnessMinutes,
                },

                // Lineage
                upstream,
                downstream,

                // Impact assessment
                impact: {
                    level: impactLevel,
                    summary: impactSummary,
                    downstreamCount,
                    upstreamCount,
                },

                // Availability
                lineageAvailable: upstream.length > 0 || downstream.length > 0,
            },
        });
    } catch (error: any) {
        console.error("Error fetching lineage data:", error);
        return NextResponse.json(
            {
                success: false,
                error: error.message || "Failed to fetch lineage data",
            },
            { status: 500 }
        );
    }
}
