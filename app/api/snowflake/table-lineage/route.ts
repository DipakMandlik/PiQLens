
import { NextRequest, NextResponse } from "next/server";
import { snowflakePool, executeQuery } from "@/lib/snowflake";
import { getServerConfig } from "@/lib/server-config";

export async function GET(request: NextRequest) {
    const searchParams = request.nextUrl.searchParams;
    const database = searchParams.get("database");
    const schema = searchParams.get("schema");
    const table = searchParams.get("table");

    if (!database || !schema || !table) {
        return NextResponse.json(
            { success: false, error: "Missing required parameters" },
            { status: 400 }
        );
    }

    try {
        const dbName = database.toUpperCase();
        const schemaName = schema.toUpperCase();
        const tableName = table.toUpperCase();

        // Get config from server storage (set by /connect API) or fallback to env vars
        const config = getServerConfig();

        if (!config && !process.env.SNOWFLAKE_ACCOUNT) {
            return NextResponse.json(
                { success: false, error: "No Snowflake connection found. Please connect via the sidebar." },
                { status: 401 }
            );
        }

        // Get connection from pool
        const connection = await snowflakePool.getConnection(config || undefined);

        // Query for Upstream dependencies (where this table is the downstream target)
        // We want to know WHO FEEDS into this table.
        const upstreamQuery = `
      SELECT 
        UPSTREAM_DATABASE, 
        UPSTREAM_SCHEMA, 
        UPSTREAM_TABLE,
        LINEAGE_TYPE,
        TRANSFORMATION_LOGIC
      FROM DATA_QUALITY_DB.DQ_METRICS.DATA_LINEAGE
      WHERE DOWNSTREAM_DATABASE = '${dbName}' 
      AND DOWNSTREAM_SCHEMA = '${schemaName}' 
      AND DOWNSTREAM_TABLE = '${tableName}'
    `;

        // Query for Downstream dependencies (where this table is the upstream source)
        // We want to know WHO THIS TABLE FEEDS.
        const downstreamQuery = `
      SELECT 
        DOWNSTREAM_DATABASE, 
        DOWNSTREAM_SCHEMA, 
        DOWNSTREAM_TABLE,
        LINEAGE_TYPE,
        TRANSFORMATION_LOGIC
      FROM DATA_QUALITY_DB.DQ_METRICS.DATA_LINEAGE
      WHERE UPSTREAM_DATABASE = '${dbName}' 
      AND UPSTREAM_SCHEMA = '${schemaName}' 
      AND UPSTREAM_TABLE = '${tableName}'
    `;

        const [upstreamResult, downstreamResult] = await Promise.all([
            executeQuery(connection, upstreamQuery),
            executeQuery(connection, downstreamQuery),
        ]);

        // executeQuery returns rows as arrays of values, based on column order
        // Order: [0] DATABASE, [1] SCHEMA, [2] TABLE, [3] TYPE, [4] CODE

        const upstream = upstreamResult.rows.map((row: any[]) => ({
            name: row[2], // TABLE
            shortName: row[2],
            type: "Table",
            database: row[0], // DATABASE
            schema: row[1],   // SCHEMA
            transformationType: row[3], // TRANSFORMATION_TYPE
            sourceCode: row[4]          // SOURCE_CODE
        }));

        const downstream = downstreamResult.rows.map((row: any[]) => ({
            name: row[2], // TABLE
            shortName: row[2],
            type: "Table",
            database: row[0], // DATABASE
            schema: row[1],   // SCHEMA
            transformationType: row[3], // TRANSFORMATION_TYPE
            sourceCode: row[4]          // SOURCE_CODE
        }));

        // Construct the node object for the current table
        const node = {
            name: tableName,
            shortName: tableName,
            healthStatus: "healthy", // Lowercase for frontend styling match
            freshnessMinutes: 0
        };

        const impact = {
            level: downstream.length > 5 ? "High" : downstream.length > 0 ? "Medium" : "Low",
            summary: `${downstream.length} downstream dependencies`,
            downstreamCount: downstream.length,
            upstreamCount: upstream.length
        };

        return NextResponse.json({
            success: true,
            data: {
                node,
                upstream,
                downstream,
                impact,
                lineageAvailable: true
            },
        });
    } catch (error: any) {
        console.error("Error fetching lineage:", error);
        return NextResponse.json(
            { success: false, error: error.message },
            { status: 500 }
        );
    }
}
