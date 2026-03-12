import { NextRequest, NextResponse } from "next/server";
import { getServerConfig } from "@/lib/server-config";
import { snowflakePool } from "@/lib/snowflake";
import { formatDistanceToNow, format } from "date-fns";

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

        const connection = await snowflakePool.getConnection(config);

        // Query to calculate freshness using fully qualified identifier
        const query = `
      SELECT 
        CREATED,
        LAST_ALTERED,
        DATEDIFF('hour', LAST_ALTERED, CURRENT_TIMESTAMP()) as HOURS_SINCE_UPDATE,
        DATEDIFF('day', LAST_ALTERED, CURRENT_TIMESTAMP()) as DAYS_SINCE_UPDATE
      FROM ${database}.INFORMATION_SCHEMA.TABLES
      WHERE TABLE_SCHEMA = ?
        AND TABLE_NAME = ?
    `;

        // Execute query
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
            const hoursSinceUpdate = row.HOURS_SINCE_UPDATE || 0;
            const daysSinceUpdate = row.DAYS_SINCE_UPDATE || 0;

            let slaStatus = "Unknown";
            if (hoursSinceUpdate < 24) slaStatus = "On-time";
            else if (hoursSinceUpdate < 48) slaStatus = "Delayed";
            else slaStatus = "Stale";

            const lastUpdated = new Date(row.LAST_ALTERED);
            const created = new Date(row.CREATED);

            return NextResponse.json({
                success: true,
                data: {
                    lastUpdated: row.LAST_ALTERED,
                    created: row.CREATED,
                    hoursSinceUpdate: hoursSinceUpdate,
                    daysSinceUpdate: daysSinceUpdate,
                    slaStatus,
                    freshnessDelayFormatted: formatDistanceToNow(lastUpdated, { addSuffix: true }),
                    createdFormatted: format(created, "MMM d, yyyy"),
                },
            });
        }

        return NextResponse.json({ success: false, error: "Table not found" }, { status: 404 });
    } catch (error: any) {
        console.error("Error fetching table freshness:", error);
        return NextResponse.json({ success: false, error: error.message || "Failed to fetch table freshness" }, { status: 500 });
    }
}
