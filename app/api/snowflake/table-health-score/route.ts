import { NextRequest, NextResponse } from "next/server";

/**
 * GET /api/snowflake/table-health-score
 * Calculates overall table health score based on multiple factors
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

        // Fetch metadata to calculate health
        const metadataRes = await fetch(
            `${request.nextUrl.origin}/api/snowflake/table-metadata?database=${encodeURIComponent(
                database
            )}&schema=${encodeURIComponent(schema)}&table=${encodeURIComponent(table)}`
        );
        const metadataData = await metadataRes.json();

        // Fetch freshness to calculate health
        const freshnessRes = await fetch(
            `${request.nextUrl.origin}/api/snowflake/table-freshness?database=${encodeURIComponent(
                database
            )}&schema=${encodeURIComponent(schema)}&table=${encodeURIComponent(table)}`
        );
        const freshnessData = await freshnessRes.json();

        if (!metadataData.success || !freshnessData.success) {
            return NextResponse.json(
                {
                    success: false,
                    error: "Failed to fetch required data for health score calculation",
                },
                { status: 500 }
            );
        }

        const { rowCount, columnCount, nullableColumnCount } = metadataData.data;
        const { hoursSinceUpdate } = freshnessData.data;

        // Calculate Freshness Score (40% weight)
        let freshnessScore = 100;
        if (hoursSinceUpdate >= 72) {
            freshnessScore = 25;
        } else if (hoursSinceUpdate >= 48) {
            freshnessScore = 50;
        } else if (hoursSinceUpdate >= 24) {
            freshnessScore = 75;
        }

        // Calculate Volume Score (30% weight)
        const volumeScore = rowCount > 0 ? 100 : 0;

        // Calculate Schema Score (30% weight)
        let schemaScore = 100;
        if (columnCount > 0) {
            const nullableRatio = nullableColumnCount / columnCount;
            if (nullableRatio >= 0.5) {
                schemaScore = 50;
            } else if (nullableRatio >= 0.3) {
                schemaScore = 75;
            }
        }

        // Calculate Overall Score (weighted average)
        const overallScore = Math.round(
            freshnessScore * 0.4 + volumeScore * 0.3 + schemaScore * 0.3
        );

        return NextResponse.json({
            success: true,
            data: {
                overallScore,
                freshnessScore,
                volumeScore,
                schemaScore,
            },
        });
    } catch (error: any) {
        console.error("Error calculating health score:", error);
        return NextResponse.json(
            {
                success: false,
                error: error.message || "Failed to calculate health score",
            },
            { status: 500 }
        );
    }
}
