import { NextRequest, NextResponse } from "next/server";
import { getServerConfig } from "@/lib/server-config";
import { snowflakePool } from "@/lib/snowflake";

/**
 * GET /api/observability/schema
 * Returns table schema metadata derived from INFORMATION_SCHEMA.COLUMNS
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

        // Query INFORMATION_SCHEMA.COLUMNS for schema metadata
        const query = `
      SELECT 
        COLUMN_NAME,
        DATA_TYPE,
        IS_NULLABLE,
        ORDINAL_POSITION,
        CHARACTER_MAXIMUM_LENGTH,
        NUMERIC_PRECISION,
        NUMERIC_SCALE,
        COLUMN_DEFAULT
      FROM ${database.toUpperCase()}.INFORMATION_SCHEMA.COLUMNS 
      WHERE TABLE_SCHEMA = '${schema.toUpperCase()}' 
        AND TABLE_NAME = '${table.toUpperCase()}'
      ORDER BY ORDINAL_POSITION
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
            return NextResponse.json({
                success: true,
                data: {
                    columnCount: 0,
                    nullableColumnCount: 0,
                    columns: [],
                },
            });
        }

        const columns = result.map((row: any) => ({
            name: row["COLUMN_NAME"],
            dataType: row["DATA_TYPE"],
            isNullable: row["IS_NULLABLE"] === "YES",
            ordinalPosition: row["ORDINAL_POSITION"],
            maxLength: row["CHARACTER_MAXIMUM_LENGTH"],
            numericPrecision: row["NUMERIC_PRECISION"],
            numericScale: row["NUMERIC_SCALE"],
            defaultValue: row["COLUMN_DEFAULT"],
        }));

        const nullableColumnCount = columns.filter((c: any) => c.isNullable).length;

        return NextResponse.json({
            success: true,
            data: {
                columnCount: columns.length,
                nullableColumnCount,
                columns,
            },
        });
    } catch (error: any) {
        console.error("Error fetching schema metadata:", error);
        return NextResponse.json(
            {
                success: false,
                error: error.message || "Failed to fetch schema metadata",
            },
            { status: 500 }
        );
    }
}
