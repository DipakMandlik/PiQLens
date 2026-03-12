import { NextResponse } from 'next/server';
import { getServerConfig } from '@/lib/server-config';

export const dynamic = 'force-dynamic';
export const revalidate = 0;

interface SchemaInfo {
    database: string;
    schema: string;
    displayName: string;
}

export async function GET() {
    try {
        const config = getServerConfig();
        if (!config) {
            return NextResponse.json(
                {
                    success: false,
                    error: 'Not connected to Snowflake. Please connect first.',
                },
                { status: 401 }
            );
        }

        // Dynamic import to avoid bundler issues
        const { snowflakePool, executeQueryObjects } = await import('@/lib/snowflake');
        const connection = await snowflakePool.getConnection(config);

        // Use SHOW SCHEMAS which doesn't require a database context
        const query = `
            SHOW SCHEMAS IN ACCOUNT
        `;

        const rows = await executeQueryObjects(connection, query);

        // SHOW SCHEMAS returns columns like: "database_name", "name" (schema_name)
        const systemDatabases = ['SNOWFLAKE', 'SNOWFLAKE_SAMPLE_DATA', 'DATA_QUALITY_DB'];
        const schemas: SchemaInfo[] = rows
            .filter((row: any) => {
                const dbName = row.database_name?.toUpperCase();
                const schemaName = row.name?.toUpperCase();

                // Exclude system databases AND information schemas
                if (systemDatabases.includes(dbName)) return false;
                if (dbName.includes('DQ')) return false;
                if (schemaName === 'INFORMATION_SCHEMA') return false;
                if (schemaName.includes('DQ')) return false;

                return true;
            })
            .map((row: any) => ({
                database: row.database_name,
                schema: row.name,
                displayName: `${row.database_name}.${row.name}`
            }));

        return NextResponse.json({
            success: true,
            data: schemas
        });

    } catch (error: any) {
        console.error('Schema fetch error:', error);
        return NextResponse.json(
            {
                success: false,
                error: error.message || 'Failed to fetch schemas from Snowflake'
            },
            { status: 500 }
        );
    }
}
