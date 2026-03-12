import { NextRequest, NextResponse } from 'next/server';
import { getServerConfig } from '@/lib/server-config';
import { getOrSetCache, buildCacheKey } from '@/lib/valkey';

export const runtime = 'nodejs';

export async function GET(
    request: NextRequest,
    props: { params: Promise<{ dataset: string }> }
) {
    try {
        const { dataset } = await props.params;
        const tableName = dataset.toUpperCase();

        const valkeyKey = buildCacheKey('catalog', 'dataset-lineage', tableName);

        const lineageRows = await getOrSetCache(valkeyKey, 600, async () => {
            const config = getServerConfig();

            if (!config) {
                throw new Error('AUTH_FAILED: Not connected to Snowflake');
            }

            const { snowflakePool, executeQueryObjects } = await import('@/lib/snowflake');
            const connection = await snowflakePool.getConnection(config);

            // Fetch Lineage from Account Usage dependencies
            const lineageQuery = `
                SELECT 
                    REFERENCING_DATABASE, REFERENCING_SCHEMA, REFERENCING_OBJECT_NAME, REFERENCING_OBJECT_DOMAIN,
                    REFERENCED_DATABASE, REFERENCED_SCHEMA, REFERENCED_OBJECT_NAME, REFERENCED_OBJECT_DOMAIN
                FROM SNOWFLAKE.ACCOUNT_USAGE.OBJECT_DEPENDENCIES
                WHERE REFERENCED_OBJECT_NAME = ? OR REFERENCING_OBJECT_NAME = ?
                LIMIT 100
            `;

            try {
                return await executeQueryObjects(connection, lineageQuery, [tableName, tableName]);
            } catch (e) {
                console.warn(`Could not fetch lineage for ${tableName}:`, e);
                return [];
            }
        });

        return NextResponse.json({
            success: true,
            data: lineageRows,
            metadata: { cached: true, timestamp: new Date().toISOString() }
        });

    } catch (error: any) {
        console.error('Error fetching lineage:', error);

        if (error.message?.includes('AUTH_FAILED')) {
            return NextResponse.json(
                { success: false, error: 'Not connected to Snowflake' },
                { status: 401 }
            );
        }

        return NextResponse.json(
            { success: false, error: error.message || 'Failed to fetch lineage metrics' },
            { status: 500 }
        );
    }
}
