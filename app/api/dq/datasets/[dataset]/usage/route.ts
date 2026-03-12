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
        const { searchParams } = new URL(request.url);

        const valkeyKey = buildCacheKey('catalog', 'dataset-usage', tableName);

        const usageRows = await getOrSetCache(valkeyKey, 300, async () => {
            const config = getServerConfig();

            if (!config) {
                throw new Error('AUTH_FAILED: Not connected to Snowflake');
            }

            // We let the async import stay here just in case since dynamic imports are better for serverless inside wrappers
            const { snowflakePool, executeQueryObjects } = await import('@/lib/snowflake');
            const connection = await snowflakePool.getConnection(config);

            const usageQuery = `
                SELECT 
                    USER_NAME,
                    COUNT(*) as QUERY_COUNT,
                    AVG(TOTAL_ELAPSED_TIME) as AVG_EXECUTION_TIME_MS,
                    MAX(START_TIME) as LAST_ACCESSED
                FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
                WHERE QUERY_TEXT ILIKE ?
                AND START_TIME > DATEADD('day', -30, CURRENT_TIMESTAMP())
                GROUP BY USER_NAME
                ORDER BY QUERY_COUNT DESC
                LIMIT 50
            `;

            const searchPattern = `%${tableName}%`;
            try {
                return await executeQueryObjects(connection, usageQuery, [searchPattern]);
            } catch (e) {
                console.warn(`Could not fetch usage for ${tableName}:`, e);
                return [];
            }
        });

        return NextResponse.json({
            success: true,
            data: usageRows,
            metadata: { cached: true, timestamp: new Date().toISOString() } // simplified
        });

    } catch (error: any) {
        console.error('Error fetching usage:', error);

        if (error.message?.includes('AUTH_FAILED')) {
            return NextResponse.json(
                { success: false, error: 'Not connected to Snowflake' },
                { status: 401 }
            );
        }

        return NextResponse.json(
            { success: false, error: error.message || 'Failed to fetch usage metrics' },
            { status: 500 }
        );
    }
}
