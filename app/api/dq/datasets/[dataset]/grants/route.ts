import { NextRequest, NextResponse } from 'next/server';
import { getServerConfig } from '@/lib/server-config';

export const runtime = 'nodejs';

export async function GET(
    request: NextRequest,
    props: { params: Promise<{ dataset: string }> }
) {
    try {
        const { dataset } = await props.params;
        const tableName = dataset.toUpperCase();

        const { searchParams } = new URL(request.url);
        const config = getServerConfig();

        if (!config) {
            return NextResponse.json({ success: false, error: 'Not connected to Snowflake' }, { status: 401 });
        }

        const database = searchParams.get('database') || config.database || 'BANKING_DW';
        const schema = searchParams.get('schema') || config.schema || 'BRONZE';

        const { snowflakePool, executeQueryObjects } = await import('@/lib/snowflake');
        const connection = await snowflakePool.getConnection(config);

        // Fetch Grants
        const grantsQuery = `SHOW GRANTS ON TABLE ${database}.${schema}.${tableName}`;
        let grantsRows = [];
        try {
            grantsRows = await executeQueryObjects(connection, grantsQuery, []);
        } catch (e) {
            console.warn(`Could not fetch grants for ${tableName}`, e);
        }

        return NextResponse.json({
            success: true,
            data: grantsRows
        });

    } catch (error: any) {
        console.error('Error fetching grants:', error);
        return NextResponse.json(
            { success: false, error: error.message || 'Failed to fetch grants' },
            { status: 500 }
        );
    }
}
