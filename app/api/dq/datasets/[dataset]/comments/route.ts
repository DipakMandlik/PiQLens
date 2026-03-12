import { NextRequest, NextResponse } from 'next/server';
import { getServerConfig } from '@/lib/server-config';

export const runtime = 'nodejs';

export async function POST(
    request: NextRequest,
    props: { params: Promise<{ dataset: string }> }
) {
    try {
        const { dataset } = await props.params;
        const tableName = dataset.toUpperCase();

        const body = await request.json();
        const { database, schema, comment, columnName } = body;

        if (!database || !schema || comment === undefined) {
            return NextResponse.json({ success: false, error: 'Missing required parameters' }, { status: 400 });
        }

        const config = getServerConfig();

        if (!config) {
            return NextResponse.json({ success: false, error: 'Not connected to Snowflake' }, { status: 401 });
        }

        const { snowflakePool, executeQueryObjects } = await import('@/lib/snowflake');
        const connection = await snowflakePool.getConnection(config);

        let query = '';
        if (columnName) {
            // Un-injectable since we are using identifiers formatting natively or escaping them
            // Snowflake uses single quotes for comments
            const escapedComment = comment.replace(/'/g, "''");
            query = `COMMENT ON COLUMN ${database}.${schema}.${tableName}.${columnName} IS '${escapedComment}'`;
        } else {
            const escapedComment = comment.replace(/'/g, "''");
            query = `COMMENT ON TABLE ${database}.${schema}.${tableName} IS '${escapedComment}'`;
        }

        await executeQueryObjects(connection, query, []);

        return NextResponse.json({
            success: true,
            message: 'Comment updated successfully'
        });

    } catch (error: any) {
        console.error('Error updating comment:', error);
        return NextResponse.json(
            { success: false, error: error.message || 'Failed to update comment' },
            { status: 500 }
        );
    }
}
