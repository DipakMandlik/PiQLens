import { NextRequest, NextResponse } from 'next/server';
import { getServerConfig } from '@/lib/server-config';

export const runtime = 'nodejs';

/**
 * GET /api/governance/policies
 * Lists organization-wide policies.
 */
export async function GET(request: NextRequest) {
    try {
        const config = getServerConfig();
        if (!config) return NextResponse.json({ success: false, error: 'Not connected' }, { status: 401 });

        const { snowflakePool, executeQueryObjects } = await import('@/lib/snowflake');
        const connection = await snowflakePool.getConnection(config);

        // Fetch Policies using fully qualified name directly
        // Note: If table doesn't exist, this will throw an error.
        // The Frontend handles this error gracefully.
        const sql = `
      SELECT *
      FROM DATA_QUALITY_DB.DQ_CONFIG.DQ_GOVERNANCE_POLICIES
      ORDER BY CREATED_AT DESC
    `;
        const rows = await executeQueryObjects(connection, sql);

        return NextResponse.json({ success: true, data: rows });

    } catch (error: any) {
        if (error.message?.includes('does not exist')) {
            return NextResponse.json({ success: true, data: [] }); // Graceful empty if not set up
        }
        return NextResponse.json({ success: false, error: error.message }, { status: 500 });
    }
}
