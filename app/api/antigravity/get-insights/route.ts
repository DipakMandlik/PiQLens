/**
 * API Endpoint: Get AI Insights
 * GET /api/antigravity/get-insights?database=...&schema=...&table=...
 *
 * Retrieves stored insights for an asset.
 */

import { NextRequest, NextResponse } from 'next/server';
import { getInsightsForAsset } from '@/lib/antigravity/insight-store';
import { getServerConfig } from '@/lib/server-config';

export async function GET(request: NextRequest) {
    try {
        const searchParams = request.nextUrl.searchParams;
        const database = searchParams.get('database');
        const schema = searchParams.get('schema');
        const table = searchParams.get('table');
        const limit = parseInt(searchParams.get('limit') || '5', 10);

        if (!database || !schema || !table) {
            return NextResponse.json(
                { success: false, error: 'Missing required parameters: database, schema, table' },
                { status: 400 }
            );
        }

        const config = getServerConfig();
        if (!config) {
            return NextResponse.json(
                { success: false, error: 'No Snowflake connection available' },
                { status: 401 }
            );
        }

        const insights = await getInsightsForAsset(database, schema, table, limit);

        return NextResponse.json({
            success: true,
            data: {
                insights,
                count: insights.length,
                assetId: `${database.toUpperCase()}.${schema.toUpperCase()}.${table.toUpperCase()}`,
            },
        });
    } catch (error: any) {
        console.error('[ANTIGRAVITY] Error fetching insights:', error);
        return NextResponse.json(
            { success: false, error: error.message || 'Failed to fetch insights' },
            { status: 500 }
        );
    }
}
