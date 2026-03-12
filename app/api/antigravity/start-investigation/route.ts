/**
 * API Endpoint: Start Investigation
 * POST /api/antigravity/start-investigation
 * 
 * Initiates a root cause analysis session and returns guided questions.
 */

import { NextRequest, NextResponse } from 'next/server';
import { startInvestigation, getQuestionsForType } from '@/lib/antigravity/investigation-controller';
import { getServerConfig } from '@/lib/server-config';

export async function POST(request: NextRequest) {
    try {
        const body = await request.json();
        const { database, schema, table, insightType, insightId } = body;

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

        const assetId = `${database.toUpperCase()}.${schema.toUpperCase()}.${table.toUpperCase()}`;
        const type = insightType || 'QUALITY';

        const result = await startInvestigation(assetId, type, insightId, 'USER');

        return NextResponse.json({
            success: true,
            data: {
                session: result.session,
                questions: result.questions.map(q => ({
                    key: q.key,
                    label: q.label,
                    description: q.description,
                    chartType: q.chartType,
                })),
            },
        });
    } catch (error: any) {
        console.error('[INVESTIGATION] Error starting investigation:', error);
        return NextResponse.json(
            { success: false, error: error.message || 'Failed to start investigation' },
            { status: 500 }
        );
    }
}
