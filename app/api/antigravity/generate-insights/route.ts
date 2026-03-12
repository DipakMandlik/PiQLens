/**
 * API Endpoint: Generate AI Insights
 * POST /api/antigravity/generate-insights
 *
 * Antigravity pipeline: Schema -> Context -> AI -> Store
 */

import { NextRequest, NextResponse } from 'next/server';
import { buildObservabilityContext, formatContextForAI } from '@/lib/antigravity/context-builder';
import { generateInsight } from '@/lib/antigravity/insight-generator';
import { storeInsight } from '@/lib/antigravity/insight-store';

export async function POST(request: NextRequest) {
    try {
        const body = await request.json();
        const { database, schema, table, apiKey } = body;

        if (!database || !schema || !table) {
            return NextResponse.json(
                { success: false, error: 'Missing required parameters: database, schema, table' },
                { status: 400 }
            );
        }

        console.log(`[ANTIGRAVITY] Generating insights for ${database}.${schema}.${table}`);

        // Build asset ID
        const assetId = `${database.toUpperCase()}.${schema.toUpperCase()}.${table.toUpperCase()}`;

        // Step 1: Build observability context
        const context = await buildObservabilityContext(database, schema, table);
        console.log('[ANTIGRAVITY] Context built');

        // Step 2: Generate insight using Gemini (primary) or Ollama (fallback)
        const contextFormatted = formatContextForAI(context);

        // Try Gemini first
        let insight: any = null;
        let usedProvider = 'gemini';

        try {
            const aiInsight = await generateInsight(contextFormatted, apiKey);

            if (aiInsight) {
                insight = {
                    type: aiInsight.type,
                    summary: aiInsight.whatHappened || aiInsight.summary || 'Insight generated',
                    bullets: [
                        aiInsight.whyItHappened,
                        aiInsight.whatIsImpacted,
                        aiInsight.whatShouldBeDone
                    ].filter(Boolean),
                    severity: aiInsight.severity,
                    isActionable: aiInsight.isActionable,
                    details: {
                        whyItHappened: aiInsight.whyItHappened,
                        whatIsImpacted: aiInsight.whatIsImpacted,
                        whatShouldBeDone: aiInsight.whatShouldBeDone
                    }
                };
            }
        } catch (geminiError: any) {
            console.warn('[ANTIGRAVITY] Gemini failed, trying Ollama fallback:', geminiError.message);
        }

        // Fallback to Ollama if Gemini failed
        if (!insight) {
            try {
                const { introspectSchema } = await import('@/lib/antigravity/schema-reader');
                const { executeWithOllama } = await import('@/antigravity/engine/ollama_execution_engine');

                const schemaRegistry = await introspectSchema(database, [schema]);
                const question = generateInsightQuestion(context);
                const result = await executeWithOllama(question, assetId, schemaRegistry);

                if (result.status === 'SUCCESS') {
                    usedProvider = 'ollama';
                    insight = {
                        type: (context.anomalyDetected ? 'ANOMALY' :
                            context.failedMetricsLast24h.length > 0 ? 'QUALITY' : 'FRESHNESS') as 'ANOMALY' | 'QUALITY' | 'FRESHNESS',
                        summary: result.interpretation?.whatHappened || 'Insight generated',
                        bullets: [
                            result.interpretation?.whyItHappened || '',
                            result.interpretation?.whatIsImpacted || '',
                            result.interpretation?.whatShouldBeDone || ''
                        ].filter(Boolean),
                        severity: result.interpretation?.severity || 'INFO',
                        isActionable: result.interpretation?.whatShouldBeDone ? true : false,
                        details: {
                            whyItHappened: result.interpretation?.whyItHappened,
                            whatIsImpacted: result.interpretation?.whatIsImpacted,
                            whatShouldBeDone: result.interpretation?.whatShouldBeDone
                        }
                    };
                } else {
                    throw new Error(result.error?.message || 'Ollama execution failed');
                }
            } catch (ollamaError: any) {
                console.error('[ANTIGRAVITY] Both Gemini and Ollama failed');
                return NextResponse.json({
                    success: false,
                    error: 'AI insight generation unavailable. Please ensure Gemini API key is configured or Ollama is running.',
                    data: {
                        stored: false,
                        hint: 'Configure GEMINI_API_KEY in .env or start Ollama with: ollama serve'
                    }
                }, { status: 503 });
            }
        }

        // Step 3: Store insight

        // Step 5: Store insight
        const storeResult = await storeInsight(context.assetId, insight, {
            quality_score: context.latestMetrics.qualityScore,
            freshness_hours: context.latestMetrics.freshnessHours,
            failed_checks: context.failedMetricsLast24h,
        });

        return NextResponse.json({
            success: true,
            data: {
                insight,
                provider: usedProvider,
                context: {
                    assetId: context.assetId,
                    qualityScore: context.latestMetrics.qualityScore,
                    freshnessHours: context.latestMetrics.freshnessHours,
                    failedChecks24h: context.failedMetricsLast24h.length,
                    anomalyDetected: context.anomalyDetected,
                },
                stored: storeResult === 'SUCCESS',
            },
        });
    } catch (error: any) {
        console.error('[ANTIGRAVITY] Error generating insights:', error);
        return NextResponse.json(
            { success: false, error: error.message || 'Failed to generate insights' },
            { status: 500 }
        );
    }
}

/**
 * Generate insight question from observability context
 */
function generateInsightQuestion(context: any): string {
    // Prioritize based on severity
    if (context.anomalyDetected) {
        return `What anomalies were detected in the data quality metrics?`;
    }

    if (context.failedMetricsLast24h.length > 0) {
        return `Why did ${context.failedMetricsLast24h.length} checks fail in the last 24 hours?`;
    }

    if (context.latestMetrics.qualityScore < 80) {
        return `Why is the quality score at ${context.latestMetrics.qualityScore}%?`;
    }

    if (context.latestMetrics.freshnessHours > 24) {
        return `Why is data freshness at ${context.latestMetrics.freshnessHours} hours?`;
    }

    // Default: general health check
    return `What is the current data quality status and are there any concerns?`;
}
