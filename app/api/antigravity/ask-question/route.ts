/**
 * API Endpoint: Ask Question
 * POST /api/antigravity/ask-question
 * 
 * Handles natural language observability questions.
 * Flow: Question → Intent Parsing → SQL Template → Execution → Chart
 */

import { NextRequest, NextResponse } from 'next/server';
import { interpretQuestion, getSuggestedQuestions, getIntentPreviewText } from '@/lib/antigravity/intent-interpreter';
import { handleManualQuestion } from '@/lib/antigravity/manual-question-handler';
import { getServerConfig } from '@/lib/server-config';

export async function POST(request: NextRequest) {
    try {
        const body = await request.json();
        // Default provider to 'gemini' if not specified
        const { question, database, schema, table, previewOnly, provider = 'gemini' } = body;

        if (!question) {
            return NextResponse.json(
                { success: false, error: 'Question is required' },
                { status: 400 }
            );
        }

        // Step 1: Interpret the question into structured intent
        console.log(`[ANTIGRAVITY] Interpreting question: "${question}" (Provider: ${provider})`);
        const intent = await interpretQuestion(question);
        const interpretedSummary = getIntentPreviewText(intent);

        // If previewOnly, return just the intent without executing
        if (previewOnly) {
            return NextResponse.json({
                success: true,
                data: {
                    question,
                    intent: {
                        type: intent.intentType,
                        metrics: intent.metrics,
                        timeWindow: intent.timeWindow,
                        confidence: intent.confidence,
                    },
                    interpretedSummary,
                    previewOnly: true,
                },
            });
        }

        if (!database || !schema || !table) {
            return NextResponse.json(
                { success: false, error: 'Asset context (database, schema, table) is required' },
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

        // Step 2: EXECUTION (Gemini or Ollama)
        const { introspectSchema } = await import('@/lib/antigravity/schema-reader');
        const schemaRegistry = await introspectSchema(database, [schema]);

        let result;

        if (provider === 'gemini') {
            console.log('[ANTIGRAVITY] Using Gemini Engine');
            const { executeWithGemini } = await import('@/lib/antigravity/gemini-execution-engine');

            // Check for API Key
            const apiKey = process.env.GEMINI_API_KEY || process.env.GOOGLE_API_KEY;

            if (!apiKey) {
                console.warn('[ANTIGRAVITY] No Gemini API key found, falling back to Ollama');
                // Fallback to Ollama if no key
                const { executeWithOllama } = await import('@/antigravity/engine/ollama_execution_engine');
                result = await executeWithOllama(question, assetId, schemaRegistry);
            } else {
                result = await executeWithGemini(question, assetId, schemaRegistry, apiKey);
            }
        } else {
            console.log('[ANTIGRAVITY] Using Ollama Engine');
            const { executeWithOllama } = await import('@/antigravity/engine/ollama_execution_engine');
            result = await executeWithOllama(question, assetId, schemaRegistry);
        }

        if (result.status !== 'SUCCESS') {
            // Return detailed error information
            return NextResponse.json({
                success: false,
                error: result.error?.message || `${provider} execution failed`,
                data: {
                    question,
                    status: result.status,
                    error: result.error,
                    plan: result.plan,
                    sql: result.sql,
                    executionId: result.executionId,
                    hint: (result.error as any)?.hint || (result.error as any)?.correctionHint,
                    llmProvider: provider
                }
            }, { status: result.status === 'OLLAMA_UNAVAILABLE' ? 503 : 400 });
        }

        // Map to response format
        const response = {
            success: true,
            sql: result.sql,
            data: result.data,
            chart: { type: 'LINE', title: 'Analysis', xAxis: 'DATE', yAxis: 'VALUE', color: '#6366f1' },
            explanation: result.interpretation?.whatHappened,
            interpretation: result.interpretation,
            executionId: result.executionId,
            timings: (result as any).timings,
            llmProvider: provider
        };

        return NextResponse.json({
            success: response.success,
            data: {
                question: question,
                intent: {
                    type: intent.intentType,
                    metrics: intent.metrics,
                    timeWindow: intent.timeWindow,
                    confidence: intent.confidence,
                },
                interpretedSummary,
                sql: response.sql,
                chart: response.chart,
                data: response.data,
                explanation: response.explanation,
                suggestedFollowups: getSuggestedQuestions().slice(0, 3),
            },
            error: undefined,
        });
    } catch (error: any) {
        console.error('[ANTIGRAVITY] Error handling question:', error);
        return NextResponse.json(
            { success: false, error: error.message || 'Failed to process question' },
            { status: 500 }
        );
    }
}

/**
 * GET: Returns suggested questions for the UI
 */
export async function GET() {
    return NextResponse.json({
        success: true,
        data: {
            suggestedQuestions: getSuggestedQuestions(),
        },
    });
}
