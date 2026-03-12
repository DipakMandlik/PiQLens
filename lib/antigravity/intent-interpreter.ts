/**
 * Antigravity Intent Interpreter
 * Parses natural language questions into structured observability intents.
 * 
 * Rule: Gemini is used ONLY for intent extraction, NOT SQL generation.
 * Rule: All intents map to predefined SQL templates.
 */

import { GoogleGenerativeAI } from '@google/generative-ai';

// Supported intent types
export type IntentType =
    | 'ROOT_CAUSE'
    | 'TREND'
    | 'COMPARISON'
    | 'RECURRENCE'
    | 'IMPACT'
    | 'DISTRIBUTION'
    | 'TIME_WINDOW'
    | 'UNSUPPORTED';

// Supported metrics
export type MetricType = 'freshness' | 'quality' | 'nulls' | 'row_count' | 'failed_checks' | 'anomaly';

// Time windows
export type TimeWindow = 'LAST_24H' | 'LAST_7D' | 'LAST_30D' | 'CUSTOM';

// Parsed intent structure
export interface ParsedIntent {
    intentType: IntentType;
    metrics: MetricType[];
    timeWindow: TimeWindow;
    comparison: { base: string; compareTo: string } | null;
    confidence: number;
    originalQuestion: string;
    rejectionReason?: string;
}

// Intent prompt - ONLY extracts intent, never generates SQL
const INTENT_PARSING_PROMPT = `You are an observability intent parser for a data quality platform.

Your ONLY job is to classify user questions into structured intents.

SUPPORTED INTENTS:
- ROOT_CAUSE: Why is something wrong? (e.g., "Why is data stale?", "What caused this issue?")
- TREND: How did something change over time? (e.g., "How did quality change?", "Show me the trend")
- COMPARISON: Compare two time periods (e.g., "Compare today vs yesterday", "Is it worse than last week?")
- RECURRENCE: Has this happened before? (e.g., "Is this recurring?", "When did this happen before?")
- IMPACT: Who or what is affected? (e.g., "What's downstream?", "Who uses this data?")
- DISTRIBUTION: What's the breakdown? (e.g., "Which checks fail most?", "Top issues")
- TIME_WINDOW: What happened at a specific time? (e.g., "What happened at 2 AM?", "Show me yesterday")
- UNSUPPORTED: Question not about data quality/observability

SUPPORTED METRICS:
- freshness: Data staleness, last update time
- quality: DQ score, pass rate
- nulls: Null percentage, missing values
- row_count: Volume, record count
- failed_checks: Failed DQ rules
- anomaly: Deviations, unexpected changes

TIME WINDOWS:
- LAST_24H: Last 24 hours
- LAST_7D: Last 7 days
- LAST_30D: Last 30 days

RULES:
1. Return ONLY valid JSON
2. Do NOT generate SQL
3. Do NOT invent metrics or intents
4. If question is not about observability, mark as UNSUPPORTED
5. Be conservative - if unsure, mark as UNSUPPORTED

USER QUESTION: {question}

Return JSON:
{
  "intentType": "ROOT_CAUSE|TREND|COMPARISON|RECURRENCE|IMPACT|DISTRIBUTION|TIME_WINDOW|UNSUPPORTED",
  "metrics": ["freshness", "quality", etc.],
  "timeWindow": "LAST_24H|LAST_7D|LAST_30D",
  "comparison": null or {"base": "today", "compareTo": "yesterday"},
  "confidence": 0.0 to 1.0,
  "rejectionReason": null or "reason if UNSUPPORTED"
}`;

/**
 * Interprets a natural language question into a structured intent.
 * Falls back to rule-based interpretation if AI is unavailable.
 */
export async function interpretQuestion(question: string, apiKey?: string): Promise<ParsedIntent> {
    const key = apiKey || process.env.GEMINI_API_KEY || process.env.GOOGLE_API_KEY;

    // Try AI-based interpretation first
    if (key) {
        try {
            return await aiInterpretation(question, key);
        } catch (e: any) {
            console.warn('[INTENT] AI interpretation failed, using rules:', e.message);
        }
    }

    // Fallback to rule-based interpretation
    return ruleBasedInterpretation(question);
}

/**
 * AI-based intent interpretation using Gemini.
 */
async function aiInterpretation(question: string, apiKey: string): Promise<ParsedIntent> {
    const genAI = new GoogleGenerativeAI(apiKey);

    // Try multiple model names
    const modelNames = ['gemini-1.5-flash', 'gemini-1.0-pro', 'gemini-pro'];

    for (const modelName of modelNames) {
        try {
            const model = genAI.getGenerativeModel({ model: modelName });
            const prompt = INTENT_PARSING_PROMPT.replace('{question}', question);

            const result = await model.generateContent(prompt);
            const response = await result.response;
            let text = response.text().trim();

            // Clean up response
            text = text.replace(/```json\n?/gi, '').replace(/```\n?/g, '').trim();

            const parsed = JSON.parse(text);

            console.log(`[INTENT] AI parsed (${modelName}):`, parsed.intentType);

            return {
                intentType: parsed.intentType || 'UNSUPPORTED',
                metrics: parsed.metrics || [],
                timeWindow: parsed.timeWindow || 'LAST_7D',
                comparison: parsed.comparison || null,
                confidence: parsed.confidence || 0.5,
                originalQuestion: question,
                rejectionReason: parsed.rejectionReason,
            };
        } catch (e) {
            continue;
        }
    }

    throw new Error('All AI models failed');
}

/**
 * Rule-based intent interpretation (fallback).
 */
function ruleBasedInterpretation(question: string): ParsedIntent {
    const q = question.toLowerCase();

    let intentType: IntentType = 'UNSUPPORTED';
    let metrics: MetricType[] = [];
    let timeWindow: TimeWindow = 'LAST_7D';
    let comparison = null;
    let confidence = 0.6;

    // Detect intent from keywords
    if (q.includes('why') || q.includes('cause') || q.includes('reason') || q.includes('stale')) {
        intentType = 'ROOT_CAUSE';
    } else if (q.includes('trend') || q.includes('change') || q.includes('over time') || q.includes('history')) {
        intentType = 'TREND';
    } else if (q.includes('compare') || q.includes('versus') || q.includes('vs') || q.includes('than')) {
        intentType = 'COMPARISON';
    } else if (q.includes('before') || q.includes('recur') || q.includes('again') || q.includes('pattern')) {
        intentType = 'RECURRENCE';
    } else if (q.includes('impact') || q.includes('affect') || q.includes('downstream') || q.includes('depend')) {
        intentType = 'IMPACT';
    } else if (q.includes('which') || q.includes('most') || q.includes('top') || q.includes('breakdown') || q.includes('distribution')) {
        intentType = 'DISTRIBUTION';
    } else if (q.includes('when') || q.includes('yesterday') || q.includes('today') || q.match(/\d+\s*(am|pm)/)) {
        intentType = 'TIME_WINDOW';
    }

    // Detect metrics
    if (q.includes('fresh') || q.includes('stale') || q.includes('old') || q.includes('update')) {
        metrics.push('freshness');
    }
    if (q.includes('quality') || q.includes('score') || q.includes('pass')) {
        metrics.push('quality');
    }
    if (q.includes('null') || q.includes('missing') || q.includes('empty')) {
        metrics.push('nulls');
    }
    if (q.includes('row') || q.includes('count') || q.includes('volume') || q.includes('record')) {
        metrics.push('row_count');
    }
    if (q.includes('fail') || q.includes('check') || q.includes('rule')) {
        metrics.push('failed_checks');
    }

    // Default metric if none detected
    if (metrics.length === 0) {
        metrics = ['quality'];
    }

    // Detect time window
    if (q.includes('today') || q.includes('24 hour') || q.includes('last hour')) {
        timeWindow = 'LAST_24H';
    } else if (q.includes('week') || q.includes('7 day')) {
        timeWindow = 'LAST_7D';
    } else if (q.includes('month') || q.includes('30 day')) {
        timeWindow = 'LAST_30D';
    }

    // Detect comparison
    if (q.includes('today') && q.includes('yesterday')) {
        comparison = { base: 'today', compareTo: 'yesterday' };
        intentType = 'COMPARISON';
    }

    console.log(`[INTENT] Rule-based parsed: ${intentType}, metrics: ${metrics.join(', ')}`);

    return {
        intentType,
        metrics,
        timeWindow,
        comparison,
        confidence,
        originalQuestion: question,
        rejectionReason: intentType === 'UNSUPPORTED' ? 'Could not determine observability intent' : undefined,
    };
}

/**
 * Gets a human-readable explanation of the intent.
 */
export function getIntentExplanation(intent: ParsedIntent): string {
    const intentLabels: Record<IntentType, string> = {
        ROOT_CAUSE: '🔍 Root Cause Analysis',
        TREND: '📈 Trend Analysis',
        COMPARISON: '⚖️ Comparison',
        RECURRENCE: '🔄 Pattern Detection',
        IMPACT: '⚡ Impact Analysis',
        DISTRIBUTION: '📊 Distribution',
        TIME_WINDOW: '⏰ Time-Based Query',
        UNSUPPORTED: '❌ Unsupported',
    };

    return intentLabels[intent.intentType] || 'Unknown';
}

/**
 * Gets suggested questions for a given asset.
 */
export function getSuggestedQuestions(): string[] {
    return [
        "Why is the data stale?",
        "How has quality changed this week?",
        "Which checks fail most often?",
        "Compare today vs yesterday",
        "What downstream tables are affected?",
        "Has this issue happened before?",
        "What happened in the last 24 hours?",
    ];
}

/**
 * Gets a human-readable preview text for the interpreted intent.
 * Shown to user before they confirm execution.
 */
export function getIntentPreviewText(intent: ParsedIntent): string {
    const timeLabels: Record<TimeWindow, string> = {
        LAST_24H: 'last 24 hours',
        LAST_7D: 'last 7 days',
        LAST_30D: 'last 30 days',
        CUSTOM: 'selected period',
    };

    const intentLabels: Record<IntentType, string> = {
        ROOT_CAUSE: 'Root cause analysis',
        TREND: 'Trend analysis',
        COMPARISON: 'Comparison',
        RECURRENCE: 'Pattern detection',
        IMPACT: 'Impact analysis',
        DISTRIBUTION: 'Distribution breakdown',
        TIME_WINDOW: 'Time-based query',
        UNSUPPORTED: 'Unable to interpret',
    };

    const metricsText = intent.metrics.length > 0
        ? intent.metrics.map(m => m.replace(/_/g, ' ')).join(', ')
        : 'general metrics';

    const timeText = timeLabels[intent.timeWindow] || 'last 7 days';

    return `${intentLabels[intent.intentType]} on ${metricsText} (${timeText})`;
}

/**
 * Gets the short label for an intent type (for chips).
 */
export function getIntentTypeLabel(intentType: IntentType): string {
    const labels: Record<IntentType, string> = {
        ROOT_CAUSE: 'ROOT CAUSE',
        TREND: 'TREND',
        COMPARISON: 'COMPARE',
        RECURRENCE: 'PATTERN',
        IMPACT: 'IMPACT',
        DISTRIBUTION: 'BREAKDOWN',
        TIME_WINDOW: 'TIME',
        UNSUPPORTED: 'UNKNOWN',
    };
    return labels[intentType] || 'UNKNOWN';
}
