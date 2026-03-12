/**
 * Antigravity Insight Generator
 * Step 4: AI Reasoning (Gemini Integration)
 *
 * This module wraps the Gemini client and enforces strict output rules.
 * Rule: Gemini is used ONLY after SQL/context is built. Never for schema.
 */

import { GoogleGenerativeAI } from '@google/generative-ai';

// Legacy interface for backward compatibility
export interface AIInsight {
    type: 'ANOMALY' | 'TREND' | 'SCHEMA_CHANGE' | 'IMPACT' | 'FRESHNESS' | 'QUALITY';
    summary: string;
    bullets: string[];
    severity: 'INFO' | 'WARNING' | 'CRITICAL';
    isActionable: boolean;
    // New Executive Answer fields
    whatHappened?: string;
    whyItHappened?: string;
    whatIsImpacted?: string;
    whatShouldBeDone?: string;
    evidenceSummary?: string[];
}

// Business-first master prompt as per UX specification
const ANTIGRAVITY_MASTER_PROMPT = `You are Antigravity, an enterprise observability advisor inside Pi_Qualytics.

CRITICAL RULES:
1. Do NOT lead with charts or visualizations
2. Explain in BUSINESS LANGUAGE, not technical jargon
3. Avoid metric terminology unless absolutely necessary
4. Always include IMPACT and RECOMMENDATION
5. Focus on what matters to DECISION-MAKERS
6. Be concise and authoritative

TONE EXAMPLES:
❌ WRONG: "Freshness metric exceeded threshold"
✅ RIGHT: "Data has not been refreshed in over 12 days, which violates operational freshness expectations"

❌ WRONG: "NULL_PERCENTAGE is 45%"
✅ RIGHT: "Nearly half of the records are missing critical values, creating reporting gaps"

Structure your response EXACTLY as:

{
  "type": "ANOMALY | TREND | SCHEMA_CHANGE | IMPACT | FRESHNESS | QUALITY",
  "what_happened": "Plain, factual paragraph describing what occurred",
  "why_it_happened": "Root cause explanation in business terms (not system jargon)",
  "what_is_impacted": "Business risk, downstream effects, who is affected",
  "what_should_be_done": "Clear, actionable next steps",
  "evidence_summary": ["Key metric: value", "Another metric: value"],
  "severity": "INFO | WARNING | CRITICAL"
}

Input you receive:
- Validated observability facts
- Precomputed trends
- Lineage impact counts

CRITICAL: Return ONLY valid JSON. No markdown. No extra text.`;

/**
 * Generates AI insights from the observability context.
 * Uses Gemini with business-first prompting.
 */
export async function generateInsight(
    contextJson: string,
    apiKey?: string
): Promise<AIInsight | null> {
    const key = apiKey || process.env.GEMINI_API_KEY || process.env.GOOGLE_API_KEY;

    if (!key) {
        console.warn('[ANTIGRAVITY] No Gemini API key available. Skipping AI insight generation.');
        return null;
    }

    try {
        const genAI = new GoogleGenerativeAI(key);

        // Try multiple model names for compatibility
        // Try multiple model names for compatibility (Priority: 2.0-flash, flash-latest)
        const modelNames = ['gemini-2.0-flash', 'gemini-flash-latest', 'gemini-pro-latest'];

        for (const modelName of modelNames) {
            try {
                const model = genAI.getGenerativeModel({ model: modelName });

                const prompt = `${ANTIGRAVITY_MASTER_PROMPT}

Observability Context:
${contextJson}

Generate business-first insight:`;

                let result;
                try {
                    result = await model.generateContent(prompt);
                } catch (e: any) {
                    if (e.message.includes('429') || e.message.includes('Too Many Requests') || e.message.includes('Quota exceeded')) {
                        let waitTime = 2000;
                        const match = e.message.match(/retry in (\d+(\.\d+)?)s/);
                        if (match && match[1]) {
                            waitTime = Math.ceil(parseFloat(match[1]) * 1000) + 1000;
                        }
                        console.warn(`[ANTIGRAVITY] Quota exceeded for ${modelName}. Retrying in ${waitTime / 1000}s...`);
                        await new Promise(resolve => setTimeout(resolve, waitTime));
                        result = await model.generateContent(prompt);
                    } else {
                        throw e;
                    }
                }
                const response = await result.response;
                const text = response.text();

                // Parse and validate response
                const cleaned = text.replace(/```json\n?/gi, '').replace(/```\n?/g, '').trim();
                const parsed = JSON.parse(cleaned);

                // Transform new format to maintain backward compatibility
                const insight: AIInsight = {
                    type: parsed.type || 'QUALITY',
                    summary: parsed.what_happened || parsed.summary || 'Analysis complete.',
                    bullets: parsed.evidence_summary || parsed.bullets || [],
                    severity: parsed.severity || 'INFO',
                    isActionable: ['WARNING', 'CRITICAL'].includes(parsed.severity),
                    // Add new Executive Answer fields
                    whatHappened: parsed.what_happened,
                    whyItHappened: parsed.why_it_happened,
                    whatIsImpacted: parsed.what_is_impacted,
                    whatShouldBeDone: parsed.what_should_be_done,
                    evidenceSummary: parsed.evidence_summary,
                };

                // Validate structure
                if (!insight.type || (!insight.summary && !insight.whatHappened)) {
                    console.error('[ANTIGRAVITY] Invalid AI response structure:', parsed);
                    continue;
                }

                // Enforce max 5 evidence items
                if (insight.bullets.length > 5) {
                    insight.bullets = insight.bullets.slice(0, 5);
                }

                // Ensure severity is valid
                if (!['INFO', 'WARNING', 'CRITICAL'].includes(insight.severity)) {
                    insight.severity = 'INFO';
                }

                console.log(`[ANTIGRAVITY] Insight generated (${modelName}): ${insight.type} - ${insight.severity}`);
                return insight;
            } catch (e: any) {
                console.warn(`[ANTIGRAVITY] Model ${modelName} failed: ${e.message}`);
                continue;
            }
        }

        console.error('[ANTIGRAVITY] All AI models failed');
        return null;
    } catch (error: any) {
        console.error('[ANTIGRAVITY] Error generating insight:', error.message);
        return null;
    }
}

/**
 * Fallback insight generator when AI is not available.
 * Uses rule-based logic with business-first language.
 */
// Fallback generator removed. Strict Gemini Mode enabled.
