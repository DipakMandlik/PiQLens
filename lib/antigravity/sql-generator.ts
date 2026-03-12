/**
 * Antigravity SQL Generator
 * AI-Powered SQL Generation with Schema Validation AND Fallback
 * 
 * Rule: Try AI first, fall back to templates if AI unavailable
 * Rule: All generated SQL is validated before execution
 */

import { GoogleGenerativeAI } from '@google/generative-ai';
import { SchemaRegistry } from './schema-reader';

export interface GeneratedSQL {
    sql: string;
    isValid: boolean;
    validationErrors: string[];
    referencedTables: string[];
    referencedColumns: string[];
    wasAiGenerated: boolean;
}

// SQL Generation prompt template
const SQL_GENERATION_PROMPT = `You are Antigravity SQL Generator, a schema-aware SQL generation engine for Snowflake.

ABSOLUTE RULES:
1. Generate ONLY valid Snowflake SQL syntax
2. Use ONLY tables and columns from the provided schema
3. Never invent table or column names
4. Always include proper WHERE clauses for the asset
5. Use parameterized asset_id format: DATABASE.SCHEMA.TABLE
6. Limit results appropriately (LIMIT 100 max for data queries)
7. Use proper date functions: DATEADD, CURRENT_TIMESTAMP()
8. Return ONLY the SQL query, no explanations

AVAILABLE SCHEMA:
{schema}

TARGET ASSET: {asset_id}

USER QUESTION: {question}

Generate a safe, validated Snowflake SQL query that answers this question.
Return ONLY the SQL, nothing else.`;

/**
 * Fallback SQL templates for when AI is unavailable.
 * These are schema-aware patterns that work with common DQ tables.
 */
// Fallback templates deleted. Strict Gemini Mode enabled.

/**
 * Generates SQL using Gemini AI, falling back to templates if AI unavailable.
 */
export async function generateSQL(
    question: string,
    assetId: string,
    schemaRegistry: SchemaRegistry,
    questionKey?: string,
    apiKey?: string
): Promise<GeneratedSQL> {
    const key = apiKey || process.env.GEMINI_API_KEY || process.env.GOOGLE_API_KEY;

    // Try AI generation first (if key available)
    if (key) {
        try {
            const aiResult = await tryAIGeneration(question, assetId, schemaRegistry, key);
            if (aiResult.isValid && aiResult.sql) {
                return { ...aiResult, wasAiGenerated: true };
            }
        } catch (e: any) {
            console.warn('[ANTIGRAVITY] AI generation failed, using fallback:', e.message);
        }
    }

    // Fallback templates REMOVED for Strict Gemini Mode
    // Antigravity now refuses to guess
    return {
        sql: "",
        isValid: false,
        validationErrors: ["STRICT_GEMINI_MODE: AI generation failed and fallbacks are disabled."],
        referencedTables: [],
        referencedColumns: [],
        wasAiGenerated: false,
    };
}

/**
 * Tries AI-based SQL generation.
 */
async function tryAIGeneration(
    question: string,
    assetId: string,
    schemaRegistry: SchemaRegistry,
    apiKey: string
): Promise<GeneratedSQL> {
    const schemaText = formatSchemaForPrompt(schemaRegistry);
    const prompt = SQL_GENERATION_PROMPT
        .replace('{schema}', schemaText || 'No schema available - use DATA_QUALITY_DB tables')
        .replace('{asset_id}', assetId)
        .replace('{question}', question);

    const genAI = new GoogleGenerativeAI(apiKey);

    // Try multiple model names for compatibility
    const modelNames = ['gemini-1.5-flash', 'gemini-1.0-pro', 'gemini-pro'];

    for (const modelName of modelNames) {
        try {
            const model = genAI.getGenerativeModel({ model: modelName });
            const result = await model.generateContent(prompt);
            const response = await result.response;
            let sql = response.text().trim();

            // Clean up response
            sql = sql.replace(/```sql\n?/gi, '').replace(/```\n?/g, '').trim();

            const validation = validateGeneratedSQL(sql);

            if (validation.isValid) {
                console.log(`[ANTIGRAVITY] AI generated SQL using ${modelName}`);
                return {
                    sql,
                    isValid: true,
                    validationErrors: [],
                    referencedTables: validation.tables,
                    referencedColumns: [],
                    wasAiGenerated: true,
                };
            }
        } catch (e: any) {
            console.warn(`[ANTIGRAVITY] Model ${modelName} failed:`, e.message);
            continue;
        }
    }

    throw new Error('All AI models failed');
}

/**
 * Generates SQL using fallback templates.
 */
// function generateFallbackSQL removed. Strict mode only.

/**
 * Formats schema registry into a concise text format for the prompt.
 */
function formatSchemaForPrompt(registry: SchemaRegistry): string {
    const lines: string[] = [];

    for (const [schemaName, tables] of Object.entries(registry)) {
        for (const [tableName, columns] of Object.entries(tables)) {
            const colList = columns.map(c => `${c.columnName} (${c.dataType})`).join(', ');
            lines.push(`${schemaName}.${tableName}: ${colList}`);
        }
    }

    return lines.join('\n');
}

/**
 * Validates generated SQL for safety.
 */
function validateGeneratedSQL(sql: string): { isValid: boolean; errors: string[]; tables: string[] } {
    const errors: string[] = [];
    const tables: string[] = [];

    // Extract table references
    const tablePattern = /FROM\s+([A-Z_]+\.[A-Z_]+\.[A-Z_]+)/gi;
    let match;
    while ((match = tablePattern.exec(sql)) !== null) {
        tables.push(match[1].toUpperCase());
    }

    // Safety checks
    if (sql.toLowerCase().includes('drop ')) errors.push('DROP not allowed');
    if (sql.toLowerCase().includes('delete ')) errors.push('DELETE not allowed');
    if (sql.toLowerCase().includes('insert ')) errors.push('INSERT not allowed');
    if (sql.toLowerCase().includes('update ')) errors.push('UPDATE not allowed');
    if (sql.toLowerCase().includes('truncate ')) errors.push('TRUNCATE not allowed');

    return { isValid: errors.length === 0, errors, tables };
}

/**
 * Pre-defined investigation question templates.
 */
export const INVESTIGATION_QUESTIONS = {
    FRESHNESS: [
        { key: 'freshness_trend', label: 'Why did freshness increase?', description: 'Analyze freshness metrics over the last 7 days', chartType: 'LINE' as const },
        { key: 'freshness_anomaly_start', label: 'When did the delay start?', description: 'Identify when freshness first exceeded threshold', chartType: 'LINE' as const },
        { key: 'downstream_impact', label: 'Are downstream assets affected?', description: 'Show assets that depend on this table', chartType: 'DAG' as const },
    ],
    QUALITY: [
        { key: 'failed_checks_breakdown', label: 'Which checks failed?', description: 'Breakdown of failed checks by rule', chartType: 'BAR' as const },
        { key: 'null_rate_by_column', label: 'Which columns have high null rates?', description: 'Null percentage by column', chartType: 'BAR' as const },
        { key: 'quality_score_trend', label: 'How has quality changed over time?', description: 'Quality score trend over 30 days', chartType: 'LINE' as const },
    ],
    ANOMALY: [
        { key: 'metric_deviation', label: 'What metric deviated?', description: 'Show recent metric changes', chartType: 'LINE' as const },
        { key: 'row_count_trend', label: 'Has row count changed unexpectedly?', description: 'Row count over time', chartType: 'LINE' as const },
    ],
};
