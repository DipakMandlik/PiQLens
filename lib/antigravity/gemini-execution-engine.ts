import { GoogleGenerativeAI } from '@google/generative-ai';
import { SchemaRegistry, validateColumns } from './schema-reader';
import { getServerConfig } from '@/lib/server-config';
import { snowflakePool, executeQuery } from '@/lib/snowflake';
import { ExecutiveAnswer } from '@/components/ExecutiveAnswerBlock';
import { createExecutiveAnswerFromLegacy } from '@/lib/antigravity/utils';

// Global Configuration
export const STRICT_GEMINI_MODE = true;

// Types
export interface GeminiExecutionPlan {
    intent: 'TREND_ANALYSIS' | 'ROOT_CAUSE' | 'COMPARISON' | 'DISTRIBUTION' | 'IMPACT' | 'ANOMALY';
    tables: string[];
    columns: string[];
    filters: Record<string, any>;
    group_by?: string[];
    order_by?: string[];
    time_window_days?: number;
    metrics?: string[];
    limit?: number;
}

export interface GeminiExecutionResult {
    status: 'SUCCESS' | 'GEMINI_PLAN_ERROR' | 'SCHEMA_VALIDATION_ERROR' | 'SQL_EXECUTION_ERROR' | 'INTERPRETATION_ERROR';
    plan?: GeminiExecutionPlan;
    sql?: string;
    data?: any[];
    interpretation?: ExecutiveAnswer;
    error?: {
        message: string;
        geminiOutput?: string;
        validationError?: string;
        correctionHint?: string;
    };
    executionId?: string;
}

// Master prompt for plan generation
const PLAN_GENERATION_PROMPT = `You are Antigravity, an observability execution planner.

You must return ONLY valid JSON.

Your task:
- Interpret the user question
- Generate an execution plan using ONLY the DQ_METRICS table
- NEVER invent tables or columns

AVAILABLE TABLE:
DATA_QUALITY_DB.DB_METRICS.DQ_METRICS
Columns: ASSET_ID, METRIC_NAME, METRIC_VALUE, METRIC_TIME, COLUMN_NAME

ABSOLUTE RULES:
1. ONLY use DATA_QUALITY_DB.DB_METRICS.DQ_METRICS table
2. ONLY use these columns: ASSET_ID, METRIC_NAME, METRIC_VALUE, METRIC_TIME, COLUMN_NAME
3. Use fully qualified table name
4. Time windows must be reasonable (max 90 days)
5. Always filter by ASSET_ID

Output JSON schema:
{
  "intent": "TREND_ANALYSIS | ROOT_CAUSE | COMPARISON | DISTRIBUTION",
  "tables": ["DATA_QUALITY_DB.DB_METRICS.DQ_METRICS"],
  "columns": ["METRIC_TIME", "METRIC_NAME", "METRIC_VALUE"],
  "metrics": ["AVG(METRIC_VALUE)", "COUNT(*)"],
  "filters": {
    "asset": "DATABASE.SCHEMA.TABLE",
    "time_window_days": 7
  },
  "group_by": ["DATE(METRIC_TIME)"],
  "order_by": ["DATE(METRIC_TIME) DESC"],
  "limit": 100
}

Now generate a plan for this question:
User Question: {question}
Asset: {asset_id}

Return ONLY the JSON plan, no explanations.`;

/**
 * Core Execution Engine
 * Orchestrates the strict Gemini execution pipeline.
 */
export async function executeWithGemini(
    question: string,
    assetId: string,
    schemaRegistry: SchemaRegistry,
    apiKey?: string
): Promise<GeminiExecutionResult> {
    const key = apiKey || process.env.GEMINI_API_KEY || process.env.GOOGLE_API_KEY;
    const executionId = crypto.randomUUID();

    if (!key) {
        return {
            status: 'GEMINI_PLAN_ERROR',
            error: {
                message: 'Gemini API key is missing. strict execution requires AI.',
                correctionHint: 'Please configure GEMINI_API_KEY in your environment.'
            },
            executionId
        };
    }

    // Step 1: Generate Plan
    let plan: GeminiExecutionPlan;
    let geminiOutput: string = '';

    try {
        const generationResult = await generateExecutionPlan(question, assetId, schemaRegistry, key);
        plan = generationResult.plan;
        geminiOutput = generationResult.rawOutput;

        // Check for refusal
        if ((plan as any).status === 'INSUFFICIENT_SCHEMA') {
            const errorResult: GeminiExecutionResult = {
                status: 'GEMINI_PLAN_ERROR',
                error: {
                    message: 'Gemini determined the schema is insufficient to answer this question.',
                    geminiOutput,
                    correctionHint: 'Try asking about standard metrics like freshness, quality, or row count.'
                },
                executionId
            };
            await logExecution(executionId, question, assetId, plan, 'INSUFFICIENT_SCHEMA', undefined, 'GEMINI_PLAN_ERROR', errorResult.error?.message);
            return errorResult;
        }

    } catch (e: any) {
        const errorResult: GeminiExecutionResult = {
            status: 'GEMINI_PLAN_ERROR',
            error: {
                message: `Failed to generate execution plan: ${e.message}`,
                geminiOutput
            },
            executionId
        };
        await logExecution(executionId, question, assetId, undefined, e.message, undefined, 'GEMINI_PLAN_ERROR', e.message);
        return errorResult;
    }

    // Step 2: Validate Schema
    try {
        const validation = validatePlanAgainstSchema(plan, schemaRegistry);
        if (!validation.valid) {
            const errorResult: GeminiExecutionResult = {
                status: 'SCHEMA_VALIDATION_ERROR',
                plan,
                error: {
                    message: `Gemini hallucinated schema elements: ${validation.errors.join(', ')}`,
                    geminiOutput,
                    validationError: validation.errors.join('\n'),
                    correctionHint: 'The AI referenced tables or columns that do not exist.'
                },
                executionId
            };
            await logExecution(executionId, question, assetId, plan, validation.errors.join(', '), undefined, 'SCHEMA_VALIDATION_ERROR', errorResult.error?.message);
            return errorResult;
        }
    } catch (e: any) {
        const errorResult: GeminiExecutionResult = {
            status: 'SCHEMA_VALIDATION_ERROR',
            plan,
            error: { message: `Validation logic failed: ${e.message}` },
            executionId
        };
        await logExecution(executionId, question, assetId, plan, e.message, undefined, 'SCHEMA_VALIDATION_ERROR', e.message);
        return errorResult;
    }

    // Step 3: Build SQL
    let sql = '';
    try {
        sql = buildSQLFromPlan(plan, assetId);
    } catch (e: any) {
        const errorResult: GeminiExecutionResult = {
            status: 'GEMINI_PLAN_ERROR',
            plan,
            error: { message: `Failed to build SQL from plan: ${e.message}` },
            executionId
        };
        await logExecution(executionId, question, assetId, plan, e.message, undefined, 'GEMINI_PLAN_ERROR', e.message);
        return errorResult;
    }

    // Step 4: Execute SQL
    let data: any[] = [];
    try {
        const config = getServerConfig();
        if (!config) throw new Error('No Snowflake connection');

        const connection = await snowflakePool.getConnection(config);
        const result = await executeQuery(connection, sql);
        data = result.rows.map(row => {
            const obj: any = {};
            // result.rows are arrays. Map them using result.columns
            result.columns.forEach((col, idx) => {
                obj[col] = row[idx];
            });
            return obj;
        });
        // (Removed duplicate mapping block: result.rows is already processed above)

    } catch (e: any) {
        const errorResult: GeminiExecutionResult = {
            status: 'SQL_EXECUTION_ERROR',
            plan,
            sql,
            error: {
                message: `SQL execution failed: ${e.message}`,
                correctionHint: 'The generated SQL was syntactically incorrect or hit a runtime error.'
            },
            executionId
        };
        await logExecution(executionId, question, assetId, plan, undefined, sql, 'SQL_EXECUTION_ERROR', e.message);
        return errorResult;
    }

    // Step 5: Interpret Results
    let interpretation: ExecutiveAnswer;
    try {
        // We reuse the existing logic but pass the real data
        interpretation = createExecutiveAnswerFromLegacy(
            data,
            { intentType: plan.intent, metrics: plan.metrics || [] },
            sql,
            { type: determineChartType(plan.intent), xAxis: plan.group_by?.[0] || 'date', yAxis: plan.metrics?.[0] || 'count' }
        );

        // In full implementations, we would call Gemini again here for the summary text.
        // For phase 1, we stick to the deterministic interpretation but powered by Gemini's Plan.

    } catch (e: any) {
        const errorResult: GeminiExecutionResult = {
            status: 'INTERPRETATION_ERROR',
            plan,
            sql,
            data,
            error: { message: `Result interpretation failed: ${e.message}` },
            executionId
        };
        await logExecution(executionId, question, assetId, plan, undefined, sql, 'INTERPRETATION_ERROR', e.message);
        return errorResult;
    }

    // Success
    await logExecution(executionId, question, assetId, plan, undefined, sql, 'SUCCESS', undefined, data.length);

    return {
        status: 'SUCCESS',
        plan,
        sql,
        data,
        interpretation,
        executionId
    };
}

/**
 * Generates the execution plan using Gemini.
 */
async function generateExecutionPlan(
    question: string,
    assetId: string,
    schemaRegistry: SchemaRegistry,
    apiKey: string
): Promise<{ plan: GeminiExecutionPlan; rawOutput: string }> {
    const genAI = new GoogleGenerativeAI(apiKey);

    // Priority: Models confirmed available for this API Key
    // 1. gemini-flash-latest (Confirmed Working)
    // 2. gemini-2.0-flash (Hitting 429s, keep as secondary)
    // 3. gemini-pro-latest (Fallback)
    const modelNames = ['gemini-flash-latest', 'gemini-2.0-flash', 'gemini-pro-latest', 'gemini-2.5-flash'];

    let lastError;

    for (const modelName of modelNames) {
        try {
            const model = genAI.getGenerativeModel({ model: modelName });

            const schemaText = Object.entries(schemaRegistry).map(([schema, tables]) => {
                return Object.entries(tables).map(([table, cols]) => {
                    return `${schema}.${table}: ${cols.map(c => c.columnName).join(', ')}`;
                }).join('\n');
            }).join('\n');

            const prompt = PLAN_GENERATION_PROMPT
                .replace('{schema}', schemaText)
                .replace('{question}', question)
                .replace('{asset_id}', assetId);

            // Helper to parsing
            const parseResponse = async (result: any) => {
                const text = result.response.text();
                const cleaned = text.replace(/```json\n?/gi, '').replace(/```\n?/g, '').trim();
                return { plan: JSON.parse(cleaned), rawOutput: text };
            };

            const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

            try {
                const result = await model.generateContent(prompt);
                return await parseResponse(result);
            } catch (e: any) {
                // Smart Retry on 429
                if (e.message.includes('429') || e.message.includes('Too Many Requests') || e.message.includes('Quota exceeded')) {
                    let waitTime = 2000; // Default 2s

                    // Try to parse "retry in 30.39s" from error
                    const match = e.message.match(/retry in (\d+(\.\d+)?)s/);
                    if (match && match[1]) {
                        waitTime = Math.ceil(parseFloat(match[1]) * 1000) + 1000; // Buffer 1s
                    } else if (e.message.includes('retryDelay')) {
                        // Some API errors return JSON/Objects in message
                        // Fallback to exponential if not found
                        waitTime = 5000;
                    }

                    console.warn(`[ANTIGRAVITY] Quota exceeded for ${modelName}. Retrying in ${waitTime / 1000}s...`);
                    await sleep(waitTime);

                    try {
                        const result = await model.generateContent(prompt);
                        return await parseResponse(result);
                    } catch (retryError: any) {
                        console.error(`[ANTIGRAVITY] Retry failed for ${modelName}: ${retryError.message}`);
                        // Fallthrough to next model
                    }
                }
                // Don't throw immediately, let loop try next model
                lastError = e;
                continue;
            }

        } catch (e: any) {
            console.warn(`[ANTIGRAVITY] Model ${modelName} failed: ${e.message}`);
            lastError = e;
            continue;
        }
    }

    throw new Error(`All Gemini models failed. Last error: ${lastError?.message || 'Unknown error'}`);
}

/**
 * Validates the plan against the schema registry.
 */
function validatePlanAgainstSchema(plan: GeminiExecutionPlan, registry: SchemaRegistry): { valid: boolean; errors: string[] } {
    const errors: string[] = [];

    // Check tables
    for (const table of plan.tables) {
        // ... (existing validation logic is fine as is, checks registry keys)
        // Note: Registry keys are now fully qualified DB.SCHEMA.
        // We assume plan.tables are also somewhat qualified or we match loosely.
        // For strictness, we just check existence.

        // Simplified check for "any schema contains this table"
        let found = false;
        const tableName = table.includes('.') ? table.split('.').pop()! : table;

        for (const [fqSchema, tables] of Object.entries(registry)) {
            if (tables[tableName.toUpperCase()]) {
                found = true;
                break; // Found it
            }
        }

        if (!found) {
            // Try to be helpful - maybe Gemini used just TABLE vs SCHEMA.TABLE
            errors.push(`Table ${table} not found in schema registry.`);
        }
    }

    return { valid: errors.length === 0, errors };
}

/**
 * deterministic SQL builder.
 */
function buildSQLFromPlan(plan: GeminiExecutionPlan, assetId: string): string {
    // Basic deterministic builder
    // SELECT {columns}, {metrics} FROM {table} WHERE {filters} GROUP BY {group_by}

    const cols = [...(plan.group_by || []), ...(plan.metrics || [])];
    const selectClause = cols.length > 0 ? cols.join(', ') : '*';
    const fromClause = plan.tables.join(', ');

    let whereClause = '1=1';
    if (plan.filters) {
        // Handle time window specifically
        if (plan.time_window_days) {
            // Using quoted 'DAY' to ensure Snowflake compatibility
            whereClause += ` AND METRIC_TIME >= DATEADD('DAY', -${plan.time_window_days}, CURRENT_DATE())`;
        }
        // Other filters from plan can be added here if Gemini output them
    }

    // Smart Asset Filtering
    // We must apply the asset filter, but different tables use different columns.
    // 1. New Tables (DQ_METRICS, DQ_AI_INSIGHTS) -> ASSET_ID
    // 2. Legacy Tables (DQ_COLUMN_PROFILE, DQ_CHECK_RESULTS, DQ_RUN_CONTROL) -> DB/SCHEMA/TABLE

    // Deconstruct assetId (DATABASE.SCHEMA.TABLE)
    const [db, schema, table] = assetId.split('.');

    // Check primary table in FROM clause
    const primaryTable = plan.tables[0].toUpperCase();

    const isLegacyTable =
        primaryTable.includes('DQ_COLUMN_PROFILE') ||
        primaryTable.includes('DQ_CHECK_RESULTS') ||
        primaryTable.includes('DQ_RUN_CONTROL');

    if (isLegacyTable) {
        // Apply legacy filters
        whereClause += ` AND UPPER(DATABASE_NAME) = '${db.toUpperCase()}'`;
        whereClause += ` AND UPPER(SCHEMA_NAME) = '${schema.toUpperCase()}'`;
        whereClause += ` AND UPPER(TABLE_NAME) = '${table.toUpperCase()}'`;
    } else {
        // Default / New tables: Assume ASSET_ID exists
        if (!whereClause.includes('ASSET_ID') && !whereClause.includes(assetId)) {
            whereClause += ` AND UPPER(ASSET_ID) = '${assetId.toUpperCase()}'`;
        }
    }

    const groupBy = plan.group_by && plan.group_by.length > 0 ? `GROUP BY ${plan.group_by.join(', ')}` : '';
    const orderBy = plan.order_by && plan.order_by.length > 0 ? `ORDER BY ${plan.order_by.join(', ')}` : '';
    const limit = plan.limit ? `LIMIT ${plan.limit}` : 'LIMIT 100';

    return `SELECT ${selectClause} FROM ${fromClause} WHERE ${whereClause} ${groupBy} ${orderBy} ${limit}`;
}

/**
 * Logs execution to Snowflake.
 */
async function logExecution(
    id: string,
    question: string,
    assetId: string,
    plan: any,
    validationError: string | undefined,
    sql: string | undefined,
    status: string,
    errorMessage: string | undefined,
    rowCount: number = 0
) {
    let connection;
    try {
        const config = getServerConfig();
        if (!config) return;
        connection = await snowflakePool.getConnection(config);

        const insertSql = `
            INSERT INTO DATA_QUALITY_DB.DB_METRICS.DQ_GEMINI_EXECUTION_LOG 
            (EXECUTION_ID, USER_QUESTION, ASSET_ID, GEMINI_PLAN, VALIDATION_ERROR, SQL_EXECUTED, EXECUTION_STATUS, ERROR_MESSAGE, RESULT_ROW_COUNT)
            SELECT ?, ?, ?, PARSE_JSON(?), ?, ?, ?, ?, ?
        `;

        const params = [
            id,
            question,
            assetId,
            JSON.stringify(plan || {}),
            validationError || null,
            sql || null,
            status,
            errorMessage || null,
            rowCount
        ];

        try {
            await executeQuery(connection, insertSql, params);
        } catch (e: any) {
            // Auto-healing: Create table if it doesn't exist
            if (e.message && (e.message.includes('does not exist') || e.message.includes('not authorized'))) {
                console.log('[ANTIGRAVITY] Log table missing, attempting creation...');
                const createSql = `
                    CREATE TABLE IF NOT EXISTS DATA_QUALITY_DB.DB_METRICS.DQ_GEMINI_EXECUTION_LOG (
                        EXECUTION_ID STRING,
                        USER_QUESTION STRING,
                        ASSET_ID STRING,
                        GEMINI_PLAN VARIANT,
                        VALIDATION_ERROR STRING,
                        SQL_EXECUTED STRING,
                        EXECUTION_STATUS STRING,
                        ERROR_MESSAGE STRING,
                        RESULT_ROW_COUNT INTEGER,
                        CREATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
                        PRIMARY KEY (EXECUTION_ID)
                    )
                `;
                await executeQuery(connection, createSql);
                // Retry insert
                await executeQuery(connection, insertSql, params);
                console.log('[ANTIGRAVITY] Log table created and record inserted.');
            } else {
                throw e;
            }
        }
    } catch (e) {
        console.error('Failed to log Gemini execution:', e);
    }
}

/**
 * Helper to determine chart type from intent
 */
function determineChartType(intent: string): string {
    switch (intent) {
        case 'TREND_ANALYSIS': return 'LINE';
        case 'DISTRIBUTION': return 'BAR';
        case 'COMPARISON': return 'BAR';
        default: return 'NONE';
    }
}
