/**
 * Antigravity Ollama Execution Engine
 * Replaces Gemini-based execution with local Ollama LLMs
 * 
 * Pipeline:
 * 1. Generate Plan (Ollama)
 * 2. Validate Plan (Schema Registry)
 * 3. Build SQL (Deterministic)
 * 4. Execute SQL (Snowflake)
 * 5. Interpret Results (Ollama)
 * 6. Log Execution (Audit)
 */

import { OllamaRouter } from './llm_router';
import { validatePlan, ExecutionPlan } from './plan_validator';
import { buildSQLFromPlan, validateSQL } from './sql_builder';
import { logExecution } from './audit_logger';
import { SchemaRegistry } from '@/lib/antigravity/schema-reader';
import { snowflakePool, executeQuery } from '@/lib/snowflake';
import { getServerConfig } from '@/lib/server-config';
import { ExecutiveAnswer } from '@/components/ExecutiveAnswerBlock';

export interface OllamaExecutionResult {
    status: 'SUCCESS' | 'OLLAMA_UNAVAILABLE' | 'PLAN_ERROR' | 'VALIDATION_ERROR' | 'SQL_ERROR' | 'INTERPRETATION_ERROR';
    plan?: ExecutionPlan;
    sql?: string;
    data?: any[];
    interpretation?: ExecutiveAnswer;
    error?: {
        message: string;
        details?: string;
        hint?: string;
    };
    executionId: string;
    timings?: {
        planGeneration: number;
        sqlExecution: number;
        interpretation: number;
        total: number;
    };
}

/**
 * Execute a user question using Ollama
 */
export async function executeWithOllama(
    question: string,
    assetId: string,
    schemaRegistry: SchemaRegistry
): Promise<OllamaExecutionResult> {
    const executionId = crypto.randomUUID();
    const startTime = Date.now();
    const timings = { planGeneration: 0, sqlExecution: 0, interpretation: 0, total: 0 };

    // Initialize Ollama router
    const router = new OllamaRouter();

    // Step 0: Health Check
    const health = await router.healthCheck();
    if (!health.healthy) {
        const result: OllamaExecutionResult = {
            status: 'OLLAMA_UNAVAILABLE',
            executionId,
            error: {
                message: 'Ollama is not available',
                details: health.error,
                hint: 'Please ensure Ollama is running: ollama serve'
            }
        };

        await logExecution({
            executionId,
            userQuestion: question,
            assetId,
            llmModel: router.getConfig().model,
            executionStatus: 'PLAN_ERROR',
            errorMessage: result.error?.message
        });

        return result;
    }

    // Step 1: Generate Plan
    let plan: ExecutionPlan;
    let planRawOutput: string;

    try {
        const planStart = Date.now();
        const schemaText = formatSchemaForPrompt(schemaRegistry);
        const planResult = await router.generatePlan(question, assetId, schemaText);

        plan = planResult.plan as ExecutionPlan;
        planRawOutput = planResult.rawOutput;
        timings.planGeneration = Date.now() - planStart;

        // Check for refusal
        if ((plan as any).status === 'INSUFFICIENT_SCHEMA') {
            const result: OllamaExecutionResult = {
                status: 'PLAN_ERROR',
                executionId,
                error: {
                    message: 'LLM determined the schema is insufficient to answer this question',
                    details: planRawOutput,
                    hint: 'Try asking about standard metrics like quality, freshness, or row count'
                }
            };

            await logExecution({
                executionId,
                userQuestion: question,
                assetId,
                llmModel: router.getConfig().model,
                plan,
                executionStatus: 'PLAN_ERROR',
                errorMessage: result.error?.message,
                llmResponseTimeMs: timings.planGeneration
            });

            return result;
        }

    } catch (e: any) {
        const result: OllamaExecutionResult = {
            status: 'PLAN_ERROR',
            executionId,
            error: {
                message: 'Failed to generate execution plan',
                details: e.message
            }
        };

        await logExecution({
            executionId,
            userQuestion: question,
            assetId,
            llmModel: router.getConfig().model,
            executionStatus: 'PLAN_ERROR',
            errorMessage: e.message,
            llmResponseTimeMs: timings.planGeneration
        });

        return result;
    }

    // Step 2: Validate Plan
    const validation = validatePlan(plan, schemaRegistry);
    if (!validation.valid) {
        const result: OllamaExecutionResult = {
            status: 'VALIDATION_ERROR',
            plan,
            executionId,
            error: {
                message: 'LLM generated plan references non-existent schema elements',
                details: validation.errors.join('\n'),
                hint: 'The AI hallucinated tables or columns. This is a model accuracy issue.'
            }
        };

        await logExecution({
            executionId,
            userQuestion: question,
            assetId,
            llmModel: router.getConfig().model,
            plan,
            validationError: validation.errors.join(', '),
            executionStatus: 'VALIDATION_ERROR',
            errorMessage: result.error?.message,
            llmResponseTimeMs: timings.planGeneration
        });

        return result;
    }

    // Step 3: Build SQL
    let sql: string;
    try {
        const sqlBuildResult = buildSQLFromPlan(plan, assetId);
        sql = sqlBuildResult.sql;

        // Additional safety check
        const sqlValidation = validateSQL(sql);
        if (!sqlValidation.safe) {
            throw new Error(`Unsafe SQL generated: ${sqlValidation.errors.join(', ')}`);
        }

    } catch (e: any) {
        const result: OllamaExecutionResult = {
            status: 'SQL_ERROR',
            plan,
            executionId,
            error: {
                message: 'Failed to build SQL from plan',
                details: e.message
            }
        };

        await logExecution({
            executionId,
            userQuestion: question,
            assetId,
            llmModel: router.getConfig().model,
            plan,
            executionStatus: 'SQL_ERROR',
            errorMessage: e.message,
            llmResponseTimeMs: timings.planGeneration
        });

        return result;
    }

    // Step 4: Execute SQL
    let data: any[] = [];
    try {
        const sqlStart = Date.now();
        const config = getServerConfig();
        if (!config) throw new Error('No Snowflake connection');

        const connection = await snowflakePool.getConnection(config);
        const result = await executeQuery(connection, sql);

        data = result.rows.map(row => {
            const obj: any = {};
            result.columns.forEach((col, idx) => {
                obj[col] = row[idx];
            });
            return obj;
        });

        timings.sqlExecution = Date.now() - sqlStart;

        if (data.length === 0) {
            console.warn('[OLLAMA] Query returned no results');
        }

    } catch (e: any) {
        const result: OllamaExecutionResult = {
            status: 'SQL_ERROR',
            plan,
            sql,
            executionId,
            error: {
                message: 'SQL execution failed',
                details: e.message,
                hint: 'The generated SQL was syntactically incorrect or hit a runtime error'
            }
        };

        await logExecution({
            executionId,
            userQuestion: question,
            assetId,
            llmModel: router.getConfig().model,
            plan,
            sqlExecuted: sql,
            executionStatus: 'SQL_ERROR',
            errorMessage: e.message,
            llmResponseTimeMs: timings.planGeneration,
            totalExecutionTimeMs: Date.now() - startTime
        });

        return result;
    }

    // Step 5: Interpret Results
    let interpretation: ExecutiveAnswer;
    try {
        const interpretStart = Date.now();
        const interpretResult = await router.interpretResults(question, assetId, data);
        timings.interpretation = interpretResult.responseTimeMs;

        // Convert to ExecutiveAnswer format (matching existing interface)
        interpretation = {
            whatHappened: interpretResult.interpretation.executive_summary || `Query returned ${data.length} rows`,
            whyItHappened: interpretResult.interpretation.key_observations?.[0] || 'Analysis completed',
            whatIsImpacted: interpretResult.interpretation.business_impact || 'Impact assessment pending',
            whatShouldBeDone: interpretResult.interpretation.recommended_actions?.[0] || 'Review results',
            evidence: {
                metrics: [
                    { label: 'Rows Returned', value: data.length.toString() },
                    { label: 'Execution Time', value: `${timings.sqlExecution}ms` },
                    { label: 'Risk Level', value: interpretResult.interpretation.risk_level || 'MEDIUM' }
                ],
                sql,
                chartData: data,
                chartConfig: {
                    type: 'LINE',
                    xAxis: plan.group_by?.[0] || 'date',
                    yAxis: plan.metrics?.[0] || 'value',
                    color: '#6366f1'
                }
            },
            severity: (interpretResult.interpretation.risk_level === 'CRITICAL' ? 'CRITICAL' :
                interpretResult.interpretation.risk_level === 'HIGH' ? 'WARNING' : 'INFO') as 'INFO' | 'WARNING' | 'CRITICAL'
        };

    } catch (e: any) {
        // Interpretation failure is not critical - we can still return data
        console.warn('[OLLAMA] Interpretation failed, using fallback:', e.message);

        interpretation = {
            whatHappened: `Query returned ${data.length} rows for ${assetId}`,
            whyItHappened: `Executed ${plan.intent} analysis on ${plan.tables.join(', ')}`,
            whatIsImpacted: 'Data retrieved successfully. Manual interpretation required.',
            whatShouldBeDone: 'Review the data below and consult with data team for insights',
            evidence: {
                metrics: [
                    { label: 'Rows Returned', value: data.length.toString() },
                    { label: 'Execution Time', value: `${timings.sqlExecution}ms` }
                ],
                sql,
                chartData: data
            },
            severity: 'INFO'
        };
    }

    // Success!
    timings.total = Date.now() - startTime;

    await logExecution({
        executionId,
        userQuestion: question,
        assetId,
        llmModel: router.getConfig().model,
        plan,
        sqlExecuted: sql,
        executionStatus: 'SUCCESS',
        resultRowCount: data.length,
        llmResponseTimeMs: timings.planGeneration + timings.interpretation,
        totalExecutionTimeMs: timings.total
    });

    return {
        status: 'SUCCESS',
        plan,
        sql,
        data,
        interpretation,
        executionId,
        timings
    };
}

/**
 * Format schema registry for LLM prompt
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
