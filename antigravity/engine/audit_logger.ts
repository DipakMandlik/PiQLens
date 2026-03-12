/**
 * Antigravity Audit Logger
 * Logs all LLM executions for learning and debugging
 */

import { snowflakePool, executeQuery } from '@/lib/snowflake';
import { getServerConfig } from '@/lib/server-config';
import { ExecutionPlan } from './plan_validator';

export interface ExecutionLog {
    executionId: string;
    userQuestion: string;
    assetId: string;
    llmModel: string;
    plan?: ExecutionPlan;
    validationError?: string;
    sqlExecuted?: string;
    executionStatus: 'SUCCESS' | 'PLAN_ERROR' | 'VALIDATION_ERROR' | 'SQL_ERROR' | 'INTERPRETATION_ERROR';
    errorMessage?: string;
    resultRowCount?: number;
    llmResponseTimeMs?: number;
    totalExecutionTimeMs?: number;
}

/**
 * Log an execution to Snowflake
 */
export async function logExecution(log: ExecutionLog): Promise<void> {
    let connection;

    try {
        const config = getServerConfig();
        if (!config) {
            console.warn('[AUDIT] No Snowflake config, skipping log');
            return;
        }

        connection = await snowflakePool.getConnection(config);

        // Ensure audit table exists before insert to avoid first-run failures.
        await createLogTable(connection);

        const insertSql = `
            INSERT INTO DATA_QUALITY_DB.DB_METRICS.DQ_LLM_EXECUTION_LOG (
                EXECUTION_ID,
                USER_QUESTION,
                ASSET_ID,
                LLM_MODEL,
                GEMINI_PLAN,
                VALIDATION_ERROR,
                SQL_EXECUTED,
                EXECUTION_STATUS,
                ERROR_MESSAGE,
                RESULT_ROW_COUNT,
                LLM_RESPONSE_TIME_MS,
                TOTAL_EXECUTION_TIME_MS
            )
            SELECT ?, ?, ?, ?, PARSE_JSON(?), ?, ?, ?, ?, ?, ?, ?
        `;

        const params = [
            log.executionId,
            log.userQuestion,
            log.assetId,
            log.llmModel,
            JSON.stringify(log.plan || {}),
            log.validationError || null,
            log.sqlExecuted || null,
            log.executionStatus,
            log.errorMessage || null,
            log.resultRowCount || 0,
            log.llmResponseTimeMs || null,
            log.totalExecutionTimeMs || null
        ];

        await executeQuery(connection, insertSql, params);
        console.log(`[AUDIT] Logged execution ${log.executionId} with status ${log.executionStatus}`);
    } catch (e: any) {
        console.error('[AUDIT] Failed to log execution:', e.message);
        // Don't throw - logging failures shouldn't break the main flow
    }
}

/**
 * Create the audit log table (auto-healing)
 */
async function createLogTable(connection: any): Promise<void> {
    const createSql = `
        CREATE TABLE IF NOT EXISTS DATA_QUALITY_DB.DB_METRICS.DQ_LLM_EXECUTION_LOG (
            EXECUTION_ID STRING PRIMARY KEY,
            USER_QUESTION STRING NOT NULL,
            ASSET_ID STRING NOT NULL,
            LLM_MODEL STRING DEFAULT 'mixtral:8x7b',
            GEMINI_PLAN VARIANT,
            VALIDATION_ERROR STRING,
            SQL_EXECUTED STRING,
            EXECUTION_STATUS STRING NOT NULL,
            ERROR_MESSAGE STRING,
            RESULT_ROW_COUNT INTEGER,
            LLM_RESPONSE_TIME_MS INTEGER,
            TOTAL_EXECUTION_TIME_MS INTEGER,
            CREATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
        )
        COMMENT = 'Audit log for all Antigravity LLM executions'
    `;

    await executeQuery(connection, createSql);
}

/**
 * Get execution statistics for monitoring
 */
export async function getExecutionStats(days: number = 7): Promise<{
    totalExecutions: number;
    successRate: number;
    avgResponseTimeMs: number;
    commonErrors: Array<{ error: string; count: number }>;
}> {
    try {
        const config = getServerConfig();
        if (!config) {
            throw new Error('No Snowflake config');
        }

        const connection = await snowflakePool.getConnection(config);

        const statsSql = `
            SELECT
                COUNT(*) as total_executions,
                SUM(CASE WHEN EXECUTION_STATUS = 'SUCCESS' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as success_rate,
                AVG(TOTAL_EXECUTION_TIME_MS) as avg_response_time_ms
            FROM DATA_QUALITY_DB.DB_METRICS.DQ_LLM_EXECUTION_LOG
            WHERE CREATED_AT >= DATEADD('DAY', -${days}, CURRENT_TIMESTAMP())
        `;

        const statsResult = await executeQuery(connection, statsSql);
        const stats = statsResult.rows[0];

        const errorsSql = `
            SELECT
                ERROR_MESSAGE as error,
                COUNT(*) as count
            FROM DATA_QUALITY_DB.DB_METRICS.DQ_LLM_EXECUTION_LOG
            WHERE CREATED_AT >= DATEADD('DAY', -${days}, CURRENT_TIMESTAMP())
                AND EXECUTION_STATUS != 'SUCCESS'
                AND ERROR_MESSAGE IS NOT NULL
            GROUP BY ERROR_MESSAGE
            ORDER BY count DESC
            LIMIT 10
        `;

        const errorsResult = await executeQuery(connection, errorsSql);

        return {
            totalExecutions: parseInt(stats[0] || '0'),
            successRate: parseFloat(stats[1] || '0'),
            avgResponseTimeMs: parseFloat(stats[2] || '0'),
            commonErrors: errorsResult.rows.map(row => ({
                error: row[0] as string,
                count: parseInt(row[1] as string)
            }))
        };

    } catch (e: any) {
        console.error('[AUDIT] Failed to get stats:', e.message);
        return {
            totalExecutions: 0,
            successRate: 0,
            avgResponseTimeMs: 0,
            commonErrors: []
        };
    }
}
