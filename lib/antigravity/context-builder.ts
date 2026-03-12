/**
 * Antigravity Context Builder
 * Step 2: Observability Context Builder
 *
 * This module aggregates raw metrics into AI-consumable facts.
 * Rule: No raw data. No assumptions. Only computed facts.
 * 
 * SCHEMA: Uses DATA_QUALITY_DB.DQ_METRICS (not DQ_RESULTS)
 */

import { snowflakePool, executeQuery } from '@/lib/snowflake';
import { getServerConfig } from '@/lib/server-config';

export interface ObservabilityContext {
    assetId: string;
    database: string;
    schema: string;
    table: string;
    metricsAvailable: string[];
    latestMetrics: {
        rowCount?: number;
        freshnessHours?: number;
        nullRate?: number;
        qualityScore?: number;
    };
    failedMetricsLast24h: string[];
    anomalyDetected: boolean;
    anomalyTrend?: string;
    downstreamCount: number;
    upstreamCount: number;
    lastProfileTime?: string;
    schemaChangeDetected: boolean;
}

/**
 * Builds a comprehensive observability context for an asset.
 * This is the ONLY input to Gemini / AI reasoning.
 */
export async function buildObservabilityContext(
    database: string,
    schema: string,
    table: string
): Promise<ObservabilityContext> {
    const config = getServerConfig();
    if (!config) {
        throw new Error('No Snowflake connection available.');
    }

    const connection = await snowflakePool.getConnection(config);
    const assetId = `${database.toUpperCase()}.${schema.toUpperCase()}.${table.toUpperCase()}`;

    const context: ObservabilityContext = {
        assetId,
        database: database.toUpperCase(),
        schema: schema.toUpperCase(),
        table: table.toUpperCase(),
        metricsAvailable: [],
        latestMetrics: {},
        failedMetricsLast24h: [],
        anomalyDetected: false,
        downstreamCount: 0,
        upstreamCount: 0,
        schemaChangeDetected: false,
    };

    // 1. Get Quality Score from DQ_DAILY_SUMMARY (uses DQ_METRICS schema)
    try {
        const scoreResult = await new Promise<any>((resolve, reject) => {
            connection.execute({
                sqlText: `
                    SELECT DQ_SCORE
                    FROM DATA_QUALITY_DB.DQ_METRICS.DQ_DAILY_SUMMARY
                    WHERE UPPER(DATABASE_NAME) = ?
                      AND UPPER(SCHEMA_NAME) = ?
                      AND UPPER(TABLE_NAME) = ?
                      AND DQ_SCORE IS NOT NULL
                    ORDER BY SUMMARY_DATE DESC
                    LIMIT 1
                `,
                binds: [database.toUpperCase(), schema.toUpperCase(), table.toUpperCase()],
                complete: (err: any, stmt: any, rows: any) => {
                    if (err) reject(err);
                    else resolve({ rows: rows || [], columns: stmt?.getColumns() || [] });
                }
            });
        });
        if (scoreResult.rows.length > 0) {
            context.latestMetrics.qualityScore = parseFloat(scoreResult.rows[0].DQ_SCORE || scoreResult.rows[0][0]) || undefined;
            context.metricsAvailable.push('quality_score');
        }
    } catch { /* Table might not exist */ }

    // 2. Get failed checks from DQ_CHECK_RESULTS (uses DQ_METRICS schema)
    try {
        const failedResult = await new Promise<any>((resolve, reject) => {
            connection.execute({
                sqlText: `
                    SELECT DISTINCT RULE_NAME
                    FROM DATA_QUALITY_DB.DQ_METRICS.DQ_CHECK_RESULTS
                    WHERE UPPER(DATABASE_NAME) = ?
                      AND UPPER(SCHEMA_NAME) = ?
                      AND UPPER(TABLE_NAME) = ?
                      AND CHECK_STATUS = 'FAILED'
                      AND CHECK_TIMESTAMP >= DATEADD(hour, -24, CURRENT_TIMESTAMP())
                `,
                binds: [database.toUpperCase(), schema.toUpperCase(), table.toUpperCase()],
                complete: (err: any, stmt: any, rows: any) => {
                    if (err) reject(err);
                    else resolve({ rows: rows || [], columns: stmt?.getColumns() || [] });
                }
            });
        });
        context.failedMetricsLast24h = failedResult.rows.map((r: any) => r.RULE_NAME || r[0]);
        if (context.failedMetricsLast24h.length > 0) {
            context.metricsAvailable.push('failed_checks');
        }
    } catch { /* Ignore */ }

    // 3. Check for anomalies in column profile (high null rates)
    try {
        const anomalyResult = await new Promise<any>((resolve, reject) => {
            connection.execute({
                sqlText: `
                    SELECT COUNT(*) AS CNT
                    FROM DATA_QUALITY_DB.DQ_METRICS.DQ_COLUMN_PROFILE
                    WHERE UPPER(DATABASE_NAME) = ?
                      AND UPPER(SCHEMA_NAME) = ?
                      AND UPPER(TABLE_NAME) = ?
                      AND (NULL_COUNT * 100.0 / NULLIF(TOTAL_RECORDS, 0)) > 50
                      AND CREATED_AT >= DATEADD(day, -7, CURRENT_TIMESTAMP())
                `,
                binds: [database.toUpperCase(), schema.toUpperCase(), table.toUpperCase()],
                complete: (err: any, stmt: any, rows: any) => {
                    if (err) reject(err);
                    else resolve({ rows: rows || [], columns: stmt?.getColumns() || [] });
                }
            });
        });
        const highNullCount = parseInt(anomalyResult.rows[0]?.CNT || anomalyResult.rows[0]?.[0] || '0', 10);
        if (highNullCount > 0) {
            context.anomalyDetected = true;
            context.anomalyTrend = 'high_null_rate_detected';
            context.metricsAvailable.push('null_rate');
        }
    } catch { /* Ignore */ }

    // 4. Get freshness from table metadata
    try {
        const dbSchemaTable = `${database.toUpperCase()}.INFORMATION_SCHEMA.TABLES`;
        const freshnessResult = await new Promise<any>((resolve, reject) => {
            connection.execute({
                sqlText: `
                    SELECT TIMESTAMPDIFF(hour, LAST_ALTERED, CURRENT_TIMESTAMP()) AS HOURS_SINCE_UPDATE
                    FROM IDENTIFIER(?)
                    WHERE TABLE_SCHEMA = ?
                      AND TABLE_NAME = ?
                `,
                binds: [dbSchemaTable, schema.toUpperCase(), table.toUpperCase()],
                complete: (err: any, stmt: any, rows: any) => {
                    if (err) reject(err);
                    else resolve({ rows: rows || [], columns: stmt?.getColumns() || [] });
                }
            });
        });
        if (freshnessResult.rows.length > 0) {
            context.latestMetrics.freshnessHours = parseFloat(freshnessResult.rows[0].HOURS_SINCE_UPDATE || freshnessResult.rows[0][0]) || undefined;
            context.metricsAvailable.push('freshness');
        }
    } catch { /* Ignore */ }

    // 5. Get row count from DQ_COLUMN_PROFILE (most recent profile)
    try {
        const rowResult = await new Promise<any>((resolve, reject) => {
            connection.execute({
                sqlText: `
                    SELECT TOTAL_RECORDS
                    FROM DATA_QUALITY_DB.DQ_METRICS.DQ_COLUMN_PROFILE
                    WHERE UPPER(DATABASE_NAME) = ?
                      AND UPPER(SCHEMA_NAME) = ?
                      AND UPPER(TABLE_NAME) = ?
                      AND TOTAL_RECORDS IS NOT NULL
                    ORDER BY CREATED_AT DESC
                    LIMIT 1
                `,
                binds: [database.toUpperCase(), schema.toUpperCase(), table.toUpperCase()],
                complete: (err: any, stmt: any, rows: any) => {
                    if (err) reject(err);
                    else resolve({ rows: rows || [], columns: stmt?.getColumns() || [] });
                }
            });
        });
        if (rowResult.rows.length > 0) {
            context.latestMetrics.rowCount = parseInt(rowResult.rows[0].TOTAL_RECORDS || rowResult.rows[0][0], 10) || undefined;
            context.metricsAvailable.push('row_count');
        }
    } catch { /* Ignore */ }

    // 6. Lineage counts (simplified - using access history if available)
    // For now, set to 0. Can be enhanced with ACCOUNT_USAGE.ACCESS_HISTORY.
    context.downstreamCount = 0;
    context.upstreamCount = 0;

    console.log(`[ANTIGRAVITY] Context built for ${assetId}. Metrics: ${context.metricsAvailable.join(', ')}`);
    return context;
}

/**
 * Formats context into a prompt-safe JSON string.
 * This is what gets sent to the AI.
 */
export function formatContextForAI(context: ObservabilityContext): string {
    return JSON.stringify({
        asset: context.assetId,
        quality_score: context.latestMetrics.qualityScore ?? 'N/A',
        freshness_hours: context.latestMetrics.freshnessHours ?? 'N/A',
        row_count: context.latestMetrics.rowCount ?? 'N/A',
        failed_checks_24h: context.failedMetricsLast24h,
        anomaly_detected: context.anomalyDetected,
        anomaly_trend: context.anomalyTrend ?? 'none',
        metrics_available: context.metricsAvailable,
    }, null, 2);
}
