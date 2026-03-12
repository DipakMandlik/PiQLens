/**
 * Antigravity SQL Builder
 * Deterministic SQL generation from validated execution plans
 * 
 * CRITICAL: LLM generates PLANS, not SQL syntax
 * This module translates plans into safe, parameterized SQL
 */

import { ExecutionPlan } from './plan_validator';

export interface SQLBuildResult {
    sql: string;
    params: any[];
    estimatedComplexity: 'LOW' | 'MEDIUM' | 'HIGH';
}

/**
 * Builds deterministic SQL from a validated execution plan
 */
export function buildSQLFromPlan(
    plan: ExecutionPlan,
    assetId: string
): SQLBuildResult {
    // Determine primary table
    const primaryTable = plan.tables[0];

    // Build SELECT clause
    const selectClause = buildSelectClause(plan);

    // Build FROM clause
    const fromClause = plan.tables.join(', ');

    // Build WHERE clause
    const { whereClause, params } = buildWhereClause(plan, assetId, primaryTable);

    // Build GROUP BY clause
    const groupByClause = plan.group_by && plan.group_by.length > 0
        ? `GROUP BY ${plan.group_by.join(', ')}`
        : '';

    // Build ORDER BY clause
    const orderByClause = plan.order_by && plan.order_by.length > 0
        ? `ORDER BY ${plan.order_by.join(', ')}`
        : '';

    // Build LIMIT clause
    const limitClause = `LIMIT ${plan.limit || 100}`;

    // Assemble final SQL
    const sql = `
SELECT ${selectClause}
FROM ${fromClause}
WHERE ${whereClause}
${groupByClause}
${orderByClause}
${limitClause}
    `.trim();

    // Estimate complexity
    const estimatedComplexity = estimateComplexity(plan);

    return { sql, params, estimatedComplexity };
}

/**
 * Build SELECT clause from plan
 */
function buildSelectClause(plan: ExecutionPlan): string {
    const columns: string[] = [];

    // Add group_by columns first (required for GROUP BY)
    if (plan.group_by && plan.group_by.length > 0) {
        columns.push(...plan.group_by);
    }

    // Add metrics
    if (plan.metrics && plan.metrics.length > 0) {
        columns.push(...plan.metrics);
    }

    // Add other columns - ONLY if not aggregating
    // If we have metrics or group_by, strictly adhere to them to prevent SQL errors
    // (e.g. selecting a non-grouped column along with an aggregate)
    const isAggregated = (plan.metrics && plan.metrics.length > 0) || (plan.group_by && plan.group_by.length > 0);

    if (!isAggregated && plan.columns && plan.columns.length > 0) {
        for (const col of plan.columns) {
            if (!columns.includes(col)) {
                columns.push(col);
            }
        }
    }

    // If no columns specified, use *
    if (columns.length === 0) {
        return '*';
    }

    return columns.join(', ');
}

/**
 * Build WHERE clause with smart asset filtering
 */
function buildWhereClause(
    plan: ExecutionPlan,
    assetId: string,
    primaryTable: string
): { whereClause: string; params: any[] } {
    const conditions: string[] = ['1=1'];
    const params: any[] = [];

    // Determine table type for asset filtering
    const isLegacyTable = isLegacyDQTable(primaryTable);

    if (isLegacyTable) {
        // Legacy tables use DATABASE_NAME, SCHEMA_NAME, TABLE_NAME
        const [db, schema, table] = assetId.split('.');
        conditions.push(`UPPER(DATABASE_NAME) = '${db.toUpperCase()}'`);
        conditions.push(`UPPER(SCHEMA_NAME) = '${schema.toUpperCase()}'`);
        conditions.push(`UPPER(TABLE_NAME) = '${table.toUpperCase()}'`);
    } else {
        // New tables use ASSET_ID
        conditions.push(`UPPER(ASSET_ID) = '${assetId.toUpperCase()}'`);
    }

    // Add time window filter
    if (plan.time_window_days !== undefined && plan.time_window_days > 0) {
        // Determine time column based on table
        const timeColumn = getTimeColumnForTable(primaryTable);
        if (timeColumn) {
            conditions.push(`${timeColumn} >= DATEADD('DAY', -${plan.time_window_days}, CURRENT_DATE())`);
        }
    }

    // Add custom filters from plan
    if (plan.filters) {
        for (const [key, value] of Object.entries(plan.filters)) {
            // Skip asset and time_window_days as they're handled above
            if (key === 'asset' || key === 'time_window_days') continue;

            if (typeof value === 'string') {
                conditions.push(`UPPER(${key}) = '${value.toUpperCase()}'`);
            } else if (typeof value === 'number') {
                conditions.push(`${key} = ${value}`);
            } else if (Array.isArray(value)) {
                const values = value.map(v => typeof v === 'string' ? `'${v.toUpperCase()}'` : v).join(', ');
                conditions.push(`${key} IN (${values})`);
            }
        }
    }

    return {
        whereClause: conditions.join(' AND '),
        params
    };
}

/**
 * Check if table is a legacy DQ table (uses DATABASE_NAME/SCHEMA_NAME/TABLE_NAME)
 */
function isLegacyDQTable(tableName: string): boolean {
    const legacyTables = [
        'DQ_COLUMN_PROFILE',
        'DQ_CHECK_RESULTS',
        'DQ_RUN_CONTROL',
        'DQ_ANOMALY_DETECTION'
    ];

    const tableUpper = tableName.toUpperCase();
    return legacyTables.some(legacy => tableUpper.includes(legacy));
}

/**
 * Get the appropriate time column for a table
 */
function getTimeColumnForTable(tableName: string): string | null {
    const tableUpper = tableName.toUpperCase();

    if (tableUpper.includes('DQ_METRICS')) {
        return 'METRIC_TIME';
    } else if (tableUpper.includes('DQ_CHECK_RESULTS')) {
        return 'CHECK_TIMESTAMP';
    } else if (tableUpper.includes('DQ_COLUMN_PROFILE')) {
        return 'CREATED_AT';
    } else if (tableUpper.includes('DQ_AI_INSIGHTS')) {
        return 'CREATED_AT';
    }

    // Default fallback - do NOT guess CREATED_AT for user tables
    return null;
}

/**
 * Estimate query complexity
 */
function estimateComplexity(plan: ExecutionPlan): 'LOW' | 'MEDIUM' | 'HIGH' {
    let score = 0;

    // Multiple tables = more complex
    if (plan.tables.length > 1) score += 2;

    // Large time window = more complex
    if (plan.time_window_days && plan.time_window_days > 30) score += 1;
    if (plan.time_window_days && plan.time_window_days > 60) score += 1;

    // GROUP BY = more complex
    if (plan.group_by && plan.group_by.length > 0) score += 1;

    // Many columns = more complex
    const totalColumns = (plan.columns?.length || 0) + (plan.metrics?.length || 0);
    if (totalColumns > 10) score += 1;

    // Large limit = more complex
    if (plan.limit && plan.limit > 500) score += 1;

    if (score >= 4) return 'HIGH';
    if (score >= 2) return 'MEDIUM';
    return 'LOW';
}

/**
 * Validate SQL for safety (additional safety check)
 */
export function validateSQL(sql: string): { safe: boolean; errors: string[] } {
    const errors: string[] = [];
    const sqlUpper = sql.toUpperCase();

    // Check for dangerous operations
    const dangerousKeywords = ['DROP', 'DELETE', 'INSERT', 'UPDATE', 'TRUNCATE', 'ALTER', 'CREATE'];
    for (const keyword of dangerousKeywords) {
        if (sqlUpper.includes(keyword)) {
            errors.push(`Dangerous keyword detected: ${keyword}`);
        }
    }

    // Check for SQL injection patterns
    if (sql.includes('--') || sql.includes(';--') || sql.includes('/*')) {
        errors.push('Potential SQL injection pattern detected');
    }

    return {
        safe: errors.length === 0,
        errors
    };
}
