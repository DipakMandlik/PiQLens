/**
 * Antigravity Plan Validator
 * Validates execution plans against schema registry
 * CRITICAL: This is the anti-hallucination gate
 */

import { SchemaRegistry } from '@/lib/antigravity/schema-reader';

export interface ExecutionPlan {
    intent: 'TREND_ANALYSIS' | 'ROOT_CAUSE' | 'COMPARISON' | 'DISTRIBUTION' | 'IMPACT' | 'ANOMALY';
    tables: string[];
    columns: string[];
    metrics?: string[];
    filters?: Record<string, any>;
    group_by?: string[];
    order_by?: string[];
    limit?: number;
    time_window_days?: number;
}

export interface ValidationResult {
    valid: boolean;
    errors: string[];
    warnings: string[];
}

/**
 * Validates an execution plan against the schema registry
 * Returns detailed errors if validation fails
 */
export function validatePlan(
    plan: ExecutionPlan,
    schemaRegistry: SchemaRegistry
): ValidationResult {
    const errors: string[] = [];
    const warnings: string[] = [];

    // Rule 1: Plan must have at least one table
    if (!plan.tables || plan.tables.length === 0) {
        errors.push('Plan must specify at least one table');
        return { valid: false, errors, warnings };
    }

    // Rule 2: Validate each table exists in schema registry
    for (const tableName of plan.tables) {
        const tableFound = findTableInRegistry(tableName, schemaRegistry);

        if (!tableFound) {
            errors.push(`Table not found in schema: ${tableName}`);
        }
    }

    // Rule 3: Validate columns exist in specified tables
    if (plan.columns && plan.columns.length > 0) {
        for (const columnName of plan.columns) {
            const columnFound = findColumnInTables(columnName, plan.tables, schemaRegistry);

            if (!columnFound) {
                errors.push(`Column not found in any specified table: ${columnName}`);
            }
        }
    }

    // Rule 4: Validate metrics (if specified)
    if (plan.metrics && plan.metrics.length > 0) {
        for (const metricName of plan.metrics) {
            const metricFound = findColumnInTables(metricName, plan.tables, schemaRegistry);

            if (!metricFound) {
                errors.push(`Metric column not found: ${metricName}`);
            }
        }
    }

    // Rule 5: Validate time window is reasonable
    if (plan.time_window_days !== undefined) {
        if (plan.time_window_days < 0) {
            errors.push('Time window cannot be negative');
        } else if (plan.time_window_days > 90) {
            warnings.push(`Time window of ${plan.time_window_days} days is very large (max recommended: 90)`);
        }
    }

    // Rule 6: Validate limit is reasonable
    if (plan.limit !== undefined) {
        if (plan.limit < 1) {
            errors.push('Limit must be at least 1');
        } else if (plan.limit > 1000) {
            warnings.push(`Limit of ${plan.limit} is very large (max recommended: 1000)`);
        }
    }

    // Rule 7: Validate no wildcard SELECT * (check if columns is empty but should have values)
    if ((!plan.columns || plan.columns.length === 0) && (!plan.metrics || plan.metrics.length === 0)) {
        warnings.push('No specific columns selected - will use all columns (may be slow)');
    }

    // Rule 8: Validate group_by columns exist (skip SQL functions)
    if (plan.group_by && plan.group_by.length > 0) {
        for (const groupCol of plan.group_by) {
            // Skip validation for SQL function expressions (e.g., DATE(column), CAST(column AS type))
            const isSqlFunction = /^[A-Z_]+\(/.test(groupCol.trim());

            if (!isSqlFunction) {
                const found = findColumnInTables(groupCol, plan.tables, schemaRegistry);
                if (!found) {
                    errors.push(`GROUP BY column not found: ${groupCol}`);
                }
            }
        }
    }

    return {
        valid: errors.length === 0,
        errors,
        warnings
    };
}

/**
 * Find a table in the schema registry (case-insensitive, partial match)
 */
function findTableInRegistry(tableName: string, registry: SchemaRegistry): boolean {
    const tableUpper = tableName.toUpperCase();

    // Try exact match first
    for (const [schemaKey, tables] of Object.entries(registry)) {
        for (const registryTableName of Object.keys(tables)) {
            if (registryTableName.toUpperCase() === tableUpper) {
                return true;
            }

            // Also try matching just the table name without schema prefix
            const tableOnly = tableUpper.split('.').pop() || '';
            if (registryTableName.toUpperCase() === tableOnly) {
                return true;
            }
        }
    }

    return false;
}

/**
 * Find a column in any of the specified tables
 */
function findColumnInTables(
    columnName: string,
    tables: string[],
    registry: SchemaRegistry
): boolean {
    const columnUpper = columnName.toUpperCase();

    for (const tableName of tables) {
        // Find the table in registry
        for (const [schemaKey, schemaTables] of Object.entries(registry)) {
            for (const [registryTableName, columns] of Object.entries(schemaTables)) {
                const tableMatches =
                    registryTableName.toUpperCase() === tableName.toUpperCase() ||
                    registryTableName.toUpperCase() === tableName.split('.').pop()?.toUpperCase();

                if (tableMatches) {
                    // Check if column exists in this table
                    const columnExists = columns.some(
                        col => col.columnName.toUpperCase() === columnUpper
                    );

                    if (columnExists) {
                        return true;
                    }
                }
            }
        }
    }

    return false;
}

/**
 * Get available columns for a specific table
 */
export function getTableColumns(
    tableName: string,
    registry: SchemaRegistry
): string[] {
    const tableUpper = tableName.toUpperCase();

    for (const [schemaKey, tables] of Object.entries(registry)) {
        for (const [registryTableName, columns] of Object.entries(tables)) {
            const tableMatches =
                registryTableName.toUpperCase() === tableUpper ||
                registryTableName.toUpperCase() === tableUpper.split('.').pop();

            if (tableMatches) {
                return columns.map(col => col.columnName);
            }
        }
    }

    return [];
}
