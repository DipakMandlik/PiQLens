/**
 * Antigravity Schema Reader
 * Step 1: Schema Introspection
 *
 * This module reads Snowflake INFORMATION_SCHEMA to build an internal registry.
 * Antigravity REFUSES to generate SQL without validating schema first.
 */

import { snowflakePool, executeQuery } from '@/lib/snowflake';
import { getServerConfig } from '@/lib/server-config';

export interface ColumnSchema {
    columnName: string;
    dataType: string;
    isNullable: boolean;
    ordinalPosition: number;
}

export interface TableSchema {
    tableName: string;
    columns: ColumnSchema[];
}

export interface SchemaRegistry {
    [schemaName: string]: {
        [tableName: string]: ColumnSchema[];
    };
}

/**
 * Reads the schema from Snowflake INFORMATION_SCHEMA.
 * This is the MANDATORY first step before any SQL generation.
 */
export async function introspectSchema(
    database: string,
    targetSchemas?: string[]
): Promise<SchemaRegistry> {
    const config = getServerConfig();
    if (!config) {
        throw new Error('No Snowflake connection available.');
    }

    const connection = await snowflakePool.getConnection(config);

    // Use the custom view if available, otherwise query INFORMATION_SCHEMA directly
    const sql = `
    SELECT
      TABLE_CATALOG,
      TABLE_SCHEMA,
      TABLE_NAME,
      COLUMN_NAME,
      DATA_TYPE,
      IS_NULLABLE,
      ORDINAL_POSITION
    FROM ${database}.INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA NOT IN ('INFORMATION_SCHEMA', 'PUBLIC')
    ${targetSchemas && targetSchemas.length > 0 ? `AND TABLE_SCHEMA IN (${targetSchemas.map(s => `'${s.toUpperCase()}'`).join(',')})` : ''}
    
    UNION ALL
    
    SELECT
      TABLE_CATALOG,
      TABLE_SCHEMA,
      TABLE_NAME,
      COLUMN_NAME,
      DATA_TYPE,
      IS_NULLABLE,
      ORDINAL_POSITION
    FROM DATA_QUALITY_DB.INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'DB_METRICS'
    ORDER BY 1, 2, 3, 7
  `;

    const result = await executeQuery(connection, sql);

    const registry: SchemaRegistry = {};

    for (const row of result.rows) {
        const dbName = row[0] as string;
        const schemaName = row[1] as string;
        const tableName = row[2] as string;
        const columnName = row[3] as string;
        const dataType = row[4] as string;
        const isNullable = row[5] === 'YES';
        const ordinalPosition = row[6] as number;

        // Use Fully Qualified Schema Name as Key to prevent ambiguity
        const fqSchema = `${dbName}.${schemaName}`.toUpperCase();

        if (!registry[fqSchema]) {
            registry[fqSchema] = {};
        }
        if (!registry[fqSchema][tableName]) {
            registry[fqSchema][tableName] = [];
        }

        registry[fqSchema][tableName].push({
            columnName,
            dataType,
            isNullable,
            ordinalPosition,
        });
    }

    console.log(`[ANTIGRAVITY] Schema introspection complete. Found ${Object.keys(registry).length} schemas.`);
    return registry;
}

/**
 * Validates that a specific table and columns exist in the registry.
 * This MUST be called before generating any SQL that references them.
 */
export function validateColumns(
    registry: SchemaRegistry,
    schemaName: string,
    tableName: string,
    columnNames: string[]
): { valid: boolean; missing: string[] } {
    const tableColumns = registry[schemaName?.toUpperCase()]?.[tableName?.toUpperCase()];

    if (!tableColumns) {
        return { valid: false, missing: columnNames };
    }

    const columnSet = new Set(tableColumns.map(c => c.columnName.toUpperCase()));
    const missing = columnNames.filter(c => !columnSet.has(c.toUpperCase()));

    return { valid: missing.length === 0, missing };
}

/**
 * Gets available metrics for a specific asset from DQ_METRICS.
 */
export async function getAvailableMetrics(
    database: string,
    schema: string,
    table: string
): Promise<string[]> {
    const config = getServerConfig();
    if (!config) {
        throw new Error('No Snowflake connection available.');
    }

    const connection = await snowflakePool.getConnection(config);
    const assetId = `${database.toUpperCase()}.${schema.toUpperCase()}.${table.toUpperCase()}`;

    const sql = `
    SELECT DISTINCT METRIC_NAME
    FROM DATA_QUALITY_DB.DB_METRICS.DQ_METRICS
    WHERE ASSET_ID = ?
    ORDER BY METRIC_NAME
  `;

    try {
        const result = await new Promise<any[]>((resolve, reject) => {
            connection.execute({
                sqlText: sql,
                binds: [assetId],
                complete: (err: any, _stmt: any, rows: any) => {
                    if (err) resolve([]); // Return empty on error (table might not exist yet)
                    else resolve(rows || []);
                },
            });
        });

        return result.map((row: any) => row.METRIC_NAME || row[0]);
    } catch {
        return [];
    }
}
