import { snowflakePool, executeQueryObjects } from './snowflake';
import { DataCatalogTable, CatalogListEntry, ColumnMeta, ConstraintMeta } from '../types/catalog';
import { getOrSetCache, buildCacheKey } from './valkey';

/**
 * Service for fetching and building Data Catalog metadata from Snowflake.
 * Uses Valkey getOrSetCache to prevent overloading Snowflake for heavy metadata queries.
 */

// 1. Helper to get catalog overview
export async function getCatalogOverview(
  forceRefresh: boolean = false
): Promise<CatalogListEntry[]> {
  const cacheKey = buildCacheKey('catalog', 'dataset-list');

  // Valkey wrapper expects (key, ttlSeconds, fetchFn)
  return getOrSetCache<CatalogListEntry[]>(
    cacheKey,
    600, // 10 minutes TTL
    async () => {
      const conn = await snowflakePool.getConnection();
      const config = snowflakePool.getCurrentConfig();
      const currentDb = config?.database;

      if (!currentDb) {
        throw new Error('No active database configured for Snowflake connection.');
      }

      // Query tables from INFORMATION_SCHEMA and left join with the latest DQ summary
      const tablesQuery = `
        SELECT 
          t.TABLE_CATALOG AS "database", 
          t.TABLE_SCHEMA AS "schema", 
          t.TABLE_NAME AS "table",
          NULLIF(t.TABLE_CATALOG || '.' || t.TABLE_SCHEMA || '.' || t.TABLE_NAME, '') AS "id",
          COALESCE(t.ROW_COUNT, 0) AS "rowCount",
          COALESCE(t.BYTES, 0) AS "sizeBytes",
          t.LAST_ALTERED AS "lastModified",
          COALESCE(dq.DQ_SCORE, NULL) AS "dqScore",
          dq.TRUST_LEVEL AS "trustLevel",
          dq.QUALITY_GRADE AS "qualityGrade"
        FROM IDENTIFIER(?).INFORMATION_SCHEMA.TABLES t
        LEFT JOIN (
          SELECT * FROM DATA_QUALITY_DB.DQ_METRICS.DQ_DAILY_SUMMARY
          WHERE SUMMARY_DATE = (SELECT MAX(SUMMARY_DATE) FROM DATA_QUALITY_DB.DQ_METRICS.DQ_DAILY_SUMMARY)
        ) dq 
          ON t.TABLE_CATALOG = dq.DATABASE_NAME 
         AND t.TABLE_SCHEMA = dq.SCHEMA_NAME 
         AND t.TABLE_NAME = dq.TABLE_NAME
        WHERE t.TABLE_TYPE = 'BASE TABLE'
          AND t.TABLE_SCHEMA != 'INFORMATION_SCHEMA'
      `;

      // We'll run the base query.
      const tablesRaw = await executeQueryObjects(conn, tablesQuery, [currentDb]);

      // Let's also fetch 30-day usage count separately to avoid horrifying join performance with ACCOUNT_USAGE
      let usageMap = new Map<string, number>();
      try {
        const usageQuery = `
          SELECT
            f.value:objectName::string AS "fullyQualifiedName",
            COUNT(*) as "queryCount"
          FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY ah,
          LATERAL FLATTEN(input => ah.base_objects_accessed) f
          WHERE ah.query_start_time >= DATEADD(day, -30, CURRENT_DATE())
            AND f.value:objectDomain::string = 'Table'
          GROUP BY 1
        `;
        const usageRaw = await executeQueryObjects(conn, usageQuery);
        usageRaw.forEach((row: any) => {
          if (row.fullyQualifiedName) {
            usageMap.set(row.fullyQualifiedName.toUpperCase(), Number(row.queryCount));
          }
        });
      } catch (err) {
        console.warn("Could not fetch usage from ACCOUNT_USAGE (requires SNOWFLAKE DB access):", err);
        // Gracefully handle missing privileges or delay.
      }

      // Merge and classify
      const results: CatalogListEntry[] = tablesRaw.map((row: any) => {
        const fqn = `${row.database}.${row.schema}.${row.table}`.toUpperCase();
        const qCount = usageMap.get(fqn) || 0;

        let classification = 'Low';
        if (qCount > 100) classification = 'High';
        else if (qCount >= 20) classification = 'Medium';
        else if (qCount === 0) classification = 'Unknown';

        return {
          id: row.id || fqn,
          database: row.database || '',
          schema: row.schema || '',
          table: row.table || '',
          businessDomain: row.schema || 'Default', // fallback to schema if no explicit domain
          rowCount: Number(row.rowCount) || 0,
          sizeBytes: Number(row.sizeBytes) || 0,
          lastModified: row.lastModified ? new Date(row.lastModified).toISOString() : new Date().toISOString(),
          usageClassification: classification,
          trustLevel: row.trustLevel || 'Unknown',
          dqScore: row.dqScore ? Number(row.dqScore) : null,
          qualityGrade: row.qualityGrade || null,
          tags: {} // Setup for subsequent metadata tagging logic
        };
      });

      return results;
    }
  );
}

// 2. Fetch specific table deep-dive metadata
export async function getTableDetails(
  database: string,
  schema: string,
  table: string
): Promise<DataCatalogTable | null> {
  const cacheKey = buildCacheKey('catalog', 'dataset-overview', `${database}.${schema}.${table}`);

  return getOrSetCache<DataCatalogTable | null>(
    cacheKey,
    900, // 15 minutes TTL
    async () => {
      const conn = await snowflakePool.getConnection();

      // We will perform multiple queries in parallel for efficiency

      const tablesQuery = `
        SELECT 
          t.TABLE_CATALOG AS "database", 
          t.TABLE_SCHEMA AS "schema", 
          t.TABLE_NAME AS "table",
          t.TABLE_OWNER AS "owner",
          COALESCE(t.ROW_COUNT, 0) AS "rowCount",
          COALESCE(t.BYTES, 0) AS "sizeBytes",
          t.CREATED AS "createdAt",
          t.LAST_ALTERED AS "lastModified",
          COALESCE(dq.DQ_SCORE, NULL) AS "dqScore",
          dq.FAILURE_RATE AS "failureRate",
          dq.TRUST_LEVEL AS "trustLevel",
          dq.QUALITY_GRADE AS "qualityGrade",
          dq.IS_SLA_MET AS "slaMet"
        FROM ${database}.INFORMATION_SCHEMA.TABLES t
        LEFT JOIN (
          SELECT * FROM DATA_QUALITY_DB.DQ_METRICS.DQ_DAILY_SUMMARY
          WHERE DATABASE_NAME = ? AND SCHEMA_NAME = ? AND TABLE_NAME = ?
          ORDER BY SUMMARY_DATE DESC LIMIT 1
        ) dq 
          ON t.TABLE_CATALOG = dq.DATABASE_NAME 
         AND t.TABLE_SCHEMA = dq.SCHEMA_NAME 
         AND t.TABLE_NAME = dq.TABLE_NAME
        WHERE t.TABLE_CATALOG = ? AND t.TABLE_SCHEMA = ? AND t.TABLE_NAME = ?
          AND t.TABLE_TYPE = 'BASE TABLE'
      `;

      const columnsQuery = `
        SELECT 
          COLUMN_NAME AS "name",
          DATA_TYPE AS "dataType",
          IS_NULLABLE AS "isNullable",
          ORDINAL_POSITION AS "ordinalPosition",
          COMMENT AS "comment",
          COLUMN_DEFAULT AS "defaultValue"
        FROM ${database}.INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
        ORDER BY ORDINAL_POSITION
      `;

      const constraintsQuery = `
        SELECT 
          tc.CONSTRAINT_NAME AS "constraintName",
          tc.CONSTRAINT_TYPE AS "constraintType",
          kcu.COLUMN_NAME AS "columnName"
        FROM ${database}.INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
        JOIN ${database}.INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
          ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
         AND tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA 
        WHERE tc.TABLE_SCHEMA = ? AND tc.TABLE_NAME = ?
      `;

      // Parallel execution
      const [tableRaw, columnsRaw, constraintsRaw] = await Promise.all([
        executeQueryObjects(conn, tablesQuery, [database, schema, table, database, schema, table]),
        executeQueryObjects(conn, columnsQuery, [schema, table]),
        executeQueryObjects(conn, constraintsQuery, [schema, table])
      ]);

      if (!tableRaw || tableRaw.length === 0) {
        return null;
      }

      const tData = tableRaw[0];

      // Format Columns
      const columns: ColumnMeta[] = columnsRaw.map((c: any) => ({
        name: c.name,
        dataType: c.dataType,
        isNullable: c.isNullable === 'YES',
        ordinalPosition: Number(c.ordinalPosition),
        comment: c.comment,
        defaultValue: c.defaultValue
      }));

      // Format Constraints
      const constraints: ConstraintMeta[] = constraintsRaw.map((c: any) => ({
        constraintName: c.constraintName,
        constraintType: c.constraintType,
        columnName: c.columnName
      }));

      // Extract Usage 30d
      let queryCount30d = 0;
      let lastAccessed = new Date().toISOString();
      const fqn = `${database}.${schema}.${table}`.toUpperCase();

      try {
        const usageQuery = `
           SELECT COUNT(*) as "queryCount", MAX(query_start_time) as "lastAccessed"
           FROM SNOWFLAKE.ACCOUNT_USAGE.ACCESS_HISTORY ah,
           LATERAL FLATTEN(input => ah.base_objects_accessed) f
           WHERE f.value:objectName::string = ?
             AND ah.query_start_time >= DATEADD(day, -30, CURRENT_DATE())
        `;
        const usageRaw = await executeQueryObjects(conn, usageQuery, [fqn]);
        if (usageRaw && usageRaw.length > 0) {
          queryCount30d = Number(usageRaw[0].queryCount) || 0;
          lastAccessed = usageRaw[0].lastAccessed
            ? new Date(usageRaw[0].lastAccessed).toISOString()
            : lastAccessed;
        }
      } catch (err) {
        console.warn(`Could not fetch usage details for ${fqn}`, err);
      }

      let classification = 'Low';
      if (queryCount30d > 100) classification = 'High';
      else if (queryCount30d >= 20) classification = 'Medium';
      else if (queryCount30d === 0) classification = 'Unknown';

      // Assemble final object
      const result: DataCatalogTable = {
        database: tData.database,
        schema: tData.schema,
        table: tData.table,
        businessDomain: tData.schema || 'Default',
        columns,
        constraints,
        rowCount: Number(tData.rowCount) || 0,
        sizeBytes: Number(tData.sizeBytes) || 0,
        createdAt: tData.createdAt ? new Date(tData.createdAt).toISOString() : new Date().toISOString(),
        lastModified: tData.lastModified ? new Date(tData.lastModified).toISOString() : new Date().toISOString(),
        owner: tData.owner || 'UNKNOWN',
        upstream: [], // Implement object_dependencies query if needed
        downstream: [],
        queryCount30d,
        lastAccessed,
        usageClassification: classification as any,
        classification: 'Internal', // Placeholder for tagging
        tags: {},
        dqScore: tData.dqScore ? Number(tData.dqScore) : null,
        failureRate: tData.failureRate !== null ? Number(tData.failureRate) : null,
        trustLevel: tData.trustLevel,
        qualityGrade: tData.qualityGrade,
        slaMet: tData.slaMet === true || tData.slaMet === 'true' || tData.slaMet === 1
      };

      return result;
    }
  );
}
