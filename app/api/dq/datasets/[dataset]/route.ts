import { NextRequest, NextResponse } from 'next/server';
import { getServerConfig } from '@/lib/server-config';
import { getOrSetCache, buildCacheKey } from '@/lib/valkey';

export const runtime = 'nodejs';

/**
 * GET /api/dq/datasets/[dataset]
 * 
 * Fetches detailed metadata for a specific table (dataset).
 * [dataset] param is the TABLE NAME.
 * 
 * Returns:
 * - Metadata (Row Count, Created, etc)
 * - Columns (Name, Type)
 * - Onboarding Status (is it in DATASET_CONFIG?)
 * - DQ Summary (if onboarded)
 * 
 * STRICT SNOWFLAKE ENFORCEMENT. No Mocks.
 */
export async function GET(
    request: NextRequest,
    props: { params: Promise<{ dataset: string }> }
) {
    let datasetName = 'unknown';

    try {
        const { dataset } = await props.params;
        datasetName = dataset;

        const tableName = dataset.toUpperCase(); // Table Name
        const valkeyKey = buildCacheKey('dq', 'dataset-detail', tableName);

        const datasetData = await getOrSetCache(valkeyKey, 600, async () => {
            const config = getServerConfig();

            if (!config) {
                throw new Error('AUTH_FAILED: Not connected to Snowflake');
            }

            const { snowflakePool, executeQueryObjects } = await import('@/lib/snowflake');
            const connection = await snowflakePool.getConnection(config);

            const { searchParams } = new URL(request.url);

            // Context from configuration or query parameters
            const database = (searchParams.get('database') || config.database || 'BANKING_DW').toUpperCase();
            const schema = (searchParams.get('schema') || config.schema || 'BRONZE').toUpperCase();

            // 1. Fetch Table Metadata
            const metaQuery = `
              SELECT TABLE_NAME, ROW_COUNT, BYTES, CREATED, LAST_ALTERED, COMMENT
              FROM ${database}.INFORMATION_SCHEMA.TABLES
              WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
            `;
            const metaRows = await executeQueryObjects(connection, metaQuery, [schema, tableName]);

            if (metaRows.length === 0) {
                throw new Error('NOT_FOUND: Table not found in Snowflake');
            }
            const tableMeta = metaRows[0];

            // 2. Fetch Columns
            const colQuery = `
              SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, ORDINAL_POSITION, COMMENT
              FROM ${database}.INFORMATION_SCHEMA.COLUMNS
              WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
              ORDER BY ORDINAL_POSITION
            `;
            const colRows = await executeQueryObjects(connection, colQuery, [schema, tableName]);

            // 3. Check Onboarding (DATASET_CONFIG) and Data Quality metrics
            let isOnboarded = false;
            let datasetId = null;
            let latestDqScore = null;

            try {
                const configQuery = `
                  SELECT DATASET_ID 
                  FROM DATA_QUALITY_DB.DQ_CONFIG.DATASET_CONFIG
                  WHERE SOURCE_DATABASE = ? AND SOURCE_SCHEMA = ? AND SOURCE_TABLE = ?
                    AND IS_ACTIVE = TRUE
                `;
                const configRows = await executeQueryObjects(connection, configQuery, [database, schema, tableName]);

                if (configRows.length > 0) {
                    isOnboarded = true;
                    datasetId = configRows[0].DATASET_ID;

                    // Fetch latest DQ Score if onboarded
                    const dqQuery = `
                      SELECT DQ_SCORE
                      FROM DATA_QUALITY_DB.DQ_METRICS.DQ_DAILY_SUMMARY
                      WHERE TABLE_NAME = ?
                      ORDER BY SUMMARY_DATE DESC
                      LIMIT 1
                    `;
                    const dqRows = await executeQueryObjects(connection, dqQuery, [tableName]);
                    if (dqRows.length > 0) {
                        latestDqScore = dqRows[0].DQ_SCORE;
                    }
                }
            } catch (e) {
                console.warn('Could not check onboarding status or DQ (tables might be missing):', e);
            }

            return {
                name: tableMeta.TABLE_NAME,
                database,
                schema,
                rowCount: tableMeta.ROW_COUNT || 0,
                bytes: tableMeta.BYTES || 0,
                comment: tableMeta.COMMENT,
                created: tableMeta.CREATED,
                lastAltered: tableMeta.LAST_ALTERED,
                columns: colRows,
                isOnboarded,
                datasetId,
                latestDqScore
            };
        });

        return NextResponse.json({
            success: true,
            data: datasetData,
            metadata: { cached: true, timestamp: new Date().toISOString() }
        });

    } catch (error: any) {
        console.error(`Error fetching details for ${datasetName}:`, error);

        if (error.message?.includes('AUTH_FAILED')) {
            return NextResponse.json({ success: false, error: 'Not connected to Snowflake' }, { status: 401 });
        }
        if (error.message?.includes('NOT_FOUND')) {
            return NextResponse.json({ success: false, error: `Table not found in Snowflake` }, { status: 404 });
        }
        return NextResponse.json({
            success: false,
            error: error.message || 'Failed to fetch dataset details'
        }, { status: 500 });
    }
}
