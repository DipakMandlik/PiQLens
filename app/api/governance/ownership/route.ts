import { NextRequest, NextResponse } from 'next/server';
import { getServerConfig } from '@/lib/server-config';
import { invalidateCache } from '@/lib/valkey';

export const runtime = 'nodejs';

/**
 * GET /api/governance/ownership
 * Lists all datasets (BRONZE tables) with their ownership metadata.
 */
export async function GET(request: NextRequest) {
    try {
        const config = getServerConfig();
        if (!config) {
            return NextResponse.json({ success: false, error: 'Not connected to Snowflake' }, { status: 401 });
        }

        const { snowflakePool, executeQueryObjects } = await import('@/lib/snowflake');
        const connection = await snowflakePool.getConnection(config);

        // Fetch all tables + join with ownership config using fully qualified paths
        const sql = `
      SELECT 
          T.TABLE_NAME,
          O.OWNERSHIP_ID,
          O.DATA_OWNER,
          O.DATA_STEWARD,
          O.CRITICALITY,
          O.CONTACT_EMAIL
      FROM ${config.database}.INFORMATION_SCHEMA.TABLES T
      LEFT JOIN DATA_QUALITY_DB.DQ_CONFIG.DQ_DATASET_OWNERSHIP O 
        ON UPPER(O.DATASET_NAME) = UPPER(T.TABLE_NAME)
        AND UPPER(O.SCHEMA_NAME) = UPPER(T.TABLE_SCHEMA)
      WHERE T.TABLE_SCHEMA = 'BRONZE'
      ORDER BY T.TABLE_NAME
    `;

        const rows = await executeQueryObjects(connection, sql);

        return NextResponse.json({ success: true, data: rows });

    } catch (error: any) {
        console.error('Error fetching ownership:', error);
        // If table doesn't exist yet, return empty list gracefully
        return NextResponse.json({ success: false, error: error.message }, { status: 500 });
    }
}

/**
 * POST /api/governance/ownership
 * Updates or Creates ownership record for a dataset.
 */
export async function POST(request: NextRequest) {
    try {
        const body = await request.json();
        const { dataset, owner, steward, criticality, email } = body;

        if (!dataset) return NextResponse.json({ success: false, error: 'Dataset is required' }, { status: 400 });

        const config = getServerConfig();
        if (!config) return NextResponse.json({ success: false, error: 'Not connected' }, { status: 401 });

        const { snowflakePool, executeQuery } = await import('@/lib/snowflake');
        const connection = await snowflakePool.getConnection(config);

        // Merge into Ownership Table using fully qualified names
        const mergeSql = `
      MERGE INTO DATA_QUALITY_DB.DQ_CONFIG.DQ_DATASET_OWNERSHIP AS target
      USING (SELECT ? AS T, ? as S, ? as D, ? as O, ? as ST, ? as C, ? as E) AS source
      ON target.DATASET_NAME = source.T AND target.SCHEMA_NAME = source.S AND target.DATABASE_NAME = source.D
      WHEN MATCHED THEN
        UPDATE SET 
            DATA_OWNER = source.O, 
            DATA_STEWARD = source.ST, 
            CRITICALITY = source.C, 
            CONTACT_EMAIL = source.E,
            UPDATED_AT = CURRENT_TIMESTAMP()
      WHEN NOT MATCHED THEN
        INSERT (DATASET_NAME, SCHEMA_NAME, DATABASE_NAME, DATA_OWNER, DATA_STEWARD, CRITICALITY, CONTACT_EMAIL)
        VALUES (source.T, source.S, source.D, source.O, source.ST, source.C, source.E)
    `;

        await executeQuery(connection, mergeSql, [
            dataset, 'BRONZE', 'BANKING_DW', // Hardcoded context for V1
            owner, steward, criticality, email
        ]);

        // Audit Log
        const auditSql = `
      INSERT INTO DATA_QUALITY_DB.DQ_CONFIG.DQ_GOVERNANCE_AUDIT (ENTITY_TYPE, ENTITY_ID, ACTION, CHANGED_BY, NEW_VALUE)
      SELECT 'OWNERSHIP', ?, 'UPDATE', 'SYSTEM_USER', PARSE_JSON(?)
    `;
        await executeQuery(connection, auditSql, [dataset, JSON.stringify(body)]);

        // --- CACHE INVALIDATION HOOK ---
        Promise.allSettled([
            invalidateCache('piqlens:*:catalog:overview'), // clear global list
            invalidateCache(`piqlens:*:catalog:dataset:${dataset}`), // clear specific dataset
            invalidateCache('piqlens:*:dashboard:*') // Just in case metrics update
        ]).catch(err => console.error('Failed to invalidate caches post-ownership updates:', err));

        return NextResponse.json({ success: true });

    } catch (error: any) {
        console.error('Error updating ownership:', error);
        return NextResponse.json({ success: false, error: error.message }, { status: 500 });
    }
}
