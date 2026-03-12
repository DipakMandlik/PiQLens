import { NextRequest, NextResponse } from 'next/server';
import { getServerConfig } from '@/lib/server-config';

export const runtime = 'nodejs';

/**
 * GET /api/monitoring/list
 * Returns a list of active monitors derived from Snowflake metadata and DQ metrics.
 * 
 * Monitor Types:
 * - FRESHNESS: Based on LAST_ALTERED vs Threshold (default 60 mins)
 * - VOLUME: Based on ROW_COUNT
 * - QUALITY: Based on latest DQ_SCORE
 * 
 * STRICT SNOWFLAKE ENFORCEMENT.
 */
export async function GET(request: NextRequest) {
    try {
        const config = getServerConfig();
        if (!config) {
            return NextResponse.json({ success: false, error: 'Not connected to Snowflake' }, { status: 401 });
        }

        const { snowflakePool, executeQueryObjects } = await import('@/lib/snowflake');
        const connection = await snowflakePool.getConnection(config);

        // Context
        const database = config.database || 'BANKING_DW';
        const schema = config.schema || 'BRONZE';

        // Query: Get Tables + Latest DQ Score
        // We use a LEFT JOIN to get all tables, even those without DQ scores.
        const sql = `
      WITH LATEST_DQ AS (
          SELECT TABLE_NAME, DQ_SCORE, SUMMARY_DATE
          FROM DATA_QUALITY_DB.DQ_METRICS.DQ_DAILY_SUMMARY
          QUALIFY ROW_NUMBER() OVER (PARTITION BY TABLE_NAME ORDER BY SUMMARY_DATE DESC) = 1
      )
      SELECT 
          T.TABLE_NAME, 
          T.ROW_COUNT, 
          T.LAST_ALTERED, 
          D.DQ_SCORE,
          D.SUMMARY_DATE as DQ_DATE
      FROM ${database}.INFORMATION_SCHEMA.TABLES T
      LEFT JOIN LATEST_DQ D ON UPPER(D.TABLE_NAME) = UPPER(T.TABLE_NAME)
      WHERE T.TABLE_SCHEMA = ?
      ORDER BY T.TABLE_NAME
    `;

        const rows = await executeQueryObjects(connection, sql, [schema]);

        const monitors = [];

        for (const row of rows) {
            const tableName = row.TABLE_NAME;
            const now = new Date();
            const lastAltered = row.LAST_ALTERED ? new Date(row.LAST_ALTERED) : null;

            // 1. FRESHNESS MONITOR
            let freshnessStatus = 'HEALTHY';
            let freshnessValue = 0; // minutes

            if (lastAltered) {
                freshnessValue = Math.floor((now.getTime() - lastAltered.getTime()) / (1000 * 60));
                // Default Threshold: 60 mins
                if (freshnessValue > 60) freshnessStatus = 'BREACHED';
                else if (freshnessValue > 45) freshnessStatus = 'WARNING';
            }

            monitors.push({
                id: `${tableName}-FRESHNESS`,
                name: `${tableName}`,
                type: 'FRESHNESS',
                status: freshnessStatus,
                value: `${freshnessValue} min`,
                threshold: '< 60 min',
                lastEvaluated: lastAltered ? lastAltered.toISOString() : null,
                message: freshnessStatus === 'BREACHED' ? 'Data is stale' : 'Data is fresh',
                table: tableName
            });

            // 2. VOLUME MONITOR
            // Simple heuristic for V1: Just reporting. Always Healthy unless 0.
            const rowCount = row.ROW_COUNT || 0;
            let volumeStatus = 'HEALTHY';
            if (rowCount === 0) volumeStatus = 'WARNING';

            monitors.push({
                id: `${tableName}-VOLUME`,
                name: `${tableName}`,
                type: 'VOLUME',
                status: volumeStatus,
                value: `${rowCount.toLocaleString()} rows`,
                threshold: '> 0',
                lastEvaluated: new Date().toISOString(), // Real-time check
                message: rowCount === 0 ? 'Table is empty' : 'Volume normal',
                table: tableName
            });

            // 3. QUALITY MONITOR (Only if DQ data exists)
            if (row.DQ_SCORE !== undefined && row.DQ_SCORE !== null) {
                const score = row.DQ_SCORE;
                let qualityStatus = 'HEALTHY';
                if (score < 90) qualityStatus = 'WARNING';
                if (score < 80) qualityStatus = 'BREACHED';

                monitors.push({
                    id: `${tableName}-QUALITY`,
                    name: `${tableName}`,
                    type: 'QUALITY',
                    status: qualityStatus,
                    value: `${score}%`,
                    threshold: '> 90%',
                    lastEvaluated: row.DQ_DATE ? new Date(row.DQ_DATE).toISOString() : null,
                    message: qualityStatus === 'BREACHED' ? 'Critical quality issues' : 'Quality checks passed',
                    table: tableName
                });
            }
        }

        return NextResponse.json({
            success: true,
            data: monitors,
            timestamp: new Date().toISOString()
        });

    } catch (error: any) {
        console.error('Error fetching monitors:', error);
        return NextResponse.json({
            success: false,
            error: error.message || 'Failed to fetch monitors'
        }, { status: 500 });
    }
}
