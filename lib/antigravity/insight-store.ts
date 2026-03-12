/**
 * Antigravity Insight Store
 * Step 5: Insight Validation & Storage
 *
 * Stores validated insights to DQ_AI_INSIGHTS for audit and display.
 * Rule: Every insight must reference metrics and be reproducible.
 */

import { snowflakePool, executeQuery } from '@/lib/snowflake';
import { getServerConfig } from '@/lib/server-config';
import { AIInsight } from './insight-generator';

export interface StoredInsight {
    insightId: string;
    assetId: string;
    type: string;
    summary: string;
    details: {
        bullets: string[];
        evidence?: Record<string, any>;
    };
    severity: string;
    isActionable: boolean;
    createdAt: string;
}

/**
 * Stores a validated insight to DQ_AI_INSIGHTS table.
 */
export async function storeInsight(
    assetId: string,
    insight: AIInsight,
    evidence?: Record<string, any>
): Promise<string | null> {
    const config = getServerConfig();
    if (!config) {
        console.error('[ANTIGRAVITY] Cannot store insight: No Snowflake connection.');
        return null;
    }

    const connection = await snowflakePool.getConnection(config);

    const details = JSON.stringify({
        bullets: insight.bullets,
        evidence: evidence || {},
    });

    const sql = `
    INSERT INTO DATA_QUALITY_DB.DB_METRICS.DQ_AI_INSIGHTS (
      ASSET_ID,
      INSIGHT_TYPE,
      SUMMARY,
      DETAILS,
      SEVERITY,
      IS_ACTIONABLE
    )
    SELECT
      ?,
      ?,
      ?,
      PARSE_JSON(?),
      ?,
      ?
  `;

    try {
        await new Promise<void>((resolve, reject) => {
            connection.execute({
                sqlText: sql,
                binds: [
                    assetId.toUpperCase(),
                    insight.type,
                    insight.summary.slice(0, 500).replace(/'/g, "''"), // Truncate to match column size of 500
                    details,
                    insight.severity,
                    insight.isActionable,
                ],
                complete: (err: any) => {
                    if (err) reject(err);
                    else resolve();
                },
            });
        });

        console.log(`[ANTIGRAVITY] Insight stored for ${assetId}`);
        return 'SUCCESS';
    } catch (error: any) {
        console.error('[ANTIGRAVITY] Error storing insight:', error.message);
        // Table might not exist - this is OK for initial setup
        return null;
    }
}

/**
 * Retrieves recent insights for an asset.
 */
export async function getInsightsForAsset(
    database: string,
    schema: string,
    table: string,
    limit: number = 5
): Promise<StoredInsight[]> {
    const config = getServerConfig();
    if (!config) {
        return [];
    }

    const connection = await snowflakePool.getConnection(config);
    const assetId = `${database.toUpperCase()}.${schema.toUpperCase()}.${table.toUpperCase()}`;

    const sql = `
    SELECT
      INSIGHT_ID,
      ASSET_ID,
      INSIGHT_TYPE,
      SUMMARY,
      DETAILS,
      SEVERITY,
      IS_ACTIONABLE,
      CREATED_AT
    FROM DATA_QUALITY_DB.DB_METRICS.DQ_AI_INSIGHTS
    WHERE ASSET_ID = ?
    ORDER BY CREATED_AT DESC
    LIMIT ?
  `;

    try {
        const result = await new Promise<any[]>((resolve, reject) => {
            connection.execute({
                sqlText: sql,
                binds: [assetId, limit],
                complete: (err: any, _stmt: any, rows: any) => {
                    if (err) resolve([]);
                    else resolve(rows || []);
                },
            });
        });

        return result.map((row: any) => ({
            insightId: row.INSIGHT_ID || row[0],
            assetId: row.ASSET_ID || row[1],
            type: row.INSIGHT_TYPE || row[2],
            summary: row.SUMMARY || row[3],
            details: typeof row.DETAILS === 'string' ? JSON.parse(row.DETAILS) : (row.DETAILS || row[4]),
            severity: row.SEVERITY || row[5],
            isActionable: row.IS_ACTIONABLE || row[6],
            createdAt: row.CREATED_AT || row[7],
        }));
    } catch (error) {
        console.error('[ANTIGRAVITY] Error fetching insights:', error);
        return [];
    }
}
