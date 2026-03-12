import { executeQuery } from '@/lib/snowflake';
import { getOrSetCache, buildCacheKey } from '@/lib/valkey';

export type QualityHistoryPoint = {
  scan_date: string;   // YYYY-MM-DD
  dq_score: number;    // 0-100, read directly from DQ_DAILY_SUMMARY
};

/**
 * Fetch the last 5 available DQ_SCORE values directly
 * from DQ_DAILY_SUMMARY. No calculation — just read the score.
 * Returns data ordered ASC (oldest first) for graph plotting.
 */
export async function getLast5QualityHistory(
  connection: any,
  database: string,
  schema: string,
  table: string
): Promise<QualityHistoryPoint[]> {
  const db = database.toUpperCase();
  const sch = schema.toUpperCase();
  const tbl = table.toUpperCase();

  const valkeyKey = buildCacheKey('dq', 'quality-score-history', `${db}:${sch}:${tbl}:LAST_5`);
  const ttlSeconds = 5 * 60;

  return getOrSetCache(valkeyKey, ttlSeconds, async () => {
    const query = `
      SELECT
        TO_CHAR(SUMMARY_DATE, 'YYYY-MM-DD') AS scan_date,
        DQ_SCORE
      FROM DATA_QUALITY_DB.DQ_METRICS.DQ_DAILY_SUMMARY
      WHERE UPPER(DATABASE_NAME) = ?
        AND UPPER(SCHEMA_NAME) = ?
        AND UPPER(TABLE_NAME) = ?
        AND DQ_SCORE IS NOT NULL
      ORDER BY SUMMARY_DATE DESC
      LIMIT 5
    `;

    const result = await executeQuery(connection, query, [db, sch, tbl]);

    // Map to contract, then reverse to ASC for graph plotting
    const output: QualityHistoryPoint[] = result.rows
      .map((row: any[]) => ({
        scan_date: row[0],
        dq_score: Number(row[1]) || 0
      }))
      .reverse(); // oldest first for graph X-axis

    return output;
  });
}
