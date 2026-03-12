import { NextRequest, NextResponse } from 'next/server';
import { getServerConfig } from '@/lib/server-config';
import { snowflakePool } from '@/lib/snowflake';

export async function GET(
    request: NextRequest,
    { params }: { params: Promise<{ table: string }> }
) {
    try {
        const table = (await params).table; // This is the table name from the URL
        console.log(`[API] Fetching columns for table: ${table}`);
        const searchParams = request.nextUrl.searchParams;
        const database = searchParams.get('database');
        const schema = searchParams.get('schema');

        if (!database || !schema) {
            return NextResponse.json(
                { success: false, error: 'Missing required parameters: database, schema' },
                { status: 400 }
            );
        }

        const dqDatabase = 'DATA_QUALITY_DB';
        const observabilitySchema = 'DQ_METRICS';

        const config = getServerConfig();
        const conn = await snowflakePool.getConnection(config || undefined);

        // SQL to fetch columns with health status and basic stats
        // Uses DQ_COLUMN_PROFILE and DQ_CHECK_RESULTS from legacy setup
        const sql = `
      WITH LatestExecution AS (
        SELECT MAX(RUN_ID) as RUN_ID
        FROM ${dqDatabase}.${observabilitySchema}.DQ_COLUMN_PROFILE
        WHERE UPPER(TABLE_NAME) = ?
          AND UPPER(SCHEMA_NAME) = ?
          AND UPPER(DATABASE_NAME) = ?
      ),
      ColumnStats AS (
         SELECT 
            UPPER(COLUMN_NAME) as COLUMN_NAME, 
            MAX(NULL_COUNT) as NULL_COUNT, 
            MAX(TOTAL_RECORDS) as ROW_COUNT, 
            MAX(DISTINCT_COUNT) as DISTINCT_COUNT,
            MAX(RUN_ID) as RUN_ID
         FROM ${dqDatabase}.${observabilitySchema}.DQ_COLUMN_PROFILE
         WHERE RUN_ID = (SELECT RUN_ID FROM LatestExecution)
           AND UPPER(TABLE_NAME) = ?
           AND UPPER(SCHEMA_NAME) = ?
           AND UPPER(DATABASE_NAME) = ?
         GROUP BY UPPER(COLUMN_NAME)
      ),
      ColumnChecks AS (
         SELECT 
            UPPER(COLUMN_NAME) as COLUMN_NAME,
            COUNT(CASE WHEN CHECK_STATUS = 'FAILED' THEN 1 END) as FAILED_CHECKS,
            COUNT(CASE WHEN CHECK_STATUS = 'WARNING' THEN 1 END) as WARNING_CHECKS
         FROM ${dqDatabase}.${observabilitySchema}.DQ_CHECK_RESULTS
         WHERE UPPER(TABLE_NAME) = ? 
           AND UPPER(SCHEMA_NAME) = ? 
           AND UPPER(DATABASE_NAME) = ?
           AND CREATED_TS >= DATEADD(day, -1, CURRENT_TIMESTAMP()) -- Look at recent checks
         GROUP BY UPPER(COLUMN_NAME)
      )
      SELECT 
        c.COLUMN_NAME,
        c.DATA_TYPE,
        c.ORDINAL_POSITION,
        c.IS_NULLABLE,
        s.NULL_COUNT,
        s.ROW_COUNT,
        s.DISTINCT_COUNT,
        ZEROIFNULL(ch.FAILED_CHECKS) as FAILED_CHECKS,
        ZEROIFNULL(ch.WARNING_CHECKS) as WARNING_CHECKS,
        0 as HIGH_ANOMALIES,
        0 as MEDIUM_ANOMALIES,
        0 as DRIFT_EVENTS
      FROM ${database}.INFORMATION_SCHEMA.COLUMNS c
      LEFT JOIN ColumnStats s ON UPPER(c.COLUMN_NAME) = UPPER(s.COLUMN_NAME)
      LEFT JOIN ColumnChecks ch ON UPPER(c.COLUMN_NAME) = UPPER(ch.COLUMN_NAME)
      WHERE UPPER(c.TABLE_CATALOG) = ? 
        AND UPPER(c.TABLE_SCHEMA) = ? 
        AND UPPER(c.TABLE_NAME) = ?
      ORDER BY c.ORDINAL_POSITION
    `;

        // Note: Binds order must match ? in query
        // LatestExecution: table, schema, database
        // ColumnStats: table, schema, database
        // ColumnChecks: table, schema, database
        // Main Query: database, schema, table

        const binds = [
            table.toUpperCase(), schema.toUpperCase(), database.toUpperCase(), // LatestExecution
            table.toUpperCase(), schema.toUpperCase(), database.toUpperCase(), // ColumnStats
            table.toUpperCase(), schema.toUpperCase(), database.toUpperCase(), // ColumnChecks
            database.toUpperCase(), schema.toUpperCase(), table.toUpperCase()  // Filters
        ];

        const result = await new Promise<any[]>((resolve, reject) => {
            conn.execute({
                sqlText: sql,
                binds: binds,
                complete: (err: any, _stmt: any, rows: any) => {
                    if (err) reject(err);
                    else resolve(rows || []);
                },
            });
        });

        const columns = result.map(row => {
            let status = 'Healthy';
            if (row.FAILED_CHECKS > 0 || row.HIGH_ANOMALIES > 0) {
                status = 'Critical';
            } else if (row.WARNING_CHECKS > 0 || row.MEDIUM_ANOMALIES > 0 || row.DRIFT_EVENTS > 0) {
                status = 'Warning';
            }

            const rowCount = row.ROW_COUNT || 0;
            const nullCount = row.NULL_COUNT || 0;
            const nullPct = rowCount > 0 ? (nullCount / rowCount) * 100 : 0;

            return {
                columnName: row.COLUMN_NAME,
                dataType: row.DATA_TYPE,
                ordinalPosition: row.ORDINAL_POSITION,
                isNullable: row.IS_NULLABLE === 'YES',
                nullPct: parseFloat(nullPct.toFixed(2)),
                distinctCount: row.DISTINCT_COUNT,
                status,
                failedChecks: row.FAILED_CHECKS,
                anomalies: row.HIGH_ANOMALIES + row.MEDIUM_ANOMALIES,
                driftEvents: row.DRIFT_EVENTS
            };
        });

        return NextResponse.json({ success: true, data: columns });

    } catch (error: any) {
        console.error('API Error:', error);
        return NextResponse.json(
            { success: false, error: error.message || 'Internal Server Error' },
            { status: 500 }
        );
    }
}
