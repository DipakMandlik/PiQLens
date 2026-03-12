import { NextRequest, NextResponse } from 'next/server';
import { getServerConfig } from '@/lib/server-config';
import { snowflakePool } from '@/lib/snowflake';

export async function GET(
    request: NextRequest,
    { params }: { params: Promise<{ table: string; column: string }> }
) {
    try {
        const { table, column } = await params;
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

        const sql = `
        SELECT 
            RULE_NAME as CHECK_NAME,
            CHECK_STATUS as STATUS,
            FAILURE_REASON as ERROR_MESSAGE,
            CREATED_TS as EXECUTED_AT
        FROM ${dqDatabase}.${observabilitySchema}.DQ_CHECK_RESULTS
        WHERE UPPER(TABLE_NAME) = ?
          AND UPPER(SCHEMA_NAME) = ?
          AND UPPER(DATABASE_NAME) = ?
          AND UPPER(COLUMN_NAME) = ?
        ORDER BY CREATED_TS DESC
        LIMIT 50
    `;

        const result = await new Promise<any[]>((resolve, reject) => {
            conn.execute({
                sqlText: sql,
                binds: [table.toUpperCase(), schema.toUpperCase(), database.toUpperCase(), column.toUpperCase()],
                complete: (err: any, _stmt: any, rows: any) => {
                    if (err) reject(err); else resolve(rows || []);
                },
            });
        });

        const checks = result.map(row => ({
            checkName: row.CHECK_NAME,
            status: row.STATUS, // PASSED, FAILED, WARNING
            errorMessage: row.ERROR_MESSAGE,
            executedAt: row.EXECUTED_AT
        }));

        return NextResponse.json({ success: true, data: checks });

    } catch (error: any) {
        console.error('API Error:', error);
        return NextResponse.json(
            { success: false, error: error.message || 'Internal Server Error' },
            { status: 500 }
        );
    }
}
