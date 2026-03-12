import { NextRequest, NextResponse } from "next/server";
import { getServerConfig } from "@/lib/server-config";
import { snowflakePool } from "@/lib/snowflake";

export const dynamic = 'force-dynamic';

export async function GET(request: NextRequest) {
    try {
        const config = getServerConfig();
        const conn = await snowflakePool.getConnection(config!);

        const result = await new Promise<any[]>((resolve, reject) => {
            conn.execute({
                sqlText: `
                    SELECT 
                        RUN_ID, 
                        TABLE_NAME, 
                        RUN_STATUS, 
                        START_TS, 
                        TRIGGERED_BY 
                    FROM DATA_QUALITY_DB.DQ_RESULTS.DQ_RUN_CONTROL 
                    ORDER BY START_TS DESC 
                    LIMIT 10
                `,
                complete: (err: any, _stmt: any, rows: any) => {
                    if (err) reject(err);
                    else resolve(rows || []);
                }
            });
        });

        return NextResponse.json({ success: true, count: result.length, data: result });
    } catch (e: any) {
        return NextResponse.json({ success: false, error: e.message });
    }
}
