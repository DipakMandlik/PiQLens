import { NextRequest, NextResponse } from 'next/server';
import { executeQueryObjects, snowflakePool } from '@/lib/snowflake';
import { getServerConfig } from '@/lib/server-config';

// =====================================================
// Types
// =====================================================

interface ReportHistoryItem {
    reportId: string;
    reportType: string;
    scope: string;
    generatedAt: string;
    generatedBy: string;
    format: string;
    fileSize: number;
    downloadCount: number;
}

export async function GET(request: NextRequest) {
    try {
        const searchParams = request.nextUrl.searchParams;
        const page = parseInt(searchParams.get('page') || '1');
        const limit = parseInt(searchParams.get('limit') || '10');
        const scope = searchParams.get('scope'); // optional filter
        const startDate = searchParams.get('startDate'); // optional filter

        const offset = (page - 1) * limit;

        const config = getServerConfig();
        if (!config) {
            return NextResponse.json(
                { success: false, error: 'Database configuration missing' },
                { status: 500 }
            );
        }

        const connection = await snowflakePool.getConnection(config);

        // Build Query
        let whereClause = 'WHERE 1=1';
        const binds: any[] = [];

        if (scope) {
            whereClause += ' AND REPORT_SCOPE = ?';
            binds.push(scope.toUpperCase());
        }

        if (startDate) {
            whereClause += ' AND DATE(GENERATED_AT) >= ?';
            binds.push(startDate);
        }

        // Count Query
        const countSql = `SELECT COUNT(*) as TOTAL FROM DATA_QUALITY_DB.DB_METRICS.DQ_REPORTS ${whereClause}`;
        const countResult = await executeQueryObjects(connection, countSql, binds);
        const total = countResult[0]?.TOTAL || 0;

        // Data Query
        const dataSql = `
            SELECT 
                REPORT_ID,
                REPORT_TYPE,
                REPORT_SCOPE,
                GENERATED_AT,
                GENERATED_BY,
                FILE_FORMAT,
                FILE_SIZE_BYTES,
                DOWNLOAD_COUNT
            FROM DATA_QUALITY_DB.DB_METRICS.DQ_REPORTS
            ${whereClause}
            ORDER BY GENERATED_AT DESC
            LIMIT ? OFFSET ?
        `;

        // Add limit/offset to binds (make sure they are numbers)
        const dataBinds = [...binds, limit, offset];

        const rows = await executeQueryObjects(connection, dataSql, dataBinds);

        const reports: ReportHistoryItem[] = rows.map((row: any) => ({
            reportId: row.REPORT_ID,
            reportType: row.REPORT_TYPE,
            scope: row.REPORT_SCOPE,
            generatedAt: row.GENERATED_AT,
            generatedBy: row.GENERATED_BY,
            format: row.FILE_FORMAT,
            fileSize: row.FILE_SIZE_BYTES,
            downloadCount: row.DOWNLOAD_COUNT || 0
        }));

        return NextResponse.json({
            success: true,
            data: {
                reports,
                pagination: {
                    page,
                    limit,
                    total,
                    totalPages: Math.ceil(total / limit)
                }
            }
        });

    } catch (error: any) {
        console.error('Report history error:', error);
        return NextResponse.json(
            { success: false, error: error.message || 'Internal Server Error' },
            { status: 500 }
        );
    }
}
