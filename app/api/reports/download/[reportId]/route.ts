import { NextRequest, NextResponse } from 'next/server';
import { readFile } from 'fs/promises';
import { executeQuery, executeQueryObjects, snowflakePool } from '@/lib/snowflake';
import { getServerConfig } from '@/lib/server-config';
import path from 'path';
import fs from 'fs';

// =====================================================
// Download Report API
// =====================================================

export async function GET(
    request: NextRequest,
    context: { params: Promise<{ reportId: string }> }
) {
    // Await params in Next.js 15+
    const { reportId } = await context.params;

    try {
        const config = getServerConfig();
        if (!config) {
            return NextResponse.json(
                { success: false, error: 'Database configuration missing' },
                { status: 500 }
            );
        }

        const connection = await snowflakePool.getConnection(config);

        // 1. Get Report Details
        const query = `
            SELECT FILE_PATH, FILE_FORMAT
            FROM DATA_QUALITY_DB.DB_METRICS.DQ_REPORTS
            WHERE REPORT_ID = ?
        `;

        const rows = await executeQueryObjects(connection, query, [reportId]);

        if (rows.length === 0) {
            return NextResponse.json(
                { success: false, error: 'Report not found' },
                { status: 404 }
            );
        }

        const report = rows[0];
        const filePath = report.FILE_PATH;
        const format = report.FILE_FORMAT?.toLowerCase();

        // 2. Validate File Exists
        if (!fs.existsSync(filePath)) {
            return NextResponse.json(
                { success: false, error: 'Report file missing from storage' },
                { status: 404 }
            );
        }

        // 3. Read File
        const fileBuffer = await readFile(filePath);

        // 4. Update Download Count
        const updateSql = `
            UPDATE DATA_QUALITY_DB.DB_METRICS.DQ_REPORTS
            SET DOWNLOAD_COUNT = COALESCE(DOWNLOAD_COUNT, 0) + 1,
                LAST_DOWNLOADED_AT = CURRENT_TIMESTAMP()
            WHERE REPORT_ID = ?
        `;

        await executeQuery(connection, updateSql, [reportId]);

        // 5. Serve File
        const contentType = format === 'csv' ? 'text/csv' : 'application/json';
        const filename = path.basename(filePath);

        return new NextResponse(fileBuffer, {
            headers: {
                'Content-Type': contentType,
                'Content-Disposition': `attachment; filename="${filename}"`,
                'Content-Length': fileBuffer.length.toString()
            }
        });

    } catch (error: any) {
        console.error('Download error:', error);
        return NextResponse.json(
            { success: false, error: error.message || 'Internal Server Error' },
            { status: 500 }
        );
    }
}