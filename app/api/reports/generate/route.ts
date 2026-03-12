import { NextRequest, NextResponse } from 'next/server';
import { v4 as uuidv4 } from 'uuid';
import {
    aggregatePlatformReportData,
    aggregateDatasetReportData,
    aggregateIncidentReportData
} from '@/lib/reporting/data-aggregator';
import {
    buildPlatformReport,
    buildDatasetReport,
    buildIncidentReport
} from '@/lib/reporting/report-builder';
import { renderJSON } from '@/lib/reporting/renderers/json-renderer';
import { renderCSV } from '@/lib/reporting/renderers/csv-renderer';
import { executeQuery, snowflakePool } from '@/lib/snowflake';
import { getServerConfig } from '@/lib/server-config';
import { writeFile, mkdir } from 'fs/promises';
import path from 'path';
import fs from 'fs';

// =====================================================
// Types
// =====================================================

interface GenerateReportRequest {
    scope: 'platform' | 'dataset' | 'incident';
    format: 'json' | 'csv' | 'pdf';
    dataset?: string; // Required for dataset scope (db.schema.table)
    incidentId?: string; // Required for incident scope
    date?: string; // YYYY-MM-DD, defaults to today
    generatedBy?: string;
}

export async function POST(request: NextRequest) {
    try {
        const body: GenerateReportRequest = await request.json();
        const {
            scope,
            format: reportFormat = 'json',
            dataset: datasetId,
            incidentId,
            date = new Date().toISOString().split('T')[0],
            generatedBy = 'system'
        } = body;

        // Validate request
        if (!['platform', 'dataset', 'incident'].includes(scope)) {
            return NextResponse.json(
                { success: false, error: 'Invalid report scope' },
                { status: 400 }
            );
        }

        if (!['json', 'csv', 'pdf'].includes(reportFormat)) {
            return NextResponse.json(
                { success: false, error: 'Invalid report format' },
                { status: 400 }
            );
        }

        if (scope === 'dataset' && !datasetId) {
            return NextResponse.json(
                { success: false, error: 'Dataset ID is required for dataset reports' },
                { status: 400 }
            );
        }

        // 1. Connectivity Check
        const config = getServerConfig();
        if (!config) {
            return NextResponse.json(
                { success: false, error: 'Database configuration missing' },
                { status: 500 }
            );
        }

        const connection = await snowflakePool.getConnection(config);
        if (!connection) {
            return NextResponse.json(
                { success: false, error: 'Failed to connect to Snowflake' },
                { status: 500 }
            );
        }

        // 2. Generate Report Data & Build Report
        let report: any;
        let fileName = '';

        const reportId = uuidv4();
        const timestamp = new Date().toISOString().replace(/[:.]/g, '-');

        if (scope === 'platform') {
            const data = await aggregatePlatformReportData(date, generatedBy);
            report = buildPlatformReport(data, reportId);
            fileName = `platform_report_${date}_${timestamp}`;
        } else if (scope === 'dataset') {
            const [database, schema, table] = datasetId!.split('.');
            const data = await aggregateDatasetReportData(database, schema, table, date, generatedBy);
            report = buildDatasetReport(data, reportId);
            fileName = `dataset_report_${database}_${table}_${date}_${timestamp}`;
        } else if (scope === 'incident') {
            const data = await aggregateIncidentReportData(incidentId || 'unknown', generatedBy);
            report = buildIncidentReport(data, reportId);
            fileName = `incident_report_${incidentId}_${timestamp}`;
        }

        // 3. Render Report
        let fileContent: string | Buffer = '';
        let fileExtension = '';
        let mimeType = '';

        switch (reportFormat) {
            case 'json':
                fileContent = renderJSON(report);
                fileExtension = 'json';
                mimeType = 'application/json';
                break;
            case 'csv':
                // renderCSV is generic but types might restrict it? 
                // We cast report to any to allow implementation to handle specific logic
                // But our renderCSV supports PlatformReport | DatasetReport... imports.
                fileContent = await renderCSV(report);
                fileExtension = 'csv';
                mimeType = 'text/csv';
                break;
            case 'pdf':
                // PDF not implemented yet
                return NextResponse.json(
                    { success: false, error: 'PDF generation not implemented yet' },
                    { status: 501 }
                );
            default:
                throw new Error(`Unsupported format: ${reportFormat}`);
        }

        // 4. Save to Storage (Local for now)
        const storagePath = process.env.REPORTS_STORAGE_PATH || path.join(process.cwd(), 'reports');

        // Ensure directory exists
        if (!fs.existsSync(storagePath)) {
            await mkdir(storagePath, { recursive: true });
        }

        const fullFileName = `${fileName}.${fileExtension}`;
        const filePath = path.join(storagePath, fullFileName);

        await writeFile(filePath, fileContent);

        // 5. Store Metadata in Snowflake
        // Insert into DQ_REPORTS table
        const metaSql = `
      INSERT INTO DATA_QUALITY_DB.DB_METRICS.DQ_REPORTS (
        REPORT_ID,
        REPORT_TYPE,
        REPORT_SCOPE,
        GENERATED_AT,
        GENERATED_BY,
        FILE_FORMAT,
        FILE_PATH,
        FILE_SIZE_BYTES,
        METADATA
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, PARSE_JSON(?))
    `;

        const binds = [
            reportId,
            report.title || `${scope} Report`, // Simplified title
            scope.toUpperCase(),
            new Date().toISOString(),
            generatedBy,
            reportFormat.toUpperCase(),
            filePath,
            Buffer.byteLength(fileContent),
            JSON.stringify(report.metadata || {})
        ];

        try {
            await executeQuery(connection, metaSql, binds); // Fixed: distinct args
        } catch (dbError) {
            console.error('Failed to save report metadata to DB:', dbError);
            // Construct non-blocking warning? We continue to return success since file is generated.
        }

        // 6. Return Response
        return NextResponse.json({
            success: true,
            data: {
                reportId,
                fileName: fullFileName,
                downloadUrl: `/api/reports/download/${reportId}`,
                generatedAt: report.metadata.generatedAt
            }
        });

    } catch (error: any) {
        console.error('Report generation error:', error);
        return NextResponse.json(
            { success: false, error: error.message || 'Internal Server Error' },
            { status: 500 }
        );
    }
}
