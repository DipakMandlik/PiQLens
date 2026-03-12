
import { NextRequest, NextResponse } from "next/server";
import fs from "fs";
import path from "path";
// Use standalone pdfkit to avoid ENOENT errors for fonts
const PDFDocument = require("pdfkit/js/pdfkit.standalone");
import { snowflakePool, executeQuery } from "@/lib/snowflake";
import { getServerConfig } from "@/lib/server-config";

// Force dynamic and Node.js runtime to ensure fs works
export const dynamic = 'force-dynamic';
export const runtime = 'nodejs';

export async function GET(request: NextRequest) {
    console.log("PDF GENERATION STARTED - NODEJS RUNTIME");
    try {
        const config = getServerConfig();
        if (!config && !process.env.SNOWFLAKE_ACCOUNT) {
            return NextResponse.json(
                { success: false, error: "No Snowflake connection found." },
                { status: 401 }
            );
        }

        const connection = await snowflakePool.getConnection(config || undefined);

        // 1. DATA AGGREGATION QUERY (Latest Run Per Table for Today)
        const aggregationQuery = `
            WITH LatestRunsPerTable AS (
                SELECT 
                    c.TABLE_NAME, 
                    c.SCHEMA_NAME,
                    MAX(r.START_TS) as MAX_TS
                FROM DATA_QUALITY_DB.DQ_METRICS.DQ_CHECK_RESULTS c
                JOIN DATA_QUALITY_DB.DQ_METRICS.DQ_RUN_CONTROL r ON c.RUN_ID = r.RUN_ID
                WHERE r.START_TS::DATE = CURRENT_DATE()
                GROUP BY c.TABLE_NAME, c.SCHEMA_NAME
            )
            SELECT 
                c.TABLE_NAME, 
                c.SCHEMA_NAME,
                COUNT(*) as CHECKS_EXECUTED,
                SUM(CASE WHEN c.CHECK_STATUS = 'FAILED' THEN 1 ELSE 0 END) as FAILURES,
                MAX(c.TOTAL_RECORDS) as RECORD_COUNT,
                MAX(r.TOTAL_DATASETS) as DATASETS_IN_RUN
            FROM DATA_QUALITY_DB.DQ_METRICS.DQ_CHECK_RESULTS c
            JOIN DATA_QUALITY_DB.DQ_METRICS.DQ_RUN_CONTROL r ON c.RUN_ID = r.RUN_ID
            JOIN LatestRunsPerTable lr 
                ON c.TABLE_NAME = lr.TABLE_NAME 
                AND c.SCHEMA_NAME = lr.SCHEMA_NAME
                AND r.START_TS = lr.MAX_TS
            GROUP BY c.TABLE_NAME, c.SCHEMA_NAME
            ORDER BY FAILURES DESC, c.TABLE_NAME
            LIMIT 12;
        `;

        const tableResult = await executeQuery(connection, aggregationQuery);
        const hasData = tableResult.rows.length > 0;

        // Create PDF Stream - STRICT ONE PAGE
        const doc = new PDFDocument({ margin: 30, size: 'A4', bufferPages: true });

        const chunks: Buffer[] = [];
        doc.on('data', (chunk: any) => chunks.push(chunk));

        const pdfPromise = new Promise<Buffer>((resolve, reject) => {
            doc.on('end', () => resolve(Buffer.concat(chunks)));
            doc.on('error', reject);
        });


        // --- PDF GENERATION ---

        // 1. HEADER (Fixed Height)
        // Use text-based logo for reliable PDF rendering
        doc.fontSize(22).font('Helvetica-Bold').fillColor('#4F46E5').text('πQLens', 30, 30);
        doc.fontSize(9).font('Helvetica').fillColor('#6B7280').text('Data Quality Platform', 30, 58);

        // Title Section (Right Aligned) - Compact
        doc.fontSize(18).font('Helvetica-Bold').fillColor('#111827').text('Daily Data Quality Report', 200, 35, { align: 'right' });
        doc.fontSize(9).font('Helvetica').fillColor('#6B7280').text(`Generated: ${new Date().toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' })}`, 200, 60, { align: 'right' });
        doc.text('Scope: Today\'s Data Scans', 200, 75, { align: 'right' });

        if (!hasData) {
            doc.moveDown(5);
            doc.fontSize(16).fillColor('#DC2626').text('No data quality scan was executed for today’s data load.', { align: 'center' });
            doc.fontSize(12).fillColor('#374151').text('Please run a scan to generate this report.', { align: 'center' });
        } else {

            // Calculate Global KPIs from Aggregated Table Data
            let totalRecords = 0;
            let totalChecks = 0;
            let failedChecks = 0;
            let totalDatasets = tableResult.rows.length; // Count of unique tables found

            tableResult.rows.forEach((row: any[]) => {
                totalChecks += (row[2] || 0); // CHECKS_EXECUTED
                failedChecks += (row[3] || 0); // FAILURES
                totalRecords += (row[4] || 0); // RECORD_COUNT
            });

            const passedChecks = totalChecks - failedChecks;
            const score = totalChecks > 0 ? Math.round((passedChecks / totalChecks) * 100) : 0;
            const displayScoreColor = score >= 85 ? '#059669' : score >= 70 ? '#D97706' : '#DC2626';

            // 2. KPI STRIP (One Row, 6 Items)
            const kpiY = 110;
            const kpiW = 85;
            const drawKPI = (label: string, value: string | number, x: number, color: string = 'black', isMain: boolean = false) => {
                doc.fontSize(7).font('Helvetica-Bold').fillColor('#6B7280').text(label.toUpperCase(), x, kpiY, { width: kpiW, align: 'left' });
                const safeValue = value !== null && value !== undefined ? value.toString() : "0";
                doc.fontSize(16).font('Helvetica-Bold').fillColor(color).text(safeValue, x, kpiY + 15, { width: kpiW, align: 'left' });
            };

            drawKPI('OVERALL DATA', `${score}%`, 30, displayScoreColor, true);
            drawKPI('RECORDS SCANNED', totalRecords.toLocaleString(), 120);
            drawKPI('TABLES LOADED', totalDatasets, 220);
            drawKPI('CHECKS', totalChecks, 310);
            drawKPI('CHECKS', failedChecks, 400, failedChecks > 0 ? '#DC2626' : '#059669');
            drawKPI('DATASETS COVERED', totalDatasets, 490);

            // 3. CHECK RESULTS SUMMARY (Header + Chart)
            const chartY = 170;
            doc.fontSize(11).font('Helvetica-Bold').fillColor('#111827').text('Check Results Summary', 30, chartY);

            const barX = 30;
            const barY = chartY + 25;
            const fullBarWidth = 535;
            const barHeight = 30;

            // Background
            doc.rect(barX, barY, fullBarWidth, barHeight).fill('#F3F4F6');

            // Passed Green Bar
            const passedWidth = totalChecks > 0 ? (passedChecks / totalChecks) * fullBarWidth : 0;
            if (passedWidth > 0) doc.rect(barX, barY, passedWidth, barHeight).fill('#10B981'); // Green

            // Failed Red Bar
            if (failedChecks > 0) {
                const failedWidth = fullBarWidth - passedWidth;
                doc.rect(barX + passedWidth, barY, failedWidth, barHeight).fill('#EF4444'); // Red
            }

            // Text Labels inside bars
            doc.fontSize(10).font('Helvetica-Bold').fillColor('white');
            if (passedWidth > 100) {
                doc.text(`${passedChecks} Passed (${Math.round(passedChecks / totalChecks * 100)}%)`, barX + 10, barY + 9);
            }
            if (failedChecks > 0 && (fullBarWidth - passedWidth) > 80) {
                doc.text(`${failedChecks} Failed`, barX + passedWidth + 10, barY + 9);
            }

            // Legend
            doc.fontSize(8).font('Helvetica-Bold');
            doc.fillColor('#10B981').text(`● ${passedChecks} Checks Passed`, 30, barY + 40);
            if (failedChecks > 0) {
                doc.fillColor('#EF4444').text(`● ${failedChecks} Failed Checks`, 150, barY + 40);
            }


            // 4. TABLE QUALITY SUMMARY (Single Table)
            const tableY = 260;
            doc.fontSize(11).font('Helvetica-Bold').fillColor('#111827').text('Table Quality Summary (Today)', 30, tableY);

            const thY = tableY + 25;

            // Header
            doc.rect(30, thY, 535, 25).fill('#F3F4F6');
            doc.fillColor('#111827').fontSize(7).font('Helvetica-Bold');

            // Adjusted Column Positions
            const colX = { name: 40, schema: 170, recs: 230, exec: 300, fail: 360, score: 420, status: 480 };

            doc.text('TABLE NAME', colX.name, thY + 8);
            doc.text('SCHEMA', colX.schema, thY + 8);
            doc.text('RECORDS SCANNED', colX.recs, thY + 8);
            doc.text('CHECKS', colX.exec, thY + 8);
            doc.text('FAILED', colX.fail, thY + 8);
            doc.text('QUALITY', colX.score, thY + 8);
            doc.text('STATUS', colX.status, thY + 8);

            let rowY = thY + 25;
            doc.font('Helvetica').fontSize(8);

            tableResult.rows.forEach((row: any[], i: number) => {
                const [table, schema, exec, fails, recs] = row;
                const tScore = exec > 0 ? Math.round(((exec - fails) / exec) * 100) : 0;
                const status = fails > 0 ? 'ACTION\nREQUIRED' : 'OK';

                // Row BG (Zebra)
                if (i % 2 === 0) doc.rect(30, rowY, 535, 25).fill('#FFFFFF');
                else doc.rect(30, rowY, 535, 25).fill('#FAFAFA');

                const textY = rowY + 6;

                doc.fillColor('#111827').text(table, colX.name, textY, { width: 120, ellipsis: true });
                doc.text(schema, colX.schema, textY);
                doc.text((recs || 0).toLocaleString(), colX.recs, textY);
                doc.text(exec.toString(), colX.exec, textY);

                doc.fillColor(fails > 0 ? '#DC2626' : '#111827').text(fails.toString(), colX.fail, textY);

                // Score Color
                doc.fillColor(tScore >= 90 ? '#059669' : '#DC2626')
                    .text(`${tScore}%`, colX.score, textY);

                // Status Icon + Text
                if (fails > 0) {
                    doc.fillColor('#DC2626').font('Helvetica-Bold');
                    doc.circle(colX.status - 5, textY + 3, 3).fill(); // Red dot
                    doc.text('ACTION', colX.status + 5, textY - 2);
                    doc.fontSize(6).text('REQUIRED', colX.status + 5, textY + 7);
                } else {
                    doc.fillColor('#059669').font('Helvetica-Bold');
                    doc.circle(colX.status - 5, textY + 3, 3).fill(); // Green dot
                    doc.fontSize(8).text('OK', colX.status + 5, textY);
                }
                doc.font('Helvetica').fontSize(8);

                rowY += 25;
            });
        }

        // 5. FOOTER (Fixed)
        const range = doc.bufferedPageRange();
        for (let i = 0; i < range.count; i++) {
            doc.switchToPage(i);
            const footerY = doc.page.height - 40;
            doc.moveTo(30, footerY - 10).lineTo(565, footerY - 10).strokeColor('#E5E7EB').lineWidth(0.5).stroke();
            doc.fontSize(7).font('Helvetica').fillColor('#6B7280');
            doc.text('πQLens Enterprise Governance Report | Powered by Pi by 3™ | Scope: Today\'s Data Load | *Based on available DQ Scan Results', 30, footerY, { align: 'center', width: 535 });
        }

        doc.end();
        const buffer = await pdfPromise;

        const dateStr = new Date().toISOString().split('T')[0].replace(/-/g, '_');
        return new NextResponse(buffer as any, {
            headers: {
                'Content-Type': 'application/pdf',
                'Content-Disposition': `attachment; filename="PI_QLens_Daily_Data_Quality_Report_${dateStr}.pdf"`,
            },
        });

    } catch (error: any) {
        console.error("PDF Generation Error:", error);
        return NextResponse.json(
            { success: false, error: error.message },
            { status: 500 }
        );
    }
}
