import { NextResponse } from 'next/server';
import { listReportHistory, processPendingJobs } from '@/lib/reporting/v2/job-runner';

function getMetadataValue(metadata: Record<string, unknown>, key: string): unknown {
  return Object.prototype.hasOwnProperty.call(metadata, key) ? metadata[key] : undefined;
}

function getRequestMetadata(metadata: Record<string, unknown>): Record<string, unknown> {
  const req = getMetadataValue(metadata, 'request');
  if (req && typeof req === 'object' && !Array.isArray(req)) {
    return req as Record<string, unknown>;
  }
  return {};
}

export async function GET() {
  try {
    await processPendingJobs(1);
    const rows = await listReportHistory(100);

    const data = rows.map((row) => {
      const metadata = row.metadata || {};
      const reqMeta = getRequestMetadata(metadata);

      return {
        REPORT_ID: row.reportId,
        REPORT_DATE: row.reportDate,
        RUN_ID: (getMetadataValue(reqMeta, 'runId') as string | undefined) || row.scope || '',
        GENERATED_AT: row.generatedAt,
        GENERATED_BY: row.generatedBy,
        FORMAT: (row.format || '').toUpperCase(),
        FILE_PATH: row.filePath,
        TOTAL_DATASETS: Number(getMetadataValue(metadata, 'totalDatasets') || 0),
        TOTAL_FAILED: Number(getMetadataValue(metadata, 'failedChecks') || 0),
        DQ_SCORE: Number(getMetadataValue(metadata, 'successRate') || 0),
        STATUS: row.status,
      };
    });

    return NextResponse.json({
      success: true,
      data,
    });
  } catch {
    return NextResponse.json(
      { success: false, error: 'Failed to fetch report history' },
      { status: 500 }
    );
  }
}
