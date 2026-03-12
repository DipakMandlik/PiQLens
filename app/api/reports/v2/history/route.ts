import { NextRequest, NextResponse } from 'next/server';
import { listReportHistory, processPendingJobs } from '@/lib/reporting/v2/job-runner';

export const runtime = 'nodejs';
export const dynamic = 'force-dynamic';

function getErrorMessage(error: unknown, fallback: string): string {
  return error instanceof Error ? error.message : fallback;
}

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

export async function GET(request: NextRequest) {
  try {
    const limit = Math.min(Number(request.nextUrl.searchParams.get('limit') || 100), 500);

    await processPendingJobs(1);
    const rows = await listReportHistory(limit);

    const data = rows.map((row) => {
      const metadata = row.metadata || {};
      const reqMeta = getRequestMetadata(metadata);

      return {
        reportId: row.reportId,
        reportDate: row.reportDate,
        generatedAt: row.generatedAt,
        generatedBy: row.generatedBy,
        format: (row.format || '').toUpperCase(),
        status: row.status,
        scope: row.scope,
        filePath: row.filePath,
        totalDatasets: Number(getMetadataValue(metadata, 'totalDatasets') || 0),
        totalChecks: Number(getMetadataValue(metadata, 'totalChecks') || 0),
        failedChecks: Number(getMetadataValue(metadata, 'failedChecks') || 0),
        successRate: Number(getMetadataValue(metadata, 'successRate') || 0),
        mode: (getMetadataValue(reqMeta, 'mode') as string | undefined) || null,
        variant: (getMetadataValue(reqMeta, 'variant') as string | undefined) || null,
        errorMessage: (getMetadataValue(metadata, 'errorMessage') as string | undefined) || null,
        rowCountExported: Number(getMetadataValue(metadata, 'rowCountExported') || 0),
        fileSizeBytes: Number(getMetadataValue(metadata, 'fileSizeBytes') || 0),
      };
    });

    return NextResponse.json({
      success: true,
      data,
    });
  } catch (error: unknown) {
    return NextResponse.json(
      {
        success: false,
        error: getErrorMessage(error, 'Failed to fetch report history'),
      },
      { status: 500 }
    );
  }
}
