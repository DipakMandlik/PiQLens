import { NextResponse } from 'next/server';
import { enqueueReportJob, processPendingJobs } from '@/lib/reporting/v2/job-runner';
import { GenerateReportV2Request } from '@/lib/reporting/v2/types';

function normalizeLegacyFormat(format: string | undefined): 'csv' | 'xlsx' {
  const normalized = String(format || '').toLowerCase();
  if (normalized === 'csv') return 'csv';
  return 'xlsx';
}

function getErrorMessage(error: unknown, fallback: string): string {
  return error instanceof Error ? error.message : fallback;
}

export async function POST(request: Request) {
  try {
    const body = await request.json();
    const { date, dataset, format, includeInsights = false, includeTrend = false } = body;

    if (!date) {
      return NextResponse.json(
        { success: false, error: 'Date is required.' },
        { status: 400 }
      );
    }

    const isPlatform = !dataset || String(dataset).trim().toUpperCase() === 'PLATFORM';

    const payload: GenerateReportV2Request = {
      format: normalizeLegacyFormat(format),
      variant: includeInsights || includeTrend ? 'detailed' : 'summary',
      mode: 'date_aggregate',
      date,
      scope: isPlatform ? 'platform' : 'dataset',
      dataset: isPlatform ? undefined : dataset,
      generatedBy: 'legacy-intelligence-api',
    };

    const queued = await enqueueReportJob(payload);

    setTimeout(() => {
      void processPendingJobs(2).catch(() => undefined);
    }, 0);

    return NextResponse.json({
      success: true,
      data: {
        reportId: queued.reportId,
        report_id: queued.reportId,
        status: queued.status,
        message: 'Report generation started successfully.',
      },
    });
  } catch (error: unknown) {
    return NextResponse.json(
      { success: false, error: getErrorMessage(error, 'Internal server error') },
      { status: 500 }
    );
  }
}
