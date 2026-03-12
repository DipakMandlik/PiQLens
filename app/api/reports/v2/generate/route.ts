import { NextRequest, NextResponse } from 'next/server';
import { enqueueReportJob, processPendingJobs } from '@/lib/reporting/v2/job-runner';
import { GenerateReportV2Request } from '@/lib/reporting/v2/types';

export const runtime = 'nodejs';
export const dynamic = 'force-dynamic';

function getErrorMessage(error: unknown, fallback: string): string {
  return error instanceof Error ? error.message : fallback;
}

export async function POST(request: NextRequest) {
  try {
    const body = (await request.json()) as GenerateReportV2Request;
    const result = await enqueueReportJob(body);

    setTimeout(() => {
      void processPendingJobs(2).catch(() => undefined);
    }, 0);

    return NextResponse.json({
      success: true,
      data: result,
    });
  } catch (error: unknown) {
    return NextResponse.json(
      {
        success: false,
        error: getErrorMessage(error, 'Failed to queue report generation'),
      },
      { status: 400 }
    );
  }
}
