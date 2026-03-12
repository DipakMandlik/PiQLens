import { NextResponse } from 'next/server';
import { getReportStatus, processPendingJobs } from '@/lib/reporting/v2/job-runner';

export const runtime = 'nodejs';
export const dynamic = 'force-dynamic';

function getErrorMessage(error: unknown, fallback: string): string {
  return error instanceof Error ? error.message : fallback;
}

export async function GET(
  request: Request,
  context: { params: Promise<{ reportId: string }> }
) {
  const { reportId } = await context.params;

  try {
    await processPendingJobs(1);
    const status = await getReportStatus(reportId);

    if (!status) {
      return NextResponse.json(
        {
          success: false,
          error: 'Report not found',
        },
        { status: 404 }
      );
    }

    return NextResponse.json({
      success: true,
      data: status,
    });
  } catch (error: unknown) {
    return NextResponse.json(
      {
        success: false,
        error: getErrorMessage(error, 'Failed to fetch report status'),
      },
      { status: 500 }
    );
  }
}
