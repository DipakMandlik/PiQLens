import { NextRequest, NextResponse } from 'next/server';
import { getLatestCompletedReport } from '@/lib/reporting/v2/job-runner';

function getErrorMessage(error: unknown, fallback: string): string {
  return error instanceof Error ? error.message : fallback;
}

export async function GET(
  request: NextRequest,
  { params }: { params: Promise<{ id: string }> }
) {
  try {
    const resolvedParams = await params;
    const reportId = resolvedParams.id;

    if (!reportId) {
      return new NextResponse('Report ID required', { status: 400 });
    }

    if (reportId === 'latest') {
      const latest = await getLatestCompletedReport();
      if (!latest) {
        return new NextResponse('No completed report found', { status: 404 });
      }
      return NextResponse.redirect(new URL(`/api/reports/v2/download/${latest.reportId}`, request.url));
    }

    return NextResponse.redirect(new URL(`/api/reports/v2/download/${reportId}`, request.url));
  } catch (error: unknown) {
    return new NextResponse(`Internal Server Error: ${getErrorMessage(error, 'unknown')}`, { status: 500 });
  }
}
