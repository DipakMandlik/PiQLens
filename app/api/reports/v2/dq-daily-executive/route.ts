import { NextRequest, NextResponse } from 'next/server';
import { renderDQDailyExecutiveReport } from '@/lib/reporting/v2/formatters/dq-daily-executive';
import type { DQDailyExecutiveRequest } from '@/lib/reporting/v2/types';

export const runtime = 'nodejs';
export const dynamic = 'force-dynamic';

function getErrorMessage(error: unknown, fallback: string): string {
  return error instanceof Error ? error.message : fallback;
}

function isValidationError(message: string): boolean {
  const lower = message.toLowerCase();
  return (
    lower.includes('validation failed') ||
    lower.includes('must be') ||
    lower.includes('expected') ||
    lower.includes('non-empty')
  );
}

export async function POST(request: NextRequest) {
  try {
    const body = (await request.json()) as DQDailyExecutiveRequest;

    const { buffer, fileName } = await renderDQDailyExecutiveReport(body);

    return new NextResponse(new Uint8Array(buffer), {
      headers: {
        'Content-Type': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        'Content-Disposition': `attachment; filename="${fileName}"`,
        'Content-Length': String(buffer.length),
      },
    });
  } catch (error: unknown) {
    const message = getErrorMessage(error, 'Failed to generate daily executive Excel report.');
    const status = isValidationError(message) ? 400 : 500;

    return NextResponse.json(
      {
        success: false,
        error: message,
      },
      { status }
    );
  }
}


