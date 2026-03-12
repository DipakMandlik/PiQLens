import { NextResponse } from 'next/server';
import { readFile } from 'fs/promises';
import fs from 'fs';
import { getReportJobRecord, incrementDownloadCount } from '@/lib/reporting/v2/job-runner';

export const runtime = 'nodejs';
export const dynamic = 'force-dynamic';

function getContentType(format: string | null): string {
  if (format?.toLowerCase() === 'xlsx') {
    return 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet';
  }
  return 'text/csv';
}

function getErrorMessage(error: unknown, fallback: string): string {
  return error instanceof Error ? error.message : fallback;
}

function formatDownloadDate(value: string | null | undefined): string {
  if (!value) {
    const now = new Date();
    return `${String(now.getDate()).padStart(2, '0')}-${String(now.getMonth() + 1).padStart(2, '0')}-${now.getFullYear()}`;
  }

  const match = String(value).slice(0, 10).match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (!match) return String(value).slice(0, 10);

  return `${match[3]}-${match[2]}-${match[1]}`;
}

function buildDownloadFilename(reportDate: string | null, format: string | null): string {
  const ext = (format || 'CSV').toLowerCase();
  const datePart = formatDownloadDate(reportDate);
  return `Pi_Qlens report for ${datePart}.${ext}`;
}

function parseMetadata(metadata: unknown): Record<string, unknown> {
  if (!metadata) return {};
  if (typeof metadata === 'object') return metadata as Record<string, unknown>;

  try {
    return JSON.parse(String(metadata)) as Record<string, unknown>;
  } catch {
    return {};
  }
}

function resolvePreferredFilename(metadata: Record<string, unknown>): string | null {
  const value = metadata.preferredFileName;
  if (typeof value !== 'string') return null;
  const trimmed = value.trim();
  return trimmed || null;
}

export async function GET(
  request: Request,
  context: { params: Promise<{ reportId: string }> }
) {
  const { reportId } = await context.params;

  try {
    const job = await getReportJobRecord(reportId);

    if (!job) {
      return NextResponse.json({ success: false, error: 'Report not found' }, { status: 404 });
    }

    if (job.status !== 'COMPLETED') {
      return NextResponse.json(
        { success: false, error: 'Report is not ready yet' },
        { status: 400 }
      );
    }

    if (!job.filePath || !fs.existsSync(job.filePath)) {
      return NextResponse.json(
        { success: false, error: 'Report file missing from storage' },
        { status: 404 }
      );
    }

    const buffer = await readFile(job.filePath);
    await incrementDownloadCount(reportId);

    const metadata = parseMetadata(job.metadata);
    const filename = resolvePreferredFilename(metadata) || buildDownloadFilename(job.reportDate, job.format);

    return new NextResponse(buffer, {
      headers: {
        'Content-Type': getContentType(job.format),
        'Content-Disposition': `attachment; filename="${filename}"`,
        'Content-Length': buffer.length.toString(),
      },
    });
  } catch (error: unknown) {
    return NextResponse.json(
      {
        success: false,
        error: getErrorMessage(error, 'Failed to download report'),
      },
      { status: 500 }
    );
  }
}

