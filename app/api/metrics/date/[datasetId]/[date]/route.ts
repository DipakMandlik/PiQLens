import { NextRequest, NextResponse } from "next/server";
import { createErrorResponse } from "@/lib/errors";
import { getDateMetrics } from "@/lib/overview/metrics-service";

function isValidDate(value: string): boolean {
  return /^\d{4}-\d{2}-\d{2}$/.test(value);
}

export async function GET(
  _request: NextRequest,
  { params }: { params: Promise<{ datasetId: string; date: string }> }
) {
  try {
    const { datasetId, date } = await params;
    const normalizedDatasetId = decodeURIComponent(datasetId || "").trim();
    const normalizedDate = decodeURIComponent(date || "").trim();

    if (!normalizedDatasetId || !normalizedDate) {
      return NextResponse.json(
        { success: false, error: "Missing dataset_id or date" },
        { status: 400 }
      );
    }

    if (!isValidDate(normalizedDate)) {
      return NextResponse.json(
        { success: false, error: "Date must be in YYYY-MM-DD format" },
        { status: 400 }
      );
    }

    const payload = await getDateMetrics(normalizedDatasetId, normalizedDate);
    return NextResponse.json({ success: true, ...payload });
  } catch (error: unknown) {
    return NextResponse.json(createErrorResponse(error), { status: 500 });
  }
}
