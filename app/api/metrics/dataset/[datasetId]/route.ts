import { NextRequest, NextResponse } from "next/server";
import { createErrorResponse } from "@/lib/errors";
import { getDatasetMetrics } from "@/lib/overview/metrics-service";

export async function GET(
  _request: NextRequest,
  { params }: { params: Promise<{ datasetId: string }> }
) {
  try {
    const { datasetId } = await params;
    const normalizedDatasetId = decodeURIComponent(datasetId || "").trim();

    if (!normalizedDatasetId) {
      return NextResponse.json(
        { success: false, error: "Missing dataset_id" },
        { status: 400 }
      );
    }

    const payload = await getDatasetMetrics(normalizedDatasetId);
    return NextResponse.json({ success: true, ...payload });
  } catch (error: unknown) {
    return NextResponse.json(createErrorResponse(error), { status: 500 });
  }
}
