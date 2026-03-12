import { NextRequest, NextResponse } from "next/server";
import { createErrorResponse } from "@/lib/errors";
import { getRunDelta } from "@/lib/overview/metrics-service";

export async function GET(
  _request: NextRequest,
  { params }: { params: Promise<{ runId: string }> }
) {
  try {
    const { runId } = await params;
    const normalizedRunId = decodeURIComponent(runId || "").trim();

    if (!normalizedRunId) {
      return NextResponse.json(
        { success: false, error: "Missing run_id" },
        { status: 400 }
      );
    }

    const payload = await getRunDelta(normalizedRunId);
    return NextResponse.json({ success: true, ...payload });
  } catch (error: unknown) {
    return NextResponse.json(createErrorResponse(error), { status: 500 });
  }
}
