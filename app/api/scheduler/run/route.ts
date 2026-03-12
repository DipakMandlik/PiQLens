/* eslint-disable @typescript-eslint/no-explicit-any */
import { NextRequest, NextResponse } from "next/server";
import { getServerConfig } from "@/lib/server-config";
import { snowflakePool } from "@/lib/snowflake";

const PROCESS_PROC = "CALL DATA_QUALITY_DB.DQ_METRICS.SP_PROCESS_DUE_SCHEDULES(?)";

function parseSchedulerResult(rows: any[]): { message: string } {
  if (!rows?.length) {
    return { message: "Scheduler procedure executed" };
  }

  const first = rows[0];
  const firstValue = first ? first[Object.keys(first)[0]] : null;

  if (typeof firstValue === "string" && firstValue.trim()) {
    return { message: firstValue };
  }

  if (firstValue && typeof firstValue === "object") {
    const maybeMessage = (firstValue as Record<string, unknown>).message;
    if (typeof maybeMessage === "string" && maybeMessage.trim()) {
      return { message: maybeMessage };
    }
  }

  return { message: "Scheduler procedure executed" };
}

async function invokeSchedulerProcedure(forceScheduleId: string | null) {
  const config = getServerConfig();
  if (!config) {
    throw new Error("No Snowflake connection found. Please connect first.");
  }

  const conn = await snowflakePool.getConnection(config);
  const rows = await new Promise<any[]>((resolve, reject) => {
    conn.execute({
      sqlText: PROCESS_PROC,
      binds: [forceScheduleId],
      complete: (err: any, _stmt: any, resultRows: any[]) => {
        if (err) reject(err);
        else resolve(resultRows || []);
      },
    });
  });

  return parseSchedulerResult(rows);
}

/**
 * GET /api/scheduler/run
 * Trigger Snowflake-native scheduler procedure for due schedules.
 */
export async function GET() {
  try {
    const result = await invokeSchedulerProcedure(null);

    return NextResponse.json({
      success: true,
      data: {
        executed: 0,
        fetched: 0,
        skipped: 0,
        failed: 0,
        message: result.message,
      },
    });
  } catch (error: any) {
    console.error("Scheduler tick error:", error);
    return NextResponse.json(
      { success: false, error: error?.message || "Scheduler tick failed" },
      { status: 500 }
    );
  }
}

/**
 * POST /api/scheduler/run
 * Force execute a specific schedule immediately via Snowflake procedure.
 */
export async function POST(request: NextRequest) {
  try {
    const body = await request.json();
    const scheduleId = String(body?.scheduleId || "").trim() || null;

    const result = await invokeSchedulerProcedure(scheduleId);

    return NextResponse.json({
      success: true,
      data: {
        scheduleId,
        executed: scheduleId ? 1 : 0,
        message: result.message,
      },
    });
  } catch (error: any) {
    console.error("Force execute error:", error);
    return NextResponse.json(
      { success: false, error: error?.message || "Failed to execute schedule" },
      { status: 500 }
    );
  }
}

