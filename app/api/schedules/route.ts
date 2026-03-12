/* eslint-disable @typescript-eslint/no-explicit-any */
import { NextRequest, NextResponse } from "next/server";
import { ensureSchedulerBootstrapped } from "@/lib/scheduler/bootstrap";
import {
  buildCronExpressionFromInput,
  normalizeFrequencyType,
  normalizeRunType,
  normalizeTimezone,
  resolveInitialNextRunAtUtc,
  shouldRunOnce,
} from "@/lib/scheduler/cron";
import {
  createSchedule,
  listSchedulesByTable,
  resolveDatasetId,
  updateScheduleStatus,
} from "@/lib/scheduler/repository";
import { CanonicalSchedule } from "@/lib/scheduler/types";

function toBoolean(value: unknown, fallback = false): boolean {
  if (typeof value === "boolean") return value;
  if (typeof value === "number") return value !== 0;
  if (typeof value === "string") {
    const normalized = value.trim().toUpperCase();
    if (["TRUE", "1", "Y", "YES"].includes(normalized)) return true;
    if (["FALSE", "0", "N", "NO"].includes(normalized)) return false;
  }
  return fallback;
}

function toStringArray(value: unknown): string[] {
  if (!Array.isArray(value)) return [];
  return value
    .map((item) => String(item || "").trim())
    .filter(Boolean);
}

function summarizeSchedule(schedule: CanonicalSchedule): string {
  if (!schedule.is_recurring || schedule.run_once) {
    return `One-time at ${schedule.schedule_time || "00:00"} ${schedule.timezone}`;
  }

  if (schedule.schedule_type === "hourly") {
    return "Every hour";
  }

  if (schedule.schedule_type === "weekly") {
    const days = schedule.schedule_days?.length ? schedule.schedule_days.join(", ") : "Mon";
    return `Weekly on ${days} at ${schedule.schedule_time || "00:00"} ${schedule.timezone}`;
  }

  return `Daily at ${schedule.schedule_time || "00:00"} ${schedule.timezone}`;
}

function toScheduleResponse(schedule: CanonicalSchedule) {
  return {
    scheduleId: schedule.schedule_id,
    databaseName: schedule.database_name,
    schemaName: schedule.schema_name,
    tableName: schedule.table_name,
    scanType: schedule.scan_type,
    runType: schedule.run_type,
    executionMode: schedule.execution_mode,
    frequencyType: schedule.frequency_type,
    cronExpression: schedule.cron_expression,
    isRecurring: schedule.is_recurring,
    scheduleType: schedule.schedule_type,
    scheduleTime: schedule.schedule_time,
    scheduleDays: schedule.schedule_days,
    timezone: schedule.timezone,
    startDate: schedule.start_date,
    endDate: schedule.end_date,
    skipIfRunning: schedule.skip_if_running,
    onFailureAction: schedule.on_failure_action,
    maxFailures: schedule.max_failures,
    failureCount: schedule.failure_count,
    notifyOnFailure: schedule.notify_on_failure,
    notifyOnSuccess: schedule.notify_on_success,
    status: schedule.is_active ? "active" : schedule.run_once ? "completed" : "paused",
    isActive: schedule.is_active,
    runOnce: schedule.run_once,
    nextRunAt: schedule.next_run_at_utc,
    lastRunAt: schedule.last_run_at_utc,
    createdBy: schedule.created_by,
    createdAt: schedule.created_at_utc,
    updatedAt: schedule.updated_at_utc,
    summary: summarizeSchedule(schedule),
    nextRunFormatted: schedule.next_run_at_utc,
    lastRunFormatted: schedule.last_run_at_utc,
  };
}

/**
 * GET /api/schedules
 * List schedules for a table
 */
export async function GET(request: NextRequest) {
  try {
    ensureSchedulerBootstrapped("api:schedules:get");

    const searchParams = request.nextUrl.searchParams;
    const database = String(searchParams.get("database") || "").trim();
    const schema = String(searchParams.get("schema") || "").trim();
    const table = String(searchParams.get("table") || "").trim();

    if (!database || !schema || !table) {
      return NextResponse.json(
        { success: false, error: "Missing required parameters" },
        { status: 400 }
      );
    }

    const schedules = await listSchedulesByTable({
      databaseName: database,
      schemaName: schema,
      tableName: table,
    });

    return NextResponse.json({
      success: true,
      data: {
        schedules: schedules.map(toScheduleResponse),
      },
    });
  } catch (error: any) {
    console.error("Error fetching schedules:", error);
    return NextResponse.json(
      { success: false, error: error?.message || "Failed to fetch schedules" },
      { status: 500 }
    );
  }
}

/**
 * POST /api/schedules
 * Create a new schedule
 */
export async function POST(request: NextRequest) {
  try {
    ensureSchedulerBootstrapped("api:schedules:post");

    const body = await request.json();
    const database = String(body.database || "").trim();
    const schema = String(body.schema || "").trim();
    const table = String(body.table || "").trim();
    const scanType = String(body.scanType || "").trim();

    if (!database || !schema || !table || !scanType) {
      return NextResponse.json(
        { success: false, error: "Missing required fields" },
        { status: 400 }
      );
    }

    const timezone = normalizeTimezone(body.timezone);
    const isRecurring = toBoolean(body.isRecurring, true);
    const scheduleType = String(body.scheduleType || "daily").trim().toLowerCase();
    const scheduleTime = body.scheduleTime ? String(body.scheduleTime) : null;
    const scheduleDays = toStringArray(body.scheduleDays);

    const runType = normalizeRunType(body.scanType, body.runType);
    const frequencyType = normalizeFrequencyType(body.scheduleType, body.frequencyType);
    const cronExpression = buildCronExpressionFromInput({
      scheduleType,
      scheduleTime,
      scheduleDays,
      cronExpression: body.cronExpression,
    });

    const nextRunAtUtc = resolveInitialNextRunAtUtc({
      cronExpression,
      timezone,
      initialNextRunAt: body.initialNextRunAt,
      isRecurring,
      runDate: body.runDate,
      scheduleTime,
    }).toISOString();

    const datasetId = await resolveDatasetId({
      datasetId: body.datasetId || body.dataset_id || null,
      databaseName: database,
      schemaName: schema,
      tableName: table,
    });

    const created = await createSchedule({
      datasetId,
      databaseName: database,
      schemaName: schema,
      tableName: table,
      runType,
      frequencyType,
      cronExpression,
      timezone,
      nextRunAtUtc,
      isActive: true,
      createdBy: body.createdBy || null,
      scanType,
      isRecurring,
      scheduleType,
      scheduleTime,
      scheduleDays,
      startDate: body.startDate || null,
      endDate: body.endDate || null,
      skipIfRunning: toBoolean(body.skipIfRunning, false),
      onFailureAction: body.onFailureAction || "continue",
      maxFailures: Number(body.maxFailures || 3),
      notifyOnFailure: toBoolean(body.notifyOnFailure, false),
      notifyOnSuccess: toBoolean(body.notifyOnSuccess, false),
      runOnce: shouldRunOnce(isRecurring, scheduleType),
      customConfig: {
        customRules: toStringArray(body.customRules),
        scope: typeof body.scope === "string" ? body.scope : "table",
        selectedColumns: toStringArray(body.selectedColumns),
      },
      retryEnabled: toBoolean(body.retryEnabled, true),
      retryDelayMinutes: Number(body.retryDelayMinutes || 5),
    });

    return NextResponse.json({
      success: true,
      data: {
        scheduleId: created.schedule_id,
        summary: summarizeSchedule(created),
        message: "Schedule created successfully",
        schedule: toScheduleResponse(created),
      },
    });
  } catch (error: any) {
    console.error("Error creating schedule:", error);
    return NextResponse.json(
      { success: false, error: error?.message || "Failed to create schedule" },
      { status: 500 }
    );
  }
}

/**
 * PUT /api/schedules
 * Update a schedule (pause/resume/delete/force-run)
 */
export async function PUT(request: NextRequest) {
  try {
    ensureSchedulerBootstrapped("api:schedules:put");

    const body = await request.json();
    const scheduleId = String(body.scheduleId || "").trim();

    if (!scheduleId) {
      return NextResponse.json(
        { success: false, error: "Missing scheduleId" },
        { status: 400 }
      );
    }

    await updateScheduleStatus({
      scheduleId,
      status: body.status ? String(body.status) : undefined,
      forceRunNow: toBoolean(body.forceRunNow, false),
    });

    const message = toBoolean(body.forceRunNow, false)
      ? "Schedule marked for immediate execution"
      : `Schedule ${body.status || "updated"}`;

    return NextResponse.json({
      success: true,
      data: { message },
    });
  } catch (error: any) {
    console.error("Error updating schedule:", error);
    return NextResponse.json(
      { success: false, error: error?.message || "Failed to update schedule" },
      { status: 500 }
    );
  }
}

/**
 * DELETE /api/schedules
 * Soft delete a schedule
 */
export async function DELETE(request: NextRequest) {
  try {
    ensureSchedulerBootstrapped("api:schedules:delete");

    const searchParams = request.nextUrl.searchParams;
    const scheduleId = String(searchParams.get("scheduleId") || "").trim();

    if (!scheduleId) {
      return NextResponse.json(
        { success: false, error: "Missing scheduleId" },
        { status: 400 }
      );
    }

    await updateScheduleStatus({ scheduleId, status: "deleted" });

    return NextResponse.json({
      success: true,
      data: { message: "Schedule deleted" },
    });
  } catch (error: any) {
    console.error("Error deleting schedule:", error);
    return NextResponse.json(
      { success: false, error: error?.message || "Failed to delete schedule" },
      { status: 500 }
    );
  }
}

