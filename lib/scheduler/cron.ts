import { CronExpressionParser } from "cron-parser";
import { ScheduleFrequencyType, ScheduleRunType } from "@/lib/scheduler/types";

const WEEKDAY_TO_CRON: Record<string, string> = {
  SUN: "0",
  MON: "1",
  TUE: "2",
  WED: "3",
  THU: "4",
  FRI: "5",
  SAT: "6",
};

export function normalizeTimezone(input?: string | null): string {
  const fallback = "UTC";
  if (!input) return fallback;
  const value = String(input).trim();
  if (!value) return fallback;

  try {
    // Throws on invalid IANA timezone.
    Intl.DateTimeFormat("en-US", { timeZone: value }).format(new Date());
    return value;
  } catch {
    return fallback;
  }
}

export function normalizeRunType(scanType?: string | null, explicitRunType?: string | null): ScheduleRunType {
  const forced = String(explicitRunType || "").toUpperCase();
  if (["INCREMENTAL", "INCREMENTAL_SCAN"].includes(forced)) return "INCREMENTAL_SCAN";
  if (["FULL", "FULL_SCAN"].includes(forced)) return "FULL_SCAN";

  const normalizedScanType = String(scanType || "").toUpperCase();
  if (["INCREMENTAL", "INCREMENTAL_SCAN"].includes(normalizedScanType)) return "INCREMENTAL_SCAN";
  return "FULL_SCAN";
}

export function normalizeFrequencyType(scheduleType?: string | null, explicitFrequency?: string | null): ScheduleFrequencyType {
  const forced = String(explicitFrequency || "").toUpperCase();
  if (["DAILY", "WEEKLY", "CUSTOM_CRON"].includes(forced)) {
    return forced as ScheduleFrequencyType;
  }

  const normalized = String(scheduleType || "").toLowerCase();
  if (normalized === "weekly") return "WEEKLY";
  if (normalized === "daily") return "DAILY";
  if (normalized === "hourly" || normalized === "once") return "CUSTOM_CRON";
  return "CUSTOM_CRON";
}

function parseTime(scheduleTime?: string | null): { hour: number; minute: number } {
  if (!scheduleTime) return { hour: 0, minute: 0 };
  const [hourRaw, minuteRaw] = scheduleTime.split(":");
  const hour = Math.max(0, Math.min(23, Number(hourRaw || 0)));
  const minute = Math.max(0, Math.min(59, Number(minuteRaw || 0)));
  return { hour, minute };
}

export function buildCronExpressionFromInput(params: {
  scheduleType?: string | null;
  scheduleTime?: string | null;
  scheduleDays?: string[] | null;
  cronExpression?: string | null;
}): string {
  const explicitCron = String(params.cronExpression || "").trim();
  if (explicitCron) return explicitCron;

  const scheduleType = String(params.scheduleType || "").toLowerCase();
  const { hour, minute } = parseTime(params.scheduleTime);

  if (scheduleType === "hourly") {
    return `0 * * * *`;
  }

  if (scheduleType === "weekly") {
    const days = (params.scheduleDays || [])
      .map((d) => WEEKDAY_TO_CRON[String(d || "").slice(0, 3).toUpperCase()])
      .filter(Boolean);
    const dayExpr = days.length > 0 ? Array.from(new Set(days)).join(",") : "1";
    return `${minute} ${hour} * * ${dayExpr}`;
  }

  // Default daily
  return `${minute} ${hour} * * *`;
}

export function computeNextRunAtUtc(params: {
  cronExpression: string;
  timezone: string;
  currentDate?: Date;
}): Date {
  const iterator = CronExpressionParser.parse(params.cronExpression, {
    currentDate: params.currentDate ?? new Date(),
    tz: params.timezone,
  });

  return iterator.next().toDate();
}

export function resolveInitialNextRunAtUtc(params: {
  cronExpression: string;
  timezone: string;
  initialNextRunAt?: string | null;
  isRecurring: boolean;
  runDate?: string | null;
  scheduleTime?: string | null;
}): Date {
  const allowClientTimestamp = String(process.env.SCHEDULER_ALLOW_CLIENT_NEXT_RUN_AT || "false").toLowerCase() === "true";
  if (allowClientTimestamp && params.initialNextRunAt) {
    const parsed = new Date(params.initialNextRunAt);
    if (!Number.isNaN(parsed.getTime())) return parsed;
  }

  // For one-time schedules, prefer runDate+scheduleTime when provided.
  if (!params.isRecurring && params.runDate) {
    const { hour, minute } = parseTime(params.scheduleTime);
    const day = String(params.runDate);
    const isoGuess = `${day}T${String(hour).padStart(2, "0")}:${String(minute).padStart(2, "0")}:00`;

    const parsed = new Date(isoGuess);
    if (!Number.isNaN(parsed.getTime()) && parsed.getTime() > Date.now()) {
      return parsed;
    }
  }

  return computeNextRunAtUtc({
    cronExpression: params.cronExpression,
    timezone: params.timezone,
    currentDate: new Date(),
  });
}

export function shouldRunOnce(isRecurring: boolean, scheduleType?: string | null): boolean {
  if (!isRecurring) return true;
  return String(scheduleType || "").toLowerCase() === "once";
}

