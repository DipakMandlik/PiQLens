import * as os from "os";
import { createHash, randomUUID } from "crypto";
import { acquireLease, releaseLease, renewLease, redisAvailableForScheduler } from "@/lib/scheduler/redis-lock";
import {
  claimExecution,
  getScheduleById,
  hasRunningExecution,
  listDueSchedules,
  markExecutionFinal,
  markExecutionRunning,
  updateScheduleHeartbeat,
  updateSchedulePostExecution,
} from "@/lib/scheduler/repository";
import { computeNextRunAtUtc } from "@/lib/scheduler/cron";
import { executeScheduledRun } from "@/lib/scheduler/runner";
import { CanonicalSchedule, SchedulerConfig, SchedulerTickResult } from "@/lib/scheduler/types";

declare global {
  var __dqSchedulerWarned: boolean | undefined;
}

const LEADER_KEY = "dq:scheduler:leader";

function parsePositiveInt(value: string | undefined, fallback: number): number {
  const parsed = Number(value);
  return Number.isFinite(parsed) && parsed > 0 ? Math.floor(parsed) : fallback;
}

function getSchedulerConfig(): SchedulerConfig {
  const baseUrl =
    process.env.SCHEDULER_BASE_URL ||
    process.env.NEXT_PUBLIC_APP_URL ||
    `http://127.0.0.1:${process.env.PORT || "3000"}`;

  return {
    tickSeconds: parsePositiveInt(process.env.SCHEDULER_TICK_SECONDS, 30),
    maxConcurrency: parsePositiveInt(process.env.SCHEDULER_MAX_CONCURRENCY, 5),
    executionTimeoutSec: parsePositiveInt(process.env.SCHEDULER_EXECUTION_TIMEOUT_SEC, 1800),
    lockLeaseMs: parsePositiveInt(process.env.SCHEDULER_LOCK_LEASE_MS, 1950000),
    leaderLeaseMs: parsePositiveInt(process.env.SCHEDULER_LEADER_LEASE_MS, 45000),
    leaderRenewMs: parsePositiveInt(process.env.SCHEDULER_LEADER_RENEW_MS, 15000),
    batchSize: parsePositiveInt(process.env.SCHEDULER_BATCH_SIZE, 25),
    shadowMode: String(process.env.SCHEDULER_SHADOW_MODE || "false").toLowerCase() === "true",
    retryEnabled: String(process.env.SCHEDULER_RETRY_ENABLED || "true").toLowerCase() === "true",
    retryMaxAttempts: parsePositiveInt(process.env.SCHEDULER_RETRY_MAX_ATTEMPTS, 1),
    retryDelayMinutes: parsePositiveInt(process.env.SCHEDULER_RETRY_DELAY_MINUTES, 5),
    baseUrl,
  };
}

function idempotencyKey(scheduleId: string, dueAtIso: string): string {
  return createHash("sha256").update(`${scheduleId}|${dueAtIso}`).digest("hex");
}

function chunk<T>(items: T[], size: number): T[][] {
  if (size <= 1) return items.map((i) => [i]);
  const chunks: T[][] = [];
  for (let i = 0; i < items.length; i += size) {
    chunks.push(items.slice(i, i + size));
  }
  return chunks;
}

function addMinutes(base: Date, minutes: number): Date {
  return new Date(base.getTime() + minutes * 60 * 1000);
}

function safeNextRun(schedule: CanonicalSchedule, baseline: Date): Date {
  try {
    return computeNextRunAtUtc({
      cronExpression: schedule.cron_expression,
      timezone: schedule.timezone,
      currentDate: baseline,
    });
  } catch {
    return addMinutes(baseline, 60);
  }
}

class SchedulerOrchestrator {
  private started = false;
  private tickTimer: NodeJS.Timeout | null = null;
  private leaderTimer: NodeJS.Timeout | null = null;
  private hasLeadership = false;
  private tickInFlight = false;
  private readonly config: SchedulerConfig;
  private readonly ownerId: string;

  constructor() {
    this.config = getSchedulerConfig();
    this.ownerId = `${os.hostname()}-${process.pid}-${randomUUID().slice(0, 8)}`;
  }

  start(): void {
    if (this.started) return;
    if (String(process.env.ENABLE_APP_SCHEDULER || "true").toLowerCase() === "false") {
      console.log("[SCHEDULER] disabled via ENABLE_APP_SCHEDULER=false");
      return;
    }

    this.started = true;
    this.tryLeadership().catch((err) => {
      console.error("[SCHEDULER] initial leadership check failed:", err);
    });

    this.leaderTimer = setInterval(() => {
      this.tryLeadership().catch((err) => {
        console.error("[SCHEDULER] leadership error:", err);
      });
    }, this.config.leaderRenewMs);

    this.tickTimer = setInterval(() => {
      this.runTick().catch((err) => {
        console.error("[SCHEDULER] tick error:", err);
      });
    }, this.config.tickSeconds * 1000);

    // Immediate first tick.
    this.runTick().catch((err) => {
      console.error("[SCHEDULER] immediate tick error:", err);
    });

    console.log("[SCHEDULER] started", {
      ownerId: this.ownerId,
      tickSeconds: this.config.tickSeconds,
      shadowMode: this.config.shadowMode,
      redis: redisAvailableForScheduler(),
    });
  }

  async stop(): Promise<void> {
    if (this.tickTimer) {
      clearInterval(this.tickTimer);
      this.tickTimer = null;
    }
    if (this.leaderTimer) {
      clearInterval(this.leaderTimer);
      this.leaderTimer = null;
    }

    if (this.hasLeadership) {
      await releaseLease(LEADER_KEY, this.ownerId);
      this.hasLeadership = false;
    }

    this.started = false;
  }

  async runTick(): Promise<SchedulerTickResult> {
    if (!this.started || this.tickInFlight) {
      return { fetched: 0, executed: 0, skipped: 0, failed: 0 };
    }

    this.tickInFlight = true;
    try {
      if (!this.hasLeadership) {
        return { fetched: 0, executed: 0, skipped: 0, failed: 0 };
      }

      // Check if Snowflake credentials are configured
      // This is required for the scheduler to function
      const { getServerConfig } = await import("@/lib/server-config");
      const config = getServerConfig();
      if (!config) {
        // Silently skip this tick if Snowflake is not configured
        // Log a one-time warning with instructions
        if (!globalThis.__dqSchedulerWarned) {
          console.warn(
            "[SCHEDULER] ⚠️  Snowflake credentials not configured. " +
            "Scheduler is running but cannot execute scheduled scans.\n" +
            "To enable auto-execution, create .env.local with:\n" +
            "  SNOWFLAKE_ACCOUNT=your_account_identifier\n" +
            "  SNOWFLAKE_USER=your_username\n" +
            "  SNOWFLAKE_PASSWORD=your_password\n" +
            "See SCHEDULER_SNOWFLAKE_SETUP.md for details."
          );
          globalThis.__dqSchedulerWarned = true;
        }
        return { fetched: 0, executed: 0, skipped: 0, failed: 0 };
      }

      const due = await listDueSchedules(this.config.batchSize);
      if (due.length === 0) {
        return { fetched: 0, executed: 0, skipped: 0, failed: 0 };
      }

      const result: SchedulerTickResult = {
        fetched: due.length,
        executed: 0,
        skipped: 0,
        failed: 0,
      };

      const groups = chunk(due, this.config.maxConcurrency);
      for (const group of groups) {
        const outcomes = await Promise.all(group.map((schedule) => this.processSchedule(schedule)));
        outcomes.forEach((outcome) => {
          if (outcome === "executed") result.executed += 1;
          else if (outcome === "failed") result.failed += 1;
          else result.skipped += 1;
        });
      }

      return result;
    } finally {
      this.tickInFlight = false;
    }
  }

  async executeScheduleNow(scheduleId: string): Promise<{ success: boolean; message: string }> {
    const schedule = await getScheduleById(scheduleId);
    if (!schedule) {
      return { success: false, message: "Schedule not found" };
    }

    const outcome = await this.processSchedule(schedule, new Date(), true);
    if (outcome === "executed") return { success: true, message: "Schedule executed" };
    if (outcome === "failed") return { success: false, message: "Schedule execution failed" };
    return { success: false, message: "Schedule skipped" };
  }

  private async tryLeadership(): Promise<void> {
    if (!redisAvailableForScheduler()) {
      // Single-node fallback when Redis is missing.
      this.hasLeadership = true;
      return;
    }

    if (this.hasLeadership) {
      const renewed = await renewLease(LEADER_KEY, this.ownerId, this.config.leaderLeaseMs);
      if (!renewed) {
        this.hasLeadership = false;
      }
      return;
    }

    const acquired = await acquireLease(LEADER_KEY, this.ownerId, this.config.leaderLeaseMs);
    this.hasLeadership = acquired;
  }

  private async processSchedule(schedule: CanonicalSchedule, forcedDueAt?: Date, ignoreLeadership = false): Promise<"executed" | "skipped" | "failed"> {
    if (!ignoreLeadership && !this.hasLeadership) return "skipped";

    const dueAt = forcedDueAt ?? new Date(schedule.next_run_at_utc);
    if (Number.isNaN(dueAt.getTime())) return "skipped";

    if (schedule.skip_if_running) {
      const running = await hasRunningExecution(schedule.schedule_id);
      if (running) {
        await updateScheduleHeartbeat(schedule.schedule_id);
        return "skipped";
      }
    }

    const dueIso = dueAt.toISOString();
    const idem = idempotencyKey(schedule.schedule_id, dueIso);
    const executionLeaseKey = `dq:schedule:${schedule.schedule_id}:${Math.floor(dueAt.getTime() / 1000)}`;

    if (redisAvailableForScheduler()) {
      const lockAcquired = await acquireLease(executionLeaseKey, this.ownerId, this.config.lockLeaseMs);
      if (!lockAcquired) return "skipped";
    }

    try {
      const lockExpiry = new Date(Date.now() + this.config.executionTimeoutSec * 1000 + 120000).toISOString();
      const maxAttempts = schedule.retry_enabled
        ? Math.max(1, Math.min(this.config.retryMaxAttempts, schedule.max_failures || this.config.retryMaxAttempts))
        : 0;

      const claim = await claimExecution({
        scheduleId: schedule.schedule_id,
        dueAtUtc: dueIso,
        idempotencyKey: idem,
        lockOwner: this.ownerId,
        lockExpiresAtUtc: lockExpiry,
        allowRetry: this.config.retryEnabled && schedule.retry_enabled,
        maxRetryAttempts: maxAttempts,
      });

      if (!claim.claimed) {
        return "skipped";
      }

      await markExecutionRunning({
        executionId: claim.executionId,
        lockOwner: this.ownerId,
        lockExpiresAtUtc: lockExpiry,
      });

      if (this.config.shadowMode) {
        await markExecutionFinal({
          executionId: claim.executionId,
          status: "SKIPPED",
          errorMessage: "Shadow mode enabled",
        });
        return "skipped";
      }

      const runResult = await executeScheduledRun(schedule, this.config.baseUrl);

      if (runResult.success) {
        const nextRun = schedule.run_once
          ? null
          : safeNextRun(schedule, dueAt).toISOString();

        await updateSchedulePostExecution({
          scheduleId: schedule.schedule_id,
          success: true,
          nextRunAtUtc: nextRun,
          runOnce: schedule.run_once,
        });

        await markExecutionFinal({
          executionId: claim.executionId,
          status: "SUCCEEDED",
          runId: runResult.runId,
        });

        return "executed";
      }

      const retryAllowed = this.config.retryEnabled && schedule.retry_enabled && claim.attemptNo <= maxAttempts;
      const nextRun = retryAllowed
        ? addMinutes(new Date(), schedule.retry_delay_minutes || this.config.retryDelayMinutes).toISOString()
        : safeNextRun(schedule, dueAt).toISOString();

      await updateSchedulePostExecution({
        scheduleId: schedule.schedule_id,
        success: false,
        nextRunAtUtc: nextRun,
        incrementFailure: true,
      });

      await markExecutionFinal({
        executionId: claim.executionId,
        status: "FAILED",
        runId: runResult.runId,
        errorMessage: runResult.error || "Scheduled run failed",
      });

      return "failed";
    } catch (error) {
      console.error("[SCHEDULER] schedule processing error", {
        scheduleId: schedule.schedule_id,
        error: error instanceof Error ? error.message : String(error),
      });
      return "failed";
    } finally {
      if (redisAvailableForScheduler()) {
        await releaseLease(executionLeaseKey, this.ownerId);
      }
    }
  }
}

let orchestratorSingleton: SchedulerOrchestrator | null = null;

export function getSchedulerOrchestrator(): SchedulerOrchestrator {
  if (!orchestratorSingleton) {
    orchestratorSingleton = new SchedulerOrchestrator();
  }
  return orchestratorSingleton;
}

