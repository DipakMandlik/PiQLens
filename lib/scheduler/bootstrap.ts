import { getSchedulerOrchestrator } from "@/lib/scheduler/orchestrator";

declare global {
  var __dqSchedulerBootstrapped: boolean | undefined;
}

function isNodeRuntime(): boolean {
  if (typeof process === "undefined") return false;
  const runtime = process.env.NEXT_RUNTIME;
  return !runtime || runtime === "nodejs";
}

function isAppSchedulerEnabled(): boolean {
  return String(process.env.ENABLE_APP_SCHEDULER || "false").toLowerCase() === "true";
}

export function ensureSchedulerBootstrapped(reason = "unknown"): void {
  if (!isNodeRuntime()) return;
  if (!isAppSchedulerEnabled()) return;

  if (globalThis.__dqSchedulerBootstrapped) {
    return;
  }

  globalThis.__dqSchedulerBootstrapped = true;

  try {
    const orchestrator = getSchedulerOrchestrator();
    orchestrator.start();
    console.log(`[SCHEDULER] bootstrap initialized (${reason})`);
  } catch (error) {
    console.error("[SCHEDULER] bootstrap failed:", error);
    globalThis.__dqSchedulerBootstrapped = false;
  }
}

