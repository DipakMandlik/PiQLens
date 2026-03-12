export async function register(): Promise<void> {
  if (process.env.NEXT_RUNTIME === "edge") {
    return;
  }

  // Only import scheduler bootstrap if enabled and in Node runtime
  // This prevents Edge Runtime errors from Node.js APIs (os, crypto, process.pid)
  if (String(process.env.ENABLE_APP_SCHEDULER || "false").toLowerCase() === "true") {
    const { ensureSchedulerBootstrapped } = await import("@/lib/scheduler/bootstrap");
    ensureSchedulerBootstrapped("instrumentation");
  }
}
