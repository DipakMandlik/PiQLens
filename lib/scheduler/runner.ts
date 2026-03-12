import { CanonicalSchedule } from "@/lib/scheduler/types";

function extractRunId(payload: unknown): string | null {
  if (!payload || typeof payload !== "object") return null;
  const data = payload as Record<string, unknown>;

  const direct = ["run_id", "runId", "new_run_id"];
  for (const key of direct) {
    const value = data[key];
    if (typeof value === "string" && value) return value;
  }

  const nested = data.data;
  if (nested && typeof nested === "object") {
    const record = nested as Record<string, unknown>;
    for (const key of direct) {
      const value = record[key];
      if (typeof value === "string" && value) return value;
    }
  }

  return null;
}

async function callJson(url: string, body: Record<string, unknown>): Promise<Record<string, unknown>> {
  const response = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    cache: "no-store",
    body: JSON.stringify(body),
  });

  const json = (await response.json()) as Record<string, unknown>;

  if (!response.ok || json.success === false) {
    const error = typeof json.error === "string" ? json.error : `HTTP ${response.status}`;
    throw new Error(error);
  }

  return json;
}

export async function executeScheduledRun(schedule: CanonicalSchedule, baseUrl: string): Promise<{ success: boolean; runId: string | null; error?: string }> {
  try {
    const scanType = String(schedule.scan_type || "").toLowerCase();

    // Incremental canonical flow.
    if (schedule.run_type === "INCREMENTAL_SCAN" || scanType === "incremental") {
      const result = await callJson(`${baseUrl}/api/dq/run-scan`, {
        scanType: "incremental",
        datasetId: schedule.dataset_id,
        timezone: schedule.timezone,
        triggered_by: "SCHEDULER",
      });

      return {
        success: true,
        runId: extractRunId(result),
      };
    }

    // Legacy-compatible flows used by existing UI scan types.
    if (scanType === "profiling" || scanType === "anomalies") {
      const result = await callJson(`${baseUrl}/api/dq/run-profiling`, {
        database: schedule.database_name,
        schema: schedule.schema_name,
        table: schedule.table_name,
        profile_level: "BASIC",
        triggered_by: "scheduled",
        timezone: schedule.timezone,
      });

      return {
        success: true,
        runId: extractRunId(result),
      };
    }

    if (scanType === "checks" || scanType === "custom") {
      const customConfig = schedule.custom_config ? JSON.parse(schedule.custom_config) : {};
      const result = await callJson(`${baseUrl}/api/dq/run-custom-scan`, {
        dataset_id: schedule.dataset_id,
        database: schedule.database_name,
        schema: schedule.schema_name,
        table: schedule.table_name,
        rule_names: Array.isArray((customConfig as Record<string, unknown>).customRules)
          ? (customConfig as Record<string, unknown>).customRules
          : undefined,
        columns: Array.isArray((customConfig as Record<string, unknown>).selectedColumns)
          ? (customConfig as Record<string, unknown>).selectedColumns
          : undefined,
        scope: typeof (customConfig as Record<string, unknown>).scope === "string"
          ? (customConfig as Record<string, unknown>).scope
          : "table",
        triggered_by: "scheduled",
        timezone: schedule.timezone,
      });

      return {
        success: true,
        runId: extractRunId(result),
      };
    }

    // "full" default path keeps old behavior: profiling then checks.
    const profile = await callJson(`${baseUrl}/api/dq/run-profiling`, {
      database: schedule.database_name,
      schema: schedule.schema_name,
      table: schedule.table_name,
      profile_level: "BASIC",
      triggered_by: "scheduled",
      timezone: schedule.timezone,
    });

    const checks = await callJson(`${baseUrl}/api/dq/run-custom-scan`, {
      dataset_id: schedule.dataset_id,
      database: schedule.database_name,
      schema: schedule.schema_name,
      table: schedule.table_name,
      triggered_by: "scheduled",
      timezone: schedule.timezone,
    });

    return {
      success: true,
      runId: extractRunId(checks) || extractRunId(profile),
    };
  } catch (error) {
    return {
      success: false,
      runId: null,
      error: error instanceof Error ? error.message : "Unknown scheduler execution error",
    };
  }
}

