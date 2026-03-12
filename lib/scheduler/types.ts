export type ScheduleRunType = "FULL_SCAN" | "INCREMENTAL_SCAN";
export type ScheduleFrequencyType = "DAILY" | "WEEKLY" | "CUSTOM_CRON";
export type ScheduleExecutionMode = "SCHEDULED";

export type ScheduleExecutionStatus =
  | "CLAIMED"
  | "RUNNING"
  | "SUCCEEDED"
  | "FAILED"
  | "TIMED_OUT"
  | "SKIPPED";

export interface CanonicalSchedule {
  schedule_id: string;
  dataset_id: string;
  database_name: string;
  schema_name: string;
  table_name: string;
  run_type: ScheduleRunType;
  execution_mode: ScheduleExecutionMode;
  frequency_type: ScheduleFrequencyType;
  cron_expression: string;
  next_run_at_utc: string;
  last_run_at_utc: string | null;
  is_active: boolean;
  timezone: string;
  created_by: string | null;
  created_at_utc: string;
  updated_at_utc: string;

  // Compatibility fields used by existing UI/API payloads.
  scan_type: string;
  is_recurring: boolean;
  schedule_type: string;
  schedule_time: string | null;
  schedule_days: string[];
  start_date: string | null;
  end_date: string | null;
  skip_if_running: boolean;
  on_failure_action: string;
  max_failures: number;
  failure_count: number;
  notify_on_failure: boolean;
  notify_on_success: boolean;
  run_once: boolean;
  custom_config: string | null;
  retry_enabled: boolean;
  retry_delay_minutes: number;
}

export interface ScheduleExecutionRecord {
  execution_id: string;
  schedule_id: string;
  due_at_utc: string;
  idempotency_key: string;
  status: ScheduleExecutionStatus;
  lock_owner: string | null;
  lock_expires_at_utc: string | null;
  run_id: string | null;
  attempt_no: number;
  error_message: string | null;
  started_at_utc: string | null;
  finished_at_utc: string | null;
  created_at_utc: string;
  updated_at_utc: string;
}

export interface SchedulerConfig {
  tickSeconds: number;
  maxConcurrency: number;
  executionTimeoutSec: number;
  lockLeaseMs: number;
  leaderLeaseMs: number;
  leaderRenewMs: number;
  batchSize: number;
  shadowMode: boolean;
  retryEnabled: boolean;
  retryMaxAttempts: number;
  retryDelayMinutes: number;
  baseUrl: string;
}

export interface SchedulerTickResult {
  fetched: number;
  executed: number;
  skipped: number;
  failed: number;
}

export interface ScheduleUpsertInput {
  scheduleId?: string;
  datasetId: string;
  databaseName: string;
  schemaName: string;
  tableName: string;
  runType: ScheduleRunType;
  frequencyType: ScheduleFrequencyType;
  cronExpression: string;
  timezone: string;
  nextRunAtUtc: string;
  isActive: boolean;
  createdBy?: string | null;

  // Compatibility input
  scanType: string;
  isRecurring: boolean;
  scheduleType: string;
  scheduleTime?: string | null;
  scheduleDays?: string[];
  startDate?: string | null;
  endDate?: string | null;
  skipIfRunning?: boolean;
  onFailureAction?: string;
  maxFailures?: number;
  notifyOnFailure?: boolean;
  notifyOnSuccess?: boolean;
  runOnce?: boolean;
  customConfig?: Record<string, unknown> | null;
  retryEnabled?: boolean;
  retryDelayMinutes?: number;
}
