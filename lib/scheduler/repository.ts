/* eslint-disable @typescript-eslint/no-explicit-any */
import { randomUUID } from "crypto";
import { getServerConfig } from "@/lib/server-config";
import {
  ensureConnectionContext,
  executeQuery,
  executeQueryObjects,
  snowflakePool,
} from "@/lib/snowflake";
import { CanonicalSchedule, ScheduleExecutionRecord, ScheduleExecutionStatus, ScheduleUpsertInput } from "@/lib/scheduler/types";

const DQ_DB = "DATA_QUALITY_DB";
const DQ_CONFIG_SCHEMA = "DQ_CONFIG";

const SCHEDULE_TABLE = `${DQ_DB}.${DQ_CONFIG_SCHEMA}.SCAN_SCHEDULES`;
const EXECUTION_TABLE = `${DQ_DB}.${DQ_CONFIG_SCHEMA}.DQ_SCHEDULE_EXECUTION`;
const DATASET_TABLE = `${DQ_DB}.${DQ_CONFIG_SCHEMA}.DATASET_CONFIG`;

let schemaValidated = false;
let scheduleTableColumnsCache: Set<string> | null = null;
let executionTableColumnsCache: Set<string> | null = null;
let executionColumnMapCache: ExecutionColumnMap | null = null;
let executionColumnMapLogged = false;

type ExecutionSchemaMode = "modern" | "legacy" | "hybrid";

type ExecutionColumnMap = {
  schemaMode: ExecutionSchemaMode;
  dueAt: string;
  lockExpiresAt: string;
  startedAt: string;
  finishedAt: string;
  createdAt: string;
  updatedAt: string;
};

function toBool(value: unknown): boolean {
  if (typeof value === "boolean") return value;
  if (typeof value === "number") return value !== 0;
  if (typeof value === "string") return ["1", "TRUE", "Y", "YES"].includes(value.toUpperCase());
  return false;
}

function toNum(value: unknown, fallback = 0): number {
  const num = Number(value);
  return Number.isFinite(num) ? num : fallback;
}

function toStringValue(value: unknown, fallback = ""): string {
  if (value === null || value === undefined) return fallback;
  return String(value);
}

function toJsonText(value: unknown): string | null {
  if (value === null || value === undefined) return null;
  if (typeof value === "string") return value;
  try {
    return JSON.stringify(value);
  } catch {
    return String(value);
  }
}

function escapeSqlLiteral(value: string): string {
  return value.replace(/'/g, "''");
}

function parseStringArray(value: unknown): string[] {
  if (Array.isArray(value)) return value.map((item) => String(item));

  if (typeof value === "string") {
    try {
      const parsed = JSON.parse(value);
      if (Array.isArray(parsed)) return parsed.map((item) => String(item));
    } catch {
      return [];
    }
  }

  if (typeof value === "object" && value !== null) {
    try {
      const asText = JSON.stringify(value);
      const parsed = JSON.parse(asText);
      if (Array.isArray(parsed)) return parsed.map((item) => String(item));
    } catch {
      return [];
    }
  }

  return [];
}

function normalizeRunType(value: unknown, scanType: unknown): CanonicalSchedule["run_type"] {
  const runType = String(value || "").toUpperCase();
  if (["INCREMENTAL", "INCREMENTAL_SCAN"].includes(runType)) return "INCREMENTAL_SCAN";
  if (["FULL", "FULL_SCAN"].includes(runType)) return "FULL_SCAN";

  const scan = String(scanType || "").toUpperCase();
  if (["INCREMENTAL", "INCREMENTAL_SCAN"].includes(scan)) return "INCREMENTAL_SCAN";
  return "FULL_SCAN";
}

function normalizeFrequencyType(value: unknown, scheduleType: unknown): CanonicalSchedule["frequency_type"] {
  const frequency = String(value || "").toUpperCase();
  if (["DAILY", "WEEKLY", "CUSTOM_CRON"].includes(frequency)) {
    return frequency as CanonicalSchedule["frequency_type"];
  }

  const schedule = String(scheduleType || "").toLowerCase();
  if (schedule === "daily") return "DAILY";
  if (schedule === "weekly") return "WEEKLY";
  return "CUSTOM_CRON";
}

function rowToSchedule(row: Record<string, unknown>): CanonicalSchedule {
  const status = String(row.STATUS || "active").toLowerCase();
  const isActive = row.IS_ACTIVE === undefined ? status === "active" : toBool(row.IS_ACTIVE);
  const scanType = toStringValue(row.SCAN_TYPE, "full");

  const customConfigText = toJsonText(row.CUSTOM_CONFIG);
  let customConfigRecord: Record<string, unknown> = {};
  if (customConfigText) {
    try {
      const parsed = JSON.parse(customConfigText);
      if (parsed && typeof parsed === "object" && !Array.isArray(parsed)) {
        customConfigRecord = parsed as Record<string, unknown>;
      }
    } catch {
      // keep empty fallback
    }
  }

  const datasetId = toStringValue(
    row.DATASET_ID,
    toStringValue(customConfigRecord.dataset_id, "")
  );

  return {
    schedule_id: toStringValue(row.SCHEDULE_ID),
    dataset_id: datasetId,
    database_name: toStringValue(row.DATABASE_NAME),
    schema_name: toStringValue(row.SCHEMA_NAME),
    table_name: toStringValue(row.TABLE_NAME),
    run_type: normalizeRunType(row.RUN_TYPE, scanType),
    execution_mode: "SCHEDULED",
    frequency_type: normalizeFrequencyType(row.FREQUENCY_TYPE, row.SCHEDULE_TYPE),
    cron_expression: toStringValue(row.CRON_EXPRESSION),
    next_run_at_utc: toStringValue(row.NEXT_RUN_AT),
    last_run_at_utc: row.LAST_RUN_AT ? toStringValue(row.LAST_RUN_AT) : null,
    is_active: isActive,
    timezone: toStringValue(row.TIMEZONE, "UTC"),
    created_by: row.CREATED_BY ? toStringValue(row.CREATED_BY) : null,
    created_at_utc: toStringValue(row.CREATED_AT),
    updated_at_utc: toStringValue(row.UPDATED_AT),

    scan_type: scanType,
    is_recurring: row.IS_RECURRING === undefined ? true : toBool(row.IS_RECURRING),
    schedule_type: toStringValue(row.SCHEDULE_TYPE, "daily"),
    schedule_time: row.SCHEDULE_TIME ? toStringValue(row.SCHEDULE_TIME) : null,
    schedule_days: parseStringArray(row.SCHEDULE_DAYS),
    start_date: row.START_DATE ? toStringValue(row.START_DATE) : null,
    end_date: row.END_DATE ? toStringValue(row.END_DATE) : null,
    skip_if_running: toBool(row.SKIP_IF_RUNNING),
    on_failure_action: toStringValue(row.ON_FAILURE_ACTION, "continue"),
    max_failures: toNum(row.MAX_FAILURES, 3),
    failure_count: toNum(row.FAILURE_COUNT, 0),
    notify_on_failure: toBool(row.NOTIFY_ON_FAILURE),
    notify_on_success: toBool(row.NOTIFY_ON_SUCCESS),
    run_once: row.RUN_ONCE === undefined ? false : toBool(row.RUN_ONCE),
    custom_config: customConfigText,
    retry_enabled: row.RETRY_ENABLED === undefined ? true : toBool(row.RETRY_ENABLED),
    retry_delay_minutes: toNum(row.RETRY_DELAY_MINUTES, 5),
  };
}

function rowToExecution(row: Record<string, unknown>): ScheduleExecutionRecord {
  return {
    execution_id: toStringValue(row.EXECUTION_ID),
    schedule_id: toStringValue(row.SCHEDULE_ID),
    due_at_utc: toStringValue(row.DUE_AT ?? row.DUE_AT_UTC),
    idempotency_key: toStringValue(row.IDEMPOTENCY_KEY),
    status: toStringValue(row.STATUS) as ScheduleExecutionStatus,
    lock_owner: row.LOCK_OWNER ? toStringValue(row.LOCK_OWNER) : null,
    lock_expires_at_utc: row.LOCK_EXPIRES_AT
      ? toStringValue(row.LOCK_EXPIRES_AT)
      : row.LOCK_EXPIRES_AT_UTC
        ? toStringValue(row.LOCK_EXPIRES_AT_UTC)
        : null,
    run_id: row.RUN_ID ? toStringValue(row.RUN_ID) : null,
    attempt_no: toNum(row.ATTEMPT_NO, 1),
    error_message: row.ERROR_MESSAGE ? toStringValue(row.ERROR_MESSAGE) : null,
    started_at_utc: row.STARTED_AT
      ? toStringValue(row.STARTED_AT)
      : row.STARTED_AT_UTC
        ? toStringValue(row.STARTED_AT_UTC)
        : null,
    finished_at_utc: row.FINISHED_AT
      ? toStringValue(row.FINISHED_AT)
      : row.FINISHED_AT_UTC
        ? toStringValue(row.FINISHED_AT_UTC)
        : null,
    created_at_utc: toStringValue(row.CREATED_AT ?? row.CREATED_AT_UTC),
    updated_at_utc: toStringValue(row.UPDATED_AT ?? row.UPDATED_AT_UTC),
  };
}

async function getSchedulerConnection(): Promise<any> {
  const config = getServerConfig();
  if (!config) {
    throw new Error("No Snowflake connection");
  }

  const connection = await snowflakePool.getConnection(config);
  await ensureConnectionContext(connection, config);
  await ensureSchedulerSchema(connection);
  return connection;
}

export async function ensureSchedulerSchema(connection: any): Promise<void> {
  if (schemaValidated) return;

  const rows = await executeQueryObjects(
    connection,
    `
      SELECT TABLE_NAME
      FROM ${DQ_DB}.INFORMATION_SCHEMA.TABLES
      WHERE TABLE_SCHEMA = ?
        AND TABLE_NAME IN ('SCAN_SCHEDULES', 'DQ_SCHEDULE_EXECUTION')
    `,
    [DQ_CONFIG_SCHEMA]
  );

  const found = new Set(rows.map((row) => String(row.TABLE_NAME || "").toUpperCase()));
  if (!found.has("SCAN_SCHEDULES")) {
    throw new Error("Missing DQ_CONFIG.SCAN_SCHEDULES. Run sql/production/17_Native_Task_Scheduler_Cutover.sql");
  }

  if (!found.has("DQ_SCHEDULE_EXECUTION")) {
    throw new Error("Missing DQ_CONFIG.DQ_SCHEDULE_EXECUTION. Run sql/production/17_Native_Task_Scheduler_Cutover.sql");
  }

  await getExecutionColumnMap(connection);
  schemaValidated = true;
}

async function getScheduleTableColumns(connection: any): Promise<Set<string>> {
  if (scheduleTableColumnsCache) return scheduleTableColumnsCache;

  const rows = await executeQueryObjects(
    connection,
    `
      SELECT COLUMN_NAME
      FROM ${DQ_DB}.INFORMATION_SCHEMA.COLUMNS
      WHERE TABLE_SCHEMA = ?
        AND TABLE_NAME = 'SCAN_SCHEDULES'
    `,
    [DQ_CONFIG_SCHEMA]
  );

  scheduleTableColumnsCache = new Set(
    rows.map((row) => String(row.COLUMN_NAME || "").toUpperCase())
  );

  return scheduleTableColumnsCache;
}

async function getExecutionTableColumns(connection: any): Promise<Set<string>> {
  if (executionTableColumnsCache) return executionTableColumnsCache;

  const rows = await executeQueryObjects(
    connection,
    `
      SELECT COLUMN_NAME
      FROM ${DQ_DB}.INFORMATION_SCHEMA.COLUMNS
      WHERE TABLE_SCHEMA = ?
        AND TABLE_NAME = 'DQ_SCHEDULE_EXECUTION'
    `,
    [DQ_CONFIG_SCHEMA]
  );

  executionTableColumnsCache = new Set(
    rows.map((row) => String(row.COLUMN_NAME || "").toUpperCase())
  );

  return executionTableColumnsCache;
}

function resolveExecutionColumn(columns: Set<string>, modern: string, legacy: string): string {
  if (columns.has(modern)) return modern;
  if (columns.has(legacy)) return legacy;
  throw new Error(
    `DQ_SCHEDULE_EXECUTION missing required column ${modern} (or legacy ${legacy}). `
    + `Run sql/production/17_Native_Task_Scheduler_Cutover.sql to backfill execution journal columns.`
  );
}

async function getExecutionColumnMap(connection: any): Promise<ExecutionColumnMap> {
  if (executionColumnMapCache) return executionColumnMapCache;

  const columns = await getExecutionTableColumns(connection);
  const modern = {
    dueAt: columns.has("DUE_AT"),
    lockExpiresAt: columns.has("LOCK_EXPIRES_AT"),
    startedAt: columns.has("STARTED_AT"),
    finishedAt: columns.has("FINISHED_AT"),
    createdAt: columns.has("CREATED_AT"),
    updatedAt: columns.has("UPDATED_AT"),
  };
  const legacy = {
    dueAt: columns.has("DUE_AT_UTC"),
    lockExpiresAt: columns.has("LOCK_EXPIRES_AT_UTC"),
    startedAt: columns.has("STARTED_AT_UTC"),
    finishedAt: columns.has("FINISHED_AT_UTC"),
    createdAt: columns.has("CREATED_AT_UTC"),
    updatedAt: columns.has("UPDATED_AT_UTC"),
  };

  const modernCount = Object.values(modern).filter(Boolean).length;
  const legacyCount = Object.values(legacy).filter(Boolean).length;
  const schemaMode: ExecutionSchemaMode =
    modernCount === 6 && legacyCount === 0
      ? "modern"
      : legacyCount === 6 && modernCount === 0
        ? "legacy"
        : "hybrid";

  executionColumnMapCache = {
    schemaMode,
    dueAt: resolveExecutionColumn(columns, "DUE_AT", "DUE_AT_UTC"),
    lockExpiresAt: resolveExecutionColumn(columns, "LOCK_EXPIRES_AT", "LOCK_EXPIRES_AT_UTC"),
    startedAt: resolveExecutionColumn(columns, "STARTED_AT", "STARTED_AT_UTC"),
    finishedAt: resolveExecutionColumn(columns, "FINISHED_AT", "FINISHED_AT_UTC"),
    createdAt: resolveExecutionColumn(columns, "CREATED_AT", "CREATED_AT_UTC"),
    updatedAt: resolveExecutionColumn(columns, "UPDATED_AT", "UPDATED_AT_UTC"),
  };

  if (!executionColumnMapLogged) {
    executionColumnMapLogged = true;
    console.log("[SCHEDULER] DQ_SCHEDULE_EXECUTION schema compatibility", executionColumnMapCache);
  }

  return executionColumnMapCache;
}

export async function resolveDatasetId(params: {
  datasetId?: string | null;
  databaseName: string;
  schemaName: string;
  tableName: string;
}): Promise<string> {
  const connection = await getSchedulerConnection();

  if (params.datasetId && String(params.datasetId).trim()) {
    const requestedDatasetId = String(params.datasetId).trim();
    const byIdRows = await executeQueryObjects(
      connection,
      `
        SELECT DATASET_ID
        FROM ${DATASET_TABLE}
        WHERE DATASET_ID = ?
        LIMIT 1
      `,
      [requestedDatasetId]
    );

    if (byIdRows.length && byIdRows[0].DATASET_ID) {
      return String(byIdRows[0].DATASET_ID);
    }
  }

  const rows = await executeQueryObjects(
    connection,
    `
      SELECT DATASET_ID
      FROM ${DATASET_TABLE}
      WHERE UPPER(SOURCE_DATABASE) = UPPER(?)
        AND UPPER(SOURCE_SCHEMA) = UPPER(?)
        AND UPPER(SOURCE_TABLE) = UPPER(?)
      LIMIT 1
    `,
    [params.databaseName, params.schemaName, params.tableName]
  );

  if (!rows.length || !rows[0].DATASET_ID) {
    throw new Error(`Dataset not found for ${params.databaseName}.${params.schemaName}.${params.tableName}`);
  }

  return String(rows[0].DATASET_ID);
}

export async function createSchedule(input: ScheduleUpsertInput): Promise<CanonicalSchedule> {
  const connection = await getSchedulerConnection();

  const scheduleId = input.scheduleId || randomUUID();
  const scheduleDays = JSON.stringify(input.scheduleDays || []);
  const customConfig = JSON.stringify({
    ...(input.customConfig || {}),
    dataset_id: input.datasetId || null,
    run_type: input.runType,
    frequency_type: input.frequencyType,
    execution_mode: "SCHEDULED",
  });

  const columns = await getScheduleTableColumns(connection);

  const insertColumns: string[] = [];
  const insertValues: string[] = [];
  const binds: unknown[] = [];

  const addColumn = (
    column: string,
    valueExpr: string,
    bindValue?: unknown,
    withBind = true
  ) => {
    if (!columns.has(column)) return;
    insertColumns.push(column);
    insertValues.push(valueExpr);
    if (withBind) {
      binds.push(bindValue === undefined ? null : bindValue);
    }
  };

  addColumn("SCHEDULE_ID", "?", scheduleId);
  addColumn("DATASET_ID", "?", input.datasetId || null);
  addColumn("DATABASE_NAME", "?", input.databaseName);
  addColumn("SCHEMA_NAME", "?", input.schemaName);
  addColumn("TABLE_NAME", "?", input.tableName);
  addColumn("SCAN_TYPE", "?", input.scanType);
  addColumn("RUN_TYPE", "?", input.runType);
  addColumn("EXECUTION_MODE", "?", "SCHEDULED");
  addColumn("FREQUENCY_TYPE", "?", input.frequencyType);
  addColumn("CRON_EXPRESSION", "?", input.cronExpression);
  addColumn("SCHEDULE_TYPE", "?", input.scheduleType);
  addColumn("SCHEDULE_TIME", "?", input.scheduleTime || null);
  addColumn("SCHEDULE_DAYS", `PARSE_JSON('${escapeSqlLiteral(scheduleDays)}')`, null, false);
  addColumn("TIMEZONE", "?", input.timezone);
  addColumn("NEXT_RUN_AT", "TO_TIMESTAMP_TZ(?)", input.nextRunAtUtc);
  addColumn("LAST_RUN_AT", "NULL", null, false);
  addColumn("IS_RECURRING", "?", input.isRecurring);
  addColumn("RUN_ONCE", "?", input.runOnce ?? false);
  addColumn("START_DATE", "?", input.startDate || null);
  addColumn("END_DATE", "?", input.endDate || null);
  addColumn("SKIP_IF_RUNNING", "?", input.skipIfRunning ?? false);
  addColumn("ON_FAILURE_ACTION", "?", input.onFailureAction || "continue");
  addColumn("MAX_FAILURES", "?", input.maxFailures ?? 3);
  addColumn("FAILURE_COUNT", "?", 0);
  addColumn("NOTIFY_ON_FAILURE", "?", input.notifyOnFailure ?? false);
  addColumn("NOTIFY_ON_SUCCESS", "?", input.notifyOnSuccess ?? false);
  addColumn("RETRY_ENABLED", "?", input.retryEnabled ?? true);
  addColumn("RETRY_DELAY_MINUTES", "?", input.retryDelayMinutes ?? 5);
  addColumn("CUSTOM_CONFIG", `PARSE_JSON('${escapeSqlLiteral(customConfig)}')`, null, false);
  addColumn("STATUS", "?", input.isActive ? "active" : "paused");
  addColumn("IS_ACTIVE", "?", input.isActive);
  addColumn("CREATED_BY", "?", input.createdBy || null);
  addColumn("CREATED_AT", "CURRENT_TIMESTAMP()", null, false);
  addColumn("UPDATED_AT", "CURRENT_TIMESTAMP()", null, false);

  if (!insertColumns.length) {
    throw new Error("SCAN_SCHEDULES has no writable columns for schedule insert");
  }

  await executeQuery(
    connection,
    `INSERT INTO ${SCHEDULE_TABLE} (${insertColumns.join(", ")}) SELECT ${insertValues.join(", ")}`,
    binds
  );

  const created = await getScheduleById(scheduleId);
  if (!created) {
    throw new Error("Failed to create schedule");
  }

  return created;
}

export async function listSchedulesByTable(params: {
  databaseName: string;
  schemaName: string;
  tableName: string;
}): Promise<CanonicalSchedule[]> {
  const connection = await getSchedulerConnection();

  const rows = await executeQueryObjects(
    connection,
    `
      SELECT *
      FROM ${SCHEDULE_TABLE}
      WHERE UPPER(DATABASE_NAME) = UPPER(?)
        AND UPPER(SCHEMA_NAME) = UPPER(?)
        AND UPPER(TABLE_NAME) = UPPER(?)
        AND COALESCE(STATUS, 'active') <> 'deleted'
      ORDER BY COALESCE(CREATED_AT, UPDATED_AT) DESC
    `,
    [params.databaseName, params.schemaName, params.tableName]
  );

  return rows.map((row) => rowToSchedule(row as Record<string, unknown>));
}

export async function getScheduleById(scheduleId: string): Promise<CanonicalSchedule | null> {
  const connection = await getSchedulerConnection();
  const rows = await executeQueryObjects(
    connection,
    `SELECT * FROM ${SCHEDULE_TABLE} WHERE SCHEDULE_ID = ? LIMIT 1`,
    [scheduleId]
  );

  if (!rows.length) return null;
  return rowToSchedule(rows[0] as Record<string, unknown>);
}

export async function updateScheduleStatus(params: {
  scheduleId: string;
  status?: string;
  forceRunNow?: boolean;
}): Promise<void> {
  const connection = await getSchedulerConnection();
  const columns = await getScheduleTableColumns(connection);

  const updates: string[] = [];
  const binds: unknown[] = [];

  if (columns.has("UPDATED_AT")) {
    updates.push("UPDATED_AT = CURRENT_TIMESTAMP()");
  }

  if (params.status) {
    const normalized = String(params.status).toLowerCase();
    if (normalized === "deleted") {
      if (columns.has("STATUS")) updates.push("STATUS = 'deleted'");
      if (columns.has("IS_ACTIVE")) updates.push("IS_ACTIVE = FALSE");
    } else if (normalized === "active") {
      if (columns.has("STATUS")) updates.push("STATUS = 'active'");
      if (columns.has("IS_ACTIVE")) updates.push("IS_ACTIVE = TRUE");
    } else if (normalized === "paused") {
      if (columns.has("STATUS")) updates.push("STATUS = 'paused'");
      if (columns.has("IS_ACTIVE")) updates.push("IS_ACTIVE = FALSE");
    }
  }

  if (params.forceRunNow) {
    if (columns.has("NEXT_RUN_AT")) updates.push("NEXT_RUN_AT = CURRENT_TIMESTAMP()");
    if (columns.has("IS_ACTIVE")) updates.push("IS_ACTIVE = TRUE");
    if (columns.has("STATUS")) updates.push("STATUS = 'active'");
  }

  if (!updates.length) {
    return;
  }

  binds.push(params.scheduleId);

  await executeQuery(
    connection,
    `
      UPDATE ${SCHEDULE_TABLE}
      SET ${updates.join(", ")}
      WHERE SCHEDULE_ID = ?
    `,
    binds
  );
}

export async function listDueSchedules(limit: number): Promise<CanonicalSchedule[]> {
  const connection = await getSchedulerConnection();
  const rows = await executeQueryObjects(
    connection,
    `
      SELECT *
      FROM ${SCHEDULE_TABLE}
      WHERE COALESCE(IS_ACTIVE, TRUE) = TRUE
        AND COALESCE(STATUS, 'active') = 'active'
        AND NEXT_RUN_AT <= CURRENT_TIMESTAMP()
        AND (START_DATE IS NULL OR START_DATE <= CURRENT_DATE())
        AND (END_DATE IS NULL OR END_DATE >= CURRENT_DATE())
      ORDER BY NEXT_RUN_AT ASC
      LIMIT ?
    `,
    [limit]
  );

  return rows.map((row) => rowToSchedule(row as Record<string, unknown>));
}

export async function getExecutionByIdempotency(idempotencyKey: string): Promise<ScheduleExecutionRecord | null> {
  const connection = await getSchedulerConnection();
  const colMap = await getExecutionColumnMap(connection);
  const rows = await executeQueryObjects(
    connection,
    `
      SELECT
        EXECUTION_ID,
        SCHEDULE_ID,
        ${colMap.dueAt} AS DUE_AT,
        IDEMPOTENCY_KEY,
        STATUS,
        LOCK_OWNER,
        ${colMap.lockExpiresAt} AS LOCK_EXPIRES_AT,
        RUN_ID,
        ATTEMPT_NO,
        ERROR_MESSAGE,
        ${colMap.startedAt} AS STARTED_AT,
        ${colMap.finishedAt} AS FINISHED_AT,
        ${colMap.createdAt} AS CREATED_AT,
        ${colMap.updatedAt} AS UPDATED_AT
      FROM ${EXECUTION_TABLE}
      WHERE IDEMPOTENCY_KEY = ?
      LIMIT 1
    `,
    [idempotencyKey]
  );

  if (!rows.length) return null;
  return rowToExecution(rows[0] as Record<string, unknown>);
}

export async function claimExecution(params: {
  scheduleId: string;
  dueAtUtc: string;
  idempotencyKey: string;
  lockOwner: string;
  lockExpiresAtUtc: string;
  allowRetry: boolean;
  maxRetryAttempts: number;
}): Promise<{ claimed: boolean; executionId: string; attemptNo: number; reason?: string }> {
  const connection = await getSchedulerConnection();
  const colMap = await getExecutionColumnMap(connection);
  const existing = await getExecutionByIdempotency(params.idempotencyKey);

  if (!existing) {
    const executionId = randomUUID();
    await executeQuery(
      connection,
      `
        INSERT INTO ${EXECUTION_TABLE} (
          EXECUTION_ID, SCHEDULE_ID, ${colMap.dueAt}, IDEMPOTENCY_KEY,
          STATUS, LOCK_OWNER, ${colMap.lockExpiresAt}, ATTEMPT_NO,
          ${colMap.createdAt}, ${colMap.updatedAt}
        )
        SELECT ? AS EXECUTION_ID,
               ? AS SCHEDULE_ID,
               TO_TIMESTAMP_TZ(?) AS ${colMap.dueAt},
               ? AS IDEMPOTENCY_KEY,
               'CLAIMED' AS STATUS,
               ? AS LOCK_OWNER,
               TO_TIMESTAMP_TZ(?) AS ${colMap.lockExpiresAt},
               1 AS ATTEMPT_NO,
               CURRENT_TIMESTAMP() AS ${colMap.createdAt},
               CURRENT_TIMESTAMP() AS ${colMap.updatedAt}
      `,
      [
        executionId,
        params.scheduleId,
        params.dueAtUtc,
        params.idempotencyKey,
        params.lockOwner,
        params.lockExpiresAtUtc,
      ]
    );

    return { claimed: true, executionId, attemptNo: 1 };
  }

  if (["RUNNING", "CLAIMED", "SUCCEEDED"].includes(existing.status)) {
    return {
      claimed: false,
      executionId: existing.execution_id,
      attemptNo: existing.attempt_no,
      reason: `already_${existing.status.toLowerCase()}`
    };
  }

  if (!params.allowRetry || existing.attempt_no >= params.maxRetryAttempts + 1) {
    return {
      claimed: false,
      executionId: existing.execution_id,
      attemptNo: existing.attempt_no,
      reason: "retry_exhausted"
    };
  }

  const nextAttempt = existing.attempt_no + 1;

  await executeQuery(
    connection,
    `
      UPDATE ${EXECUTION_TABLE}
      SET STATUS = 'CLAIMED',
          ATTEMPT_NO = ?,
          LOCK_OWNER = ?,
          ${colMap.lockExpiresAt} = TO_TIMESTAMP_TZ(?),
          ERROR_MESSAGE = NULL,
          ${colMap.updatedAt} = CURRENT_TIMESTAMP()
      WHERE EXECUTION_ID = ?
    `,
    [nextAttempt, params.lockOwner, params.lockExpiresAtUtc, existing.execution_id]
  );

  return { claimed: true, executionId: existing.execution_id, attemptNo: nextAttempt };
}

export async function markExecutionRunning(params: {
  executionId: string;
  lockOwner: string;
  lockExpiresAtUtc: string;
}): Promise<void> {
  const connection = await getSchedulerConnection();
  const colMap = await getExecutionColumnMap(connection);
  await executeQuery(
    connection,
    `
      UPDATE ${EXECUTION_TABLE}
      SET STATUS = 'RUNNING',
          LOCK_OWNER = ?,
          ${colMap.lockExpiresAt} = TO_TIMESTAMP_TZ(?),
          ${colMap.startedAt} = CURRENT_TIMESTAMP(),
          ${colMap.updatedAt} = CURRENT_TIMESTAMP()
      WHERE EXECUTION_ID = ?
    `,
    [params.lockOwner, params.lockExpiresAtUtc, params.executionId]
  );
}

export async function markExecutionFinal(params: {
  executionId: string;
  status: ScheduleExecutionStatus;
  runId?: string | null;
  errorMessage?: string | null;
}): Promise<void> {
  const connection = await getSchedulerConnection();
  const colMap = await getExecutionColumnMap(connection);
  await executeQuery(
    connection,
    `
      UPDATE ${EXECUTION_TABLE}
      SET STATUS = ?,
          RUN_ID = ?,
          ERROR_MESSAGE = ?,
          ${colMap.finishedAt} = CURRENT_TIMESTAMP(),
          ${colMap.updatedAt} = CURRENT_TIMESTAMP()
      WHERE EXECUTION_ID = ?
    `,
    [params.status, params.runId || null, params.errorMessage || null, params.executionId]
  );
}

export async function updateSchedulePostExecution(params: {
  scheduleId: string;
  success: boolean;
  nextRunAtUtc?: string | null;
  runOnce?: boolean;
  incrementFailure?: boolean;
}): Promise<void> {
  const connection = await getSchedulerConnection();

  const updates: string[] = [
    "UPDATED_AT = CURRENT_TIMESTAMP()",
    "LAST_RUN_AT = CURRENT_TIMESTAMP()",
  ];
  const binds: unknown[] = [];

  if (params.success) {
    updates.push("FAILURE_COUNT = 0");
  } else if (params.incrementFailure) {
    updates.push("FAILURE_COUNT = COALESCE(FAILURE_COUNT, 0) + 1");
  }

  if (params.runOnce && params.success) {
    updates.push("IS_ACTIVE = FALSE");
    updates.push("STATUS = 'completed'");
    updates.push("NEXT_RUN_AT = NULL");
  } else if (params.nextRunAtUtc) {
    updates.push("NEXT_RUN_AT = TO_TIMESTAMP_TZ(?)");
    binds.push(params.nextRunAtUtc);
    updates.push("STATUS = 'active'");
    updates.push("IS_ACTIVE = TRUE");
  }

  binds.push(params.scheduleId);

  await executeQuery(
    connection,
    `UPDATE ${SCHEDULE_TABLE} SET ${updates.join(", ")} WHERE SCHEDULE_ID = ?`,
    binds
  );
}

export async function updateScheduleHeartbeat(scheduleId: string): Promise<void> {
  const connection = await getSchedulerConnection();
  await executeQuery(
    connection,
    `
      UPDATE ${SCHEDULE_TABLE}
      SET UPDATED_AT = CURRENT_TIMESTAMP()
      WHERE SCHEDULE_ID = ?
    `,
    [scheduleId]
  );
}

export async function hasRunningExecution(scheduleId: string): Promise<boolean> {
  const connection = await getSchedulerConnection();
  const rows = await executeQueryObjects(
    connection,
    `SELECT EXECUTION_ID FROM ${EXECUTION_TABLE} WHERE SCHEDULE_ID = ? AND STATUS = 'RUNNING' LIMIT 1`,
    [scheduleId]
  );
  return rows.length > 0;
}



