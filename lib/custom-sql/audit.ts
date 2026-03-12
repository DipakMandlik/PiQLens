import { executeQuery } from '@/lib/snowflake';

interface SnowflakeConnection {
  execute: (options: {
    sqlText: string;
    binds?: unknown[];
    complete: (err: unknown, stmt: unknown, rows: unknown[]) => void;
  }) => unknown;
}

export interface QueryAuditInit {
  auditId: string;
  datasetId: string;
  databaseName: string;
  schemaName: string;
  tableName: string;
  executedBy: string;
  executedRole: string;
  warehouseUsed: string;
  rawSql: string;
  compiledSql: string | null;
  commandType: string | null;
  isAdminExecution: boolean;
}

export interface QueryAuditFinalize {
  auditId: string;
  status: 'SUCCESS' | 'FAILED' | 'BLOCKED';
  queryId?: string | null;
  rowsReturned?: number | null;
  rowsUpdated?: number | null;
  executionTimeMs?: number | null;
  errorCode?: string | null;
  errorMessage?: string | null;
  compiledSql?: string | null;
  commandType?: string | null;
}

let auditTableEnsured = false;

const ensureAuditTableSql = `
  CREATE TABLE IF NOT EXISTS DATA_QUALITY_DB.DB_METRICS.DQ_QUERY_AUDIT (
    AUDIT_ID VARCHAR(64) PRIMARY KEY,
    DATASET_ID VARCHAR(255) NOT NULL,
    DATABASE_NAME VARCHAR(255) NOT NULL,
    SCHEMA_NAME VARCHAR(255) NOT NULL,
    TABLE_NAME VARCHAR(255) NOT NULL,
    EXECUTED_BY VARCHAR(255),
    EXECUTED_ROLE VARCHAR(255),
    WAREHOUSE_USED VARCHAR(255),
    RAW_SQL STRING,
    COMPILED_SQL STRING,
    COMMAND_TYPE VARCHAR(50),
    IS_ADMIN_EXECUTION BOOLEAN DEFAULT FALSE,
    IS_BLOCKED BOOLEAN DEFAULT FALSE,
    STATUS VARCHAR(20) NOT NULL,
    QUERY_ID VARCHAR(255),
    ROWS_RETURNED NUMBER,
    ROWS_UPDATED NUMBER,
    EXECUTION_TIME_MS NUMBER,
    ERROR_CODE VARCHAR(100),
    ERROR_MESSAGE STRING,
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    UPDATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
  )
`;

const insertAuditSql = `
  INSERT INTO DATA_QUALITY_DB.DB_METRICS.DQ_QUERY_AUDIT (
    AUDIT_ID,
    DATASET_ID,
    DATABASE_NAME,
    SCHEMA_NAME,
    TABLE_NAME,
    EXECUTED_BY,
    EXECUTED_ROLE,
    WAREHOUSE_USED,
    RAW_SQL,
    COMPILED_SQL,
    COMMAND_TYPE,
    IS_ADMIN_EXECUTION,
    IS_BLOCKED,
    STATUS,
    CREATED_AT,
    UPDATED_AT
  )
  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, FALSE, 'RECEIVED', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())
`;

const updateAuditSql = `
  UPDATE DATA_QUALITY_DB.DB_METRICS.DQ_QUERY_AUDIT
  SET
    STATUS = ?,
    IS_BLOCKED = ?,
    QUERY_ID = ?,
    ROWS_RETURNED = ?,
    ROWS_UPDATED = ?,
    EXECUTION_TIME_MS = ?,
    ERROR_CODE = ?,
    ERROR_MESSAGE = ?,
    COMPILED_SQL = COALESCE(?, COMPILED_SQL),
    COMMAND_TYPE = COALESCE(?, COMMAND_TYPE),
    UPDATED_AT = CURRENT_TIMESTAMP()
  WHERE AUDIT_ID = ?
`;

export async function ensureQueryAuditTable(connection: SnowflakeConnection): Promise<void> {
  if (auditTableEnsured) return;
  await executeQuery(connection, ensureAuditTableSql);
  auditTableEnsured = true;
}

export async function insertReceivedQueryAudit(connection: SnowflakeConnection, payload: QueryAuditInit): Promise<void> {
  await executeQuery(connection, insertAuditSql, [
    payload.auditId,
    payload.datasetId,
    payload.databaseName,
    payload.schemaName,
    payload.tableName,
    payload.executedBy,
    payload.executedRole,
    payload.warehouseUsed,
    payload.rawSql,
    payload.compiledSql,
    payload.commandType,
    payload.isAdminExecution,
  ]);
}

export async function finalizeQueryAudit(connection: SnowflakeConnection, payload: QueryAuditFinalize): Promise<void> {
  const isBlocked = payload.status === 'BLOCKED';

  await executeQuery(connection, updateAuditSql, [
    payload.status,
    isBlocked,
    payload.queryId || null,
    payload.rowsReturned ?? null,
    payload.rowsUpdated ?? null,
    payload.executionTimeMs ?? null,
    payload.errorCode || null,
    payload.errorMessage || null,
    payload.compiledSql || null,
    payload.commandType || null,
    payload.auditId,
  ]);
}
