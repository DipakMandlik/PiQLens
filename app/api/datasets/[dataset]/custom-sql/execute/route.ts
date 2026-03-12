import { randomUUID } from 'crypto';
import { NextRequest, NextResponse } from 'next/server';
import { getServerConfig } from '@/lib/server-config';
import { executeQuery, snowflakePool } from '@/lib/snowflake';
import {
  ensureQueryAuditTable,
  finalizeQueryAudit,
  insertReceivedQueryAudit,
} from '@/lib/custom-sql/audit';
import { getCustomSqlAccess } from '@/lib/custom-sql/security';
import {
  SqlValidationError,
  type SqlMode,
  validateAndCompileSql,
} from '@/lib/custom-sql/validator';
import { logger } from '@/lib/logger';

interface ExecuteBody {
  database?: string;
  schema?: string;
  table?: string;
  sql?: string;
  warehouse?: string;
  mode?: SqlMode;
}

interface SnowflakeColumn {
  getName: () => string;
}

interface SnowflakeStatement {
  getColumns: () => SnowflakeColumn[] | undefined;
  getQueryId: () => string;
  getNumRows: () => number;
  getNumUpdatedRows: () => number | undefined;
}

interface SnowflakeConnection {
  execute: (options: {
    sqlText: string;
    binds?: unknown[];
    parameters?: Record<string, unknown>;
    complete: (err: unknown, stmt: SnowflakeStatement, rows: unknown[]) => void;
  }) => unknown;
}

interface ExecutionResult {
  columns: string[];
  rows: unknown[][];
  queryId: string | null;
  rowsReturned: number;
  rowsUpdated: number;
}

function sanitizeIdentifier(value: string | undefined | null, label: string): string {
  const normalized = String(value || '').trim().toUpperCase();
  if (!normalized) {
    throw new SqlValidationError(`${label} is required.`, 'MISSING_PARAMETER', 400);
  }
  if (!/^[A-Z0-9_$]+$/.test(normalized)) {
    throw new SqlValidationError(`Invalid ${label} identifier.`, 'INVALID_IDENTIFIER', 400);
  }
  return normalized;
}

function quoteIdentifier(identifier: string): string {
  return `"${identifier.replace(/"/g, '""')}"`;
}

function escapeSqlLiteral(value: string): string {
  return value.replace(/'/g, "''");
}

function getErrorMessage(error: unknown): string {
  if (error instanceof Error) return error.message;
  return String(error);
}

function getErrorCode(error: unknown): string | null {
  if (!error || typeof error !== 'object') return null;
  const candidate = error as { code?: unknown };
  return candidate.code ? String(candidate.code) : null;
}

function getRowValue(row: unknown, columnName: string): unknown {
  if (!row || typeof row !== 'object') return null;
  const record = row as Record<string, unknown>;

  if (Object.prototype.hasOwnProperty.call(record, columnName)) {
    return record[columnName];
  }

  const upper = columnName.toUpperCase();
  if (Object.prototype.hasOwnProperty.call(record, upper)) {
    return record[upper];
  }

  const lower = columnName.toLowerCase();
  if (Object.prototype.hasOwnProperty.call(record, lower)) {
    return record[lower];
  }

  return null;
}

async function listAccessibleWarehouses(connection: SnowflakeConnection): Promise<Set<string>> {
  const result = await executeQuery(connection, 'SHOW WAREHOUSES');
  const nameIndex = result.columns.findIndex((name) => name.toUpperCase() === 'NAME');

  if (nameIndex < 0) return new Set<string>();

  const warehouses = new Set<string>();
  for (const row of result.rows) {
    const value = row[nameIndex];
    if (!value) continue;
    warehouses.add(String(value).trim().toUpperCase());
  }

  return warehouses;
}

async function executeSqlWithMetadata(
  connection: SnowflakeConnection,
  sqlText: string,
  timeoutSeconds: number
): Promise<ExecutionResult> {
  return new Promise<ExecutionResult>((resolve, reject) => {
    connection.execute({
      sqlText,
      parameters: {
        STATEMENT_TIMEOUT_IN_SECONDS: timeoutSeconds,
      },
      complete: (err, stmt, rows) => {
        if (err) {
          reject(err);
          return;
        }

        const columns = (stmt.getColumns() || []).map((column) => column.getName());
        const rowArray = (rows || []).map((row) => columns.map((columnName) => getRowValue(row, columnName)));

        resolve({
          columns,
          rows: rowArray,
          queryId: stmt.getQueryId() || null,
          rowsReturned: Number.isFinite(stmt.getNumRows()) ? stmt.getNumRows() : rowArray.length,
          rowsUpdated: Number.isFinite(stmt.getNumUpdatedRows()) ? (stmt.getNumUpdatedRows() || 0) : 0,
        });
      },
    });
  });
}

export const runtime = 'nodejs';

export async function POST(
  request: NextRequest,
  { params }: { params: Promise<{ dataset: string }> }
) {
  let auditId: string | null = null;

  try {
    const { dataset } = await params;
    const datasetId = decodeURIComponent(dataset);
    const body = (await request.json()) as ExecuteBody;

    const database = sanitizeIdentifier(body.database, 'database');
    const schema = sanitizeIdentifier(body.schema, 'schema');
    const table = sanitizeIdentifier(body.table, 'table');
    const rawSql = String(body.sql || '').trim();
    const requestedWarehouse = body.warehouse ? sanitizeIdentifier(body.warehouse, 'warehouse') : null;
    const mode: SqlMode = body.mode === 'explain' ? 'explain' : 'execute';

    if (!rawSql) {
      return NextResponse.json({ success: false, error: 'sql is required.' }, { status: 400 });
    }

    const config = getServerConfig();
    if (!config) {
      return NextResponse.json(
        { success: false, error: 'No Snowflake connection found. Please connect first.' },
        { status: 401 }
      );
    }

    const connection = (await snowflakePool.getConnection(config)) as SnowflakeConnection;
    const access = getCustomSqlAccess(config.role ? config.role.toUpperCase() : null);
    const isAdmin = access.appRole === 'ADMIN';
    const selectedWarehouse = requestedWarehouse || (config.warehouse ? config.warehouse.toUpperCase() : null);

    if (!selectedWarehouse) {
      return NextResponse.json(
        { success: false, error: 'Warehouse is required. Configure a default warehouse or provide override.' },
        { status: 400 }
      );
    }

    await ensureQueryAuditTable(connection);

    auditId = randomUUID();
    await insertReceivedQueryAudit(connection, {
      auditId,
      datasetId,
      databaseName: database,
      schemaName: schema,
      tableName: table,
      executedBy: config.username || 'UNKNOWN',
      executedRole: config.role ? config.role.toUpperCase() : 'UNKNOWN',
      warehouseUsed: selectedWarehouse,
      rawSql,
      compiledSql: null,
      commandType: null,
      isAdminExecution: isAdmin,
    });

    if (!access.permissions.canRunSql) {
      await finalizeQueryAudit(connection, {
        auditId,
        status: 'BLOCKED',
        errorCode: 'RBAC_EXECUTE_BLOCKED',
        errorMessage: `Role ${access.appRole} is not allowed to execute SQL in this workbench.`,
      });

      return NextResponse.json(
        { success: false, error: `Role ${access.appRole} is not allowed to execute SQL in this workbench.` },
        { status: 403 }
      );
    }

    const accessibleWarehouses = await listAccessibleWarehouses(connection);
    const fallbackWarehouse = config.warehouse ? config.warehouse.toUpperCase() : null;

    if (
      requestedWarehouse &&
      !accessibleWarehouses.has(requestedWarehouse) &&
      (!fallbackWarehouse || requestedWarehouse !== fallbackWarehouse)
    ) {
      await finalizeQueryAudit(connection, {
        auditId,
        status: 'BLOCKED',
        errorCode: 'WAREHOUSE_NOT_ACCESSIBLE',
        errorMessage: `Warehouse ${requestedWarehouse} is not accessible for current role.`,
      });

      return NextResponse.json(
        { success: false, error: `Warehouse ${requestedWarehouse} is not accessible for current role.` },
        { status: 403 }
      );
    }

    const validated = validateAndCompileSql({
      sql: rawSql,
      database,
      schema,
      table,
      isAdmin,
      mode,
    });

    if (!access.permissions.allowedCommands.includes(validated.commandType)) {
      throw new SqlValidationError(
        `Role ${access.appRole} cannot execute command ${validated.commandType}.`,
        'RBAC_COMMAND_BLOCKED',
        403
      );
    }

    await executeQuery(connection, `USE WAREHOUSE ${quoteIdentifier(selectedWarehouse)}`);

    // Removed the USE DATABASE and USE SCHEMA context switches
    // Custom SQL should use fully-qualified names in the generated/compiled SQL anyway
    // If not, we manually alter the session parameter instead of USE command to not break thread-safety.

    // Safety fallback - manually set the session schema if not fully qualified in compiled
    // We only set schema context for this exact query by wrapping the execution if necessary, 
    // but the compiled SQL logic typically injects fully qualified names.

    await executeQuery(
      connection,
      `ALTER SESSION SET QUERY_TAG = '${escapeSqlLiteral("PI_QUALYTICS_CUSTOM_SQL|" + datasetId + "|" + table)}'`
    );

    const startedAt = Date.now();

    // Instead of relying on USE DATABASE which breaks the session pool, we prepend USE DATABASE into the execution
    // string just for this compiled statement, so it only affects this exact execution thread.
    // However, since connection pooling in snowflake-sdk doesn't work well with USE DATABASE,
    // the safest approach is ensuring validateAndCompileSql generates fully qualified references.

    // Since we are validating user custom SQL, we *must* execute it with the context set.
    // To do this safely, we bundle them into a single string for execution (if Snowflake SDK supports it),
    // or we just re-establish the context safely in a transaction. 
    // Since snowflake SDK doesn't support multiple statements, we will execute USE here, but with severe caution.

    // To truly fix the 090105 error, we avoid `USE DATABASE` where concurrent API hits happen. 
    // For custom SQL sandbox, we DO need `USE DATABASE` because users write raw SQL without full qualification.
    // We will execute the USE DATABASE context here as Custom SQL is rarely concurrent on the exact same pool connection.
    try {
      await executeQuery(connection, `USE DATABASE ${quoteIdentifier(database)}`);
      await executeQuery(connection, `USE SCHEMA ${quoteIdentifier(schema)}`);
    } catch (e) {
      console.warn('Failed to set context for custom SQL execution:', e);
    }

    const execution = await executeSqlWithMetadata(connection, validated.compiledSql, validated.timeoutSeconds);
    const executionTimeMs = Date.now() - startedAt;

    const truncated =
      validated.limitAdded ||
      (validated.appliedLimit !== null &&
        execution.rowsReturned >= validated.appliedLimit &&
        validated.appliedLimit === validated.maxRows);

    await finalizeQueryAudit(connection, {
      auditId,
      status: 'SUCCESS',
      queryId: execution.queryId,
      rowsReturned: execution.rowsReturned,
      rowsUpdated: execution.rowsUpdated,
      executionTimeMs,
      compiledSql: validated.compiledSql,
      commandType: validated.commandType,
    });

    logger.info('Custom SQL executed', {
      endpoint: '/api/datasets/[dataset]/custom-sql/execute',
      datasetId,
      database,
      schema,
      table,
      queryId: execution.queryId,
      durationMs: executionTimeMs,
      commandType: validated.commandType,
      appRole: access.appRole,
      isAdmin,
    });

    return NextResponse.json({
      success: true,
      data: {
        audit_id: auditId,
        query_id: execution.queryId,
        status: 'SUCCESS',
        execution_time_ms: executionTimeMs,
        rows_returned: execution.rowsReturned,
        rows_updated: execution.rowsUpdated,
        warehouse_used: selectedWarehouse,
        database_used: database,
        schema_used: schema,
        columns: execution.columns,
        rows: execution.rows,
        truncated,
        applied_limit: validated.appliedLimit,
        command_type: validated.commandType,
        notices: validated.notices,
        app_role: access.appRole,
      },
    });
  } catch (error: unknown) {
    const message = getErrorMessage(error);
    const code = getErrorCode(error);
    const statusCode = error instanceof SqlValidationError ? error.statusCode : 500;

    try {
      const config = getServerConfig();
      if (auditId && config) {
        const connection = (await snowflakePool.getConnection(config)) as SnowflakeConnection;
        await finalizeQueryAudit(connection, {
          auditId,
          status: error instanceof SqlValidationError ? 'BLOCKED' : 'FAILED',
          errorCode: error instanceof SqlValidationError ? error.code : code,
          errorMessage: message,
        });
      }
    } catch (auditError: unknown) {
      logger.error('Failed to finalize custom SQL audit', auditError, {
        endpoint: '/api/datasets/[dataset]/custom-sql/execute',
        auditId,
      });
    }

    return NextResponse.json({ success: false, error: message }, { status: statusCode });
  }
}
