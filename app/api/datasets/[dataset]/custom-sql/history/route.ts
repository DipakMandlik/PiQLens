import { NextRequest, NextResponse } from 'next/server';
import { getServerConfig } from '@/lib/server-config';
import { executeQueryObjects, snowflakePool } from '@/lib/snowflake';
import { ensureQueryAuditTable } from '@/lib/custom-sql/audit';
import { getCustomSqlAccess } from '@/lib/custom-sql/security';

interface HistoryRow {
  AUDIT_ID: string;
  STATUS: string;
  QUERY_ID: string | null;
  COMMAND_TYPE: string | null;
  RAW_SQL: string | null;
  WAREHOUSE_USED: string | null;
  EXECUTION_TIME_MS: number | null;
  ROWS_RETURNED: number | null;
  ROWS_UPDATED: number | null;
  IS_ADMIN_EXECUTION: boolean;
  CREATED_AT: string | null;
}

function sanitizeIdentifier(value: string | null, label: string): string {
  const normalized = String(value || '').trim().toUpperCase();
  if (!normalized) {
    throw new Error(`${label} is required.`);
  }
  if (!/^[A-Z0-9_$]+$/.test(normalized)) {
    throw new Error(`Invalid ${label} identifier.`);
  }
  return normalized;
}

function clampLimit(value: string | null): number {
  const parsed = Number(value || 10);
  if (!Number.isFinite(parsed)) return 10;
  if (parsed < 1) return 1;
  if (parsed > 50) return 50;
  return Math.floor(parsed);
}

function previewSql(rawSql: string | null): string {
  if (!rawSql) return '';
  const oneLine = rawSql.replace(/\s+/g, ' ').trim();
  return oneLine.length > 220 ? `${oneLine.slice(0, 220)}...` : oneLine;
}

export async function GET(
  request: NextRequest,
  { params }: { params: Promise<{ dataset: string }> }
) {
  try {
    const { dataset } = await params;
    const datasetId = decodeURIComponent(dataset);

    const searchParams = request.nextUrl.searchParams;
    const database = sanitizeIdentifier(searchParams.get('database'), 'database');
    const schema = sanitizeIdentifier(searchParams.get('schema'), 'schema');
    const table = sanitizeIdentifier(searchParams.get('table'), 'table');
    const limit = clampLimit(searchParams.get('limit'));

    const config = getServerConfig();
    if (!config) {
      return NextResponse.json(
        { success: false, error: 'No Snowflake connection found. Please connect first.' },
        { status: 401 }
      );
    }

    const access = getCustomSqlAccess(config.role ? config.role.toUpperCase() : null);
    if (!access.permissions.canViewHistory) {
      return NextResponse.json(
        { success: false, error: `Role ${access.appRole} cannot view query history.` },
        { status: 403 }
      );
    }

    const connection = await snowflakePool.getConnection(config);
    await ensureQueryAuditTable(connection);
    const rows = (await executeQueryObjects(
      connection,
      `
      SELECT
        AUDIT_ID,
        STATUS,
        QUERY_ID,
        COMMAND_TYPE,
        RAW_SQL,
        WAREHOUSE_USED,
        EXECUTION_TIME_MS,
        ROWS_RETURNED,
        ROWS_UPDATED,
        IS_ADMIN_EXECUTION,
        CREATED_AT
      FROM DATA_QUALITY_DB.DB_METRICS.DQ_QUERY_AUDIT
      WHERE UPPER(DATASET_ID) = ?
        AND UPPER(DATABASE_NAME) = ?
        AND UPPER(SCHEMA_NAME) = ?
        AND UPPER(TABLE_NAME) = ?
      ORDER BY CREATED_AT DESC
      LIMIT ?
      `,
      [datasetId.toUpperCase(), database, schema, table, limit]
    )) as HistoryRow[];

    const history = rows.map((row) => ({
      audit_id: row.AUDIT_ID,
      status: row.STATUS,
      query_id: row.QUERY_ID,
      command_type: row.COMMAND_TYPE,
      sql_preview: previewSql(row.RAW_SQL),
      sql: row.RAW_SQL,
      warehouse_used: row.WAREHOUSE_USED,
      execution_time_ms: row.EXECUTION_TIME_MS,
      rows_returned: row.ROWS_RETURNED,
      rows_updated: row.ROWS_UPDATED,
      is_admin_execution: row.IS_ADMIN_EXECUTION,
      created_at: row.CREATED_AT,
    }));

    return NextResponse.json({
      success: true,
      data: {
        dataset_id: datasetId,
        database,
        schema,
        table,
        history,
      },
    });
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : 'Failed to fetch query history';
    const lowered = message.toLowerCase();
    const statusCode = lowered.includes('required') || lowered.includes('invalid') ? 400 : 500;
    return NextResponse.json({ success: false, error: message }, { status: statusCode });
  }
}
