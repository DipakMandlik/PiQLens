import { NextRequest, NextResponse } from 'next/server';
import { executeQueryObjects, snowflakePool, ensureConnectionContext } from '@/lib/snowflake';
import { getServerConfig } from '@/lib/server-config';

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

function sanitizeOptionalToken(value: string | null): string | null {
  const token = String(value || '').trim();
  return token.length > 0 ? token : null;
}

function parseRuleId(value: string): number {
  const parsed = Number(value);
  if (!Number.isFinite(parsed) || parsed <= 0) {
    throw new Error('Invalid ruleId.');
  }
  return Math.floor(parsed);
}

function clampWindowHours(value: string | null): number {
  const parsed = Number(value || 24);
  if (!Number.isFinite(parsed)) return 24;
  if (parsed < 1) return 1;
  if (parsed > 168) return 168;
  return Math.floor(parsed);
}

function clampPage(value: string | null): number {
  const parsed = Number(value || 1);
  if (!Number.isFinite(parsed)) return 1;
  if (parsed < 1) return 1;
  return Math.floor(parsed);
}

function clampPageSize(value: string | null): number {
  const parsed = Number(value || 50);
  if (!Number.isFinite(parsed)) return 50;
  if (parsed < 1) return 1;
  if (parsed > 50) return 50;
  return Math.floor(parsed);
}

export async function GET(
  request: NextRequest,
  { params }: { params: Promise<{ ruleId: string }> }
) {
  try {
    const { ruleId: rawRuleId } = await params;
    const ruleId = parseRuleId(rawRuleId);

    const searchParams = request.nextUrl.searchParams;
    const database = sanitizeIdentifier(searchParams.get('database'), 'database');
    const schema = sanitizeIdentifier(searchParams.get('schema'), 'schema');
    const table = sanitizeIdentifier(searchParams.get('table'), 'table');

    const windowHours = clampWindowHours(searchParams.get('window_hours'));
    const page = clampPage(searchParams.get('page'));
    const pageSize = clampPageSize(searchParams.get('page_size'));
    const offset = (page - 1) * pageSize;

    const runId = sanitizeOptionalToken(searchParams.get('run_id'));
    const recordId = sanitizeOptionalToken(searchParams.get('record_id'));

    const config = getServerConfig();
    if (!config) {
      return NextResponse.json(
        { success: false, error: 'No Snowflake connection found. Please connect first.' },
        { status: 401 }
      );
    }

    const connection = await snowflakePool.getConnection(config);
    await ensureConnectionContext(connection, config);

    const whereClauses: string[] = [
      // Use cr for database/schema (DQ_FAILED_RECORDS does not include these cols)
      "UPPER(COALESCE(cr.DATABASE_NAME, '')) = ?",
      "UPPER(COALESCE(cr.SCHEMA_NAME, '')) = ?",
      "UPPER(COALESCE(fr.TABLE_NAME, cr.TABLE_NAME, '')) = ?",
      'fr.DETECTED_TS >= DATEADD(hour, -?, CURRENT_TIMESTAMP())',
      'COALESCE(fr.RULE_ID, cr.RULE_ID) = ?',
    ];
    const binds: unknown[] = [database, schema, table, windowHours, ruleId];

    if (runId) {
      whereClauses.push('UPPER(fr.RUN_ID) = ?');
      binds.push(runId.toUpperCase());
    }

    if (recordId) {
      whereClauses.push("UPPER(COALESCE(fr.FAILED_RECORD_PK, '')) LIKE ?");
      binds.push(`%${recordId.toUpperCase()}%`);
    }

    const whereSql = whereClauses.join('\n      AND ');

    const countQuery = `
      SELECT COUNT(*) AS TOTAL_ROWS
      FROM DATA_QUALITY_DB.DQ_METRICS.DQ_FAILED_RECORDS fr
      LEFT JOIN DATA_QUALITY_DB.DQ_METRICS.DQ_CHECK_RESULTS cr
        ON fr.CHECK_ID = cr.CHECK_ID
      WHERE ${whereSql}
    `;

    const dataQuery = `
      SELECT
        fr.FAILURE_ID,
        fr.RUN_ID,
        COALESCE(fr.PRIMARY_KEY_COLUMN, '') AS PRIMARY_KEY_COLUMN,
        COALESCE(fr.FAILED_RECORD_PK, '') AS FAILED_RECORD_PK,
        COALESCE(fr.COLUMN_NAME, cr.COLUMN_NAME, '') AS COLUMN_NAME,
        COALESCE(fr.FAILED_COLUMN_VALUE, '') AS FAILED_COLUMN_VALUE,
        COALESCE(fr.FAILURE_REASON, cr.FAILURE_REASON, 'Validation failed') AS FAILURE_REASON,
        fr.DETECTED_TS
      FROM DATA_QUALITY_DB.DQ_METRICS.DQ_FAILED_RECORDS fr
      LEFT JOIN DATA_QUALITY_DB.DQ_METRICS.DQ_CHECK_RESULTS cr
        ON fr.CHECK_ID = cr.CHECK_ID
      WHERE ${whereSql}
      ORDER BY fr.DETECTED_TS DESC
      LIMIT ? OFFSET ?
    `;

    const countRows = (await executeQueryObjects(connection, countQuery, binds)) as Array<{ TOTAL_ROWS: number }>;
    const totalRows = Number(countRows?.[0]?.TOTAL_ROWS || 0);

    const dataRows = (await executeQueryObjects(connection, dataQuery, [...binds, pageSize, offset])) as Array<{
      FAILURE_ID: number;
      RUN_ID: string;
      PRIMARY_KEY_COLUMN: string;
      FAILED_RECORD_PK: string;
      COLUMN_NAME: string;
      FAILED_COLUMN_VALUE: string;
      FAILURE_REASON: string;
      DETECTED_TS: string;
    }>;

    return NextResponse.json({
      success: true,
      data: {
        records: dataRows.map((row) => ({
          failure_id: row.FAILURE_ID,
          run_id: row.RUN_ID,
          primary_key_column: row.PRIMARY_KEY_COLUMN,
          failed_record_pk: row.FAILED_RECORD_PK,
          column_name: row.COLUMN_NAME,
          failed_column_value: row.FAILED_COLUMN_VALUE,
          failure_reason: row.FAILURE_REASON,
          detected_ts: row.DETECTED_TS,
        })),
        pagination: {
          page,
          page_size: pageSize,
          total_rows: totalRows,
          total_pages: Math.max(1, Math.ceil(totalRows / pageSize)),
        },
      },
    });
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : 'Failed to fetch failed records by rule';
    const lowered = message.toLowerCase();
    const status = lowered.includes('required') || lowered.includes('invalid') ? 400 : 500;
    return NextResponse.json({ success: false, error: message }, { status });
  }
}
