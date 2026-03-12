import { NextRequest, NextResponse } from 'next/server';
import { executeQuery, snowflakePool, ensureConnectionContext } from '@/lib/snowflake';
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

function quoteIdentifier(identifier: string): string {
  return `"${identifier.replace(/"/g, '""')}"`;
}

export async function GET(request: NextRequest) {
  try {
    const searchParams = request.nextUrl.searchParams;
    const database = sanitizeIdentifier(searchParams.get('database'), 'database');
    const schema = sanitizeIdentifier(searchParams.get('schema'), 'schema');
    const table = sanitizeIdentifier(searchParams.get('table'), 'table');
    const pkColumn = sanitizeIdentifier(searchParams.get('pk_column'), 'pk_column');
    const pkValue = String(searchParams.get('pk_value') || '').trim();

    if (!pkValue) {
      return NextResponse.json({ success: false, error: 'pk_value is required.' }, { status: 400 });
    }

    const config = getServerConfig();
    if (!config) {
      return NextResponse.json(
        { success: false, error: 'No Snowflake connection found. Please connect first.' },
        { status: 401 }
      );
    }

    const connection = await snowflakePool.getConnection(config);
    await ensureConnectionContext(connection, config);

    const sql = `
      SELECT *
      FROM ${quoteIdentifier(database)}.${quoteIdentifier(schema)}.${quoteIdentifier(table)}
      WHERE TRY_TO_VARCHAR(${quoteIdentifier(pkColumn)}) = ?
      LIMIT 1
    `;

    const result = await executeQuery(connection, sql, [pkValue]);
    if (!result.rows || result.rows.length === 0) {
      return NextResponse.json(
        { success: false, error: 'Record not found for provided key.' },
        { status: 404 }
      );
    }

    const firstRow = result.rows[0];
    const record: Record<string, unknown> = {};
    result.columns.forEach((column, idx) => {
      record[column] = firstRow[idx];
    });

    return NextResponse.json({
      success: true,
      data: {
        pk_column: pkColumn,
        pk_value: pkValue,
        record,
      },
    });
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : 'Failed to fetch record detail';
    const lowered = message.toLowerCase();
    const status = lowered.includes('required') || lowered.includes('invalid') ? 400 : 500;
    return NextResponse.json({ success: false, error: message }, { status });
  }
}
