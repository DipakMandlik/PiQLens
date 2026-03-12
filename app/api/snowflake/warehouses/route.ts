import { NextResponse } from 'next/server';
import { getServerConfig } from '@/lib/server-config';
import { executeQuery, snowflakePool } from '@/lib/snowflake';
import { getCustomSqlAccess, isAdminFromConfig } from '@/lib/custom-sql/security';

interface WarehouseRow {
  name: string;
  state: string | null;
  size: string | null;
  type: string | null;
}

function getCell(columns: string[], row: unknown[], target: string): unknown {
  const index = columns.findIndex((col) => col.toUpperCase() === target.toUpperCase());
  if (index < 0) return null;
  return row[index];
}

function toText(value: unknown): string | null {
  if (value === null || value === undefined) return null;
  const text = String(value).trim();
  return text.length > 0 ? text : null;
}

export async function GET() {
  try {
    const config = getServerConfig();
    if (!config) {
      return NextResponse.json(
        { success: false, error: 'No Snowflake connection found. Please connect first.' },
        { status: 401 }
      );
    }

    const connection = await snowflakePool.getConnection(config);
    const result = await executeQuery(connection, 'SHOW WAREHOUSES');
    const currentUserResult = await executeQuery(connection, 'SELECT CURRENT_USER() AS CURRENT_USER');

    const warehouses: WarehouseRow[] = result.rows
      .map((row) => ({
        name: String(getCell(result.columns, row, 'name') || '').toUpperCase(),
        state: toText(getCell(result.columns, row, 'state')),
        size: toText(getCell(result.columns, row, 'size')),
        type: toText(getCell(result.columns, row, 'type')),
      }))
      .filter((entry) => entry.name.length > 0);

    const currentWarehouse = config.warehouse ? config.warehouse.toUpperCase() : null;
    const currentRole = config.role ? config.role.toUpperCase() : null;
    const access = getCustomSqlAccess(currentRole);
    const currentUser = String(currentUserResult.rows?.[0]?.[0] || config.username || '').toUpperCase() || null;

    return NextResponse.json({
      success: true,
      data: {
        warehouses,
        current_warehouse: currentWarehouse,
        current_role: currentRole,
        current_user: currentUser,
        is_admin: isAdminFromConfig(config),
        custom_sql_role: access.appRole,
        custom_sql_permissions: access.permissions,
      },
    });
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : 'Failed to fetch warehouses';
    return NextResponse.json({ success: false, error: message }, { status: 500 });
  }
}
