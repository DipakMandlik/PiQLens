import { NextRequest, NextResponse } from "next/server";
import { getServerConfig } from "@/lib/server-config";
import { snowflakePool, executeQuery, ensureConnectionContext } from "@/lib/snowflake";

interface InventoryResponse {
  dataset_id: string | null;
  total_active: number;
  column_level: number;
  table_level: number;
  critical: number;
  by_type: Record<string, number>;
  notices: string[];
}

function asNumber(value: unknown): number {
  const n = Number(value);
  return Number.isFinite(n) ? n : 0;
}

function asText(value: unknown): string {
  if (value === null || value === undefined) return "";
  return String(value);
}

function getCell(result: { rows: unknown[][]; columns: string[] }, rowIndex: number, column: string): unknown {
  const idx = result.columns.indexOf(column);
  if (idx < 0) return null;
  const row = result.rows[rowIndex] || [];
  return row[idx];
}

export async function GET(request: NextRequest) {
  try {
    const searchParams = request.nextUrl.searchParams;
    const datasetIdParam = searchParams.get("datasetId");
    const database = searchParams.get("database");
    const schema = searchParams.get("schema");
    const table = searchParams.get("table");

    if (!datasetIdParam && (!database || !schema || !table)) {
      return NextResponse.json(
        {
          success: false,
          error: "Provide datasetId, or database+schema+table to resolve the dataset."
        },
        { status: 400 }
      );
    }

    const config = getServerConfig();
    if (!config) {
      return NextResponse.json(
        { success: false, error: "No Snowflake connection found. Please connect first." },
        { status: 401 }
      );
    }

    const connection = await snowflakePool.getConnection(config);
    await ensureConnectionContext(connection, config);

    let resolvedDatasetId = datasetIdParam ? datasetIdParam.toUpperCase() : null;
    const notices: string[] = [];

    if (!resolvedDatasetId) {
      const db = database!.toUpperCase();
      const sch = schema!.toUpperCase();
      const tbl = table!.toUpperCase();

      const datasetLookupQuery = `
        SELECT DATASET_ID
        FROM DATA_QUALITY_DB.DQ_CONFIG.DATASET_CONFIG
        WHERE UPPER(SOURCE_DATABASE) = '${db}'
          AND UPPER(SOURCE_SCHEMA) = '${sch}'
          AND UPPER(SOURCE_TABLE) = '${tbl}'
          AND IS_ACTIVE = TRUE
        LIMIT 1
      `;

      const datasetResult = await executeQuery(connection, datasetLookupQuery);
      if (datasetResult.rows.length > 0) {
        resolvedDatasetId = asText(getCell(datasetResult, 0, "DATASET_ID"));
      }
    }

    if (!resolvedDatasetId) {
      const emptyResponse: InventoryResponse = {
        dataset_id: null,
        total_active: 0,
        column_level: 0,
        table_level: 0,
        critical: 0,
        by_type: {},
        notices: [
          "No active dataset configuration found for the selected table."
        ]
      };
      return NextResponse.json({ success: true, data: emptyResponse });
    }

    const severityColumnCheckQuery = `
      SELECT COUNT(*) AS COL_EXISTS
      FROM DATA_QUALITY_DB.INFORMATION_SCHEMA.COLUMNS
      WHERE TABLE_SCHEMA = 'DQ_CONFIG'
        AND TABLE_NAME = 'RULE_MASTER'
        AND COLUMN_NAME = 'SEVERITY'
    `;

    const severityResult = await executeQuery(connection, severityColumnCheckQuery);
    const hasSeverityColumn = asNumber(getCell(severityResult, 0, "COL_EXISTS")) > 0;
    const criticalExpression = hasSeverityColumn
      ? "UPPER(COALESCE(rm.SEVERITY, '')) = 'CRITICAL'"
      : "FALSE";

    if (!hasSeverityColumn) {
      notices.push("RULE_MASTER.SEVERITY not found; critical count defaults to 0.");
    }

    const inventoryQuery = `
      SELECT
        COUNT(*) AS TOTAL_ACTIVE_CHECKS,
        SUM(CASE WHEN drc.COLUMN_NAME IS NOT NULL AND TRIM(drc.COLUMN_NAME) <> '' THEN 1 ELSE 0 END) AS COLUMN_LEVEL,
        SUM(CASE WHEN drc.COLUMN_NAME IS NULL OR TRIM(drc.COLUMN_NAME) = '' THEN 1 ELSE 0 END) AS TABLE_LEVEL,
        SUM(CASE WHEN ${criticalExpression} THEN 1 ELSE 0 END) AS CRITICAL_COUNT
      FROM DATA_QUALITY_DB.DQ_CONFIG.DATASET_RULE_CONFIG drc
      JOIN DATA_QUALITY_DB.DQ_CONFIG.RULE_MASTER rm
        ON drc.RULE_ID = rm.RULE_ID
      WHERE drc.DATASET_ID = '${resolvedDatasetId}'
        AND drc.IS_ACTIVE = TRUE
        AND rm.IS_ACTIVE = TRUE
    `;

    const byTypeQuery = `
      SELECT rm.RULE_TYPE, COUNT(*) AS COUNT
      FROM DATA_QUALITY_DB.DQ_CONFIG.DATASET_RULE_CONFIG drc
      JOIN DATA_QUALITY_DB.DQ_CONFIG.RULE_MASTER rm
        ON drc.RULE_ID = rm.RULE_ID
      WHERE drc.DATASET_ID = '${resolvedDatasetId}'
        AND drc.IS_ACTIVE = TRUE
        AND rm.IS_ACTIVE = TRUE
      GROUP BY rm.RULE_TYPE
      ORDER BY rm.RULE_TYPE
    `;

    const [inventoryResult, byTypeResult] = await Promise.all([
      executeQuery(connection, inventoryQuery),
      executeQuery(connection, byTypeQuery)
    ]);

    const byType: Record<string, number> = {};
    for (let i = 0; i < byTypeResult.rows.length; i++) {
      const ruleType = asText(getCell(byTypeResult, i, "RULE_TYPE"));
      if (!ruleType) continue;
      byType[ruleType] = asNumber(getCell(byTypeResult, i, "COUNT"));
    }

    const response: InventoryResponse = {
      dataset_id: resolvedDatasetId,
      total_active: asNumber(getCell(inventoryResult, 0, "TOTAL_ACTIVE_CHECKS")),
      column_level: asNumber(getCell(inventoryResult, 0, "COLUMN_LEVEL")),
      table_level: asNumber(getCell(inventoryResult, 0, "TABLE_LEVEL")),
      critical: asNumber(getCell(inventoryResult, 0, "CRITICAL_COUNT")),
      by_type: byType,
      notices
    };

    return NextResponse.json({ success: true, data: response });
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : "Failed to fetch checks inventory";
    console.error("[Checks Inventory] Error:", error);
    return NextResponse.json(
      { success: false, error: message },
      { status: 500 }
    );
  }
}
