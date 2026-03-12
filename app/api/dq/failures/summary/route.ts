import { NextRequest, NextResponse } from 'next/server';
import { executeQueryObjects, snowflakePool, ensureConnectionContext } from '@/lib/snowflake';
import { getServerConfig } from '@/lib/server-config';

type FailureRuleRow = {
  RULE_ID: number | null;
  RULE_NAME: string | null;
  RULE_TYPE: string | null;
  COLUMN_NAME: string | null;
  THRESHOLD: number | null;
  PASS_RATE: number | null;
  SEVERITY: string;
  FAILED_RECORDS: number;
  LAST_FAILURE_TS: string | null;
};

type MostCriticalRow = {
  RULE_NAME: string | null;
  COLUMN_NAME: string | null;
  SEVERITY: string;
  FAILED_RECORDS: number;
};

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

function clampWindowHours(value: string | null): number {
  const parsed = Number(value || 24);
  if (!Number.isFinite(parsed)) return 24;
  if (parsed < 1) return 1;
  if (parsed > 168) return 168;
  return Math.floor(parsed);
}

export async function GET(request: NextRequest) {
  try {
    const params = request.nextUrl.searchParams;

    const database = sanitizeIdentifier(params.get('database'), 'database');
    const schema = sanitizeIdentifier(params.get('schema'), 'schema');
    const table = sanitizeIdentifier(params.get('table'), 'table');
    const windowHours = clampWindowHours(params.get('window_hours'));

    const ruleType = sanitizeOptionalToken(params.get('rule_type'));
    const severity = sanitizeOptionalToken(params.get('severity'));
    const column = sanitizeOptionalToken(params.get('column'));
    const recordId = sanitizeOptionalToken(params.get('record_id'));
    const runId = sanitizeOptionalToken(params.get('run_id'));

    const config = getServerConfig();
    if (!config) {
      return NextResponse.json(
        { success: false, error: 'No Snowflake connection found. Please connect first.' },
        { status: 401 }
      );
    }

    const connection = await snowflakePool.getConnection(config);
    await ensureConnectionContext(connection, config);

    // ── Severity expression (used in both paths) ──
    const severityCaseFR = `
      CASE
        WHEN COALESCE(fr.IS_CRITICAL, FALSE) THEN 'HIGH'
        WHEN UPPER(COALESCE(cr.RULE_LEVEL, '')) IN ('CRITICAL', 'HIGH') THEN 'HIGH'
        WHEN UPPER(COALESCE(cr.RULE_LEVEL, '')) = 'MEDIUM' THEN 'MEDIUM'
        ELSE 'LOW'
      END
    `;
    const severityCaseCR = `
      CASE
        WHEN UPPER(COALESCE(cr.RULE_LEVEL, '')) IN ('CRITICAL', 'HIGH') THEN 'HIGH'
        WHEN UPPER(COALESCE(cr.RULE_LEVEL, '')) = 'MEDIUM' THEN 'MEDIUM'
        ELSE 'LOW'
      END
    `;

    // ═══════════════════════════════════════════════════════════════
    // PATH 1: Try DQ_FAILED_RECORDS first (row-level detail)
    // ═══════════════════════════════════════════════════════════════
    const frWhereClauses: string[] = [
      "UPPER(COALESCE(cr.DATABASE_NAME, '')) = ?",
      "UPPER(COALESCE(cr.SCHEMA_NAME, '')) = ?",
      "UPPER(COALESCE(fr.TABLE_NAME, cr.TABLE_NAME, '')) = ?",
      'fr.DETECTED_TS >= DATEADD(hour, -?, CURRENT_TIMESTAMP())',
    ];
    const frBinds: unknown[] = [database, schema, table, windowHours];

    if (ruleType) {
      frWhereClauses.push("UPPER(COALESCE(fr.RULE_TYPE, cr.RULE_TYPE, '')) = ?");
      frBinds.push(ruleType.toUpperCase());
    }
    if (column) {
      frWhereClauses.push("UPPER(COALESCE(fr.COLUMN_NAME, cr.COLUMN_NAME, '')) = ?");
      frBinds.push(column.toUpperCase());
    }
    if (runId) {
      frWhereClauses.push('UPPER(fr.RUN_ID) = ?');
      frBinds.push(runId.toUpperCase());
    }
    if (recordId) {
      frWhereClauses.push("UPPER(COALESCE(fr.FAILED_RECORD_PK, '')) LIKE ?");
      frBinds.push(`%${recordId.toUpperCase()}%`);
    }
    if (severity) {
      frWhereClauses.push(`${severityCaseFR} = ?`);
      frBinds.push(severity.toUpperCase());
    }

    const frWhereSql = frWhereClauses.join('\n      AND ');

    // Check if DQ_FAILED_RECORDS has any data for this context
    const frSummaryQuery = `
      SELECT
        COUNT(DISTINCT fr.CHECK_ID) AS FAILED_CHECKS,
        COUNT(*) AS FAILED_RECORDS,
        MAX(fr.DETECTED_TS) AS LAST_FAILURE_TS
      FROM DATA_QUALITY_DB.DQ_METRICS.DQ_FAILED_RECORDS fr
      LEFT JOIN DATA_QUALITY_DB.DQ_METRICS.DQ_CHECK_RESULTS cr
        ON fr.CHECK_ID = cr.CHECK_ID
      WHERE ${frWhereSql}
    `;

    const frSummaryRows = (await executeQueryObjects(connection, frSummaryQuery, frBinds)) as Array<{
      FAILED_CHECKS: number | null;
      FAILED_RECORDS: number | null;
      LAST_FAILURE_TS: string | null;
    }>;

    const frSummary = frSummaryRows[0] || { FAILED_CHECKS: 0, FAILED_RECORDS: 0, LAST_FAILURE_TS: null };
    const hasFailedRecordRows = Number(frSummary.FAILED_RECORDS || 0) > 0;

    if (hasFailedRecordRows) {
      // DQ_FAILED_RECORDS has data — use full row-level detail path
      const mostCriticalQuery = `
        SELECT
          COALESCE(fr.RULE_NAME, cr.RULE_NAME) AS RULE_NAME,
          COALESCE(fr.COLUMN_NAME, cr.COLUMN_NAME) AS COLUMN_NAME,
          ${severityCaseFR} AS SEVERITY,
          COUNT(*) AS FAILED_RECORDS,
          MAX(fr.DETECTED_TS) AS LAST_FAILURE_TS,
          CASE
            WHEN ${severityCaseFR} = 'HIGH' THEN 3
            WHEN ${severityCaseFR} = 'MEDIUM' THEN 2
            ELSE 1
          END AS SEVERITY_RANK
        FROM DATA_QUALITY_DB.DQ_METRICS.DQ_FAILED_RECORDS fr
        LEFT JOIN DATA_QUALITY_DB.DQ_METRICS.DQ_CHECK_RESULTS cr
          ON fr.CHECK_ID = cr.CHECK_ID
        WHERE ${frWhereSql}
        GROUP BY 1, 2, 3
        ORDER BY SEVERITY_RANK DESC, FAILED_RECORDS DESC, LAST_FAILURE_TS DESC
        LIMIT 1
      `;

      const rulesQuery = `
        SELECT
          COALESCE(fr.RULE_ID, cr.RULE_ID) AS RULE_ID,
          COALESCE(fr.RULE_NAME, cr.RULE_NAME) AS RULE_NAME,
          COALESCE(fr.RULE_TYPE, cr.RULE_TYPE) AS RULE_TYPE,
          COALESCE(fr.COLUMN_NAME, cr.COLUMN_NAME) AS COLUMN_NAME,
          MAX(cr.THRESHOLD) AS THRESHOLD,
          MIN(cr.PASS_RATE) AS PASS_RATE,
          ${severityCaseFR} AS SEVERITY,
          COUNT(*) AS FAILED_RECORDS,
          MAX(fr.DETECTED_TS) AS LAST_FAILURE_TS,
          CASE
            WHEN ${severityCaseFR} = 'HIGH' THEN 3
            WHEN ${severityCaseFR} = 'MEDIUM' THEN 2
            ELSE 1
          END AS SEVERITY_RANK
        FROM DATA_QUALITY_DB.DQ_METRICS.DQ_FAILED_RECORDS fr
        LEFT JOIN DATA_QUALITY_DB.DQ_METRICS.DQ_CHECK_RESULTS cr
          ON fr.CHECK_ID = cr.CHECK_ID
        WHERE ${frWhereSql}
        GROUP BY 1, 2, 3, 4, 7
        ORDER BY SEVERITY_RANK DESC, FAILED_RECORDS DESC, LAST_FAILURE_TS DESC
      `;

      const criticalRows = (await executeQueryObjects(connection, mostCriticalQuery, frBinds)) as MostCriticalRow[];
      const ruleRows = (await executeQueryObjects(connection, rulesQuery, frBinds)) as FailureRuleRow[];
      const mostCritical = criticalRows[0];

      return NextResponse.json({
        success: true,
        data: {
          source: 'failed_records',
          summary: {
            failed_checks: Number(frSummary.FAILED_CHECKS || 0),
            failed_records: Number(frSummary.FAILED_RECORDS || 0),
            most_critical_rule: mostCritical
              ? `${mostCritical.RULE_NAME || 'UNKNOWN'}${mostCritical.COLUMN_NAME ? ` - ${mostCritical.COLUMN_NAME}` : ''}`
              : null,
            last_failure_ts: frSummary.LAST_FAILURE_TS || null,
          },
          rules: ruleRows.map((row) => ({
            rule_id: row.RULE_ID,
            rule_name: row.RULE_NAME,
            rule_type: row.RULE_TYPE,
            column_name: row.COLUMN_NAME,
            threshold: row.THRESHOLD,
            pass_rate: row.PASS_RATE,
            severity: row.SEVERITY,
            failed_records: Number(row.FAILED_RECORDS || 0),
            last_failure_ts: row.LAST_FAILURE_TS,
          })),
        },
      });
    }

    // ═══════════════════════════════════════════════════════════════
    // PATH 2: FALLBACK — DQ_FAILED_RECORDS is empty, use DQ_CHECK_RESULTS directly
    // This shows rule-level failures even when row-level capture hasn't run yet
    // ═══════════════════════════════════════════════════════════════
    const crWhereClauses: string[] = [
      "UPPER(COALESCE(cr.DATABASE_NAME, '')) = ?",
      "UPPER(COALESCE(cr.SCHEMA_NAME, '')) = ?",
      "UPPER(COALESCE(cr.TABLE_NAME, '')) = ?",
      'cr.CHECK_TIMESTAMP >= DATEADD(hour, -?, CURRENT_TIMESTAMP())',
      "cr.CHECK_STATUS = 'FAILED'",
    ];
    const crBinds: unknown[] = [database, schema, table, windowHours];

    if (ruleType) {
      crWhereClauses.push("UPPER(COALESCE(cr.RULE_TYPE, '')) = ?");
      crBinds.push(ruleType.toUpperCase());
    }
    if (column) {
      crWhereClauses.push("UPPER(COALESCE(cr.COLUMN_NAME, '')) = ?");
      crBinds.push(column.toUpperCase());
    }
    if (runId) {
      crWhereClauses.push('UPPER(cr.RUN_ID) = ?');
      crBinds.push(runId.toUpperCase());
    }
    if (severity) {
      crWhereClauses.push(`${severityCaseCR} = ?`);
      crBinds.push(severity.toUpperCase());
    }

    const crWhereSql = crWhereClauses.join('\n      AND ');

    const crSummaryQuery = `
      SELECT
        COUNT(*) AS FAILED_CHECKS,
        SUM(COALESCE(cr.INVALID_RECORDS, 0)) AS FAILED_RECORDS,
        MAX(cr.CHECK_TIMESTAMP) AS LAST_FAILURE_TS
      FROM DATA_QUALITY_DB.DQ_METRICS.DQ_CHECK_RESULTS cr
      WHERE ${crWhereSql}
    `;

    const crMostCriticalQuery = `
      SELECT
        cr.RULE_NAME,
        cr.COLUMN_NAME,
        ${severityCaseCR} AS SEVERITY,
        SUM(COALESCE(cr.INVALID_RECORDS, 0)) AS FAILED_RECORDS,
        MAX(cr.CHECK_TIMESTAMP) AS LAST_FAILURE_TS,
        CASE
          WHEN ${severityCaseCR} = 'HIGH' THEN 3
          WHEN ${severityCaseCR} = 'MEDIUM' THEN 2
          ELSE 1
        END AS SEVERITY_RANK
      FROM DATA_QUALITY_DB.DQ_METRICS.DQ_CHECK_RESULTS cr
      WHERE ${crWhereSql}
      GROUP BY 1, 2, 3
      ORDER BY SEVERITY_RANK DESC, FAILED_RECORDS DESC, LAST_FAILURE_TS DESC
      LIMIT 1
    `;

    const crRulesQuery = `
      SELECT
        cr.RULE_ID,
        cr.RULE_NAME,
        cr.RULE_TYPE,
        cr.COLUMN_NAME,
        MAX(cr.THRESHOLD) AS THRESHOLD,
        MIN(cr.PASS_RATE) AS PASS_RATE,
        ${severityCaseCR} AS SEVERITY,
        SUM(COALESCE(cr.INVALID_RECORDS, 0)) AS FAILED_RECORDS,
        MAX(cr.CHECK_TIMESTAMP) AS LAST_FAILURE_TS,
        CASE
          WHEN ${severityCaseCR} = 'HIGH' THEN 3
          WHEN ${severityCaseCR} = 'MEDIUM' THEN 2
          ELSE 1
        END AS SEVERITY_RANK
      FROM DATA_QUALITY_DB.DQ_METRICS.DQ_CHECK_RESULTS cr
      WHERE ${crWhereSql}
      GROUP BY 1, 2, 3, 4, 7
      ORDER BY SEVERITY_RANK DESC, FAILED_RECORDS DESC, LAST_FAILURE_TS DESC
    `;

    const crSummaryRows = (await executeQueryObjects(connection, crSummaryQuery, crBinds)) as Array<{
      FAILED_CHECKS: number | null;
      FAILED_RECORDS: number | null;
      LAST_FAILURE_TS: string | null;
    }>;
    const crCriticalRows = (await executeQueryObjects(connection, crMostCriticalQuery, crBinds)) as MostCriticalRow[];
    const crRuleRows = (await executeQueryObjects(connection, crRulesQuery, crBinds)) as FailureRuleRow[];

    const crSummary = crSummaryRows[0] || { FAILED_CHECKS: 0, FAILED_RECORDS: 0, LAST_FAILURE_TS: null };
    const crMostCritical = crCriticalRows[0];

    return NextResponse.json({
      success: true,
      data: {
        source: 'check_results',
        summary: {
          failed_checks: Number(crSummary.FAILED_CHECKS || 0),
          failed_records: Number(crSummary.FAILED_RECORDS || 0),
          most_critical_rule: crMostCritical
            ? `${crMostCritical.RULE_NAME || 'UNKNOWN'}${crMostCritical.COLUMN_NAME ? ` - ${crMostCritical.COLUMN_NAME}` : ''}`
            : null,
          last_failure_ts: crSummary.LAST_FAILURE_TS || null,
        },
        rules: crRuleRows.map((row) => ({
          rule_id: row.RULE_ID,
          rule_name: row.RULE_NAME,
          rule_type: row.RULE_TYPE,
          column_name: row.COLUMN_NAME,
          threshold: row.THRESHOLD,
          pass_rate: row.PASS_RATE,
          severity: row.SEVERITY,
          failed_records: Number(row.FAILED_RECORDS || 0),
          last_failure_ts: row.LAST_FAILURE_TS,
        })),
      },
    });
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : 'Failed to fetch failure summary';
    const status = message.toLowerCase().includes('required') || message.toLowerCase().includes('invalid') ? 400 : 500;
    return NextResponse.json({ success: false, error: message }, { status });
  }
}
