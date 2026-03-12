/* eslint-disable @typescript-eslint/no-explicit-any */
import { NextRequest, NextResponse } from 'next/server';
import { getServerConfig } from '@/lib/server-config';
import { snowflakePool } from '@/lib/snowflake';
import { logger } from '@/lib/logger';

type RiskLevel = 'HIGH' | 'MEDIUM' | 'LOW' | 'NONE';
type CheckStatus = 'PASSED' | 'WARNING' | 'FAILED' | 'ERROR' | 'NOT_EXECUTED';
type CheckMetricsSource = 'active_config' | 'execution_fallback' | 'none';

interface ActiveCheckMetric {
    rule_id: number | null;
    rule_name: string;
    rule_type: string;
    threshold: number | null;
    severity: 'CRITICAL' | 'MEDIUM' | 'LOW';
    severity_weight: 3 | 2 | 1;
    pass_rate: number | null;
    status: CheckStatus;
    invalid_records: number | null;
    total_records: number | null;
    last_executed_at: string | null;
    contributes_to_health: boolean;
    display_priority: number;
}

interface CheckMetrics {
    source: CheckMetricsSource;
    dataset_id: string | null;
    active_checks: ActiveCheckMetric[];
    executed_check_count: number;
    unexecuted_check_count: number;
    health_score: number | null;
    risk_level: RiskLevel;
    notices: string[];
}

interface SeverityConfig {
    severity: 'CRITICAL' | 'MEDIUM' | 'LOW';
    weight: 3 | 2 | 1;
}

function toNumber(value: any): number | null {
    const numeric = Number(value);
    return Number.isFinite(numeric) ? numeric : null;
}

function toUpperText(value: any): string {
    return String(value || '').toUpperCase();
}

function roundToOne(value: number): number {
    return Math.round(value * 10) / 10;
}

function normalizeStatus(value: any): CheckStatus {
    const normalized = toUpperText(value);
    if (normalized === 'PASS' || normalized === 'PASSED') return 'PASSED';
    if (normalized === 'FAIL' || normalized === 'FAILED') return 'FAILED';
    if (normalized === 'WARNING') return 'WARNING';
    if (normalized === 'ERROR') return 'ERROR';
    return 'NOT_EXECUTED';
}

function getSeverityConfig(ruleType: any): SeverityConfig {
    const normalized = toUpperText(ruleType);
    if (normalized === 'VALIDITY') return { severity: 'CRITICAL', weight: 3 };
    if (normalized === 'COMPLETENESS') return { severity: 'MEDIUM', weight: 2 };
    if (normalized === 'UNIQUENESS') return { severity: 'LOW', weight: 1 };
    return { severity: 'MEDIUM', weight: 2 };
}

function getDisplayPriority(ruleType: any): number {
    const normalized = toUpperText(ruleType);
    if (normalized === 'VALIDITY') return 1;
    if (normalized === 'COMPLETENESS') return 2;
    if (normalized === 'UNIQUENESS') return 3;
    return 4;
}

function derivePassRateFromResult(row: any): number | null {
    const passRate = toNumber(row?.PASS_RATE);
    if (passRate !== null) return Math.max(0, Math.min(100, passRate));

    const total = toNumber(row?.TOTAL_RECORDS);
    const valid = toNumber(row?.VALID_RECORDS);
    const invalid = toNumber(row?.INVALID_RECORDS);

    if (total !== null && total > 0 && valid !== null) {
        return roundToOne((valid / total) * 100);
    }
    if (total !== null && total > 0 && invalid !== null) {
        return roundToOne(((total - invalid) / total) * 100);
    }
    return null;
}

function calculateRiskLevel(checks: ActiveCheckMetric[]): RiskLevel {
    const executedChecks = checks.filter((check) => check.status !== 'NOT_EXECUTED');
    if (executedChecks.length === 0) return 'NONE';

    const hasCriticalFailure = executedChecks.some(
        (check) => check.severity === 'CRITICAL' && (check.status === 'FAILED' || check.status === 'ERROR')
    );
    if (hasCriticalFailure) return 'HIGH';

    const hasAnyIssue = executedChecks.some(
        (check) => check.status === 'FAILED' || check.status === 'WARNING' || check.status === 'ERROR'
    );
    if (hasAnyIssue) return 'MEDIUM';

    return 'LOW';
}

function calculateHealthScore(checks: ActiveCheckMetric[]): number | null {
    const contributingChecks = checks.filter(
        (check) => check.contributes_to_health && check.pass_rate !== null
    );

    if (contributingChecks.length === 0) return null;

    const weightedSum = contributingChecks.reduce(
        (sum, check) => sum + (check.pass_rate as number) * check.severity_weight,
        0
    );
    const totalWeight = contributingChecks.reduce((sum, check) => sum + check.severity_weight, 0);

    if (totalWeight <= 0) return null;
    return roundToOne(weightedSum / totalWeight);
}

function sortChecksByPriority(checks: ActiveCheckMetric[]): ActiveCheckMetric[] {
    return checks.sort((a, b) => {
        if (a.display_priority !== b.display_priority) {
            return a.display_priority - b.display_priority;
        }
        return a.rule_name.localeCompare(b.rule_name);
    });
}

function buildCheckMetricsFromActiveRules(
    datasetId: string,
    activeRuleRows: any[],
    latestResultRows: any[]
): CheckMetrics {
    const resultByRuleId = new Map<string, any>();
    const resultByRuleName = new Map<string, any>();

    for (const row of latestResultRows || []) {
        const ruleId = row?.RULE_ID;
        const ruleName = String(row?.RULE_NAME || '').toUpperCase();
        if (ruleId !== null && ruleId !== undefined) {
            resultByRuleId.set(String(ruleId), row);
        }
        if (ruleName) {
            resultByRuleName.set(ruleName, row);
        }
    }

    const activeChecks: ActiveCheckMetric[] = (activeRuleRows || []).map((ruleRow: any) => {
        const ruleId = ruleRow?.RULE_ID ?? null;
        const ruleName = String(ruleRow?.RULE_NAME || 'UNKNOWN_RULE');
        const ruleType = String(ruleRow?.RULE_TYPE || 'UNKNOWN');
        const thresholdConfig = toNumber(ruleRow?.THRESHOLD_VALUE);

        const matchedResult =
            (ruleId !== null && ruleId !== undefined ? resultByRuleId.get(String(ruleId)) : undefined) ||
            resultByRuleName.get(ruleName.toUpperCase());

        const status: CheckStatus = matchedResult ? normalizeStatus(matchedResult.CHECK_STATUS) : 'NOT_EXECUTED';
        const passRate = matchedResult ? derivePassRateFromResult(matchedResult) : null;
        const contributesToHealth = status !== 'NOT_EXECUTED' && passRate !== null;
        const severityConfig = getSeverityConfig(ruleType);

        return {
            rule_id: ruleId !== null && ruleId !== undefined ? Number(ruleId) : null,
            rule_name: ruleName,
            rule_type: ruleType,
            threshold: thresholdConfig !== null ? thresholdConfig : toNumber(matchedResult?.THRESHOLD),
            severity: severityConfig.severity,
            severity_weight: severityConfig.weight,
            pass_rate: passRate,
            status,
            invalid_records: toNumber(matchedResult?.INVALID_RECORDS),
            total_records: toNumber(matchedResult?.TOTAL_RECORDS),
            last_executed_at: matchedResult?.CHECK_TIMESTAMP ? new Date(matchedResult.CHECK_TIMESTAMP).toISOString() : null,
            contributes_to_health: contributesToHealth,
            display_priority: getDisplayPriority(ruleType)
        };
    });

    const sortedChecks = sortChecksByPriority(activeChecks);
    const executedCheckCount = sortedChecks.filter((check) => check.status !== 'NOT_EXECUTED').length;
    const unexecutedCheckCount = sortedChecks.filter((check) => check.status === 'NOT_EXECUTED').length;
    const healthScore = calculateHealthScore(sortedChecks);
    const riskLevel = calculateRiskLevel(sortedChecks);

    const notices: string[] = [];
    if (sortedChecks.length === 0) {
        notices.push('No active checks configured for this column.');
    } else if (unexecutedCheckCount > 0) {
        notices.push(`${unexecutedCheckCount} active check(s) are configured but not executed yet.`);
    }

    return {
        source: 'active_config',
        dataset_id: datasetId,
        active_checks: sortedChecks,
        executed_check_count: executedCheckCount,
        unexecuted_check_count: unexecutedCheckCount,
        health_score: healthScore,
        risk_level: riskLevel,
        notices
    };
}

function buildCheckMetricsFromExecutionFallback(latestResultRows: any[]): CheckMetrics {
    const fallbackChecks: ActiveCheckMetric[] = (latestResultRows || []).map((row: any) => {
        const ruleType = String(row?.RULE_TYPE || 'UNKNOWN');
        const severityConfig = getSeverityConfig(ruleType);
        const status = normalizeStatus(row?.CHECK_STATUS);
        const passRate = derivePassRateFromResult(row);
        const contributesToHealth = status !== 'NOT_EXECUTED' && passRate !== null;

        return {
            rule_id: row?.RULE_ID !== null && row?.RULE_ID !== undefined ? Number(row.RULE_ID) : null,
            rule_name: String(row?.RULE_NAME || 'UNKNOWN_RULE'),
            rule_type: ruleType,
            threshold: toNumber(row?.THRESHOLD),
            severity: severityConfig.severity,
            severity_weight: severityConfig.weight,
            pass_rate: passRate,
            status,
            invalid_records: toNumber(row?.INVALID_RECORDS),
            total_records: toNumber(row?.TOTAL_RECORDS),
            last_executed_at: row?.CHECK_TIMESTAMP ? new Date(row.CHECK_TIMESTAMP).toISOString() : null,
            contributes_to_health: contributesToHealth,
            display_priority: getDisplayPriority(ruleType)
        };
    });

    const sortedChecks = sortChecksByPriority(fallbackChecks);
    const executedCheckCount = sortedChecks.filter((check) => check.status !== 'NOT_EXECUTED').length;
    const unexecutedCheckCount = sortedChecks.filter((check) => check.status === 'NOT_EXECUTED').length;
    const healthScore = calculateHealthScore(sortedChecks);
    const riskLevel = calculateRiskLevel(sortedChecks);

    if (sortedChecks.length === 0) {
        return {
            source: 'none',
            dataset_id: null,
            active_checks: [],
            executed_check_count: 0,
            unexecuted_check_count: 0,
            health_score: null,
            risk_level: 'NONE',
            notices: ['No dataset mapping and no historical check executions found for this column.']
        };
    }

    return {
        source: 'execution_fallback',
        dataset_id: null,
        active_checks: sortedChecks,
        executed_check_count: executedCheckCount,
        unexecuted_check_count: unexecutedCheckCount,
        health_score: healthScore,
        risk_level: riskLevel,
        notices: ['Dataset mapping not found. Health is derived from latest executed checks only.']
    };
}

function buildLegacyDqSummary(checkMetrics: CheckMetrics) {
    const activeChecks = checkMetrics.active_checks || [];
    const passed = activeChecks.filter((check) => check.status === 'PASSED').length;
    const failed = activeChecks.filter((check) => check.status === 'FAILED' || check.status === 'ERROR').length;
    const warnings = activeChecks.filter((check) => check.status === 'WARNING').length;
    const criticalFailed = activeChecks.filter(
        (check) => check.severity === 'CRITICAL' && (check.status === 'FAILED' || check.status === 'ERROR')
    ).length;

    return {
        totalChecks: activeChecks.length,
        passed,
        failed,
        criticalFailed,
        warnings,
        score: checkMetrics.health_score !== null ? Math.round(checkMetrics.health_score) : null,
        riskLevel: checkMetrics.risk_level,
        source: checkMetrics.source === 'none' ? 'none' : 'checks',
        checkDetails: activeChecks.map((check) => ({
            name: check.rule_name,
            status: check.status,
            value: check.pass_rate !== null ? `${check.pass_rate.toFixed(1)}%` : 'Not Executed',
            threshold: check.threshold,
            timestamp: check.last_executed_at,
            severity: check.severity
        })),
        executedCheckCount: checkMetrics.executed_check_count,
        unexecutedCheckCount: checkMetrics.unexecuted_check_count,
        notices: checkMetrics.notices
    };
}

export async function GET(
    request: NextRequest,
    { params }: { params: Promise<{ table: string; column: string }> }
) {
    try {
        const { table, column } = await params;
        logger.debug(`Fetching details for ${table}.${column}`);
        const searchParams = request.nextUrl.searchParams;
        const database = searchParams.get('database');
        const schema = searchParams.get('schema');

        logger.debug(`Params: DB=${database}, Schema=${schema}`);

        if (!database || !schema) {
            return NextResponse.json(
                { success: false, error: 'Missing required parameters: database, schema' },
                { status: 400 }
            );
        }

        const dqDatabase = 'DATA_QUALITY_DB';
        const observabilitySchema = 'DQ_METRICS';
        const configSchema = 'DQ_CONFIG';

        const config = getServerConfig();
        const conn = await snowflakePool.getConnection(config || undefined);

        // 1. Get metadata from latest profiling run for this column.
        const metadataSql = `
        WITH LatestExecution AS (
            SELECT MAX(RUN_ID) as RUN_ID
            FROM ${dqDatabase}.${observabilitySchema}.DQ_COLUMN_PROFILE
            WHERE UPPER(TABLE_NAME) = ?
              AND UPPER(SCHEMA_NAME) = ?
              AND UPPER(DATABASE_NAME) = ?
         )
         SELECT
            COLUMN_NAME,
            DATA_TYPE,
            TOTAL_RECORDS as ROW_COUNT,
            NULL_COUNT,
            DISTINCT_COUNT,
            MIN_VALUE,
            MAX_VALUE,
            AVG_VALUE,
            STDDEV_VALUE,
            PROFILE_TS as LAST_UPDATED
         FROM ${dqDatabase}.${observabilitySchema}.DQ_COLUMN_PROFILE
         WHERE RUN_ID = (SELECT RUN_ID FROM LatestExecution)
           AND UPPER(TABLE_NAME) = ?
           AND UPPER(SCHEMA_NAME) = ?
           AND UPPER(DATABASE_NAME) = ?
           AND UPPER(COLUMN_NAME) = ?
        `;

        // 2. Get profile trend history for this column.
        const historySql = `
         SELECT
            PROFILE_TS as EXECUTION_TIMESTAMP,
            NULL_COUNT,
            DISTINCT_COUNT,
            TOTAL_RECORDS as ROW_COUNT
         FROM ${dqDatabase}.${observabilitySchema}.DQ_COLUMN_PROFILE
         WHERE UPPER(TABLE_NAME) = ?
           AND UPPER(SCHEMA_NAME) = ?
           AND UPPER(DATABASE_NAME) = ?
           AND UPPER(COLUMN_NAME) = ?
           AND PROFILE_TS >= DATEADD(day, -30, CURRENT_TIMESTAMP())
         ORDER BY PROFILE_TS ASC
        `;

        // 3. Latest check result per rule for this column.
        const latestCheckResultsSql = `
         SELECT
            RULE_ID,
            RULE_NAME,
            RULE_TYPE,
            CHECK_STATUS,
            PASS_RATE,
            VALID_RECORDS,
            INVALID_RECORDS,
            TOTAL_RECORDS,
            THRESHOLD,
            CHECK_TIMESTAMP
         FROM ${dqDatabase}.${observabilitySchema}.DQ_CHECK_RESULTS
         WHERE UPPER(TABLE_NAME) = ?
           AND UPPER(SCHEMA_NAME) = ?
           AND UPPER(DATABASE_NAME) = ?
           AND UPPER(COLUMN_NAME) = ?
         QUALIFY ROW_NUMBER() OVER (PARTITION BY COALESCE(RULE_ID, -1), UPPER(RULE_NAME) ORDER BY CHECK_TIMESTAMP DESC) = 1
         ORDER BY RULE_NAME
        `;

        // 4. Resolve dataset id from source table mapping.
        const datasetResolutionSql = `
         SELECT DATASET_ID
         FROM ${dqDatabase}.${configSchema}.DATASET_CONFIG
         WHERE UPPER(SOURCE_DATABASE) = ?
           AND UPPER(SOURCE_SCHEMA) = ?
           AND UPPER(SOURCE_TABLE) = ?
           AND IS_ACTIVE = TRUE
         LIMIT 1
        `;

        // 5. Active configured rules for this specific column.
        const activeRulesSql = `
         SELECT
            drc.RULE_ID,
            rm.RULE_NAME,
            rm.RULE_TYPE,
            drc.THRESHOLD_VALUE
         FROM ${dqDatabase}.${configSchema}.DATASET_RULE_CONFIG drc
         JOIN ${dqDatabase}.${configSchema}.RULE_MASTER rm
           ON drc.RULE_ID = rm.RULE_ID
         WHERE drc.DATASET_ID = ?
           AND UPPER(drc.COLUMN_NAME) = ?
           AND drc.IS_ACTIVE = TRUE
           AND rm.IS_ACTIVE = TRUE
         ORDER BY
           CASE UPPER(rm.RULE_TYPE)
             WHEN 'VALIDITY' THEN 1
             WHEN 'COMPLETENESS' THEN 2
             WHEN 'UNIQUENESS' THEN 3
             ELSE 4
           END,
           rm.RULE_NAME
        `;

        // 6. Governance from INFORMATION_SCHEMA.
        const governanceSql = `
         SELECT
            c.IS_NULLABLE,
            c.COLUMN_DEFAULT,
            CASE WHEN pk.COLUMN_NAME IS NOT NULL THEN TRUE ELSE FALSE END as IS_PRIMARY_KEY
         FROM ${database}.INFORMATION_SCHEMA.COLUMNS c
         LEFT JOIN (
            SELECT kcu.COLUMN_NAME
            FROM ${database}.INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
            JOIN ${database}.INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
              ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
              AND tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA
              AND tc.TABLE_NAME = kcu.TABLE_NAME
            WHERE tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
              AND UPPER(tc.TABLE_SCHEMA) = ?
              AND UPPER(tc.TABLE_NAME) = ?
         ) pk ON UPPER(c.COLUMN_NAME) = UPPER(pk.COLUMN_NAME)
         WHERE UPPER(c.TABLE_CATALOG) = ?
           AND UPPER(c.TABLE_SCHEMA) = ?
           AND UPPER(c.TABLE_NAME) = ?
           AND UPPER(c.COLUMN_NAME) = ?
        `;

        const executeQuery = (sqlText: string, binds: any[]): Promise<any[]> =>
            new Promise((resolve, reject) => {
                conn.execute({
                    sqlText,
                    binds,
                    complete: (err: any, _stmt: any, rows: any) => {
                        if (err) reject(err); else resolve(rows || []);
                    }
                });
            });

        const [metadataRows, historyRows, latestCheckResultRows, governanceRows, datasetRows] = await Promise.all([
            executeQuery(metadataSql, [
                table.toUpperCase(), schema.toUpperCase(), database.toUpperCase(),
                table.toUpperCase(), schema.toUpperCase(), database.toUpperCase(), column.toUpperCase()
            ]),
            executeQuery(historySql, [
                table.toUpperCase(), schema.toUpperCase(), database.toUpperCase(), column.toUpperCase()
            ]),
            executeQuery(latestCheckResultsSql, [
                table.toUpperCase(), schema.toUpperCase(), database.toUpperCase(), column.toUpperCase()
            ]).catch((e: any) => {
                logger.warn(`Latest check results query failed: ${e.message}`);
                return [];
            }),
            executeQuery(governanceSql, [
                schema.toUpperCase(), table.toUpperCase(),
                database.toUpperCase(), schema.toUpperCase(), table.toUpperCase(), column.toUpperCase()
            ]).catch((e: any) => {
                logger.warn(`Governance query failed: ${e.message}`);
                return [];
            }),
            executeQuery(datasetResolutionSql, [
                database.toUpperCase(), schema.toUpperCase(), table.toUpperCase()
            ]).catch((e: any) => {
                logger.warn(`Dataset resolution query failed: ${e.message}`);
                return [];
            })
        ]);

        const datasetId = datasetRows[0]?.DATASET_ID ? String(datasetRows[0].DATASET_ID) : null;

        let checkMetrics: CheckMetrics | null = null;

        if (datasetId) {
            try {
                const activeRuleRows = await executeQuery(activeRulesSql, [datasetId, column.toUpperCase()]);
                checkMetrics = buildCheckMetricsFromActiveRules(
                    datasetId,
                    activeRuleRows || [],
                    latestCheckResultRows || []
                );
            } catch (activeRuleError: any) {
                logger.warn(`Active rules query failed, using execution fallback: ${activeRuleError.message}`);
                const fallbackMetrics = buildCheckMetricsFromExecutionFallback(latestCheckResultRows || []);
                checkMetrics = {
                    ...fallbackMetrics,
                    notices: [
                        ...fallbackMetrics.notices,
                        'Failed to load active column rules from configuration. Falling back to execution history.'
                    ]
                };
            }
        } else {
            checkMetrics = buildCheckMetricsFromExecutionFallback(latestCheckResultRows || []);
        }

        if (!checkMetrics) {
            checkMetrics = buildCheckMetricsFromExecutionFallback(latestCheckResultRows || []);
        }

        const dqSummary = buildLegacyDqSummary(checkMetrics);

        const govRow = governanceRows[0] || {};
        const governance = {
            isPrimaryKey: govRow.IS_PRIMARY_KEY === true || govRow.IS_PRIMARY_KEY === 'true',
            isNullable: govRow.IS_NULLABLE === 'YES',
            columnDefault: govRow.COLUMN_DEFAULT || null
        };

        if (metadataRows.length === 0) {
            logger.debug('Column not found in DQ_COLUMN_PROFILE, trying INFORMATION_SCHEMA fallback');

            try {
                const fallbackSql = `
                    SELECT
                        COLUMN_NAME,
                        DATA_TYPE,
                        IS_NULLABLE
                    FROM ${database}.INFORMATION_SCHEMA.COLUMNS
                    WHERE UPPER(TABLE_NAME) = ?
                      AND UPPER(TABLE_SCHEMA) = ?
                      AND UPPER(COLUMN_NAME) = ?
                `;

                const fallbackRows = await executeQuery(fallbackSql, [
                    table.toUpperCase(), schema.toUpperCase(), column.toUpperCase()
                ]);

                if (fallbackRows.length === 0) {
                    return NextResponse.json({
                        success: false,
                        error: 'Column not found in database. Please verify the column name.'
                    }, { status: 404 });
                }

                const fallbackMeta = fallbackRows[0];
                logger.debug(`Found column in INFORMATION_SCHEMA: ${JSON.stringify(fallbackMeta)}`);

                return NextResponse.json({
                    success: true,
                    data: {
                        metadata: {
                            columnName: fallbackMeta.COLUMN_NAME,
                            dataType: fallbackMeta.DATA_TYPE,
                            isNullable: fallbackMeta.IS_NULLABLE === 'YES',
                            rowCount: null,
                            lastUpdated: null
                        },
                        currentStats: {
                            distinctCount: null,
                            nullCount: null,
                            rowCount: null,
                            min: null,
                            max: null,
                            avg: null,
                            stdDev: null
                        },
                        history: [],
                        profiling_metrics: {
                            current: null,
                            history: [],
                            informational_only: true
                        },
                        check_metrics: checkMetrics,
                        health_score: checkMetrics.health_score,
                        dqSummary,
                        governance: {
                            ...governance,
                            isNullable: fallbackMeta.IS_NULLABLE === 'YES'
                        },
                        needsProfiling: true
                    }
                });
            } catch (fallbackError: any) {
                logger.error('Fallback query failed', fallbackError);
                return NextResponse.json({
                    success: false,
                    error: 'Column not found. Please run profiling first.'
                }, { status: 404 });
            }
        }

        const meta = metadataRows[0];
        logger.debug(`Found metadata for ${column}`);

        const statsHistory = historyRows.map((row) => ({
            timestamp: row.EXECUTION_TIMESTAMP,
            nullCount: row.NULL_COUNT,
            distinctCount: row.DISTINCT_COUNT,
            rowCount: row.ROW_COUNT,
            nullPct: row.ROW_COUNT > 0 ? (row.NULL_COUNT / row.ROW_COUNT) * 100 : 0
        }));

        const rowCount = toNumber(meta.ROW_COUNT) || 0;
        const nullCount = toNumber(meta.NULL_COUNT) || 0;
        const distinctCount = toNumber(meta.DISTINCT_COUNT) || 0;
        const nonNullCount = Math.max(0, rowCount - nullCount);
        const completenessPct = rowCount > 0 ? roundToOne((nonNullCount / rowCount) * 100) : 100;
        const uniquenessPct = nonNullCount > 0 ? roundToOne((distinctCount / nonNullCount) * 100) : 100;

        const profilingMetrics = {
            current: {
                rowCount,
                nullCount,
                distinctCount,
                min: meta.MIN_VALUE,
                max: meta.MAX_VALUE,
                avg: toNumber(meta.AVG_VALUE),
                stdDev: toNumber(meta.STDDEV_VALUE),
                completeness_pct: completenessPct,
                uniqueness_pct: uniquenessPct
            },
            history: statsHistory.map((item) => {
                const historyRowCount = toNumber(item.rowCount) || 0;
                const historyNullCount = toNumber(item.nullCount) || 0;
                const historyDistinct = toNumber(item.distinctCount) || 0;
                const historyNonNull = Math.max(0, historyRowCount - historyNullCount);
                return {
                    ...item,
                    completeness_pct: historyRowCount > 0 ? roundToOne(((historyRowCount - historyNullCount) / historyRowCount) * 100) : 100,
                    uniqueness_pct: historyNonNull > 0 ? roundToOne((historyDistinct / historyNonNull) * 100) : 100
                };
            }),
            informational_only: true
        };

        return NextResponse.json({
            success: true,
            data: {
                metadata: {
                    columnName: meta.COLUMN_NAME,
                    dataType: meta.DATA_TYPE,
                    isNullable: governance.isNullable,
                    rowCount: meta.ROW_COUNT,
                    lastUpdated: meta.LAST_UPDATED
                },
                currentStats: {
                    distinctCount: meta.DISTINCT_COUNT,
                    nullCount: meta.NULL_COUNT,
                    rowCount: meta.ROW_COUNT,
                    min: meta.MIN_VALUE,
                    max: meta.MAX_VALUE,
                    avg: meta.AVG_VALUE,
                    stdDev: meta.STDDEV_VALUE
                },
                history: statsHistory,
                profiling_metrics: profilingMetrics,
                check_metrics: checkMetrics,
                health_score: checkMetrics.health_score,
                dqSummary,
                governance
            }
        });

    } catch (error: any) {
        logger.error('Column detail API error', error);
        return NextResponse.json(
            { success: false, error: error.message || 'Internal Server Error' },
            { status: 500 }
        );
    }
}



