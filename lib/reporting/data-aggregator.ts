/**
 * Report Data Aggregator
 * 
 * Purpose: Aggregates data for report generation using direct database queries
 * 
 * DESIGN CHANGE:
 * - Originally used internal API calls via fetch()
 * - Changed to direct DB queries to avoid server-side fetch limitations (Next.js API routes)
 * - Queries strictly follow the logic used in those APIs to ensure consistency
 * 
 * SCHEMA AWARENESS:
 * - Source tables (DQ_DAILY_SUMMARY, DQ_CHECK_RESULTS) are in DATA_QUALITY_DB.DQ_METRICS
 * - Reports table (DQ_REPORTS) is in DATA_QUALITY_DB.DB_METRICS (per creation script)
 * - Column Mappings:
 *   - Quality Score -> DQ_SCORE (DQ_DAILY_SUMMARY)
 *   - Failed Records Count -> INVALID_RECORDS (DQ_CHECK_RESULTS)
 * 
 * UPDATES:
 * - Added fallback logic to fetch Latest Date if data for requested date is missing.
 * - Added clamping for scores and coverage to prevent negative values.
 */

import { format } from 'date-fns';
import { snowflakePool, executeQueryObjects } from '@/lib/snowflake';
import { getServerConfig } from '@/lib/server-config';

//Types ... (Same as before)
export interface PlatformReportData {
    reportDate: string;
    overallQualityScore: number;
    qualityStatus: string;
    coverage: {
        totalChecks: number;
        passedChecks: number;
        failedChecks: number;
        coveragePercent: number;
        coverageStrength: string;
    };
    risk: {
        level: string;
        openAnomalies: number;
        slaBreaches: number;
        criticalDatasets: number;
    };
    topFailingDatasets: Array<{
        dataset: string;
        failedChecks: number;
        qualityScore: number;
    }>;
    slaCompliance: {
        compliancePercent: number;
        breachedDatasets: number;
    };
    observability: {
        totalAssets: number;
        monitoredAssets: number;
        lastScanTime: string;
    };
    metadata: {
        generatedAt: string;
        generatedBy: string;
    };
}

export interface DatasetReportData {
    dataset: {
        database: string;
        schema: string;
        table: string;
        fullName: string;
    };
    qualityScore: number;
    qualityStatus: string;
    checks: {
        total: number;
        passed: number;
        failed: number;
        passRate: number;
    };
    failedChecks: Array<{
        ruleName: string;
        checkStatus: string;
        failedRecords: number;
        checkTimestamp: string;
    }>;
    anomalies: {
        detected: boolean;
        count: number;
        details: any[];
    };
    freshness: {
        lastUpdated: string;
        freshnessHours: number;
        status: string;
    };
    volume: {
        rowCount: number;
        trend: string;
    };
    lastScanTime: string;
    metadata: {
        generatedAt: string;
        generatedBy: string;
    };
}

export interface IncidentReportData {
    incidentId: string;
    triggerReason: string;
    severity: string;
    impactedDatasets: Array<{
        dataset: string;
        impact: string;
    }>;
    timeline: Array<{
        timestamp: string;
        event: string;
        details: string;
    }>;
    rootCauseHints: string[];
    resolutionStatus: string;
    metadata: {
        generatedAt: string;
        generatedBy: string;
    };
}

// Helper to resolve effective date (Latest or Requested)
async function resolveEffectiveDate(connection: any, requestedDate: string): Promise<string> {
    // 1. Check if ANY data exists for requested date in Summary
    const checkSql = `
        SELECT COUNT(*) as CNT 
        FROM DATA_QUALITY_DB.DQ_METRICS.DQ_DAILY_SUMMARY 
        WHERE SUMMARY_DATE = ?
    `;
    const checkRows = await executeQueryObjects(connection, checkSql, [requestedDate]);
    const count = checkRows[0]?.CNT || 0;

    if (count > 0) {
        return requestedDate;
    }

    // 2. If no data, and requested date is Today (or close enough), try to find MAX date
    // We'll just find MAX date regardless, if requested date has no data.
    // This assumes the user wants "Latest State" if their specific date is empty.
    const maxSql = `
        SELECT MAX(SUMMARY_DATE) as LATEST_DATE
        FROM DATA_QUALITY_DB.DQ_METRICS.DQ_DAILY_SUMMARY
    `;
    const maxRows = await executeQueryObjects(connection, maxSql);
    const latest = maxRows[0]?.LATEST_DATE;

    if (latest) {
        // Return latest date (YYYY-MM-DD)
        // Snowflake date might be returned as Date object or string. ensure string.
        const d = new Date(latest);
        return format(d, 'yyyy-MM-dd');
    }

    return requestedDate; // Fallback to original if DB is empty
}

// =====================================================
// Platform Report Data Aggregation
// =====================================================

export async function aggregatePlatformReportData(
    date: string,
    generatedBy: string = 'system'
): Promise<PlatformReportData> {
    const config = getServerConfig();
    if (!config) {
        throw new Error('Database configuration not available');
    }

    const connection = await snowflakePool.getConnection(config);

    // Resolve Date
    const effectiveDate = await resolveEffectiveDate(connection, date);

    // Note: Use effectiveDate for queries
    const reportDate = effectiveDate;

    try {
        // 1. Overall Quality Score from DQ_METRICS (DQ_SCORE)
        const scoreRows = await executeQueryObjects(connection, `
            SELECT AVG(DQ_SCORE) as OVERALL_SCORE 
            FROM DATA_QUALITY_DB.DQ_METRICS.DQ_DAILY_SUMMARY 
            WHERE SUMMARY_DATE = ?
        `, [reportDate]);

        let qualityScore = 0;
        let qualityStatus = 'Unknown';

        if (scoreRows.length > 0 && scoreRows[0].OVERALL_SCORE !== null) {
            let score = scoreRows[0].OVERALL_SCORE;
            // Normalize score logic:
            if (score <= 1 && score > -1) {
                score *= 100;
            }
            qualityScore = Math.round(Math.max(0, Math.min(100, score)));

            if (qualityScore >= 90) qualityStatus = 'Excellent';
            else if (qualityScore >= 75) qualityStatus = 'Good';
            else if (qualityScore >= 60) qualityStatus = 'Fair';
            else qualityStatus = 'Poor';
        }

        // 2. Total Checks from DQ_METRICS
        // Use CHECK_TIMESTAMP date matching the summary date
        const checksRows = await executeQueryObjects(connection, `
            SELECT 
                COUNT(*) as TOTAL_CHECKS, 
                MAX(CHECK_TIMESTAMP) as LAST_EXECUTION 
            FROM DATA_QUALITY_DB.DQ_METRICS.DQ_CHECK_RESULTS 
            WHERE DATE(CHECK_TIMESTAMP) = ?
        `, [reportDate]);

        console.log(`[PlatformReport] Date: ${reportDate}, TotalChecks Raw: ${JSON.stringify(checksRows)}`);

        const totalChecks = checksRows[0]?.TOTAL_CHECKS || 0;
        const lastExecution = checksRows[0]?.LAST_EXECUTION;

        // 3. Failed Checks from DQ_METRICS
        const failedRows = await executeQueryObjects(connection, `
            SELECT COUNT(*) as FAILED_CHECKS 
            FROM DATA_QUALITY_DB.DQ_METRICS.DQ_CHECK_RESULTS 
            WHERE DATE(CHECK_TIMESTAMP) = ? AND CHECK_STATUS = 'FAILED'
        `, [reportDate]);

        const failedChecks = failedRows[0]?.FAILED_CHECKS || 0;
        // Clamp passedChecks to 0 minimum
        const passedChecks = Math.max(0, totalChecks - failedChecks);

        const coveragePercent = totalChecks > 0 ? Math.round((passedChecks / totalChecks) * 100) : 0;

        let coverageStrength = 'Weak';
        if (coveragePercent >= 90) coverageStrength = 'Strong';
        else if (coveragePercent >= 75) coverageStrength = 'Moderate';

        // 4. Critical Records & Risk (INVALID_RECORDS)
        const criticalRows = await executeQueryObjects(connection, `
            SELECT SUM(INVALID_RECORDS) as CRITICAL_RECORDS 
            FROM DATA_QUALITY_DB.DQ_METRICS.DQ_CHECK_RESULTS 
            WHERE DATE(CHECK_TIMESTAMP) = ? AND CHECK_STATUS = 'FAILED' AND SEVERITY = 'CRITICAL'
        `, [reportDate]);

        const criticalRecords = criticalRows[0]?.CRITICAL_RECORDS || 0;

        const criticalDatasetsRows = await executeQueryObjects(connection, `
            SELECT COUNT(DISTINCT TABLE_NAME) as CRITICAL_DATASETS
            FROM DATA_QUALITY_DB.DQ_METRICS.DQ_CHECK_RESULTS
            WHERE DATE(CHECK_TIMESTAMP) = ? AND SEVERITY = 'CRITICAL'
        `, [reportDate]);

        const criticalDatasets = criticalDatasetsRows[0]?.CRITICAL_DATASETS || 0;

        // 5. Top Failing Datasets
        const topFailingRows = await executeQueryObjects(connection, `
            SELECT DATABASE_NAME, SCHEMA_NAME, TABLE_NAME, COUNT(*) as FAILED_COUNT
            FROM DATA_QUALITY_DB.DQ_METRICS.DQ_CHECK_RESULTS
            WHERE DATE(CHECK_TIMESTAMP) = ? AND CHECK_STATUS = 'FAILED'
            GROUP BY DATABASE_NAME, SCHEMA_NAME, TABLE_NAME
            ORDER BY FAILED_COUNT DESC
            LIMIT 5
        `, [reportDate]);

        const topFailingDatasets = topFailingRows.map((row: any) => {
            const db = row.DATABASE_NAME;
            const sch = row.SCHEMA_NAME;
            const tbl = row.TABLE_NAME;
            const datasetName = (db && sch && tbl) ? `${db}.${sch}.${tbl}` : (tbl || 'Unknown');

            return {
                dataset: datasetName,
                failedChecks: row.FAILED_COUNT,
                qualityScore: 0 // Placeholder
            };
        });

        // 6. SLA Compliance
        let slaCompliancePct = 100;
        let slaBreaches = 0;

        try {
            const slaRows = await executeQueryObjects(connection, `
                SELECT 
                    SUM(CASE WHEN IS_SLA_MET = TRUE THEN 1 ELSE 0 END) as SLA_MET_COUNT,
                    COUNT(*) as TOTAL_DATASETS
                FROM DATA_QUALITY_DB.DQ_METRICS.DQ_DAILY_SUMMARY
                WHERE SUMMARY_DATE = ?
            `, [reportDate]);

            if (slaRows.length > 0 && slaRows[0].TOTAL_DATASETS > 0) {
                const met = slaRows[0].SLA_MET_COUNT || 0;
                const total = slaRows[0].TOTAL_DATASETS || 0;
                slaCompliancePct = Math.round((met / total) * 100);
                slaBreaches = total - met;
            }
        } catch (e) {
            slaBreaches = criticalDatasets;
            slaCompliancePct = 100;
        }

        let riskLevel = 'Low';
        if (criticalRecords > 1000 || slaBreaches > 5) riskLevel = 'High';
        else if (criticalRecords > 100 || slaBreaches > 1) riskLevel = 'Medium';

        // 7. Last Scan Time
        const lastScanTime = lastExecution
            ? format(new Date(lastExecution), 'yyyy-MM-dd HH:mm:ss')
            : 'Not available';

        // Build Report
        const reportData: PlatformReportData = {
            reportDate: reportDate, // Return effective date
            overallQualityScore: qualityScore,
            qualityStatus,
            coverage: {
                totalChecks,
                passedChecks,
                failedChecks,
                coveragePercent,
                coverageStrength,
            },
            risk: {
                level: riskLevel,
                openAnomalies: criticalRecords,
                slaBreaches,
                criticalDatasets,
            },
            topFailingDatasets,
            slaCompliance: {
                compliancePercent: slaCompliancePct,
                breachedDatasets: slaBreaches,
            },
            observability: {
                totalAssets: 0,
                monitoredAssets: 0,
                lastScanTime,
            },
            metadata: {
                generatedAt: new Date().toISOString(),
                generatedBy,
            },
        };

        return reportData;

    } catch (error) {
        console.error('[Report Aggregator] Error aggregating platform data:', error);
        throw new Error('Failed to aggregate platform report data. Check database schema/connection.');
    }
}

// =====================================================
// Dataset Report Data Aggregation
// =====================================================

export async function aggregateDatasetReportData(
    database: string,
    schema: string,
    table: string,
    date: string,
    generatedBy: string = 'system'
): Promise<DatasetReportData> {
    const config = getServerConfig();
    if (!config) {
        throw new Error('Database configuration not available');
    }

    const connection = await snowflakePool.getConnection(config);

    // Resolve Effective Date
    const effectiveDate = await resolveEffectiveDate(connection, date);
    const reportDate = effectiveDate;

    const datasetId = `${database}.${schema}.${table}`;

    try {
        // 1. Quality Score
        const scoreRows = await executeQueryObjects(connection, `
            SELECT AVG(DQ_SCORE) as QUALITY_SCORE 
            FROM DATA_QUALITY_DB.DQ_METRICS.DQ_DAILY_SUMMARY 
            WHERE SUMMARY_DATE = ? 
            AND DATABASE_NAME = ? AND SCHEMA_NAME = ? AND TABLE_NAME = ?
        `, [reportDate, database, schema, table]);

        let qualityScore = 0;
        let qualityStatus = 'Unknown';

        if (scoreRows.length > 0 && scoreRows[0].QUALITY_SCORE !== null) {
            let score = scoreRows[0].QUALITY_SCORE;
            if (score <= 1 && score > -1) {
                score *= 100;
            }
            qualityScore = Math.round(Math.max(0, Math.min(100, score)));

            if (qualityScore >= 90) qualityStatus = 'Excellent';
            else if (qualityScore >= 75) qualityStatus = 'Good';
            else if (qualityScore >= 60) qualityStatus = 'Fair';
            else qualityStatus = 'Poor';
        }

        // 2. Checks
        const checksRows = await executeQueryObjects(connection, `
            SELECT 
                COUNT(*) as TOTAL_CHECKS, 
                SUM(CASE WHEN CHECK_STATUS = 'FAILED' THEN 1 ELSE 0 END) as FAILED_CHECKS
            FROM DATA_QUALITY_DB.DQ_METRICS.DQ_CHECK_RESULTS 
            WHERE DATE(CHECK_TIMESTAMP) = ? 
            AND DATABASE_NAME = ? AND SCHEMA_NAME = ? AND TABLE_NAME = ?
        `, [reportDate, database, schema, table]);

        const totalChecks = checksRows[0]?.TOTAL_CHECKS || 0;
        const failedChecksCount = checksRows[0]?.FAILED_CHECKS || 0;
        const passedChecks = Math.max(0, totalChecks - failedChecksCount);
        const passRate = totalChecks > 0 ? Math.round((passedChecks / totalChecks) * 100) : 0;

        // 3. Failed Checks Details
        const failedDetailsRows = await executeQueryObjects(connection, `
            SELECT RULE_NAME, CHECK_STATUS, INVALID_RECORDS, CHECK_TIMESTAMP, SEVERITY
            FROM DATA_QUALITY_DB.DQ_METRICS.DQ_CHECK_RESULTS 
            WHERE DATE(CHECK_TIMESTAMP) = ? 
            AND CHECK_STATUS = 'FAILED'
            AND DATABASE_NAME = ? AND SCHEMA_NAME = ? AND TABLE_NAME = ?
            ORDER BY INVALID_RECORDS DESC
            LIMIT 100
        `, [reportDate, database, schema, table]);

        const failedChecks = failedDetailsRows.map((row: any) => ({
            ruleName: row.RULE_NAME,
            checkStatus: row.CHECK_STATUS,
            failedRecords: row.INVALID_RECORDS || 0,
            checkTimestamp: row.CHECK_TIMESTAMP ? format(new Date(row.CHECK_TIMESTAMP), 'yyyy-MM-dd HH:mm:ss') : 'Unknown'
        }));

        // 4. Build Report Data
        const reportData: DatasetReportData = {
            dataset: {
                database,
                schema,
                table,
                fullName: datasetId,
            },
            qualityScore,
            qualityStatus,
            checks: {
                total: totalChecks,
                passed: passedChecks,
                failed: failedChecksCount,
                passRate,
            },
            failedChecks,
            anomalies: {
                detected: false,
                count: 0,
                details: [],
            },
            freshness: {
                lastUpdated: 'Unknown',
                freshnessHours: 0,
                status: 'Unknown',
            },
            volume: {
                rowCount: 0,
                trend: 'Stable',
            },
            lastScanTime: 'Unknown',
            metadata: {
                generatedAt: new Date().toISOString(), // This is generation time
                generatedBy,
            },
        };
        // NOTE: We could add 'effectiveDate' to metadata if needed, but 'lastScanTime' helps.

        return reportData;

    } catch (error) {
        console.error('[Report Aggregator] Error aggregating dataset data:', error);
        throw new Error(`Failed to aggregate dataset report data for ${datasetId}`);
    }
}

// ... aggregateIncidentReportData stays same ...
export async function aggregateIncidentReportData(
    incidentId: string,
    generatedBy: string = 'system'
): Promise<IncidentReportData> {
    // Placeholder matches previous
    return {
        incidentId,
        triggerReason: 'SLA Breach',
        severity: 'High',
        impactedDatasets: [],
        timeline: [
            {
                timestamp: new Date().toISOString(),
                event: 'Incident Detected',
                details: 'Automated detection triggered',
            },
        ],
        rootCauseHints: ['Data pipeline delay detected'],
        resolutionStatus: 'Open',
        metadata: {
            generatedAt: new Date().toISOString(),
            generatedBy,
        },
    };
}
