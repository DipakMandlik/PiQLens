/**
 * Report Builder
 * 
 * Purpose: Builds structured report payloads from aggregated data
 * 
 * Responsibilities:
 * - Define report structure for each type
 * - Validate data completeness
 * - Add metadata and formatting
 * - Prepare data for renderers
 */

import { format } from 'date-fns';
import type {
    PlatformReportData,
    DatasetReportData,
    IncidentReportData,
} from './data-aggregator';

// =====================================================
// Types
// =====================================================

export interface ReportMetadata {
    reportId: string;
    reportType: 'PLATFORM' | 'DATASET' | 'INCIDENT';
    reportDate: string;
    generatedAt: string;
    generatedBy: string;
    version: string;
}

export interface PlatformReport {
    metadata: ReportMetadata;
    executiveSummary: {
        overallQualityScore: number;
        qualityStatus: string;
        riskLevel: string;
        keyHighlights: string[];
    };
    qualityMetrics: {
        score: number;
        status: string;
        trend: string; // TODO: Implement trend calculation
    };
    coverage: {
        totalChecks: number;
        passedChecks: number;
        failedChecks: number;
        coveragePercent: number;
        strength: string;
    };
    riskAssessment: {
        level: string;
        openAnomalies: number;
        slaBreaches: number;
        criticalDatasets: number;
    };
    topFailingDatasets: Array<{
        rank: number;
        dataset: string;
        failedChecks: number;
        qualityScore: number;
    }>;
    slaCompliance: {
        compliancePercent: number;
        breachedDatasets: number;
        status: string;
    };
    observability: {
        totalAssets: number;
        monitoredAssets: number;
        monitoringCoverage: number;
        lastScanTime: string;
    };
    recommendations: string[];
}

export interface DatasetReport {
    metadata: ReportMetadata;
    datasetInfo: {
        database: string;
        schema: string;
        table: string;
        fullName: string;
    };
    qualityOverview: {
        score: number;
        status: string;
        trend: string;
    };
    checksAnalysis: {
        total: number;
        passed: number;
        failed: number;
        passRate: number;
        summary: string;
    };
    failedChecksDetails: Array<{
        ruleName: string;
        status: string;
        failedRecords: number;
        timestamp: string;
        severity: string;
    }>;
    anomalies: {
        detected: boolean;
        count: number;
        details: any[];
    };
    dataFreshness: {
        lastUpdated: string;
        freshnessHours: number;
        status: string;
    };
    volumeMetrics: {
        rowCount: number;
        trend: string;
    };
    recommendations: string[];
}

export interface IncidentReport {
    metadata: ReportMetadata;
    incidentOverview: {
        incidentId: string;
        triggerReason: string;
        severity: string;
        status: string;
    };
    impact: {
        impactedDatasets: Array<{
            dataset: string;
            impact: string;
        }>;
        totalImpact: string;
    };
    timeline: Array<{
        timestamp: string;
        event: string;
        details: string;
    }>;
    rootCauseAnalysis: {
        hints: string[];
        confidence: string;
    };
    resolution: {
        status: string;
        actionItems: string[];
    };
}

// =====================================================
// Platform Report Builder
// =====================================================

export function buildPlatformReport(
    data: PlatformReportData,
    reportId: string
): PlatformReport {
    // Generate key highlights
    const keyHighlights: string[] = [];

    if (data.overallQualityScore >= 90) {
        keyHighlights.push(`Excellent overall quality score of ${data.overallQualityScore}%`);
    } else if (data.overallQualityScore < 60) {
        keyHighlights.push(`Quality score needs attention: ${data.overallQualityScore}%`);
    }

    if (data.coverage.failedChecks > 0) {
        keyHighlights.push(`${data.coverage.failedChecks} checks failed across datasets`);
    } else {
        keyHighlights.push('All data quality checks passed');
    }

    if (data.risk.level === 'High') {
        keyHighlights.push(`High risk detected: ${data.risk.openAnomalies} anomalies`);
    }

    if (data.slaCompliance.compliancePercent < 100) {
        keyHighlights.push(`SLA compliance at ${data.slaCompliance.compliancePercent}%`);
    }

    // Generate recommendations
    const recommendations: string[] = [];

    if (data.coverage.failedChecks > 0) {
        recommendations.push('Review and remediate failed data quality checks');
    }

    if (data.risk.level === 'High') {
        recommendations.push('Investigate critical anomalies immediately');
    }

    if (data.slaCompliance.compliancePercent < 95) {
        recommendations.push('Address SLA breaches to improve compliance');
    }

    if (data.coverage.coveragePercent < 80) {
        recommendations.push('Increase data quality check coverage');
    }

    // Determine SLA status
    let slaStatus = 'Compliant';
    if (data.slaCompliance.compliancePercent < 90) slaStatus = 'At Risk';
    if (data.slaCompliance.compliancePercent < 80) slaStatus = 'Non-Compliant';

    // Calculate monitoring coverage
    const monitoringCoverage = data.observability.totalAssets > 0
        ? Math.round((data.observability.monitoredAssets / data.observability.totalAssets) * 100)
        : 0;

    // Build report
    const report: PlatformReport = {
        metadata: {
            reportId,
            reportType: 'PLATFORM',
            reportDate: data.reportDate,
            generatedAt: data.metadata.generatedAt,
            generatedBy: data.metadata.generatedBy,
            version: '1.0',
        },
        executiveSummary: {
            overallQualityScore: data.overallQualityScore,
            qualityStatus: data.qualityStatus,
            riskLevel: data.risk.level,
            keyHighlights,
        },
        qualityMetrics: {
            score: data.overallQualityScore,
            status: data.qualityStatus,
            trend: 'Stable', // TODO: Calculate from historical data
        },
        coverage: {
            totalChecks: data.coverage.totalChecks,
            passedChecks: data.coverage.passedChecks,
            failedChecks: data.coverage.failedChecks,
            coveragePercent: data.coverage.coveragePercent,
            strength: data.coverage.coverageStrength,
        },
        riskAssessment: {
            level: data.risk.level,
            openAnomalies: data.risk.openAnomalies,
            slaBreaches: data.risk.slaBreaches,
            criticalDatasets: data.risk.criticalDatasets,
        },
        topFailingDatasets: data.topFailingDatasets.map((dataset, index) => ({
            rank: index + 1,
            ...dataset,
        })),
        slaCompliance: {
            compliancePercent: data.slaCompliance.compliancePercent,
            breachedDatasets: data.slaCompliance.breachedDatasets,
            status: slaStatus,
        },
        observability: {
            totalAssets: data.observability.totalAssets,
            monitoredAssets: data.observability.monitoredAssets,
            monitoringCoverage,
            lastScanTime: data.observability.lastScanTime,
        },
        recommendations,
    };

    return report;
}

// =====================================================
// Dataset Report Builder
// =====================================================

export function buildDatasetReport(
    data: DatasetReportData,
    reportId: string
): DatasetReport {
    // Generate checks summary
    let checksSummary = '';
    if (data.checks.failed === 0) {
        checksSummary = 'All data quality checks passed successfully';
    } else {
        checksSummary = `${data.checks.failed} out of ${data.checks.total} checks failed (${100 - data.checks.passRate}% failure rate)`;
    }

    // Enrich failed checks with severity
    const failedChecksDetails = data.failedChecks.map(check => ({
        ruleName: check.ruleName,
        status: check.checkStatus,
        failedRecords: check.failedRecords,
        timestamp: check.checkTimestamp,
        severity: determineSeverity(check.failedRecords),
    }));

    // Generate recommendations
    const recommendations: string[] = [];

    if (data.checks.failed > 0) {
        recommendations.push(`Address ${data.checks.failed} failed quality checks`);
    }

    if (data.anomalies.detected) {
        recommendations.push('Investigate detected anomalies');
    }

    if (data.freshness.status === 'Stale') {
        recommendations.push('Update dataset - data is stale');
    }

    if (data.qualityScore < 70) {
        recommendations.push('Implement data quality improvements');
    }

    // Build report
    const report: DatasetReport = {
        metadata: {
            reportId,
            reportType: 'DATASET',
            reportDate: data.metadata.generatedAt.split('T')[0],
            generatedAt: data.metadata.generatedAt,
            generatedBy: data.metadata.generatedBy,
            version: '1.0',
        },
        datasetInfo: {
            database: data.dataset.database,
            schema: data.dataset.schema,
            table: data.dataset.table,
            fullName: data.dataset.fullName,
        },
        qualityOverview: {
            score: data.qualityScore,
            status: data.qualityStatus,
            trend: 'Stable', // TODO: Calculate from historical data
        },
        checksAnalysis: {
            total: data.checks.total,
            passed: data.checks.passed,
            failed: data.checks.failed,
            passRate: data.checks.passRate,
            summary: checksSummary,
        },
        failedChecksDetails,
        anomalies: data.anomalies,
        dataFreshness: data.freshness,
        volumeMetrics: data.volume,
        recommendations,
    };

    return report;
}

// =====================================================
// Incident Report Builder
// =====================================================

export function buildIncidentReport(
    data: IncidentReportData,
    reportId: string
): IncidentReport {
    // Calculate total impact
    const totalImpact = data.impactedDatasets.length > 10 ? 'High' :
        data.impactedDatasets.length > 5 ? 'Medium' : 'Low';

    // Generate action items
    const actionItems: string[] = [
        'Review incident timeline',
        'Validate root cause analysis',
        'Implement preventive measures',
    ];

    if (data.resolutionStatus === 'Open') {
        actionItems.push('Assign owner for resolution');
    }

    // Build report
    const report: IncidentReport = {
        metadata: {
            reportId,
            reportType: 'INCIDENT',
            reportDate: new Date().toISOString().split('T')[0],
            generatedAt: data.metadata.generatedAt,
            generatedBy: data.metadata.generatedBy,
            version: '1.0',
        },
        incidentOverview: {
            incidentId: data.incidentId,
            triggerReason: data.triggerReason,
            severity: data.severity,
            status: data.resolutionStatus,
        },
        impact: {
            impactedDatasets: data.impactedDatasets,
            totalImpact,
        },
        timeline: data.timeline,
        rootCauseAnalysis: {
            hints: data.rootCauseHints,
            confidence: 'Medium', // TODO: Implement AI confidence scoring
        },
        resolution: {
            status: data.resolutionStatus,
            actionItems,
        },
    };

    return report;
}

// =====================================================
// Helper Functions
// =====================================================

function determineSeverity(failedRecords: number): string {
    if (failedRecords > 1000) return 'Critical';
    if (failedRecords > 100) return 'High';
    if (failedRecords > 10) return 'Medium';
    return 'Low';
}

// =====================================================
// Validation
// =====================================================

export function validateReportData(
    reportType: 'PLATFORM' | 'DATASET' | 'INCIDENT',
    data: any
): { valid: boolean; errors: string[] } {
    const errors: string[] = [];

    if (reportType === 'PLATFORM') {
        if (!data.overallQualityScore && data.overallQualityScore !== 0) {
            errors.push('Missing overall quality score');
        }
        if (!data.coverage) {
            errors.push('Missing coverage data');
        }
    }

    if (reportType === 'DATASET') {
        if (!data.dataset || !data.dataset.fullName) {
            errors.push('Missing dataset information');
        }
        if (!data.checks) {
            errors.push('Missing checks data');
        }
    }

    if (reportType === 'INCIDENT') {
        if (!data.incidentId) {
            errors.push('Missing incident ID');
        }
    }

    return {
        valid: errors.length === 0,
        errors,
    };
}
