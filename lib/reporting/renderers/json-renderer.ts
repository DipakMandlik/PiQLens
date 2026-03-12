/**
 * JSON Renderer
 * 
 * Purpose: Exports reports as structured JSON
 * Use case: API consumption, automation, machine-readable format
 */

import type { PlatformReport, DatasetReport, IncidentReport } from '../report-builder';

// =====================================================
// JSON Renderer
// =====================================================

export function renderJSON(
    report: PlatformReport | DatasetReport | IncidentReport
): string {
    try {
        // Pretty-print JSON with 2-space indentation
        return JSON.stringify(report, null, 2);
    } catch (error) {
        console.error('[JSON Renderer] Error rendering report:', error);
        throw new Error('Failed to render JSON report');
    }
}

// =====================================================
// JSON with Metadata
// =====================================================

export function renderJSONWithMetadata(
    report: PlatformReport | DatasetReport | IncidentReport,
    additionalMetadata?: Record<string, any>
): string {
    try {
        const output = {
            ...report,
            _metadata: {
                ...report.metadata,
                ...additionalMetadata,
                exportFormat: 'JSON',
                exportedAt: new Date().toISOString(),
            },
        };

        return JSON.stringify(output, null, 2);
    } catch (error) {
        console.error('[JSON Renderer] Error rendering report with metadata:', error);
        throw new Error('Failed to render JSON report with metadata');
    }
}

// =====================================================
// Minified JSON (for API responses)
// =====================================================

export function renderJSONMinified(
    report: PlatformReport | DatasetReport | IncidentReport
): string {
    try {
        return JSON.stringify(report);
    } catch (error) {
        console.error('[JSON Renderer] Error rendering minified report:', error);
        throw new Error('Failed to render minified JSON report');
    }
}

// =====================================================
// JSON Schema (for validation)
// =====================================================

export function getJSONSchema(reportType: 'PLATFORM' | 'DATASET' | 'INCIDENT'): object {
    const baseSchema = {
        $schema: 'http://json-schema.org/draft-07/schema#',
        type: 'object',
        required: ['metadata'],
        properties: {
            metadata: {
                type: 'object',
                required: ['reportId', 'reportType', 'reportDate', 'generatedAt'],
                properties: {
                    reportId: { type: 'string' },
                    reportType: { type: 'string', enum: ['PLATFORM', 'DATASET', 'INCIDENT'] },
                    reportDate: { type: 'string', format: 'date' },
                    generatedAt: { type: 'string', format: 'date-time' },
                    generatedBy: { type: 'string' },
                    version: { type: 'string' },
                },
            },
        },
    };

    // Extend based on report type
    if (reportType === 'PLATFORM') {
        return {
            ...baseSchema,
            required: [...baseSchema.required, 'executiveSummary', 'qualityMetrics', 'coverage'],
        };
    }

    if (reportType === 'DATASET') {
        return {
            ...baseSchema,
            required: [...baseSchema.required, 'datasetInfo', 'qualityOverview', 'checksAnalysis'],
        };
    }

    if (reportType === 'INCIDENT') {
        return {
            ...baseSchema,
            required: [...baseSchema.required, 'incidentOverview', 'impact', 'timeline'],
        };
    }

    return baseSchema;
}
