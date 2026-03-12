/**
 * Insight Generator Utility
 * Generates automated business insights from scan run data
 */

import { ScanRun, DimensionDeltas, RunDelta } from './deltaCalculator';

export interface Insight {
    message: string;
    severity: 'success' | 'warning' | 'critical';
    icon: string;
}

/**
 * Format dimension name for display
 */
function formatDimensionName(dim: string): string {
    return dim.charAt(0).toUpperCase() + dim.slice(1);
}

/**
 * Find the dimension with the largest absolute change
 */
function findTopChangedDimension(dimensionDeltas: DimensionDeltas): {
    name: string;
    delta: number;
    absDelta: number;
} {
    const entries = Object.entries(dimensionDeltas)
        .map(([name, delta]) => ({
            name,
            delta,
            absDelta: Math.abs(delta)
        }))
        .sort((a, b) => b.absDelta - a.absDelta);

    return entries[0];
}

/**
 * Find dimensions below SLA threshold (90%)
 */
function findDimensionsBelowSLA(dimensions: ScanRun['dimensions']): Array<{
    name: string;
    score: number;
}> {
    return Object.entries(dimensions)
        .filter(([_, score]) => score < 90)
        .map(([name, score]) => ({ name, score }))
        .sort((a, b) => a.score - b.score);
}

/**
 * Generate insight for first run (no previous run to compare)
 */
export function generateFirstRunInsight(run: ScanRun): Insight {
    if (!run.slaMet) {
        const belowSLA = findDimensionsBelowSLA(run.dimensions);
        if (belowSLA.length > 0) {
            return {
                message: `${formatDimensionName(belowSLA[0].name)} below SLA threshold (${belowSLA[0].score}%)`,
                severity: 'warning',
                icon: '⚠️'
            };
        }
    }

    if (run.failedChecks > 0) {
        return {
            message: `${run.failedChecks} check${run.failedChecks > 1 ? 's' : ''} failing across ${run.datasetsScanned} dataset${run.datasetsScanned > 1 ? 's' : ''}`,
            severity: 'warning',
            icon: '⚠️'
        };
    }

    return {
        message: 'First scan of the day completed successfully',
        severity: 'success',
        icon: '✨'
    };
}

/**
 * Generate automated business insight from run comparison
 */
export function generateInsight(
    currentRun: ScanRun,
    previousRun: ScanRun | null,
    delta: RunDelta | null
): Insight {
    // No previous run to compare
    if (!previousRun || !delta) {
        return generateFirstRunInsight(currentRun);
    }

    const topDimension = findTopChangedDimension(delta.dimensions);
    const overallDelta = delta.overall;

    // Significant improvement (>5%)
    if (overallDelta > 5) {
        return {
            message: `Score improved due to +${topDimension.delta.toFixed(1)}% increase in ${formatDimensionName(topDimension.name)}`,
            severity: 'success',
            icon: '✅'
        };
    }

    // Significant degradation (<-5%)
    if (overallDelta < -5) {
        return {
            message: `Score degraded due to ${topDimension.delta.toFixed(1)}% drop in ${formatDimensionName(topDimension.name)}`,
            severity: 'critical',
            icon: '🔴'
        };
    }

    // SLA breach
    if (!currentRun.slaMet) {
        const belowSLA = findDimensionsBelowSLA(currentRun.dimensions);
        if (belowSLA.length > 0) {
            return {
                message: `${formatDimensionName(belowSLA[0].name)} remains below SLA threshold (${belowSLA[0].score}%)`,
                severity: 'warning',
                icon: '⚠️'
            };
        }
    }

    // Failed checks present
    if (currentRun.failedChecks > 0) {
        return {
            message: `${currentRun.failedChecks} check${currentRun.failedChecks > 1 ? 's' : ''} failing across ${currentRun.datasetsScanned} dataset${currentRun.datasetsScanned > 1 ? 's' : ''}`,
            severity: 'warning',
            icon: '⚠️'
        };
    }

    // Minor improvement
    if (overallDelta > 0) {
        return {
            message: `Quality score improved by ${overallDelta.toFixed(1)}%`,
            severity: 'success',
            icon: '📈'
        };
    }

    // Minor degradation
    if (overallDelta < 0) {
        return {
            message: `Quality score decreased by ${Math.abs(overallDelta).toFixed(1)}%`,
            severity: 'warning',
            icon: '📉'
        };
    }

    // Stable
    return {
        message: 'All quality checks stable',
        severity: 'success',
        icon: '✨'
    };
}
