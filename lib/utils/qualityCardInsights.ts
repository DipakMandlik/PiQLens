/**
 * Quality Card Insight Generator
 * Generates automated micro insights for quality score cards
 */

import { DeltaResult } from './qualityCardDelta';

export interface Insight {
    message: string;
    icon: string;
    severity: 'success' | 'warning' | 'critical' | 'info';
}

export interface DimensionScores {
    completeness: number;
    validity: number;
    uniqueness: number;
    consistency: number;
    freshness: number;
    volume: number;
}

/**
 * Format dimension name for display
 */
function formatDimensionName(dim: string): string {
    return dim.charAt(0).toUpperCase() + dim.slice(1);
}

/**
 * Find dimension with largest absolute change
 */
function findTopChangedDimension(
    dimensions: DimensionScores,
    yesterdayDimensions: DimensionScores
): { name: string; delta: number; absDelta: number } {
    const dimensionDeltas = {
        completeness: dimensions.completeness - yesterdayDimensions.completeness,
        validity: dimensions.validity - yesterdayDimensions.validity,
        uniqueness: dimensions.uniqueness - yesterdayDimensions.uniqueness,
        consistency: dimensions.consistency - yesterdayDimensions.consistency,
        freshness: dimensions.freshness - yesterdayDimensions.freshness,
        volume: dimensions.volume - yesterdayDimensions.volume
    };

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
 * Generate insight for Overall Quality Score card
 */
export function generateOverallQualityInsight(
    todayScore: number,
    yesterdayScore: number | null,
    delta: DeltaResult | null,
    dimensions?: DimensionScores,
    yesterdayDimensions?: DimensionScores | null
): Insight {
    // No comparison data
    if (!delta || !dimensions || !yesterdayDimensions) {
        if (todayScore >= 90) {
            return {
                message: 'Excellent quality across all dimensions',
                icon: '✨',
                severity: 'success'
            };
        } else if (todayScore < 70) {
            return {
                message: 'Quality below acceptable threshold',
                icon: '⚠️',
                severity: 'warning'
            };
        }
        return {
            message: 'Quality metrics within normal range',
            icon: '📊',
            severity: 'info'
        };
    }

    // Find dimension with largest change
    const topChange = findTopChangedDimension(dimensions, yesterdayDimensions);

    // If overall increased: "Quality improved primarily due to increase in {dimension}."
    if (delta.value > 0) {
        return {
            message: `Quality improved primarily due to increase in ${formatDimensionName(topChange.name)}`,
            icon: '✅',
            severity: 'success'
        };
    }

    // If overall decreased: "Quality declined primarily due to drop in {dimension}."
    if (delta.value < 0) {
        return {
            message: `Quality declined primarily due to drop in ${formatDimensionName(topChange.name)}`,
            icon: '🔴',
            severity: 'critical'
        };
    }

    // If unchanged: "No material change compared to yesterday."
    return {
        message: 'No material change compared to yesterday',
        icon: '➡️',
        severity: 'info'
    };
}

/**
 * Generate insight for Coverage Score card
 */
export function generateCoverageInsight(
    todayChecks: number,
    yesterdayChecks: number | null,
    delta: DeltaResult | null
): Insight {
    // No comparison
    if (!delta || yesterdayChecks === null) {
        return {
            message: `${todayChecks} quality rules evaluated`,
            icon: '📋',
            severity: 'info'
        };
    }

    const checksDelta = todayChecks - yesterdayChecks;

    // Use dimension-like logic if possible, or fallback to checks count
    if (delta.value > 0) {
        return {
            message: 'Quality improved primarily due to increase in Completeness',
            icon: '✅',
            severity: 'success'
        };
    }

    if (delta.value < 0) {
        return {
            message: 'Quality declined primarily due to drop in Completeness',
            icon: '🔴',
            severity: 'critical'
        };
    }

    return {
        message: 'No material change compared to yesterday',
        icon: '➡️',
        severity: 'info'
    };
}

/**
 * Generate insight for Validity Score card
 */
export function generateValidityInsight(
    todayScore: number,
    todayFailed: number,
    yesterdayScore: number | null,
    delta: DeltaResult | null
): Insight {
    // No comparison
    if (!delta) {
        if (todayFailed > 0) {
            return {
                message: `${todayFailed} validation rule${todayFailed > 1 ? 's' : ''} failing`,
                icon: '⚠️',
                severity: 'warning'
            };
        }
        return {
            message: 'All validation rules passing',
            icon: '✅',
            severity: 'success'
        };
    }

    if (delta.value > 0) {
        return {
            message: 'Quality improved primarily due to increase in Validity',
            icon: '✅',
            severity: 'success'
        };
    }

    if (delta.value < 0) {
        return {
            message: 'Quality declined primarily due to drop in Validity',
            icon: '🔴',
            severity: 'critical'
        };
    }

    return {
        message: 'No material change compared to yesterday',
        icon: '➡️',
        severity: 'info'
    };
}
