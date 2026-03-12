/**
 * Quality Card Delta Calculator
 * Computes today vs yesterday deltas for quality score cards
 */

export interface DeltaResult {
    value: number;                    // Absolute delta (today - yesterday)
    percentage: number;               // Percentage change
    isPositive: boolean;              // true if delta > 0
    isNeutral: boolean;               // true if |delta| < 1
    trend: 'improving' | 'degrading' | 'stable';
}

/**
 * Calculate delta between today's score and yesterday's score
 * Returns null if yesterday score is not available
 */
export function calculateDelta(
    todayScore: number,
    yesterdayScore: number | null | undefined
): DeltaResult | null {
    // No comparison data available
    if (yesterdayScore === null || yesterdayScore === undefined) {
        return null;
    }

    const delta = todayScore - yesterdayScore;
    const percentage = yesterdayScore > 0
        ? (delta / yesterdayScore) * 100
        : 0;

    const isPositive = delta > 0;
    const isNeutral = Math.abs(delta) < 1;

    let trend: 'improving' | 'degrading' | 'stable';
    if (delta > 1) {
        trend = 'improving';
    } else if (delta < -1) {
        trend = 'degrading';
    } else {
        trend = 'stable';
    }

    return {
        value: delta,
        percentage,
        isPositive,
        isNeutral,
        trend
    };
}

/**
 * Format delta for display
 */
export function formatDeltaValue(delta: DeltaResult | null): string {
    if (!delta) {
        return '—';
    }

    if (delta.isNeutral) {
        return '0%';
    }

    const sign = delta.isPositive ? '+' : '';
    return `${sign}${delta.value.toFixed(1)}%`;
}
