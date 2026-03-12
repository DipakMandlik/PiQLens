/**
 * Delta Calculator Utility
 * Computes run-to-run deltas for scan activity timeline
 */

export interface DimensionScores {
    completeness: number;
    uniqueness: number;
    validity: number;
    consistency: number;
    freshness: number;
    volume: number;
}

export interface ScanRun {
    runId: string;
    runType: 'FULL' | 'INCREMENTAL';
    runTime: string;
    datasetsScanned: number;
    uniqueChecks: number;
    totalExecutions: number;
    overallScore: number;
    passedChecks: number;
    failedChecks: number;
    warningChecks: number;
    totalRecords: number;
    failedRecordsCount: number;
    slaMet: boolean;
    dimensions: DimensionScores;
    trustLevel: 'HIGH' | 'MEDIUM' | 'LOW';
    qualityGrade: string;
    failureRate: number;
}

export interface DimensionDeltas {
    completeness: number;
    uniqueness: number;
    validity: number;
    consistency: number;
    freshness: number;
    volume: number;
}

export interface RunDelta {
    overall: number;
    isPositive: boolean;
    dimensions: DimensionDeltas;
}

export interface RunWithDelta extends ScanRun {
    delta: RunDelta | null;
}

/**
 * Calculate deltas between current run and previous run
 */
export function calculateRunDelta(
    currentRun: ScanRun,
    previousRun: ScanRun | null
): RunDelta | null {
    if (!previousRun) {
        return null;
    }

    const overallDelta = currentRun.overallScore - previousRun.overallScore;

    const dimensionDeltas: DimensionDeltas = {
        completeness: currentRun.dimensions.completeness - previousRun.dimensions.completeness,
        uniqueness: currentRun.dimensions.uniqueness - previousRun.dimensions.uniqueness,
        validity: currentRun.dimensions.validity - previousRun.dimensions.validity,
        consistency: currentRun.dimensions.consistency - previousRun.dimensions.consistency,
        freshness: currentRun.dimensions.freshness - previousRun.dimensions.freshness,
        volume: currentRun.dimensions.volume - previousRun.dimensions.volume,
    };

    return {
        overall: overallDelta,
        isPositive: overallDelta >= 0,
        dimensions: dimensionDeltas
    };
}

/**
 * Calculate deltas for all runs in the timeline
 */
export function calculateRunDeltas(runs: ScanRun[]): RunWithDelta[] {
    return runs.map((run, index) => {
        const previousRun = runs[index + 1] || null; // Next in array (older timestamp)
        const delta = calculateRunDelta(run, previousRun);

        return {
            ...run,
            delta
        };
    });
}

/**
 * Calculate today vs yesterday delta for top-level overview
 */
export function calculateDailyDelta(
    todayLatestRun: ScanRun | null,
    yesterdayLatestRun: ScanRun | null
): number {
    if (!todayLatestRun || !yesterdayLatestRun) {
        return 0;
    }
    return todayLatestRun.overallScore - yesterdayLatestRun.overallScore;
}
