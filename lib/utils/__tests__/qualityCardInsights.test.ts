import { generateOverallQualityInsight, generateCoverageInsight, generateValidityInsight } from '../qualityCardInsights';
import { DeltaResult } from '../qualityCardDelta';

// Mock dimensions
const mockTodayDimensions = {
    completeness: 95,
    validity: 90,
    uniqueness: 98,
    consistency: 92,
    freshness: 100,
    volume: 95
};

const mockYesterdayDimensions = {
    completeness: 85, // +10% change
    validity: 90,
    uniqueness: 98,
    consistency: 92,
    freshness: 100,
    volume: 95
};

describe('qualityCardInsights', () => {
    describe('generateOverallQualityInsight', () => {
        it('should return fallback message when no delta provided', () => {
            const insight = generateOverallQualityInsight(95, null, null);
            expect(insight.severity).toBe('success');
            expect(insight.message).toContain('Excellent quality');
        });

        it('should identify significant improvement dimension', () => {
            const delta: DeltaResult = { value: 6, percentage: 5, isPositive: true, isNeutral: false, trend: 'improving' };
            const insight = generateOverallQualityInsight(96, 90, delta, mockTodayDimensions, mockYesterdayDimensions);

            expect(insight.severity).toBe('success');
            expect(insight.message).toContain('Completeness');
            expect(insight.message).toContain('+10.0%');
        });

        it('should handle stable metrics', () => {
            const delta: DeltaResult = { value: 0, percentage: 0, isPositive: false, isNeutral: true, trend: 'stable' };
            const insight = generateOverallQualityInsight(90, 90, delta, mockTodayDimensions, mockTodayDimensions);

            expect(insight.severity).toBe('info');
            expect(insight.message).toContain('stable');
        });
    });

    describe('generateCoverageInsight', () => {
        it('should report increased checks', () => {
            const delta: DeltaResult = { value: 0, percentage: 0, isPositive: false, isNeutral: true, trend: 'stable' };
            const insight = generateCoverageInsight(120, 100, delta);

            expect(insight.severity).toBe('success');
            expect(insight.message).toContain('More rules evaluated');
            expect(insight.message).toContain('+20');
        });

        it('should report fewer checks as warning', () => {
            const delta: DeltaResult = { value: 0, percentage: 0, isPositive: false, isNeutral: true, trend: 'stable' };
            const insight = generateCoverageInsight(80, 100, delta);

            expect(insight.severity).toBe('warning');
            expect(insight.message).toContain('Coverage reduced');
        });
    });

    describe('generateValidityInsight', () => {
        it('should report failing rules even without delta', () => {
            const insight = generateValidityInsight(80, 5, null, null);
            expect(insight.severity).toBe('warning');
            expect(insight.message).toBe('5 validation rules failing');
        });

        it('should report significant degradation', () => {
            const delta: DeltaResult = { value: -6, percentage: -5, isPositive: false, isNeutral: false, trend: 'degrading' };
            const insight = generateValidityInsight(80, 5, 86, delta);

            expect(insight.severity).toBe('critical');
            expect(insight.message).toContain('Invalid records increased');
        });
    });
});
