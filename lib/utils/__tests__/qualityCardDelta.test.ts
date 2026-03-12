import { calculateDelta, DeltaResult } from '../qualityCardDelta';

describe('qualityCardDelta', () => {
    describe('calculateDelta', () => {
        it('should return null when yesterday score is null or undefined', () => {
            expect(calculateDelta(90, null)).toBeNull();
            expect(calculateDelta(90, undefined)).toBeNull();
        });

        it('should calculate positive delta correctly', () => {
            // 80 -> 90 = +10 (12.5%)
            const result = calculateDelta(90, 80);
            expect(result).not.toBeNull();
            expect(result).toEqual({
                value: 10,
                percentage: 12.5,
                isPositive: true,
                isNeutral: false,
                trend: 'improving'
            });
        });

        it('should calculate negative delta correctly', () => {
            // 90 -> 80 = -10 (-11.1%)
            const result = calculateDelta(80, 90);
            expect(result).toEqual({
                value: -10,
                percentage: expect.closeTo(-11.11, 2),
                isPositive: false,
                isNeutral: false,
                trend: 'degrading'
            });
        });

        it('should handle zero delta correctly', () => {
            // 90 -> 90 = 0
            const result = calculateDelta(90, 90);
            expect(result).toEqual({
                value: 0,
                percentage: 0,
                isPositive: false,
                isNeutral: true,
                trend: 'stable'
            });
        });

        it('should handle small deltas as neutral/stable', () => {
            // 90 -> 90.5 = +0.5
            const result = calculateDelta(90.5, 90);
            expect(result).toEqual({
                value: 0.5,
                percentage: expect.any(Number),
                isPositive: true,
                isNeutral: true,
                trend: 'stable'
            });
        });
    });
});
