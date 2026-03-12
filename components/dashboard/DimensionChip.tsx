import React from 'react';

interface DimensionChipProps {
    name: string;
    score: number;
    delta: number | null;
}

export function DimensionChip({ name, score, delta }: DimensionChipProps) {
    const deltaIcon = delta === null ? null : delta > 0 ? '?' : delta < 0 ? '?' : '?';
    const deltaColor = delta === null ? 'gray' : delta > 0 ? 'green' : delta < 0 ? 'red' : 'gray';

    const scoreClass = getScoreClass(score);
    const zeroScoreTooltip = score === 0
        ? 'No valid records passed this dimension.'
        : undefined;

    return (
        <div className={`dimension-chip ${scoreClass}`} title={zeroScoreTooltip}>
            <div className="flex items-center justify-between mb-1">
                <span className="text-xs font-medium text-slate-700">{name}</span>
                {delta !== null && Math.abs(delta) > 0 && (
                    <span className={`text-[10px] font-semibold delta-${deltaColor}`}>
                        {deltaIcon} {Math.abs(delta).toFixed(0)}
                    </span>
                )}
            </div>
            <div className="text-lg font-bold text-slate-900 mb-1">{score}%</div>
            <div className="chip-bar">
                <div
                    className="chip-bar-fill"
                    style={{ width: `${score}%` }}
                />
            </div>
        </div>
    );
}

function getScoreClass(score: number): string {
    if (score >= 90) return 'score-excellent';
    if (score >= 80) return 'score-good';
    if (score >= 70) return 'score-fair';
    return 'score-poor';
}
