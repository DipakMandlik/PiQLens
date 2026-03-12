'use client';

import { Card, CardContent } from '@/components/ui/card';
import { LucideIcon, CheckCircle2, XCircle, AlertTriangle, Info } from 'lucide-react';

interface CircularMetricCardProps {
    title: string;
    type: 'overall' | 'coverage' | 'validity';
    icon: LucideIcon;
    color: 'blue' | 'green' | 'amber' | 'red';
    todayScore: number;
    todayChecks: number;
    todayFailed: number;
    qualityGrade: 'A+' | 'A' | 'B' | 'C' | 'D' | 'F';
    trustLevel: 'HIGH' | 'MEDIUM' | 'LOW';
    slaMet: boolean;
    delta: number | null;
    deltaTooltip?: string;
    microInsight: string;
}

function formatDelta(delta: number): string {
    if (delta > 0) return '\u25B2 +' + delta.toFixed(1) + '%';
    if (delta < 0) return '\u25BC ' + delta.toFixed(1) + '%';
    return '\u25CF 0%';
}

function getDeltaClasses(delta: number): string {
    if (delta > 0) return 'bg-emerald-50 text-emerald-700 border-emerald-200';
    if (delta < 0) return 'bg-red-50 text-red-700 border-red-200';
    return 'bg-slate-100 text-slate-600 border-slate-200';
}

export function CircularMetricCard({
    title,
    icon: Icon,
    color,
    todayScore,
    todayChecks,
    todayFailed,
    qualityGrade,
    trustLevel,
    slaMet,
    delta,
    deltaTooltip,
    microInsight,
}: CircularMetricCardProps) {

    const colors = {
        blue: { main: '#3b82f6', bg: '#eff6ff', text: 'text-blue-600' },
        green: { main: '#10b981', bg: '#ecfdf5', text: 'text-green-600' },
        amber: { main: '#f59e0b', bg: '#fffbeb', text: 'text-amber-600' },
        red: { main: '#ef4444', bg: '#fef2f2', text: 'text-red-600' },
    };

    const theme = colors[color];
    const clampedScore = Math.max(0, Math.min(100, todayScore));
    const circumference = 2 * Math.PI * 32;
    const strokeDashoffset = circumference - (clampedScore / 100) * circumference;

    return (
        <Card className="border-slate-200 hover:shadow-lg transition-all duration-300">
            <CardContent className="p-5">
                <div className="flex items-start justify-between mb-4">
                    <div className="flex items-center gap-2.5">
                        <div className="p-2 rounded-lg" style={{ backgroundColor: theme.bg }}>
                            <Icon className={`h-4 w-4 ${theme.text}`} />
                        </div>
                        <h3 className="text-sm font-semibold text-slate-800">{title}</h3>
                    </div>

                    {delta !== null ? (
                        <span className={`text-xs font-semibold border rounded-full px-2 py-1 ${getDeltaClasses(delta)}`}>
                            {formatDelta(delta)}
                        </span>
                    ) : (
                        <span title={deltaTooltip || 'No previous day data available'} className="text-slate-400">
                            <Info className="h-4 w-4" />
                        </span>
                    )}
                </div>

                <div className="flex items-center gap-5 mb-4">
                    <div className="relative h-20 w-20 flex-shrink-0">
                        <svg className="transform -rotate-90" viewBox="0 0 80 80">
                            <circle
                                cx="40"
                                cy="40"
                                r="32"
                                fill="none"
                                stroke="currentColor"
                                strokeWidth="6"
                                className="text-slate-100"
                            />
                            <circle
                                cx="40"
                                cy="40"
                                r="32"
                                fill="none"
                                stroke={theme.main}
                                strokeWidth="6"
                                strokeDasharray={circumference}
                                strokeDashoffset={strokeDashoffset}
                                strokeLinecap="round"
                                className="transition-all duration-500"
                            />
                        </svg>
                        <div className="absolute inset-0 flex items-center justify-center">
                            <span className={`text-xl font-bold ${theme.text}`}>
                                {Math.round(clampedScore)}%
                            </span>
                        </div>
                    </div>

                    <div className="flex-1 space-y-1">
                        <div className="flex items-baseline gap-2">
                            <span className="text-2xl font-bold text-slate-900">{Math.round(clampedScore)}</span>
                            <span className="text-sm text-slate-500">/ 100</span>
                        </div>
                        <p className="text-xs text-slate-500 font-medium">
                            {todayChecks.toLocaleString()} checks evaluated
                        </p>
                    </div>
                </div>

                <div className="grid grid-cols-2 gap-2 pt-3 border-t border-slate-100 text-xs mb-3">
                    <div className="flex items-center gap-1">
                        <XCircle className="w-3.5 h-3.5 text-red-500" />
                        <span className="font-semibold text-red-600">Failed Records: {todayFailed}</span>
                    </div>
                    <div className="flex items-center gap-1">
                        <span className="px-1.5 py-0.5 rounded bg-slate-100 text-slate-700 font-semibold">
                            Grade {qualityGrade}
                        </span>
                    </div>
                    <div className="flex items-center gap-1">
                        <span className={`px-1.5 py-0.5 rounded font-semibold ${trustLevel === 'HIGH'
                            ? 'bg-green-50 text-green-700 border border-green-200'
                            : trustLevel === 'MEDIUM'
                                ? 'bg-amber-50 text-amber-700 border border-amber-200'
                                : 'bg-red-50 text-red-700 border border-red-200'
                            }`}>
                            {trustLevel}
                        </span>
                    </div>
                    <div className="flex items-center gap-1 text-xs">
                        {slaMet ? (
                            <>
                                <CheckCircle2 className="w-3.5 h-3.5 text-green-500" />
                                <span className="font-semibold text-green-600">SLA: Met</span>
                            </>
                        ) : (
                            <>
                                <AlertTriangle className="w-3.5 h-3.5 text-amber-500" />
                                <span className="font-semibold text-amber-600">SLA: Breached</span>
                            </>
                        )}
                    </div>
                </div>

                <div className="text-xs text-slate-600 bg-slate-50 border border-slate-200 rounded-md px-3 py-2">
                    {microInsight}
                </div>
            </CardContent>
        </Card>
    );
}

