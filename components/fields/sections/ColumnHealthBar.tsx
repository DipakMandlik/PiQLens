'use client';

import { Card } from '@/components/ui/card';
import { Shield, CheckCircle2, XCircle, AlertTriangle, Clock3, AlertCircle } from 'lucide-react';

interface LegacyCheckDetail {
    name: string;
    status: string;
    value: string;
    threshold?: number;
    severity?: string;
}

interface ActiveCheckMetric {
    rule_id: number | null;
    rule_name: string;
    rule_type: string;
    threshold: number | null;
    severity: 'CRITICAL' | 'MEDIUM' | 'LOW';
    severity_weight: 3 | 2 | 1;
    pass_rate: number | null;
    status: 'PASSED' | 'WARNING' | 'FAILED' | 'ERROR' | 'NOT_EXECUTED';
    invalid_records: number | null;
    total_records: number | null;
    last_executed_at: string | null;
    contributes_to_health: boolean;
    display_priority: number;
}

interface DQSummary {
    totalChecks: number;
    passed: number;
    failed: number;
    criticalFailed: number;
    warnings: number;
    score: number | null;
    riskLevel: 'HIGH' | 'MEDIUM' | 'LOW' | 'NONE';
    source?: 'checks' | 'profiling' | 'none';
    checkDetails?: LegacyCheckDetail[];
    activeChecks?: ActiveCheckMetric[];
    executedCheckCount?: number;
    unexecutedCheckCount?: number;
    notices?: string[];
}

interface ColumnHealthBarProps {
    dqSummary: DQSummary;
}

export function ColumnHealthBar({ dqSummary }: ColumnHealthBarProps) {
    const score = dqSummary.score;
    const riskLevel = dqSummary.riskLevel || 'NONE';
    const activeChecks = dqSummary.activeChecks || [];
    const executedCheckCount = dqSummary.executedCheckCount ?? 0;
    const unexecutedCheckCount = dqSummary.unexecutedCheckCount ?? 0;
    const notices = dqSummary.notices || [];

    const hasData = score !== null;
    const hasActiveCheckModel = activeChecks.length > 0 || dqSummary.executedCheckCount !== undefined || dqSummary.unexecutedCheckCount !== undefined;

    const checksToRender = hasActiveCheckModel
        ? [...activeChecks].sort((a, b) => {
            if (a.display_priority !== b.display_priority) return a.display_priority - b.display_priority;
            return a.rule_name.localeCompare(b.rule_name);
        }).map((check) => ({
            name: check.rule_name,
            status: check.status,
            value: check.pass_rate !== null ? `${check.pass_rate.toFixed(1)}%` : 'Not Executed',
            severity: check.severity
        }))
        : (dqSummary.checkDetails || []).map((check) => ({
            name: check.name,
            status: check.status,
            value: check.value,
            severity: check.severity
        }));

    const getScoreColor = (s: number | null) => {
        if (s === null) return { ring: '#94a3b8', text: 'text-slate-500', bg: 'bg-slate-50' };
        if (s >= 95) return { ring: '#10b981', text: 'text-emerald-600', bg: 'bg-emerald-50' };
        if (s >= 80) return { ring: '#f59e0b', text: 'text-amber-600', bg: 'bg-amber-50' };
        return { ring: '#ef4444', text: 'text-red-600', bg: 'bg-red-50' };
    };

    const scoreColor = getScoreColor(score);
    const displayScore = score !== null ? score : 0;
    const circumference = 2 * Math.PI * 40;
    const strokeDashoffset = circumference - (circumference * displayScore) / 100;

    const riskConfig = {
        HIGH: { color: 'bg-red-50 border-red-200', text: 'text-red-700', label: 'High Risk', dot: 'bg-red-500', message: 'Critical quality issues detected.' },
        MEDIUM: { color: 'bg-amber-50 border-amber-200', text: 'text-amber-700', label: 'Medium Risk', dot: 'bg-amber-500', message: 'Some active checks need attention.' },
        LOW: { color: 'bg-emerald-50 border-emerald-200', text: 'text-emerald-700', label: 'Low Risk', dot: 'bg-emerald-500', message: 'Active checks are healthy.' },
        NONE: { color: 'bg-slate-50 border-slate-200', text: 'text-slate-500', label: 'No Data', dot: 'bg-slate-400', message: 'No executed active checks available.' }
    };
    const risk = riskConfig[riskLevel];

    const StatusIcon = ({ status }: { status: string }) => {
        switch (status) {
            case 'PASSED': return <CheckCircle2 className="w-3.5 h-3.5 text-emerald-500" />;
            case 'WARNING': return <AlertTriangle className="w-3.5 h-3.5 text-amber-500" />;
            case 'FAILED': return <XCircle className="w-3.5 h-3.5 text-red-500" />;
            case 'ERROR': return <AlertCircle className="w-3.5 h-3.5 text-red-600" />;
            case 'NOT_EXECUTED': return <Clock3 className="w-3.5 h-3.5 text-slate-400" />;
            default: return <Clock3 className="w-3.5 h-3.5 text-slate-400" />;
        }
    };

    const statusColor = (status: string) => {
        switch (status) {
            case 'PASSED': return 'text-emerald-600 bg-emerald-50';
            case 'WARNING': return 'text-amber-600 bg-amber-50';
            case 'FAILED': return 'text-red-600 bg-red-50';
            case 'ERROR': return 'text-red-700 bg-red-100';
            case 'NOT_EXECUTED': return 'text-slate-500 bg-slate-100';
            default: return 'text-slate-500 bg-slate-50';
        }
    };

    const basedOnText = hasActiveCheckModel
        ? `Based on ${executedCheckCount} active executed check${executedCheckCount !== 1 ? 's' : ''}${unexecutedCheckCount > 0 ? ` (${unexecutedCheckCount} not executed)` : ''}`
        : `Based on ${dqSummary.totalChecks} check${dqSummary.totalChecks !== 1 ? 's' : ''}`;

    return (
        <div className="mb-6">
            <h4 className="text-sm font-bold text-slate-900 uppercase tracking-wide flex items-center gap-2 mb-4">
                <Shield className="w-4 h-4 text-indigo-500" />
                Column Health (Rule-Based)
            </h4>

            <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                <Card className={`p-5 border ${scoreColor.bg} relative overflow-hidden`}>
                    <div className="flex items-center gap-5">
                        <div className="relative w-24 h-24 flex-shrink-0">
                            <svg className="w-24 h-24 transform -rotate-90" viewBox="0 0 96 96">
                                <circle cx="48" cy="48" r="40" stroke="#e2e8f0" strokeWidth="6" fill="none" />
                                <circle
                                    cx="48"
                                    cy="48"
                                    r="40"
                                    stroke={scoreColor.ring}
                                    strokeWidth="6"
                                    fill="none"
                                    strokeLinecap="round"
                                    strokeDasharray={circumference}
                                    strokeDashoffset={hasData ? strokeDashoffset : circumference}
                                    className="transition-all duration-1000 ease-out"
                                />
                            </svg>
                            <div className="absolute inset-0 flex flex-col items-center justify-center">
                                <span className={`text-2xl font-bold ${scoreColor.text}`}>
                                    {hasData ? `${score}%` : '-'}
                                </span>
                            </div>
                        </div>
                        <div>
                            <div className="text-xs font-bold text-slate-500 uppercase tracking-wider mb-1">Column Health Score</div>
                            <div className={`text-lg font-bold ${scoreColor.text}`}>
                                {hasData
                                    ? score! >= 95 ? 'Excellent' : score! >= 80 ? 'Good' : score! >= 60 ? 'Needs Attention' : 'Critical'
                                    : 'Not Available'}
                            </div>
                            <div className="text-xs text-slate-500 mt-1">{basedOnText}</div>
                        </div>
                    </div>
                </Card>

                <Card className="p-5 border border-slate-200">
                    <div className="text-xs font-bold text-slate-500 uppercase tracking-wider mb-3">Active Checks</div>

                    {checksToRender.length === 0 ? (
                        <div className="flex flex-col items-center justify-center h-16 text-center">
                            <p className="text-sm text-slate-400">No active checks</p>
                            <p className="text-[10px] text-slate-300 mt-1">Configure checks for this column</p>
                        </div>
                    ) : (
                        <div className="space-y-2">
                            {checksToRender.map((check, idx) => (
                                <div key={idx} className="flex items-center justify-between">
                                    <span className="flex items-center gap-2 text-sm text-slate-700">
                                        <StatusIcon status={check.status} />
                                        {check.name.replace(/_/g, ' ').replace(/\b\w/g, (c) => c.toUpperCase())}
                                        {check.severity && (
                                            <span className="text-[10px] px-1.5 py-0.5 rounded bg-slate-100 text-slate-500">
                                                {check.severity}
                                            </span>
                                        )}
                                    </span>
                                    <span className={`inline-flex items-center px-2 py-0.5 rounded-full text-[11px] font-semibold ${statusColor(check.status)}`}>
                                        {check.value}
                                    </span>
                                </div>
                            ))}
                        </div>
                    )}
                </Card>

                <Card className={`p-5 border ${risk.color} flex flex-col justify-between`}>
                    <div>
                        <div className="text-xs font-bold text-slate-500 uppercase tracking-wider mb-3">Risk Assessment</div>
                        <div className="flex items-center gap-3 mb-3">
                            <span className={`w-4 h-4 rounded-full ${risk.dot}`}></span>
                            <div className={`text-xl font-bold ${risk.text}`}>{risk.label}</div>
                        </div>
                    </div>
                    <p className="text-xs text-slate-500 leading-relaxed">{risk.message}</p>
                </Card>
            </div>

            {notices.length > 0 && (
                <div className="mt-3 p-3 rounded border border-slate-200 bg-slate-50">
                    {notices.map((notice, idx) => (
                        <p key={idx} className="text-xs text-slate-600">{notice}</p>
                    ))}
                </div>
            )}
        </div>
    );
}
