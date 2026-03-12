import React from 'react';
import { CheckCircle2, XCircle } from 'lucide-react';
import { Insight } from '@/lib/utils/insightGenerator';

interface InsightPanelProps {
    insight: Insight;
    slaMet: boolean;
    passedChecks: number;
    failedChecks: number;
    warningChecks: number;
}

export function InsightPanel({
    insight,
    slaMet,
    passedChecks,
    failedChecks,
    warningChecks
}: InsightPanelProps) {
    return (
        <div className="flex flex-col gap-3">
            <div className={`insight-message ${insight.severity}`}>
                <span className="insight-icon">{insight.icon}</span>
                <p className="text-sm leading-relaxed">{insight.message}</p>
            </div>

            <div className="flex items-center justify-between">
                {slaMet ? (
                    <div className="flex items-center gap-1.5 px-2.5 py-1 rounded-full text-xs font-semibold bg-green-50 text-green-700 border border-green-200">
                        <CheckCircle2 className="w-3.5 h-3.5" />
                        SLA Status: Met
                    </div>
                ) : (
                    <div className="flex items-center gap-1.5 px-2.5 py-1 rounded-full text-xs font-semibold bg-red-50 text-red-700 border border-red-200">
                        <XCircle className="w-3.5 h-3.5" />
                        SLA Status: Breached
                    </div>
                )}
            </div>

            <div className="flex items-center gap-2 text-xs">
                <span className="text-green-600 font-medium">{passedChecks} Rules Passed</span>
                <span className="text-slate-300">|</span>
                <span className="text-red-600 font-medium">{failedChecks} Rules Failed</span>
                {warningChecks > 0 && (
                    <>
                        <span className="text-slate-300">|</span>
                        <span className="text-amber-600 font-medium">{warningChecks} Rules Warning</span>
                    </>
                )}
            </div>
        </div>
    );
}
