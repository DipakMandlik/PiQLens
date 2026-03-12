'use client';

import { Card } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { CheckCircle2, XCircle, AlertTriangle } from 'lucide-react';
import { format } from 'date-fns';

interface CheckResult {
    checkName: string;
    status: string; // 'PASSED' | 'FAILED' | 'WARNING'
    errorMessage: string | null;
    executedAt: string;
}

interface ColumnRecentChecksProps {
    checks: CheckResult[];
}

export function ColumnRecentChecks({ checks }: ColumnRecentChecksProps) {
    if (!checks || checks.length === 0) {
        return (
            <Card className="p-6 h-full flex flex-col items-center justify-center text-center border-slate-200 shadow-sm min-h-[200px]">
                <div className="w-12 h-12 bg-slate-50 rounded-full flex items-center justify-center mb-3">
                    <CheckCircle2 className="w-6 h-6 text-slate-300" />
                </div>
                <h3 className="text-sm font-medium text-slate-900">No Recent Checks</h3>
                <p className="text-xs text-slate-500 mt-1 max-w-[200px]">
                    Run a check to see validation results here.
                </p>
            </Card>
        );
    }

    return (
        <Card className="flex flex-col h-full border-slate-200 shadow-sm overflow-hidden">
            <div className="p-4 border-b border-slate-100 bg-slate-50/50">
                <div className="flex items-center justify-between">
                    <div>
                        <h3 className="text-sm font-bold text-slate-900 uppercase tracking-wide">Recent Validations</h3>
                        <p className="text-xs text-slate-500">Last {checks.length} check results</p>
                    </div>
                </div>
            </div>

            <div className="flex-1 overflow-y-auto p-0">
                <div className="divide-y divide-slate-100">
                    {checks.map((check, idx) => (
                        <div key={idx} className="p-4 hover:bg-slate-50 transition-colors flex items-start gap-3">
                            <div className="mt-0.5">
                                {check.status === 'PASSED' && <CheckCircle2 className="w-5 h-5 text-emerald-500" />}
                                {check.status === 'FAILED' && <XCircle className="w-5 h-5 text-red-500" />}
                                {check.status === 'WARNING' && <AlertTriangle className="w-5 h-5 text-amber-500" />}
                            </div>
                            <div className="flex-1">
                                <div className="flex items-center justify-between mb-1">
                                    <span className="text-sm font-medium text-slate-900">{check.checkName}</span>
                                    <span className="text-[10px] text-slate-400">
                                        {(() => {
                                            if (!check.executedAt) return '-';
                                            const date = new Date(check.executedAt);
                                            return !isNaN(date.getTime()) ? format(date, 'MMM d, H:mm') : '-';
                                        })()}
                                    </span>
                                </div>
                                <div className="flex items-center justify-between">
                                    <div className="text-xs text-slate-500 truncate max-w-[150px]" title={check.errorMessage || ''}>
                                        {check.status === 'FAILED' && check.errorMessage ? (
                                            <span className="text-red-400">{check.errorMessage}</span>
                                        ) : check.status === 'PASSED' ? (
                                            <span className="text-emerald-500">Validation Passed</span>
                                        ) : (
                                            <span className="text-slate-400">-</span>
                                        )}
                                    </div>
                                    <Badge variant="outline" className={`text-[10px] h-5 border-0 ${check.status === 'PASSED' ? 'bg-emerald-50 text-emerald-700' :
                                            check.status === 'FAILED' ? 'bg-red-50 text-red-700' :
                                                'bg-amber-50 text-amber-700'
                                        }`}>
                                        {check.status}
                                    </Badge>
                                </div>
                            </div>
                        </div>
                    ))}
                </div>
            </div>
        </Card>
    );
}
