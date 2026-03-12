'use client';

import { CheckCircle2, XCircle, AlertTriangle } from 'lucide-react';
import { Card, CardContent } from '@/components/ui/card';
import { cn } from '@/lib/utils';

interface Check {
    checkName: string;
    checkType: string;
    condition: string;
    status: string;
    failurePct: number;
    severity: string;
    executedAt: string;
}

interface ColumnChecksProps {
    checks: Check[];
}

export function ColumnChecks({ checks }: ColumnChecksProps) {
    if (checks.length === 0) {
        return (
            <div className="mb-8">
                <h4 className="text-sm font-bold text-slate-900 uppercase tracking-wide mb-4">Active Checks</h4>
                <div className="text-sm text-slate-500 italic">No checks configured for this column.</div>
            </div>
        );
    }

    const getStatusIcon = (status: string) => {
        switch (status) {
            case 'PASS': return <CheckCircle2 className="w-5 h-5 text-emerald-500" />;
            case 'FAIL': return <XCircle className="w-5 h-5 text-red-500" />;
            case 'WARN': return <AlertTriangle className="w-5 h-5 text-amber-500" />;
            default: return null;
        }
    };

    return (
        <div className="mb-8">
            <h4 className="text-sm font-bold text-slate-900 uppercase tracking-wide mb-4">Active Checks</h4>
            <div className="space-y-3">
                {checks.map((check, idx) => (
                    <Card key={idx} className="overflow-hidden border-l-4" style={{
                        borderLeftColor: check.status === 'FAIL' ? '#ef4444' : check.status === 'WARN' ? '#f59e0b' : '#10b981'
                    }}>
                        <CardContent className="p-4 flex items-center justify-between">
                            <div className="flex items-start gap-3">
                                {getStatusIcon(check.status)}
                                <div>
                                    <div className="font-semibold text-slate-800">{check.checkName}</div>
                                    <div className="text-xs text-slate-500 font-mono mt-0.5">{check.condition}</div>
                                </div>
                            </div>

                            <div className="text-right">
                                <div className={cn(
                                    "text-sm font-bold",
                                    check.status === 'FAIL' ? "text-red-600" : "text-emerald-600"
                                )}>
                                    {check.status}
                                </div>
                                {check.status === 'FAIL' && (
                                    <div className="text-xs text-red-500">
                                        {check.failurePct.toFixed(1)}% Failed
                                    </div>
                                )}
                                <div className="text-[10px] text-slate-400 mt-1">
                                    {new Date(check.executedAt).toLocaleDateString()}
                                </div>
                            </div>
                        </CardContent>
                    </Card>
                ))}
            </div>
        </div>
    );
}
