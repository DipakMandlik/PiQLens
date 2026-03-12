'use client';

import { Card } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { CheckCircle2, XCircle, AlertTriangle, Clock, TrendingUp, Database, Hash, AlertCircle } from 'lucide-react';
import { format } from 'date-fns';

interface CheckResult {
    checkName: string;
    status: string;
    errorMessage: string | null;
    executedAt: string;
    details?: Record<string, any>;
}

interface ColumnCheckResultsProps {
    checks: CheckResult[];
}

export function ColumnCheckResults({ checks }: ColumnCheckResultsProps) {
    if (!checks || checks.length === 0) {
        return null;
    }

    return (
        <div className="space-y-6">
            {checks.map((check, idx) => {
                // Extract key metrics from details
                const details = check.details || {};
                const runId = details.run_id || details.RUN_ID || 'N/A';
                const passRate = details.pass_rate || details.PASS_RATE || 0;
                const totalRecords = details.total_records || details.TOTAL_RECORDS || details.records_processed || 0;
                const invalidRecords = details.invalid_records || details.INVALID_RECORDS || 0;

                // Calculate valid records with fallback
                let validRecords = details.valid_records || details.VALID_RECORDS || 0;
                if (validRecords === 0 && totalRecords > 0 && invalidRecords > 0) {
                    validRecords = totalRecords - invalidRecords;
                }

                const threshold = details.threshold || details.THRESHOLD || 0;
                const columnName = details.column_name || details.COLUMN_NAME || 'N/A';
                const duration = details.duration_seconds || details.DURATION_SECONDS || 0;

                const isPassed = check.status === 'PASSED';
                const isFailed = check.status === 'FAILED';
                const isWarning = check.status === 'WARNING';

                return (
                    <Card key={idx} className="border-slate-200 shadow-lg overflow-hidden">
                        {/* Header with gradient background */}
                        <div className={`p-6 ${isPassed ? 'bg-gradient-to-r from-emerald-50 to-emerald-100/50' :
                            isFailed ? 'bg-gradient-to-r from-red-50 to-red-100/50' :
                                'bg-gradient-to-r from-amber-50 to-amber-100/50'
                            }`}>
                            <div className="flex items-start justify-between mb-4">
                                <div>
                                    <h3 className="text-2xl font-bold text-slate-900 mb-1">{check.checkName}</h3>
                                    <p className="text-sm text-slate-600">Column: <span className="font-mono font-semibold">{columnName}</span></p>
                                </div>
                                <Badge variant="outline" className={`h-10 px-4 gap-2 text-base font-semibold ${isPassed ? 'bg-emerald-600 text-white border-emerald-700' :
                                    isFailed ? 'bg-red-600 text-white border-red-700' :
                                        'bg-amber-600 text-white border-amber-700'
                                    }`}>
                                    {isPassed && <CheckCircle2 className="w-5 h-5" />}
                                    {isFailed && <XCircle className="w-5 h-5" />}
                                    {isWarning && <AlertTriangle className="w-5 h-5" />}
                                    {check.status}
                                </Badge>
                            </div>

                            {/* KPI Metrics Grid */}
                            <div className="grid grid-cols-4 gap-4">
                                {/* Pass Rate */}
                                <div className="bg-white rounded-lg p-4 shadow-sm border border-slate-200">
                                    <div className="flex items-center gap-2 mb-2">
                                        <TrendingUp className="w-4 h-4 text-indigo-600" />
                                        <span className="text-xs font-bold text-slate-500 uppercase">Pass Rate</span>
                                    </div>
                                    <div className="text-3xl font-bold text-slate-900">{Number(passRate).toFixed(2)}%</div>
                                    <div className="mt-2 h-2 bg-slate-100 rounded-full overflow-hidden">
                                        <div
                                            className={`h-full transition-all ${passRate >= 95 ? 'bg-emerald-500' :
                                                passRate >= 80 ? 'bg-amber-500' : 'bg-red-500'
                                                }`}
                                            style={{ width: `${Math.min(passRate, 100)}%` }}
                                        />
                                    </div>
                                </div>

                                {/* Total Records */}
                                <div className="bg-white rounded-lg p-4 shadow-sm border border-slate-200">
                                    <div className="flex items-center gap-2 mb-2">
                                        <Database className="w-4 h-4 text-blue-600" />
                                        <span className="text-xs font-bold text-slate-500 uppercase">Total Records</span>
                                    </div>
                                    <div className="text-3xl font-bold text-slate-900">{totalRecords.toLocaleString()}</div>
                                    <div className="text-xs text-slate-500 mt-1">Processed</div>
                                </div>

                                {/* Valid Records */}
                                <div className="bg-white rounded-lg p-4 shadow-sm border border-slate-200">
                                    <div className="flex items-center gap-2 mb-2">
                                        <CheckCircle2 className="w-4 h-4 text-emerald-600" />
                                        <span className="text-xs font-bold text-slate-500 uppercase">Valid</span>
                                    </div>
                                    <div className="text-3xl font-bold text-emerald-600">{validRecords.toLocaleString()}</div>
                                    <div className="text-xs text-slate-500 mt-1">
                                        {totalRecords > 0 ? ((validRecords / totalRecords) * 100).toFixed(1) : 0}% of total
                                    </div>
                                </div>

                                {/* Invalid Records */}
                                <div className="bg-white rounded-lg p-4 shadow-sm border border-slate-200">
                                    <div className="flex items-center gap-2 mb-2">
                                        <AlertCircle className="w-4 h-4 text-red-600" />
                                        <span className="text-xs font-bold text-slate-500 uppercase">Invalid</span>
                                    </div>
                                    <div className="text-3xl font-bold text-red-600">{invalidRecords.toLocaleString()}</div>
                                    <div className="text-xs text-slate-500 mt-1">
                                        {totalRecords > 0 ? ((invalidRecords / totalRecords) * 100).toFixed(1) : 0}% of total
                                    </div>
                                </div>
                            </div>
                        </div>

                        {/* Body Content */}
                        <div className="p-6 space-y-4">
                            {/* Status Message */}
                            <div className={`p-4 rounded-lg border ${isPassed ? 'bg-emerald-50 border-emerald-200' :
                                isFailed ? 'bg-red-50 border-red-200' :
                                    'bg-amber-50 border-amber-200'
                                }`}>
                                {check.errorMessage ? (
                                    <div className="flex items-start gap-3">
                                        <AlertTriangle className={`w-5 h-5 mt-0.5 shrink-0 ${isFailed ? 'text-red-600' : 'text-amber-600'
                                            }`} />
                                        <div>
                                            <p className={`font-semibold mb-1 ${isFailed ? 'text-red-900' : 'text-amber-900'
                                                }`}>Validation Issue Detected</p>
                                            <p className={`leading-relaxed ${isFailed ? 'text-red-700' : 'text-amber-700'
                                                }`}>{check.errorMessage}</p>
                                        </div>
                                    </div>
                                ) : isPassed ? (
                                    <div className="flex items-center gap-3">
                                        <CheckCircle2 className="w-5 h-5 text-emerald-600" />
                                        <div>
                                            <p className="text-emerald-900 font-semibold">✓ Validation Passed Successfully</p>
                                            <p className="text-emerald-700 text-sm mt-1">
                                                {totalRecords > 0 ? (
                                                    <>
                                                        All {totalRecords.toLocaleString()} records meet the quality standards.
                                                        Pass rate of {Number(passRate).toFixed(2)}%
                                                        {threshold > 0 ? ` exceeds the threshold of ${threshold}%` : ''}.
                                                    </>
                                                ) : (
                                                    'Validation completed successfully with no issues detected.'
                                                )}
                                            </p>
                                        </div>
                                    </div>
                                ) : (
                                    <div className="flex items-center gap-3">
                                        <AlertTriangle className="w-5 h-5 text-amber-600" />
                                        <div>
                                            <p className="text-amber-900 font-semibold">⚠ Warning</p>
                                            <p className="text-amber-700 text-sm mt-1">
                                                Check completed with warnings. Pass rate: {Number(passRate).toFixed(2)}%
                                            </p>
                                        </div>
                                    </div>
                                )}
                            </div>

                            {/* Detailed Statistics */}
                            <div className="grid grid-cols-3 gap-3">
                                <StatCard label="Threshold" value={`${threshold}%`} icon={<Hash className="w-4 h-4" />} />
                                <StatCard label="Duration" value={`${Number(duration).toFixed(2)}s`} icon={<Clock className="w-4 h-4" />} />
                                <StatCard label="Run ID" value={runId.substring(0, 20) + '...'} icon={<Database className="w-4 h-4" />} />
                            </div>

                            {/* Additional Details */}
                            {(() => {
                                const ignoreKeys = ['STATUS', 'CHECK_STATUS', 'FAILURE_REASON', 'ERROR_MESSAGE', 'ERROR', 'CHECK_OUTPUT',
                                    'RULE_NAME', 'EXECUTED_AT', 'RULE', 'SUCCESS', 'SP_RUN_CUSTOM_RULE', 'run_id', 'RUN_ID',
                                    'pass_rate', 'PASS_RATE', 'total_records', 'TOTAL_RECORDS', 'valid_records', 'VALID_RECORDS',
                                    'invalid_records', 'INVALID_RECORDS', 'threshold', 'THRESHOLD', 'column_name', 'COLUMN_NAME',
                                    'duration_seconds', 'DURATION_SECONDS', 'records_processed', 'status'];
                                const itemsToDisplay: { label: string; value: any }[] = [];

                                if (details) {
                                    Object.entries(details).forEach(([key, value]) => {
                                        if (ignoreKeys.includes(key) || ignoreKeys.includes(key.toUpperCase())) return;
                                        if (typeof value === 'object' && value !== null) return;
                                        itemsToDisplay.push({ label: key, value });
                                    });
                                }

                                if (itemsToDisplay.length === 0) return null;

                                return (
                                    <div>
                                        <h4 className="text-sm font-semibold text-slate-700 mb-3">Additional Metrics</h4>
                                        <div className="grid grid-cols-4 gap-3">
                                            {itemsToDisplay.map((item, i) => (
                                                <div key={i} className="bg-slate-50 p-3 rounded-md border border-slate-200">
                                                    <p className="text-[10px] text-slate-400 font-bold uppercase tracking-wider mb-1">
                                                        {item.label.replace(/_/g, ' ')}
                                                    </p>
                                                    <p className="text-sm font-semibold text-slate-700 break-all">
                                                        {item.value !== null && item.value !== undefined ? String(item.value) : '-'}
                                                    </p>
                                                </div>
                                            ))}
                                        </div>
                                    </div>
                                );
                            })()}
                        </div>

                        {/* Footer */}
                        <div className="px-6 py-3 bg-slate-50 border-t border-slate-200">
                            <div className="flex items-center justify-between text-sm">
                                <div className="flex items-center gap-2 text-slate-500">
                                    <Clock className="w-4 h-4" />
                                    <span>Executed:</span>
                                    <span className="font-mono text-slate-700 font-medium">
                                        {(() => {
                                            if (!check.executedAt) return '-';
                                            const date = new Date(check.executedAt);
                                            return !isNaN(date.getTime()) ? format(date, 'MMM d, yyyy h:mm:ss a') : '-';
                                        })()}
                                    </span>
                                </div>
                                <Badge variant="outline" className="text-xs">
                                    Run Mode: ADHOC
                                </Badge>
                            </div>
                        </div>
                    </Card>
                );
            })}
        </div>
    );
}

function StatCard({ label, value, icon }: { label: string; value: string; icon: React.ReactNode }) {
    return (
        <div className="bg-slate-50 p-3 rounded-lg border border-slate-200">
            <div className="flex items-center gap-2 mb-1">
                <div className="text-slate-500">{icon}</div>
                <span className="text-xs font-bold text-slate-500 uppercase">{label}</span>
            </div>
            <p className="text-sm font-semibold text-slate-900 truncate" title={value}>{value}</p>
        </div>
    );
}
