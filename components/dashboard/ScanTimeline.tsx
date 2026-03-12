import React from 'react';
import { formatISTShort } from '@/lib/timezone-utils';
import {
    CheckCircle2,
    XCircle,
    Clock,
    Zap,
    Search,
    AlertTriangle,
    ArrowUp,
    ArrowDown,
    Database,
    FileText
} from 'lucide-react';
import { calculateRunDeltas } from '@/lib/utils/deltaCalculator';
import { generateInsight } from '@/lib/utils/insightGenerator';
import { DimensionChip } from './DimensionChip';
import { MetricRow } from './MetricRow';
import { InsightPanel } from './InsightPanel';
import type { ScanRun } from '@/lib/utils/deltaCalculator';

interface ScanTimelineProps {
    runs: ScanRun[];
    isLoading?: boolean;
}

export function ScanTimeline({ runs, isLoading }: ScanTimelineProps) {
    if (isLoading) {
        return <div className="animate-pulse space-y-4 p-6">
            <div className="h-32 bg-slate-100 rounded-lg"></div>
            <div className="h-32 bg-slate-100 rounded-lg"></div>
        </div>;
    }

    if (!runs || runs.length === 0) {
        return null;
    }

    const runsWithDeltas = calculateRunDeltas(runs);

    return (
        <div className="bg-white border border-slate-200 rounded-lg shadow-sm p-6 mt-8">
            <h2 className="text-lg font-semibold text-slate-900 mb-6 flex items-center gap-2">
                <Clock className="w-5 h-5 text-indigo-600" />
                Today's Scan Activity
            </h2>

            <div className="relative pl-6 border-l-2 border-slate-100 space-y-8">
                {runsWithDeltas.map((run, index) => {
                    const previousRun = runs[index + 1] || null;
                    const insight = generateInsight(run, previousRun, run.delta);

                    return (
                        <div key={run.runId} className="relative">
                            <div className={`absolute -left-[33px] top-4 w-4 h-4 rounded-full border-2 
                                ${run.slaMet ? 'bg-green-100 border-green-500' : 'bg-red-100 border-red-500'}`}
                            />

                            <div className="bg-slate-50 border border-slate-200 rounded-lg p-5 hover:border-slate-300 transition-colors">
                                <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-3 mb-4 pb-4 border-b border-slate-200">
                                    <div className="flex items-center gap-3">
                                        <span className="text-sm font-semibold text-slate-700 font-mono">
                                            {formatISTShort(run.runTime).split(',')[1]?.trim() || 'Unknown Time'} IST
                                        </span>

                                        <span className={`px-2 py-0.5 rounded text-[10px] font-bold uppercase tracking-wide border flex items-center gap-1
                                            ${run.runType === 'INCREMENTAL'
                                                ? 'bg-indigo-50 text-indigo-700 border-indigo-200'
                                                : 'bg-slate-100 text-slate-700 border-slate-200'}`}>
                                            {run.runType === 'INCREMENTAL' ? <Zap className="w-3 h-3" /> : <Search className="w-3 h-3" />}
                                            {run.runType}
                                        </span>
                                    </div>

                                    <div className="flex items-center gap-2">
                                        <span className="px-2 py-0.5 rounded text-xs font-semibold bg-slate-100 text-slate-700 border border-slate-200">
                                            Grade {run.qualityGrade}
                                        </span>
                                        <span className={`px-2 py-0.5 rounded text-xs font-semibold border
                                            ${run.trustLevel === 'HIGH' ? 'bg-green-50 text-green-700 border-green-200' :
                                                run.trustLevel === 'MEDIUM' ? 'bg-amber-50 text-amber-700 border-amber-200' :
                                                    'bg-red-50 text-red-700 border-red-200'}`}>
                                            {run.trustLevel}
                                        </span>
                                    </div>
                                </div>

                                <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
                                    <div className="space-y-4">
                                        <div className="mb-4">
                                            <div className="text-xs text-slate-500 mb-1">Overall Data Quality Score</div>
                                            <div className="flex items-center gap-3">
                                                <span className={`text-3xl font-bold ${run.overallScore >= 90 ? 'text-green-600' :
                                                        run.overallScore >= 80 ? 'text-blue-500' :
                                                            run.overallScore >= 70 ? 'text-amber-500' :
                                                                'text-red-500'
                                                    }`}>
                                                    {run.overallScore}%
                                                </span>
                                                {run.delta && (
                                                    <span className={`text-sm font-semibold flex items-center ${run.delta.isPositive ? 'text-green-600' : 'text-red-500'
                                                        }`}>
                                                        {run.delta.isPositive ? <ArrowUp className="w-4 h-4" /> : <ArrowDown className="w-4 h-4" />}
                                                        {Math.abs(run.delta.overall).toFixed(1)}%
                                                    </span>
                                                )}
                                            </div>
                                            <div className="mt-2 h-1.5 bg-slate-200 rounded-full overflow-hidden">
                                                <div
                                                    className={`h-full transition-all ${run.overallScore >= 90 ? 'bg-green-500' :
                                                            run.overallScore >= 80 ? 'bg-blue-500' :
                                                                run.overallScore >= 70 ? 'bg-amber-500' :
                                                                    'bg-red-500'
                                                        }`}
                                                    style={{ width: `${run.overallScore}%` }}
                                                />
                                            </div>
                                        </div>

                                        <div className="space-y-2">
                                            <div>
                                                <h4 className="text-xs font-semibold text-slate-700 uppercase tracking-wide mb-1">Execution Metrics</h4>
                                                <div className="space-y-0.5">
                                                    <MetricRow icon={Database} label="Datasets Evaluated" value={run.datasetsScanned} />
                                                    <MetricRow icon={CheckCircle2} label="Rules Executed" value={run.uniqueChecks} />
                                                    <MetricRow icon={XCircle} label="Failed Rules" value={run.failedChecks} critical={run.failedChecks > 0} />
                                                </div>
                                            </div>
                                            <div>
                                                <h4 className="text-xs font-semibold text-slate-700 uppercase tracking-wide mb-1">Record Metrics</h4>
                                                <div className="space-y-0.5">
                                                    <MetricRow icon={FileText} label="Total Records Evaluated" value={run.totalRecords} />
                                                    <MetricRow icon={AlertTriangle} label="Failed Records" value={run.failedRecordsCount} critical={run.failedRecordsCount > 0} />
                                                </div>
                                            </div>
                                        </div>
                                    </div>

                                    <div className="space-y-3">
                                        <h4 className="text-xs font-semibold text-slate-700 uppercase tracking-wide">Quality Dimensions</h4>
                                        <div className="grid grid-cols-2 gap-2">
                                            <DimensionChip
                                                name="Completeness Score"
                                                score={run.dimensions.completeness}
                                                delta={run.delta?.dimensions.completeness || null}
                                            />
                                            <DimensionChip
                                                name="Validity Score"
                                                score={run.dimensions.validity}
                                                delta={run.delta?.dimensions.validity || null}
                                            />
                                            <DimensionChip
                                                name="Uniqueness Score"
                                                score={run.dimensions.uniqueness}
                                                delta={run.delta?.dimensions.uniqueness || null}
                                            />
                                            <DimensionChip
                                                name="Consistency Score"
                                                score={run.dimensions.consistency}
                                                delta={run.delta?.dimensions.consistency || null}
                                            />
                                            <DimensionChip
                                                name="Freshness Score"
                                                score={run.dimensions.freshness}
                                                delta={run.delta?.dimensions.freshness || null}
                                            />
                                            <DimensionChip
                                                name="Volume Score"
                                                score={run.dimensions.volume}
                                                delta={run.delta?.dimensions.volume || null}
                                            />
                                        </div>
                                    </div>

                                    <div>
                                        <h4 className="text-xs font-semibold text-slate-700 uppercase tracking-wide mb-3">Quality Insight</h4>
                                        <InsightPanel
                                            insight={insight}
                                            slaMet={run.slaMet}
                                            passedChecks={run.passedChecks}
                                            failedChecks={run.failedChecks}
                                            warningChecks={run.warningChecks}
                                        />
                                    </div>
                                </div>
                            </div>
                        </div>
                    );
                })}
            </div>
        </div>
    );
}
