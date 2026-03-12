import React from 'react';
import { DataCatalogTable } from '@/types/catalog';
import { ShieldCheck, Target, AlertTriangle, CheckCircle2, XCircle, Activity, Award, Clock } from 'lucide-react';

export default function TableDataQuality({ data }: { data: DataCatalogTable }) {

    const hasDqData = data.dqScore !== null;

    if (!hasDqData) {
        return (
            <div className="flex flex-col items-center justify-center py-16 bg-white dark:bg-zinc-950 border border-zinc-200 dark:border-zinc-800 rounded-xl shadow-sm">
                <div className="p-4 bg-blue-50 rounded-full mb-4">
                    <Clock className="w-8 h-8 text-blue-400" />
                </div>
                <h3 className="text-base font-bold text-zinc-700 dark:text-zinc-300">Awaiting First Scan</h3>
                <p className="text-sm text-zinc-500 dark:text-zinc-400 max-w-sm text-center mt-1.5">
                    This table is under data quality monitoring but hasn&apos;t been scanned yet. Scores will appear after the next scheduled run.
                </p>
            </div>
        );
    }

    return (
        <div className="space-y-4">
            <div className="flex items-center justify-between">
                <h2 className="text-sm font-semibold text-zinc-900 dark:text-zinc-100 flex items-center gap-2 uppercase tracking-wider">
                    <Activity className="w-4 h-4 text-blue-500" />
                    Data Quality Intelligence
                </h2>
                <span className="text-[11px] font-medium text-zinc-400 bg-white border border-zinc-200 px-2.5 py-1 rounded-full shadow-sm">
                    Latest Daily Summary
                </span>
            </div>

            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-3">
                {/* Health Score */}
                <div className="bg-white dark:bg-zinc-950 border border-zinc-200 dark:border-zinc-800 rounded-xl p-5 shadow-sm flex flex-col items-center justify-center text-center relative overflow-hidden">
                    <div className="absolute top-0 inset-x-0 h-0.5 bg-gradient-to-r from-blue-400 to-indigo-500"></div>
                    <span className="text-[10px] font-semibold uppercase tracking-widest text-zinc-400 mb-1.5">Health Score</span>
                    <div className="text-4xl font-black text-zinc-900 dark:text-zinc-100 tracking-tight">
                        {data.dqScore}%
                    </div>
                    <div className="mt-2 flex items-center gap-1 text-xs font-semibold">
                        {data.dqScore && data.dqScore >= 90 ? (
                            <span className="text-emerald-600 flex items-center gap-1"><CheckCircle2 className="w-3.5 h-3.5" /> Excellent</span>
                        ) : data.dqScore && data.dqScore >= 70 ? (
                            <span className="text-amber-600 flex items-center gap-1"><AlertTriangle className="w-3.5 h-3.5" /> Needs Attention</span>
                        ) : (
                            <span className="text-red-600 flex items-center gap-1"><XCircle className="w-3.5 h-3.5" /> Critical</span>
                        )}
                    </div>
                </div>

                {/* Failure Rate */}
                <div className="bg-white dark:bg-zinc-950 border border-zinc-200 dark:border-zinc-800 rounded-xl p-5 shadow-sm">
                    <div className="flex items-center gap-2.5 mb-3">
                        <div className="bg-red-50 p-2 rounded-lg">
                            <AlertTriangle className="w-4 h-4 text-red-500" />
                        </div>
                        <span className="font-semibold text-sm text-zinc-700">Failure Rate</span>
                    </div>
                    <div className="text-2xl font-bold text-zinc-900">
                        {data.failureRate !== null ? `${data.failureRate.toFixed(2)}%` : '0%'}
                    </div>
                    <span className="text-[10px] text-zinc-400 mt-1.5 font-medium block">Records failing validation</span>
                </div>

                {/* Quality Grade */}
                <div className="bg-white dark:bg-zinc-950 border border-zinc-200 dark:border-zinc-800 rounded-xl p-5 shadow-sm">
                    <div className="flex items-center gap-2.5 mb-3">
                        <div className="bg-amber-50 p-2 rounded-lg">
                            <Award className="w-4 h-4 text-amber-500" />
                        </div>
                        <span className="font-semibold text-sm text-zinc-700">Quality Grade</span>
                    </div>
                    <div className="text-2xl font-bold text-zinc-900">
                        {data.qualityGrade || 'N/A'}
                    </div>
                    <span className="text-[10px] text-zinc-400 mt-1.5 font-medium block">Enterprise data tiering</span>
                </div>

                {/* SLA Status */}
                <div className="bg-white dark:bg-zinc-950 border border-zinc-200 dark:border-zinc-800 rounded-xl p-5 shadow-sm">
                    <div className="flex items-center gap-2.5 mb-3">
                        <div className="bg-emerald-50 p-2 rounded-lg">
                            <Target className="w-4 h-4 text-emerald-500" />
                        </div>
                        <span className="font-semibold text-sm text-zinc-700">SLA Compliance</span>
                    </div>
                    <div className="flex items-center gap-2">
                        {data.slaMet ? (
                            <span className="inline-flex items-center gap-1.5 text-xl font-bold text-emerald-600">
                                <CheckCircle2 className="w-5 h-5" /> MET
                            </span>
                        ) : (
                            <span className="inline-flex items-center gap-1.5 text-xl font-bold text-red-600">
                                <XCircle className="w-5 h-5" /> BREACHED
                            </span>
                        )}
                    </div>
                    <span className="text-[10px] text-zinc-400 mt-1.5 font-medium block">Service level agreement</span>
                </div>
            </div>
        </div>
    );
}
