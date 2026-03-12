'use client';

/**
 * Executive Answer Block Component
 * Renders structured AI responses in business-first format.
 * 
 * Sections:
 * - 🔎 What Happened (factual summary)
 * - 📌 Why It Happened (root cause)
 * - ⚠️ What Is Impacted (business risk)
 * - ✅ What Should Be Done (recommendations)
 * - 🧾 Evidence (collapsible metrics, SQL, chart)
 */

import React, { useState } from 'react';
import { ChevronDown, ChevronRight, Database, BarChart2, Copy, Check, AlertTriangle } from 'lucide-react';

export interface ExecutiveAnswer {
    whatHappened: string;
    whyItHappened: string;
    whatIsImpacted: string;
    whatShouldBeDone: string;
    evidence: {
        metrics: { label: string; value: string }[];
        sql?: string;
        chartData?: any[];
        chartConfig?: {
            type: string;
            xAxis: string;
            yAxis: string;
            color: string;
        };
    };
    severity: 'INFO' | 'WARNING' | 'CRITICAL';
}

interface ExecutiveAnswerBlockProps {
    answer: ExecutiveAnswer;
    renderChart?: () => React.ReactNode;
}

export function ExecutiveAnswerBlock({ answer, renderChart }: ExecutiveAnswerBlockProps) {
    const [showEvidence, setShowEvidence] = useState(false);
    const [showSql, setShowSql] = useState(false);
    const [showChart, setShowChart] = useState(false);
    const [copied, setCopied] = useState(false);

    const getSeverityStyles = () => {
        switch (answer.severity) {
            case 'CRITICAL':
                return 'border-l-red-500 bg-red-50/30';
            case 'WARNING':
                return 'border-l-amber-500 bg-amber-50/30';
            default:
                return 'border-l-emerald-500 bg-emerald-50/30';
        }
    };

    const handleCopySql = async () => {
        if (answer.evidence.sql) {
            await navigator.clipboard.writeText(answer.evidence.sql);
            setCopied(true);
            setTimeout(() => setCopied(false), 2000);
        }
    };

    // Strict Mode Error Display
    if (answer.whyItHappened && (answer.whyItHappened.includes('GEMINI_PLAN_ERROR') || answer.whyItHappened.includes('SCHEMA_VALIDATION_ERROR'))) {
        return (
            <div className="rounded-lg border-l-4 border-l-red-500 bg-red-50 p-4 space-y-4">
                <div className="flex items-center gap-2">
                    <AlertTriangle className="w-5 h-5 text-red-600" />
                    <h4 className="font-semibold text-red-800">Analysis Failed</h4>
                </div>
                <p className="text-red-700 text-sm">{answer.whatHappened}</p>

                <div className="text-xs text-red-600 font-mono bg-red-100 p-2 rounded">
                    {answer.whyItHappened}
                </div>

                {answer.whatShouldBeDone && (
                    <div className="text-sm text-red-700 mt-2">
                        <strong>Types of check:</strong> {answer.whatShouldBeDone}
                    </div>
                )}
            </div>
        );
    }

    return (
        <div className={`rounded-lg border-l-4 ${getSeverityStyles()} p-4 space-y-4`}>
            {/* 🔎 What Happened */}
            <section>
                <div className="flex items-center gap-2 mb-2">
                    <span className="text-lg">🔎</span>
                    <h4 className="font-semibold text-slate-800 text-sm uppercase tracking-wide">
                        What Happened
                    </h4>
                </div>
                <p className="text-slate-700 text-sm leading-relaxed pl-7">
                    {answer.whatHappened}
                </p>
            </section>

            {/* 📌 Why It Happened */}
            <section>
                <div className="flex items-center gap-2 mb-2">
                    <span className="text-lg">📌</span>
                    <h4 className="font-semibold text-slate-800 text-sm uppercase tracking-wide">
                        Why It Happened
                    </h4>
                </div>
                <p className="text-slate-700 text-sm leading-relaxed pl-7">
                    {answer.whyItHappened}
                </p>
            </section>

            {/* ⚠️ What Is Impacted */}
            <section>
                <div className="flex items-center gap-2 mb-2">
                    <span className="text-lg">⚠️</span>
                    <h4 className="font-semibold text-slate-800 text-sm uppercase tracking-wide">
                        What Is Impacted
                    </h4>
                </div>
                <p className="text-slate-700 text-sm leading-relaxed pl-7">
                    {answer.whatIsImpacted}
                </p>
            </section>

            {/* ✅ What Should Be Done */}
            <section>
                <div className="flex items-center gap-2 mb-2">
                    <span className="text-lg">✅</span>
                    <h4 className="font-semibold text-slate-800 text-sm uppercase tracking-wide">
                        What Should Be Done
                    </h4>
                </div>
                <p className="text-slate-700 text-sm leading-relaxed pl-7">
                    {answer.whatShouldBeDone}
                </p>
            </section>

            {/* 🧾 Evidence (Collapsible) */}
            <section className="border-t border-slate-200 pt-3">
                <button
                    onClick={() => setShowEvidence(!showEvidence)}
                    className="flex items-center gap-2 text-slate-600 hover:text-slate-800 transition-colors w-full"
                >
                    {showEvidence ? (
                        <ChevronDown className="w-4 h-4" />
                    ) : (
                        <ChevronRight className="w-4 h-4" />
                    )}
                    <span className="text-lg">🧾</span>
                    <h4 className="font-semibold text-sm uppercase tracking-wide">
                        Evidence
                    </h4>
                    <span className="text-xs text-slate-400 ml-2">
                        {showEvidence ? 'Click to collapse' : 'Click to expand'}
                    </span>
                </button>

                {showEvidence && (
                    <div className="mt-3 pl-7 space-y-3">
                        {/* Metrics Summary */}
                        {answer.evidence.metrics && answer.evidence.metrics.length > 0 && (
                            <div className="space-y-1">
                                {answer.evidence.metrics.map((metric, idx) => (
                                    <div key={idx} className="flex items-center gap-2 text-sm">
                                        <span className="text-slate-500">•</span>
                                        <span className="text-slate-600">{metric.label}:</span>
                                        <span className="font-medium text-slate-800">{metric.value}</span>
                                    </div>
                                ))}
                            </div>
                        )}

                        {/* SQL Toggle */}
                        {answer.evidence.sql && (
                            <div className="border rounded-lg overflow-hidden">
                                <div className="flex items-center justify-between w-full px-3 py-2 bg-slate-100">
                                    <button
                                        onClick={() => setShowSql(!showSql)}
                                        className="flex items-center gap-2 text-sm text-slate-600 hover:text-slate-800 transition-colors flex-1"
                                    >
                                        <Database className="w-4 h-4" />
                                        <span>SQL Query</span>
                                        {showSql ? (
                                            <ChevronDown className="w-4 h-4 text-slate-400" />
                                        ) : (
                                            <ChevronRight className="w-4 h-4 text-slate-400" />
                                        )}
                                    </button>
                                    <button
                                        onClick={handleCopySql}
                                        className="text-slate-400 hover:text-slate-600 p-1 ml-2"
                                        title="Copy SQL"
                                    >
                                        {copied ? (
                                            <Check className="w-4 h-4 text-emerald-500" />
                                        ) : (
                                            <Copy className="w-4 h-4" />
                                        )}
                                    </button>
                                </div>
                                {showSql && (
                                    <pre className="bg-slate-900 text-slate-100 p-3 text-xs overflow-x-auto max-h-48">
                                        {answer.evidence.sql}
                                    </pre>
                                )}
                            </div>
                        )}

                        {/* Chart Toggle (Optional) */}
                        {answer.evidence.chartData && answer.evidence.chartData.length > 0 && renderChart && (
                            <div className="border rounded-lg overflow-hidden">
                                <button
                                    onClick={() => setShowChart(!showChart)}
                                    className="flex items-center justify-between w-full px-3 py-2 bg-slate-100 hover:bg-slate-200 transition-colors"
                                >
                                    <div className="flex items-center gap-2 text-sm text-slate-600">
                                        <BarChart2 className="w-4 h-4" />
                                        <span>View Chart (Optional)</span>
                                    </div>
                                    {showChart ? (
                                        <ChevronDown className="w-4 h-4 text-slate-400" />
                                    ) : (
                                        <ChevronRight className="w-4 h-4 text-slate-400" />
                                    )}
                                </button>
                                {showChart && (
                                    <div className="p-4 bg-white">
                                        {renderChart()}
                                    </div>
                                )}
                            </div>
                        )}
                    </div>
                )}
            </section>
        </div>
    );
}

