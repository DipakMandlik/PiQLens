'use client';

/**
 * Antigravity Insights Component
 * Displays AI-generated observability insights for an asset.
 * Embedded in the Observability tab.
 */

import React, { useState, useEffect } from 'react';
import { Card } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import { Sparkles, AlertTriangle, CheckCircle, Info, RefreshCw, Loader2, Search } from 'lucide-react';
import { AntigravityInvestigation } from './AntigravityInvestigation';

interface Insight {
    insightId?: string;
    type: string;
    summary: string;
    details?: {
        bullets: string[];
    };
    bullets?: string[];
    severity: 'INFO' | 'WARNING' | 'CRITICAL';
    isActionable: boolean;
    createdAt?: string;
}

interface AntigravityInsightsProps {
    database: string;
    schema: string;
    table: string;
}

export function AntigravityInsights({ database, schema, table }: AntigravityInsightsProps) {
    const [insights, setInsights] = useState<Insight[]>([]);
    const [isLoading, setIsLoading] = useState(false);
    const [isGenerating, setIsGenerating] = useState(false);
    const [error, setError] = useState<string | null>(null);
    const [lastGenerated, setLastGenerated] = useState<Insight | null>(null);

    // Investigation mode state
    const [isInvestigating, setIsInvestigating] = useState(false);
    const [investigationInsight, setInvestigationInsight] = useState<Insight | null>(null);
    const [lastProvider, setLastProvider] = useState<string>('AI');

    // Fetch existing insights on mount
    useEffect(() => {
        fetchInsights();
    }, [database, schema, table]);

    const fetchInsights = async () => {
        setIsLoading(true);
        setError(null);
        try {
            const response = await fetch(
                `/api/antigravity/get-insights?database=${encodeURIComponent(database)}&schema=${encodeURIComponent(schema)}&table=${encodeURIComponent(table)}`
            );
            const result = await response.json();
            if (result.success) {
                setInsights(result.data.insights || []);
            }
        } catch (err: any) {
            console.log('[AntigravityInsights] Could not fetch insights:', err.message);
        } finally {
            setIsLoading(false);
        }
    };

    const generateNewInsight = async () => {
        setIsGenerating(true);
        setError(null);
        try {
            const response = await fetch('/api/antigravity/generate-insights', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ database, schema, table }),
            });
            const result = await response.json();
            if (result.success) {
                setLastGenerated(result.data.insight);
                setLastProvider(result.data.provider === 'ollama' ? 'Ollama' : 'Gemini');
                await fetchInsights();
            } else {
                setError(result.error || 'Failed to generate insight');
            }
        } catch (err: any) {
            setError(err.message || 'Failed to generate insight');
        } finally {
            setIsGenerating(false);
        }
    };

    const startInvestigation = (insight: Insight) => {
        setInvestigationInsight(insight);
        setIsInvestigating(true);
    };

    const getSeverityIcon = (severity: string) => {
        switch (severity) {
            case 'CRITICAL':
                return <AlertTriangle className="w-5 h-5 text-red-500" />;
            case 'WARNING':
                return <AlertTriangle className="w-5 h-5 text-amber-500" />;
            default:
                return <CheckCircle className="w-5 h-5 text-emerald-500" />;
        }
    };

    const getSeverityStyle = (severity: string) => {
        switch (severity) {
            case 'CRITICAL':
                return 'border-red-200 bg-red-50';
            case 'WARNING':
                return 'border-amber-200 bg-amber-50';
            default:
                return 'border-emerald-200 bg-emerald-50';
        }
    };

    const getTypeLabel = (type: string) => {
        const labels: Record<string, string> = {
            ANOMALY: '🔍 Anomaly',
            TREND: '📈 Trend',
            SCHEMA_CHANGE: '🔧 Schema Change',
            IMPACT: '⚡ Impact',
            FRESHNESS: '⏰ Freshness',
            QUALITY: '✅ Quality',
        };
        return labels[type] || type;
    };

    return (
        <>
            <Card className="p-6 bg-gradient-to-br from-indigo-50/50 to-purple-50/50 border border-indigo-100">
                {/* Header */}
                <div className="flex items-center justify-between mb-4">
                    <div className="flex items-center gap-2">
                        <Sparkles className="w-5 h-5 text-indigo-600" />
                        <h3 className="font-semibold text-slate-800">Antigravity AI Insights</h3>
                        <span className="text-xs bg-indigo-100 text-indigo-700 px-2 py-0.5 rounded-full" id="ai-provider-badge">
                            Powered by {lastProvider}
                        </span>
                    </div>
                    <Button
                        size="sm"
                        onClick={generateNewInsight}
                        disabled={isGenerating}
                        className="bg-indigo-600 hover:bg-indigo-700 text-white"
                    >
                        {isGenerating ? (
                            <>
                                <Loader2 className="w-4 h-4 mr-1 animate-spin" />
                                Analyzing...
                            </>
                        ) : (
                            <>
                                <Sparkles className="w-4 h-4 mr-1" />
                                Analyze Now
                            </>
                        )}
                    </Button>
                </div>

                {/* Error State */}
                {error && (
                    <div className="mb-4 p-3 bg-red-50 border border-red-200 rounded-lg text-red-700 text-sm">
                        {error}
                    </div>
                )}

                {/* Loading State */}
                {isLoading && (
                    <div className="flex items-center justify-center py-8">
                        <Loader2 className="w-6 h-6 text-indigo-500 animate-spin" />
                        <span className="ml-2 text-slate-500">Loading insights...</span>
                    </div>
                )}

                {/* Latest Generated Insight (Highlighted) */}
                {lastGenerated && (
                    <div className={`mb-4 p-4 rounded-lg border-2 ${getSeverityStyle(lastGenerated.severity)} border-dashed`}>
                        <div className="flex items-start gap-3">
                            {getSeverityIcon(lastGenerated.severity)}
                            <div className="flex-1">
                                <div className="flex items-center gap-2 mb-1">
                                    <span className="text-xs font-medium text-slate-500">{getTypeLabel(lastGenerated.type)}</span>
                                    <span className="text-xs text-indigo-600 font-medium">Just generated</span>
                                </div>
                                <p className="font-medium text-slate-800">{lastGenerated.summary}</p>
                                {lastGenerated.bullets && lastGenerated.bullets.length > 0 && (
                                    <ul className="mt-2 space-y-1">
                                        {lastGenerated.bullets.map((bullet, idx) => (
                                            <li key={idx} className="text-sm text-slate-600 flex items-start gap-2">
                                                <span className="text-indigo-400 mt-1">•</span>
                                                {bullet}
                                            </li>
                                        ))}
                                    </ul>
                                )}
                                <button
                                    onClick={() => startInvestigation(lastGenerated)}
                                    className="mt-3 flex items-center gap-1 text-xs font-medium text-indigo-600 hover:text-indigo-800"
                                >
                                    <Search className="w-3 h-3" />
                                    Drill Down
                                </button>
                            </div>
                        </div>
                    </div>
                )}

                {/* Stored Insights List */}
                {!isLoading && insights.length === 0 && !lastGenerated && (
                    <div className="text-center py-8">
                        <Info className="w-10 h-10 text-slate-300 mx-auto mb-2" />
                        <p className="text-slate-500 text-sm">No insights yet. Click "Analyze Now" to generate.</p>
                        <p className="text-slate-400 text-xs mt-1">
                            AI will analyze quality scores, freshness, and anomalies.
                        </p>
                    </div>
                )}

                {insights.length > 0 && (
                    <div className="space-y-3">
                        <div className="text-xs font-medium text-slate-500 uppercase tracking-wide">
                            Recent Insights
                        </div>
                        {insights.slice(0, 5).map((insight, idx) => (
                            <div
                                key={insight.insightId || idx}
                                className={`p-3 rounded-lg border ${getSeverityStyle(insight.severity)}`}
                            >
                                <div className="flex items-start gap-2">
                                    {getSeverityIcon(insight.severity)}
                                    <div className="flex-1">
                                        <div className="flex items-center gap-2 mb-1">
                                            <span className="text-xs font-medium text-slate-500">{getTypeLabel(insight.type)}</span>
                                            {insight.createdAt && (
                                                <span className="text-xs text-slate-400">
                                                    {new Date(insight.createdAt).toLocaleString()}
                                                </span>
                                            )}
                                        </div>
                                        <p className="text-sm font-medium text-slate-700">{insight.summary}</p>
                                        {(insight.details?.bullets || insight.bullets) && (
                                            <ul className="mt-1 space-y-0.5">
                                                {(insight.details?.bullets || insight.bullets || []).slice(0, 2).map((bullet, bidx) => (
                                                    <li key={bidx} className="text-xs text-slate-600">
                                                        • {bullet}
                                                    </li>
                                                ))}
                                            </ul>
                                        )}
                                        <button
                                            onClick={() => startInvestigation(insight)}
                                            className="mt-2 flex items-center gap-1 text-xs font-medium text-indigo-600 hover:text-indigo-800"
                                        >
                                            <Search className="w-3 h-3" />
                                            Drill Down
                                        </button>
                                    </div>
                                </div>
                            </div>
                        ))}
                    </div>
                )}

                {/* Footer */}
                <div className="mt-4 pt-3 border-t border-indigo-100 flex items-center justify-between">
                    <p className="text-xs text-slate-400">
                        All insights are SQL-verified and traceable to metrics.
                    </p>
                    <button
                        onClick={fetchInsights}
                        className="text-xs text-indigo-600 hover:text-indigo-800 flex items-center gap-1"
                    >
                        <RefreshCw className="w-3 h-3" />
                        Refresh
                    </button>
                </div>
            </Card>

            {/* Investigation Modal */}
            <AntigravityInvestigation
                database={database}
                schema={schema}
                table={table}
                insightType={investigationInsight?.type || 'QUALITY'}
                insightSummary={investigationInsight?.summary || ''}
                isOpen={isInvestigating}
                onClose={() => {
                    setIsInvestigating(false);
                    setInvestigationInsight(null);
                }}
            />
        </>
    );
}

