'use client';

/**
 * Antigravity Investigation Component
 * Full investigation mode panel with guided questions and evidence.
 * 
 * UX Design:
 * - Explicit "Analyze Question" button (no auto-execute)
 * - Intent preview before execution
 * - Executive Answer Block format (text-first)
 * - Charts are optional/collapsible evidence
 */

import React, { useState, useRef, useEffect } from 'react';
import { Card } from '@/components/ui/card';
import { Button } from '@/components/ui/button';
import {
    Dialog,
    DialogContent,
    DialogHeader,
    DialogTitle,
} from '@/components/ui/dialog';
import {
    Search, Loader2, BarChart2, TrendingUp, GitBranch,
    ChevronRight, AlertTriangle, Database, Sparkles, Send
} from 'lucide-react';
import {
    ResponsiveContainer,
    LineChart,
    Line,
    BarChart,
    Bar,
    XAxis,
    YAxis,
    CartesianGrid,
    Tooltip,
    ReferenceLine,
} from 'recharts';
import { ExecutiveAnswerBlock, ExecutiveAnswer } from './ExecutiveAnswerBlock';
import { createExecutiveAnswerFromLegacy } from '@/lib/antigravity/utils';

interface Question {
    key: string;
    label: string;
    description: string;
    chartType: 'LINE' | 'BAR' | 'AREA' | 'DAG' | 'NONE';
}

interface IntentPreview {
    type: string;
    metrics: string[];
    timeWindow: string;
    confidence: number;
    interpretedSummary: string;
}

interface AntigravityInvestigationProps {
    database: string;
    schema: string;
    table: string;
    insightType: string;
    insightSummary: string;
    isOpen: boolean;
    onClose: () => void;
}

export function AntigravityInvestigation({
    database,
    schema,
    table,
    insightType,
    insightSummary,
    isOpen,
    onClose,
}: AntigravityInvestigationProps) {
    const [sessionId, setSessionId] = useState<string | null>(null);
    const [questions, setQuestions] = useState<Question[]>([]);
    const [isLoading, setIsLoading] = useState(false);
    const [activeQuestion, setActiveQuestion] = useState<string | null>(null);
    const [evidenceHistory, setEvidenceHistory] = useState<any[]>([]);

    // Manual question state
    const [questionText, setQuestionText] = useState('');
    const [intentPreview, setIntentPreview] = useState<IntentPreview | null>(null);
    const [isPreviewLoading, setIsPreviewLoading] = useState(false);
    const [executiveAnswer, setExecutiveAnswer] = useState<ExecutiveAnswer | null>(null);
    const [chartData, setChartData] = useState<any>(null);
    const [aiProvider, setAiProvider] = useState<'gemini' | 'ollama'>('gemini');
    const [ollamaStatus, setOllamaStatus] = useState<'checking' | 'online' | 'offline'>('checking');
    const debounceRef = useRef<NodeJS.Timeout | null>(null);

    // Check Ollama health when provider switches to ollama
    useEffect(() => {
        if (aiProvider === 'ollama') {
            checkOllamaHealth();
        }
    }, [aiProvider]);

    const checkOllamaHealth = async () => {
        setOllamaStatus('checking');
        try {
            const res = await fetch('/api/antigravity/ollama-health');
            if (res.ok) {
                setOllamaStatus('online');
            } else {
                setOllamaStatus('offline');
            }
        } catch {
            setOllamaStatus('offline');
        }
    };



    // Start investigation when dialog opens
    useEffect(() => {
        if (isOpen && !sessionId) {
            startInvestigation();
        }
    }, [isOpen]);

    // Debounced intent preview while typing
    useEffect(() => {
        if (debounceRef.current) {
            clearTimeout(debounceRef.current);
        }

        if (questionText.trim().length >= 5) {
            debounceRef.current = setTimeout(() => {
                fetchIntentPreview(questionText);
            }, 500);
        } else {
            setIntentPreview(null);
        }

        return () => {
            if (debounceRef.current) {
                clearTimeout(debounceRef.current);
            }
        };
    }, [questionText]);

    const fetchIntentPreview = async (text: string) => {
        setIsPreviewLoading(true);
        try {
            const response = await fetch('/api/antigravity/ask-question', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ question: text, previewOnly: true }),
            });
            const result = await response.json();
            if (result.success && result.data.previewOnly) {
                setIntentPreview({
                    type: result.data.intent.type,
                    metrics: result.data.intent.metrics,
                    timeWindow: result.data.intent.timeWindow,
                    confidence: result.data.intent.confidence,
                    interpretedSummary: result.data.interpretedSummary,
                });
            }
        } catch (err) {
            console.error('Intent preview error:', err);
        } finally {
            setIsPreviewLoading(false);
        }
    };

    // Execute question with explicit button click
    const executeQuestion = async () => {
        if (!questionText.trim()) return;

        setIsLoading(true);
        setExecutiveAnswer(null);
        setChartData(null);

        try {
            const response = await fetch('/api/antigravity/ask-question', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ question: questionText, database, schema, table, provider: aiProvider }),
            });
            const result = await response.json();

            if (result.success) {
                // Transform to Executive Answer format: Use backend interpretation if valid
                const answer = result.data.interpretation || createExecutiveAnswerFromLegacy(
                    result.data.data || [],
                    { intentType: result.data.intent.type, metrics: result.data.intent.metrics },
                    result.data.sql,
                    result.data.chart
                );
                setExecutiveAnswer(answer);

                // Store chart data for optional display
                if (result.data.chart && result.data.data?.length > 0) {
                    setChartData({
                        type: result.data.chart.type,
                        title: result.data.chart.title,
                        xAxis: result.data.chart.xAxis,
                        yAxis: result.data.chart.yAxis,
                        color: result.data.chart.color,
                        data: result.data.data,
                    });
                }

                setEvidenceHistory(prev => [...prev, result.data]);
            } else {
                setExecutiveAnswer({
                    whatHappened: 'Unable to process your question.',
                    whyItHappened: result.error || 'An error occurred during analysis.',
                    whatIsImpacted: 'No analysis could be performed.',
                    whatShouldBeDone: 'Please try rephrasing your question or select a guided question.',
                    evidence: { metrics: [] },
                    severity: 'WARNING',
                });
            }
        } catch (err) {
            console.error('Question execution error:', err);
            setExecutiveAnswer({
                whatHappened: 'Failed to connect to the analysis service.',
                whyItHappened: 'A network or server error occurred.',
                whatIsImpacted: 'No analysis could be performed.',
                whatShouldBeDone: 'Please check your connection and try again.',
                evidence: { metrics: [] },
                severity: 'CRITICAL',
            });
        } finally {
            setIsLoading(false);
        }
    };

    const startInvestigation = async () => {
        setIsLoading(true);
        try {
            const response = await fetch('/api/antigravity/start-investigation', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ database, schema, table, insightType }),
            });
            const result = await response.json();
            if (result.success) {
                setSessionId(result.data.session.sessionId);
                setQuestions(result.data.questions);
            }
        } catch (err) {
            console.error('Failed to start investigation:', err);
        } finally {
            setIsLoading(false);
        }
    };

    const askGuidedQuestion = async (questionKey: string, label: string) => {
        setActiveQuestion(questionKey);
        setQuestionText(label);
        setIsLoading(true);
        setExecutiveAnswer(null);
        setChartData(null);

        try {
            const response = await fetch('/api/antigravity/get-evidence', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    sessionId,
                    database,
                    schema,
                    table,
                    questionKey,
                    insightType,
                }),
            });
            const result = await response.json();
            if (result.success) {
                const answer = createExecutiveAnswerFromLegacy(
                    result.data.chart?.data || [],
                    { intentType: 'TREND', metrics: ['quality'] },
                    result.data.sql,
                    result.data.chart
                );
                setExecutiveAnswer(answer);

                if (result.data.chart?.data?.length > 0) {
                    setChartData(result.data.chart);
                }

                setEvidenceHistory(prev => [...prev, result.data]);
            }
        } catch (err) {
            console.error('Failed to get evidence:', err);
        } finally {
            setIsLoading(false);
        }
    };

    const renderChart = () => {
        if (!chartData?.data?.length) return null;

        if (chartData.type === 'LINE') {
            return (
                <ResponsiveContainer width="100%" height={220}>
                    <LineChart data={chartData.data}>
                        <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                        <XAxis
                            dataKey={chartData.xAxis}
                            tick={{ fontSize: 11 }}
                            tickFormatter={(val) => {
                                if (typeof val === 'string' && val.includes('T')) {
                                    return new Date(val).toLocaleDateString();
                                }
                                return String(val).substring(0, 10);
                            }}
                        />
                        <YAxis tick={{ fontSize: 11 }} />
                        <Tooltip contentStyle={{ fontSize: 12 }} />
                        {chartData.threshold && (
                            <ReferenceLine
                                y={chartData.threshold}
                                stroke="#ef4444"
                                strokeDasharray="5 5"
                            />
                        )}
                        <Line
                            type="monotone"
                            dataKey={chartData.yAxis}
                            stroke={chartData.color || '#6366f1'}
                            strokeWidth={2}
                            dot={{ r: 3 }}
                        />
                    </LineChart>
                </ResponsiveContainer>
            );
        }

        if (chartData.type === 'BAR') {
            return (
                <ResponsiveContainer width="100%" height={220}>
                    <BarChart data={chartData.data}>
                        <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                        <XAxis dataKey={chartData.xAxis} tick={{ fontSize: 10 }} />
                        <YAxis tick={{ fontSize: 11 }} />
                        <Tooltip contentStyle={{ fontSize: 12 }} />
                        <Bar dataKey={chartData.yAxis} fill={chartData.color || '#6366f1'} radius={[4, 4, 0, 0]} />
                    </BarChart>
                </ResponsiveContainer>
            );
        }

        return null;
    };

    const getQuestionIcon = (chartType: string) => {
        switch (chartType) {
            case 'LINE':
                return <TrendingUp className="w-4 h-4" />;
            case 'BAR':
                return <BarChart2 className="w-4 h-4" />;
            case 'DAG':
                return <GitBranch className="w-4 h-4" />;
            default:
                return <Search className="w-4 h-4" />;
        }
    };

    const getIntentChipColor = (intentType: string) => {
        switch (intentType) {
            case 'ROOT_CAUSE':
                return 'bg-red-100 text-red-700 border-red-200';
            case 'TREND':
                return 'bg-blue-100 text-blue-700 border-blue-200';
            case 'COMPARISON':
                return 'bg-purple-100 text-purple-700 border-purple-200';
            case 'IMPACT':
                return 'bg-amber-100 text-amber-700 border-amber-200';
            case 'DISTRIBUTION':
                return 'bg-emerald-100 text-emerald-700 border-emerald-200';
            default:
                return 'bg-slate-100 text-slate-700 border-slate-200';
        }
    };

    return (
        <Dialog open={isOpen} onOpenChange={(open) => !open && onClose()}>
            <DialogContent className="max-w-5xl max-h-[90vh] overflow-hidden flex flex-col">
                <DialogHeader className="border-b pb-4">
                    <div className="flex items-center justify-between">
                        <div>
                            <DialogTitle className="flex items-center gap-2">
                                <Sparkles className="w-5 h-5 text-indigo-600" />
                                Antigravity Investigation
                            </DialogTitle>
                            <p className="text-sm text-slate-500 mt-1">
                                {database}.{schema}.{table}
                            </p>
                        </div>
                        <div className="flex items-center gap-2">
                            <span className={`px-2 py-1 rounded-full text-xs font-medium ${insightType === 'CRITICAL' ? 'bg-red-100 text-red-700' :
                                insightType === 'WARNING' ? 'bg-amber-100 text-amber-700' :
                                    'bg-blue-100 text-blue-700'
                                }`}>
                                {insightType}
                            </span>
                        </div>
                    </div>
                    {insightSummary && (
                        <div className="mt-2 p-3 bg-slate-50 rounded-lg text-sm text-slate-700">
                            {insightSummary}
                        </div>
                    )}
                </DialogHeader>

                <div className="flex-1 overflow-hidden flex gap-4 py-4">
                    {/* Left Panel - Question Input */}
                    <div className="w-1/3 space-y-4 overflow-auto pr-2">
                        {/* Manual Question Input with Explicit Submit */}
                        <Card className="p-4 bg-gradient-to-br from-indigo-50/50 to-purple-50/50 border-indigo-100">
                            <h4 className="text-sm font-semibold text-slate-800 mb-3 flex items-center gap-2">
                                <Sparkles className="w-4 h-4 text-indigo-600" />
                                Ask Anything
                            </h4>

                            <div className="space-y-3">
                                <input
                                    type="text"
                                    value={questionText}
                                    onChange={(e) => setQuestionText(e.target.value)}
                                    placeholder="e.g., Why is the data stale?"
                                    className="w-full px-3 py-2 text-sm border border-slate-200 rounded-lg focus:ring-2 focus:ring-indigo-300 focus:border-indigo-300"
                                    onKeyDown={(e) => {
                                        if (e.key === 'Enter' && !e.shiftKey) {
                                            e.preventDefault();
                                            executeQuestion();
                                        }
                                    }}
                                />

                                {/* Intent Preview */}
                                {intentPreview && (
                                    <div className="p-2 bg-white rounded-lg border border-slate-100 space-y-2">
                                        <div className="flex items-center gap-2 flex-wrap">
                                            <span className={`px-2 py-0.5 text-xs font-medium rounded-full border ${getIntentChipColor(intentPreview.type)}`}>
                                                {intentPreview.type.replace('_', ' ')}
                                            </span>
                                            {intentPreview.metrics.map((m, i) => (
                                                <span key={i} className="px-2 py-0.5 text-xs bg-slate-100 text-slate-600 rounded-full">
                                                    {m.replace('_', ' ')}
                                                </span>
                                            ))}
                                        </div>
                                        <p className="text-xs text-slate-500">
                                            <span className="font-medium">Interpreted as:</span>{' '}
                                            {intentPreview.interpretedSummary}
                                        </p>
                                    </div>
                                )}

                                {isPreviewLoading && (
                                    <div className="flex items-center gap-2 text-xs text-slate-400">
                                        <Loader2 className="w-3 h-3 animate-spin" />
                                        Interpreting...
                                    </div>
                                )}

                                {/* Analyze Button */}
                                <div className="space-y-3">
                                    <div className="flex items-center justify-between px-1">
                                        <div className="flex flex-col">
                                            <label className="text-xs font-medium text-slate-500">AI Provider</label>
                                            {aiProvider === 'ollama' && (
                                                <span className={`text-[10px] flex items-center gap-1 ${ollamaStatus === 'online' ? 'text-emerald-600' : 'text-red-500'}`}>
                                                    <span className={`w-1.5 h-1.5 rounded-full ${ollamaStatus === 'online' ? 'bg-emerald-500' : 'bg-red-500 animate-pulse'}`}></span>
                                                    {ollamaStatus === 'checking' ? 'Checking...' :
                                                        ollamaStatus === 'online' ? 'Online' : 'Offline'}
                                                </span>
                                            )}
                                        </div>
                                        <div className="flex bg-slate-100 p-0.5 rounded-lg">
                                            <button
                                                onClick={() => setAiProvider('gemini')}
                                                className={`px-2 py-0.5 text-[10px] font-medium rounded-md transition-all ${aiProvider === 'gemini'
                                                    ? 'bg-white text-indigo-600 shadow-sm'
                                                    : 'text-slate-500 hover:text-slate-700'
                                                    }`}
                                            >
                                                Gemini (Fast)
                                            </button>
                                            <button
                                                onClick={() => setAiProvider('ollama')}
                                                className={`px-2 py-0.5 text-[10px] font-medium rounded-md transition-all ${aiProvider === 'ollama'
                                                    ? 'bg-white text-emerald-600 shadow-sm'
                                                    : 'text-slate-500 hover:text-slate-700'
                                                    }`}
                                            >
                                                Local (Private)
                                            </button>
                                        </div>
                                    </div>

                                    {/* Warning for Offline Ollama */}
                                    {aiProvider === 'ollama' && ollamaStatus === 'offline' && (
                                        <div className="text-[10px] text-red-600 bg-red-50 px-2 py-1 rounded border border-red-200">
                                            ⚠️ Ensure Ollama is running (`ollama serve`).
                                            <button onClick={checkOllamaHealth} className="ml-1 underline font-medium">Retry</button>
                                        </div>
                                    )}

                                    <Button
                                        onClick={executeQuestion}
                                        disabled={isLoading || !questionText.trim() || (aiProvider === 'ollama' && ollamaStatus === 'offline')}
                                        className={`w-full text-white transition-all ${aiProvider === 'gemini'
                                            ? 'bg-indigo-600 hover:bg-indigo-700'
                                            : ollamaStatus === 'offline'
                                                ? 'bg-slate-400 cursor-not-allowed'
                                                : 'bg-emerald-600 hover:bg-emerald-700'
                                            }`}
                                    >
                                        {isLoading ? (
                                            <>
                                                <Loader2 className="w-4 h-4 mr-2 animate-spin" />
                                                Analyzing with {aiProvider === 'gemini' ? 'Gemini' : 'Ollama'}...
                                            </>
                                        ) : (
                                            <>
                                                <Send className="w-4 h-4 mr-2" />
                                                Ask {aiProvider === 'gemini' ? 'Gemini' : 'Local AI'}
                                            </>
                                        )}
                                    </Button>
                                </div>

                                {/* Quick Tags */}
                                <div className="flex gap-1 flex-wrap">
                                    {['Freshness', 'Quality', 'Nulls', 'Trend'].map(tag => (
                                        <button
                                            key={tag}
                                            type="button"
                                            onClick={() => {
                                                const questions: Record<string, string> = {
                                                    Freshness: 'Why is the data stale?',
                                                    Quality: 'How has quality changed this week?',
                                                    Nulls: 'Which columns have the most nulls?',
                                                    Trend: 'Show me the quality trend',
                                                };
                                                setQuestionText(questions[tag] || '');
                                            }}
                                            className="text-xs px-2 py-1 bg-white border border-slate-200 rounded-full hover:bg-indigo-50 hover:border-indigo-200 transition-colors"
                                        >
                                            {tag}
                                        </button>
                                    ))}
                                </div>
                            </div>
                        </Card>

                        {/* Guided Questions */}
                        <div className="border-t border-slate-200 pt-3">
                            <h4 className="text-xs font-semibold text-slate-500 uppercase tracking-wide mb-3">
                                Guided Questions
                            </h4>
                            {isLoading && questions.length === 0 ? (
                                <div className="flex items-center gap-2 text-slate-400 text-sm">
                                    <Loader2 className="w-4 h-4 animate-spin" />
                                    Loading questions...
                                </div>
                            ) : (
                                questions.map((q) => (
                                    <button
                                        key={q.key}
                                        onClick={() => askGuidedQuestion(q.key, q.label)}
                                        disabled={isLoading}
                                        className={`w-full text-left p-3 rounded-lg border transition-all mb-2 ${activeQuestion === q.key
                                            ? 'bg-indigo-50 border-indigo-300 shadow-sm'
                                            : 'bg-white border-slate-200 hover:border-indigo-200 hover:bg-indigo-50/50'
                                            }`}
                                    >
                                        <div className="flex items-start gap-2">
                                            <div className={`mt-0.5 ${activeQuestion === q.key ? 'text-indigo-600' : 'text-slate-400'}`}>
                                                {getQuestionIcon(q.chartType)}
                                            </div>
                                            <div className="flex-1">
                                                <p className="text-sm font-medium text-slate-800">{q.label}</p>
                                                <p className="text-xs text-slate-500 mt-0.5">{q.description}</p>
                                            </div>
                                            <ChevronRight className={`w-4 h-4 ml-auto mt-1 transition-transform ${activeQuestion === q.key ? 'text-indigo-600 translate-x-1' : 'text-slate-300'}`} />
                                        </div>
                                    </button>
                                ))
                            )}
                        </div>
                    </div>

                    {/* Right Panel - Executive Answer */}
                    <div className="flex-1 border-l pl-4 overflow-auto">
                        {!executiveAnswer && !isLoading ? (
                            <div className="h-full flex items-center justify-center text-slate-400">
                                <div className="text-center">
                                    <Search className="w-12 h-12 mx-auto mb-3 opacity-30" />
                                    <p className="font-medium">Ask a question to get insights</p>
                                    <p className="text-sm mt-1">Type your question and click "Analyze"</p>
                                </div>
                            </div>
                        ) : isLoading ? (
                            <div className="h-full flex items-center justify-center">
                                <div className="text-center">
                                    <Loader2 className="w-8 h-8 mx-auto mb-3 text-indigo-500 animate-spin" />
                                    <p className="text-slate-600">Analyzing your question...</p>
                                    <p className="text-sm text-slate-400 mt-1">Generating business insights</p>
                                </div>
                            </div>
                        ) : executiveAnswer && (
                            <div className="space-y-4">
                                <div className="flex items-center gap-2 text-sm text-slate-500">
                                    <Database className="w-4 h-4" />
                                    <span>Analysis for: "{questionText}"</span>
                                </div>
                                <ExecutiveAnswerBlock
                                    answer={executiveAnswer}
                                    renderChart={chartData?.data?.length > 0 ? renderChart : undefined}
                                />
                            </div>
                        )}
                    </div>
                </div>

                {/* Footer */}
                <div className="border-t pt-4 flex items-center justify-between">
                    <p className="text-xs text-slate-400">
                        {evidenceHistory.length} questions answered in this session
                    </p>
                    <Button variant="outline" onClick={onClose}>
                        Close Investigation
                    </Button>
                </div>
            </DialogContent>
        </Dialog>
    );
}
