'use client';

import { useState } from 'react';
import { Card, CardHeader, CardTitle, CardContent } from '@/components/ui/card';
import { Area, AreaChart, ResponsiveContainer, Tooltip, XAxis, YAxis, CartesianGrid } from 'recharts';
import { ArrowUp, ArrowDown, Activity, Ruler, Sigma, ChevronDown, ChevronRight } from 'lucide-react';

interface ColumnStatisticsProps {
    history: any[];
    stats?: {
        min?: number | string;
        max?: number | string;
        avg?: number;
        stdDev?: number;
        distinctCount?: number;
        rowCount?: number;
    } | null;
}

export function ColumnStatistics({ history, stats }: ColumnStatisticsProps) {
    const [showAdvanced, setShowAdvanced] = useState(false);

    const formatDate = (ts: string) => {
        return new Date(ts).toLocaleDateString(undefined, { month: 'short', day: 'numeric' });
    };

    const chartData = history?.map(h => ({
        ...h,
        completeness: h.rowCount > 0 ? ((1 - (h.nullCount / h.rowCount)) * 100).toFixed(1) : 0,
        uniqueness: h.rowCount > 0 ? ((h.distinctCount / h.rowCount) * 100).toFixed(1) : 0
    })) || [];

    const validStats = [
        { label: 'Minimum', value: stats?.min, icon: <ArrowDown className="w-3 h-3 text-red-500" /> },
        { label: 'Maximum', value: stats?.max, icon: <ArrowUp className="w-3 h-3 text-emerald-500" /> },
        { label: 'Average', value: stats?.avg?.toFixed(2), icon: <Ruler className="w-3 h-3 text-blue-500" /> },
        { label: 'Std Dev', value: stats?.stdDev?.toFixed(2), icon: <Sigma className="w-3 h-3 text-purple-500" /> }
    ].filter(stat => stat.value != null);

    if (validStats.length === 0 && chartData.length === 0) return null;

    return (
        <div className="space-y-6">
            {/* Descriptive Statistics — always visible */}
            {validStats.length > 0 && (
                <div className="space-y-4">
                    <h4 className="text-sm font-bold text-slate-900 uppercase tracking-wide flex items-center gap-2">
                        <Activity className="w-4 h-4 text-indigo-500" />
                        Distribution Insights
                    </h4>
                    <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                        {validStats.map((stat, i) => (
                            <StatCard key={i} label={stat.label} value={stat.value} icon={stat.icon} />
                        ))}
                    </div>
                </div>
            )}

            {/* Advanced Insights — collapsible */}
            {chartData.length > 0 && (
                <div className="border border-slate-200 rounded-lg overflow-hidden">
                    <button
                        onClick={() => setShowAdvanced(!showAdvanced)}
                        className="w-full flex items-center justify-between px-4 py-3 bg-slate-50 hover:bg-slate-100 transition-colors"
                    >
                        <span className="text-sm font-bold text-slate-700 uppercase tracking-wide flex items-center gap-2">
                            <Activity className="w-4 h-4 text-slate-400" />
                            Advanced Insights — Behavioral Trends
                        </span>
                        {showAdvanced
                            ? <ChevronDown className="w-4 h-4 text-slate-400" />
                            : <ChevronRight className="w-4 h-4 text-slate-400" />
                        }
                    </button>

                    {showAdvanced && (
                        <div className="p-4">
                            <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                                {/* Completeness Trend */}
                                <Card>
                                    <CardHeader className="p-4 pb-2 border-b border-slate-50/50">
                                        <CardTitle className="text-xs font-semibold text-slate-500 uppercase flex justify-between items-center">
                                            Completeness History
                                            <span className="text-[10px] font-normal text-emerald-600 bg-emerald-50 px-2 py-0.5 rounded-full">Higher is better</span>
                                        </CardTitle>
                                    </CardHeader>
                                    <CardContent className="p-4 h-48">
                                        <ResponsiveContainer width="100%" height="100%">
                                            <AreaChart data={chartData}>
                                                <defs>
                                                    <linearGradient id="colorCompleteness" x1="0" y1="0" x2="0" y2="1">
                                                        <stop offset="5%" stopColor="#10b981" stopOpacity={0.2} />
                                                        <stop offset="95%" stopColor="#10b981" stopOpacity={0} />
                                                    </linearGradient>
                                                </defs>
                                                <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="#f1f5f9" />
                                                <Tooltip
                                                    contentStyle={{ fontSize: '12px', borderRadius: '4px', border: 'none', boxShadow: '0 4px 6px -1px rgb(0 0 0 / 0.1)' }}
                                                    labelFormatter={formatDate}
                                                    formatter={(value: any) => [`${value}%`, 'Completeness']}
                                                />
                                                <XAxis dataKey="timestamp" hide />
                                                <YAxis hide domain={[0, 100]} />
                                                <Area
                                                    type="monotone"
                                                    dataKey="completeness"
                                                    stroke="#10b981"
                                                    fillOpacity={1}
                                                    fill="url(#colorCompleteness)"
                                                    strokeWidth={2}
                                                />
                                            </AreaChart>
                                        </ResponsiveContainer>
                                    </CardContent>
                                </Card>

                                {/* Uniqueness/Cardinality Trend */}
                                <Card>
                                    <CardHeader className="p-4 pb-2 border-b border-slate-50/50">
                                        <CardTitle className="text-xs font-semibold text-slate-500 uppercase">Distinct Values History</CardTitle>
                                    </CardHeader>
                                    <CardContent className="p-4 h-48">
                                        <ResponsiveContainer width="100%" height="100%">
                                            <AreaChart data={chartData}>
                                                <defs>
                                                    <linearGradient id="colorDistinct" x1="0" y1="0" x2="0" y2="1">
                                                        <stop offset="5%" stopColor="#6366f1" stopOpacity={0.2} />
                                                        <stop offset="95%" stopColor="#6366f1" stopOpacity={0} />
                                                    </linearGradient>
                                                </defs>
                                                <CartesianGrid strokeDasharray="3 3" vertical={false} stroke="#f1f5f9" />
                                                <Tooltip
                                                    contentStyle={{ fontSize: '12px', borderRadius: '4px', border: 'none', boxShadow: '0 4px 6px -1px rgb(0 0 0 / 0.1)' }}
                                                    labelFormatter={formatDate}
                                                    formatter={(value: any) => [value, 'Distinct Values']}
                                                />
                                                <XAxis dataKey="timestamp" hide />
                                                <YAxis hide />
                                                <Area
                                                    type="monotone"
                                                    dataKey="distinctCount"
                                                    stroke="#6366f1"
                                                    fillOpacity={1}
                                                    fill="url(#colorDistinct)"
                                                    strokeWidth={2}
                                                />
                                            </AreaChart>
                                        </ResponsiveContainer>
                                    </CardContent>
                                </Card>
                            </div>
                        </div>
                    )}
                </div>
            )}
        </div>
    );
}

function StatCard({ label, value, icon }: { label: string, value: any, icon?: React.ReactNode }) {
    return (
        <Card className="p-4 border-slate-200 shadow-sm bg-slate-50/50">
            <div className="flex items-center gap-2 mb-1">
                {icon}
                <span className="text-xs text-slate-500 font-medium uppercase">{label}</span>
            </div>
            <div className="text-lg font-bold text-slate-900 font-mono">
                {value ?? '-'}
            </div>
        </Card>
    );
}
