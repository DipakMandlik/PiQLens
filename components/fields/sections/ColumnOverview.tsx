'use client';

import { Card } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { Database, FileText, Hash, AlertCircle, Key, ShieldCheck } from 'lucide-react';
import { PieChart, Pie, Cell, ResponsiveContainer } from 'recharts';

interface Governance {
    isPrimaryKey: boolean;
    isNullable: boolean;
    columnDefault: string | null;
}

interface ColumnOverviewProps {
    metadata: {
        columnName: string;
        dataType: string;
        isNullable: boolean;
    };
    stats: {
        rowCount: number;
        nullCount: number;
        distinctCount: number;
    } | null;
    governance?: Governance | null;
}

export function ColumnOverview({ metadata, stats, governance }: ColumnOverviewProps) {
    if (!stats) return null;

    // --- Derived Metrics ---
    const nonNullCount = stats.rowCount - stats.nullCount;
    const completenessPct = stats.rowCount > 0 ? (nonNullCount / stats.rowCount) * 100 : 0;
    const uniquenessPct = nonNullCount > 0 ? (stats.distinctCount / nonNullCount) * 100 : 0;

    const completenessData = [
        { name: 'Valid', value: completenessPct, color: '#10b981' },
        { name: 'Null', value: 100 - completenessPct, color: '#e2e8f0' },
    ];

    const uniquenessData = [
        { name: 'Unique', value: uniquenessPct, color: '#6366f1' },
        { name: 'Recurring', value: 100 - uniquenessPct, color: '#e2e8f0' },
    ];

    const isNullable = governance?.isNullable ?? metadata.isNullable;

    return (
        <div className="space-y-4">
            {/* Row 2: Core Profiling Metrics */}
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">

                {/* 1. Completeness Score */}
                <Card className="p-4 flex flex-col justify-between relative overflow-hidden">
                    <div className="flex justify-between items-start mb-2">
                        <div>
                            <div className="text-xs text-slate-500 font-bold uppercase tracking-wider">Completeness</div>
                            <div className="text-sm text-slate-400">Valid Data Ratio</div>
                        </div>
                        <Badge variant="outline" className="bg-emerald-50 text-emerald-700 border-emerald-200">Business</Badge>
                    </div>
                    <div className="flex items-center gap-4">
                        <div className="h-16 w-16">
                            <ResponsiveContainer width="100%" height="100%">
                                <PieChart>
                                    <Pie
                                        data={completenessData}
                                        cx="50%" cy="50%"
                                        innerRadius={20} outerRadius={30}
                                        startAngle={90} endAngle={-270}
                                        dataKey="value"
                                    >
                                        {completenessData.map((entry, index) => (
                                            <Cell key={`cell-${index}`} fill={entry.color} />
                                        ))}
                                    </Pie>
                                </PieChart>
                            </ResponsiveContainer>
                        </div>
                        <div>
                            <div className="text-3xl font-bold text-slate-900">{completenessPct.toFixed(1)}%</div>
                            <div className="text-xs text-slate-500">Avg. Fill Rate</div>
                        </div>
                    </div>
                </Card>

                {/* 2. Uniqueness Score */}
                <Card className="p-4 flex flex-col justify-between relative overflow-hidden">
                    <div className="flex justify-between items-start mb-2">
                        <div>
                            <div className="text-xs text-slate-500 font-bold uppercase tracking-wider">Uniqueness</div>
                            <div className="text-sm text-slate-400">Distinct Ratio</div>
                        </div>
                        <Badge variant="outline" className="bg-indigo-50 text-indigo-700 border-indigo-200">Technical</Badge>
                    </div>
                    <div className="flex items-center gap-4">
                        <div className="h-16 w-16">
                            <ResponsiveContainer width="100%" height="100%">
                                <PieChart>
                                    <Pie
                                        data={uniquenessData}
                                        cx="50%" cy="50%"
                                        innerRadius={20} outerRadius={30}
                                        startAngle={90} endAngle={-270}
                                        dataKey="value"
                                    >
                                        {uniquenessData.map((entry, index) => (
                                            <Cell key={`cell-${index}`} fill={entry.color} />
                                        ))}
                                    </Pie>
                                </PieChart>
                            </ResponsiveContainer>
                        </div>
                        <div>
                            <div className="text-3xl font-bold text-slate-900">{uniquenessPct.toFixed(1)}%</div>
                            <div className="text-xs text-slate-500">Cardinality</div>
                        </div>
                    </div>
                </Card>

                {/* 3. Data Type + Governance Badges */}
                <Card className="p-4 flex flex-col justify-between">
                    <div className="flex items-center gap-2 mb-2">
                        <div className="p-1.5 bg-slate-100 rounded-md">
                            <Database className="w-4 h-4 text-slate-600" />
                        </div>
                        <span className="text-xs text-slate-500 font-bold uppercase">Data Type</span>
                    </div>
                    <div className="mb-3">
                        <div className="text-2xl font-bold text-slate-900 font-mono tracking-tight">{metadata.dataType}</div>
                    </div>
                    {/* Governance Badges */}
                    <div className="flex flex-wrap gap-1.5">
                        {governance?.isPrimaryKey && (
                            <Badge className="bg-violet-100 text-violet-700 border-violet-200 text-[10px] font-bold gap-1">
                                <Key className="w-3 h-3" /> Primary Key
                            </Badge>
                        )}
                        <Badge variant="outline" className={`text-[10px] font-medium ${isNullable ? 'bg-amber-50 text-amber-600 border-amber-200' : 'bg-emerald-50 text-emerald-600 border-emerald-200'}`}>
                            {isNullable ? 'Nullable' : 'Not Nullable'}
                        </Badge>
                        {governance?.columnDefault && (
                            <Badge variant="outline" className="bg-slate-50 text-slate-600 border-slate-200 text-[10px]">
                                Default: {governance.columnDefault}
                            </Badge>
                        )}
                    </div>
                </Card>

                {/* 4. Total Rows */}
                <MetricCard
                    label="Total Rows"
                    value={stats.rowCount.toLocaleString()}
                    icon={<FileText className="w-4 h-4 text-slate-600" />}
                    bgClass="bg-slate-100"
                />

                {/* 5. Distinct Values */}
                <MetricCard
                    label="Distinct Values"
                    value={stats.distinctCount.toLocaleString()}
                    icon={<Hash className="w-4 h-4 text-emerald-600" />}
                    bgClass="bg-emerald-50"
                />

                {/* 6. Null Count */}
                <MetricCard
                    label="Null Count"
                    value={stats.nullCount.toLocaleString()}
                    icon={<AlertCircle className="w-4 h-4 text-amber-600" />}
                    bgClass="bg-amber-50"
                    valueClass={stats.nullCount > 0 ? "text-amber-600" : "text-slate-900"}
                />
            </div>
        </div>
    );
}

function MetricCard({ label, value, subtext, icon, bgClass, valueClass = "text-slate-900" }: any) {
    return (
        <Card className="p-4 flex flex-col justify-between">
            <div className="flex items-center gap-2 mb-2">
                <div className={`p-1.5 rounded-md ${bgClass}`}>
                    {icon}
                </div>
                <span className="text-xs text-slate-500 font-bold uppercase">{label}</span>
            </div>
            <div>
                <div className={`text-2xl font-bold ${valueClass}`}>{value}</div>
                {subtext && <div className="text-xs text-slate-400">{subtext}</div>}
            </div>
        </Card>
    );
}
