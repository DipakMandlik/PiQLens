'use client';

import { useState } from 'react';
import { Search, AlertCircle, CheckCircle2, AlertTriangle, Hash, Calendar, Type } from 'lucide-react';
import { cn } from '@/lib/utils';
import { Badge } from '@/components/ui/badge';
import { Input } from '@/components/ui/input';

export interface ColumnItem {
    columnName: string;
    dataType: string;
    ordinalPosition: number;
    isNullable: boolean;
    nullPct: number;
    distinctCount: number;
    status: 'Critical' | 'Warning' | 'Healthy';
    failedChecks: number;
    anomalies: number;
    driftEvents: number;
}

interface ColumnListPanelProps {
    columns: ColumnItem[];
    selectedColumn: string | null;
    onSelectColumn: (column: string) => void;
    isLoading: boolean;
}

export function ColumnListPanel({ columns, selectedColumn, onSelectColumn, isLoading }: ColumnListPanelProps) {
    const [searchTerm, setSearchTerm] = useState('');
    const [filter, setFilter] = useState<'All' | 'Critical' | 'Warning'>('All');

    const filteredColumns = columns.filter((col) => {
        const matchesSearch = col.columnName.toLowerCase().includes(searchTerm.toLowerCase());
        const matchesFilter = filter === 'All' || col.status === filter;
        return matchesSearch && matchesFilter;
    });

    const getStatusIcon = (status: string) => {
        switch (status) {
            case 'Critical':
                return <AlertCircle className="w-4 h-4 text-red-500" />;
            case 'Warning':
                return <AlertTriangle className="w-4 h-4 text-amber-500" />;
            default:
                return <CheckCircle2 className="w-4 h-4 text-emerald-500" />;
        }
    };

    const getTypeIcon = (dataType: string) => {
        if (dataType.includes('TIMESTAMP') || dataType.includes('DATE')) return <Calendar className="w-3 h-3 text-slate-400" />;
        if (dataType.includes('NUMBER') || dataType.includes('INT') || dataType.includes('FLOAT')) return <Hash className="w-3 h-3 text-slate-400" />;
        return <Type className="w-3 h-3 text-slate-400" />;
    };

    if (isLoading && columns.length === 0) {
        return (
            <div className="w-80 border-r border-slate-200 bg-white h-full flex flex-col items-center justify-center p-4">
                <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-indigo-600"></div>
                <p className="text-slate-500 text-sm mt-2">Loading columns...</p>
            </div>
        );
    }

    return (
        <div className="w-80 border-r border-slate-200 bg-white h-full flex flex-col">
            {/* Header & Search */}
            <div className="p-4 border-b border-slate-200 space-y-3">
                <div className="flex items-center justify-between">
                    <h3 className="font-semibold text-slate-900">Columns</h3>
                    <Badge variant="secondary" className="text-xs">{columns.length}</Badge>
                </div>

                <div className="relative">
                    <Search className="absolute left-2.5 top-2.5 h-4 w-4 text-slate-400" />
                    <Input
                        placeholder="Search columns..."
                        className="pl-9 h-9 bg-slate-50 border-slate-200 focus:bg-white transition-all"
                        value={searchTerm}
                        onChange={(e: React.ChangeEvent<HTMLInputElement>) => setSearchTerm(e.target.value)}
                    />
                </div>

                {/* Quick Filters */}
                <div className="flex gap-2">
                    {(['All', 'Critical', 'Warning'] as const).map((f) => (
                        <button
                            key={f}
                            onClick={() => setFilter(f)}
                            className={cn(
                                "text-xs px-2.5 py-1 rounded-full border transition-colors",
                                filter === f
                                    ? f === 'Critical' ? "bg-red-50 border-red-200 text-red-700"
                                        : f === 'Warning' ? "bg-amber-50 border-amber-200 text-amber-700"
                                            : "bg-slate-100 border-slate-200 text-slate-700"
                                    : "bg-white border-transparent text-slate-500 hover:bg-slate-50"
                            )}
                        >
                            {f}
                        </button>
                    ))}
                </div>
            </div>

            {/* Column List */}
            <div className="flex-1 overflow-y-auto">
                {filteredColumns.length === 0 ? (
                    <div className="p-8 text-center text-slate-500 text-sm">
                        No {filter !== 'All' ? filter.toLowerCase() : ''} columns found.
                    </div>
                ) : (
                    <div className="divide-y divide-slate-100">
                        {filteredColumns.map((col) => (
                            <button
                                key={col.columnName}
                                onClick={() => onSelectColumn(col.columnName)}
                                className={cn(
                                    "w-full text-left p-3 hover:bg-slate-50 transition-all border-l-4",
                                    selectedColumn === col.columnName
                                        ? "bg-indigo-50/50 border-l-indigo-500"
                                        : "border-l-transparent"
                                )}
                            >
                                <div className="flex items-start justify-between mb-1">
                                    <span className={cn(
                                        "text-sm font-medium truncate max-w-[180px]",
                                        selectedColumn === col.columnName ? "text-indigo-700" : "text-slate-700"
                                    )}>
                                        {col.columnName}
                                    </span>
                                    {getStatusIcon(col.status)}
                                </div>

                                <div className="flex items-center gap-2 text-xs text-slate-500">
                                    <span className="flex items-center gap-1 bg-slate-100 px-1.5 py-0.5 rounded text-[10px] font-mono">
                                        {getTypeIcon(col.dataType)}
                                        {col.dataType}
                                    </span>
                                    {col.nullPct > 0 && (
                                        <span className="text-amber-600 bg-amber-50 px-1.5 py-0.5 rounded ml-auto">
                                            {col.nullPct}% null
                                        </span>
                                    )}
                                </div>
                            </button>
                        ))}
                    </div>
                )}
            </div>
        </div>
    );
}
