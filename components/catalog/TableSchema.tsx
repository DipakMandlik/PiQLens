import React, { useState } from 'react';
import { DataCatalogTable } from '@/types/catalog';
import { Search, Key, Hash, AlignLeft, Calendar, FileText, ToggleLeft } from 'lucide-react';

export default function TableSchema({ data }: { data: DataCatalogTable }) {
    const [search, setSearch] = useState('');

    const filteredColumns = data.columns.filter(col =>
        col.name.toLowerCase().includes(search.toLowerCase()) ||
        (col.comment && col.comment.toLowerCase().includes(search.toLowerCase()))
    );

    const getDataTypeIcon = (type: string) => {
        const t = type.toUpperCase();
        if (t.includes('VARCHAR') || t.includes('STRING') || t.includes('TEXT')) return <AlignLeft className="w-4 h-4 text-blue-500" />;
        if (t.includes('NUMBER') || t.includes('INT') || t.includes('FLOAT') || t.includes('DECIMAL')) return <Hash className="w-4 h-4 text-emerald-500" />;
        if (t.includes('DATE') || t.includes('TIME')) return <Calendar className="w-4 h-4 text-purple-500" />;
        if (t.includes('BOOLEAN')) return <ToggleLeft className="w-4 h-4 text-amber-500" />;
        return <FileText className="w-4 h-4 text-zinc-500" />;
    };

    const isPrimaryKey = (colName: string) => {
        return data.constraints.some(c => c.constraintType === 'PRIMARY KEY' && c.columnName === colName);
    };

    return (
        <div className="bg-white dark:bg-zinc-950 border border-zinc-200 dark:border-zinc-800 rounded-xl shadow-sm overflow-hidden flex flex-col h-full">
            <div className="p-4 border-b border-zinc-200 dark:border-zinc-800 flex items-center justify-between bg-zinc-50/50 dark:bg-zinc-900/30">
                <h3 className="font-semibold text-zinc-900 dark:text-zinc-100 flex items-center gap-2">
                    Schema Architecture
                    <span className="px-2 py-0.5 rounded-full bg-zinc-200 dark:bg-zinc-800 text-xs text-zinc-700 dark:text-zinc-300 ml-2">
                        {data.columns.length} columns
                    </span>
                </h3>
                <div className="relative w-64">
                    <input
                        type="text"
                        placeholder="Search columns..."
                        value={search}
                        onChange={e => setSearch(e.target.value)}
                        className="w-full pl-9 pr-3 py-1.5 text-sm rounded-lg border border-zinc-200 dark:border-zinc-800 bg-white dark:bg-zinc-900 focus:ring-2 focus:ring-blue-500 focus:border-blue-500 transition-all outline-none"
                    />
                    <Search className="w-4 h-4 text-zinc-400 absolute left-3 top-1/2 -translate-y-1/2 pointer-events-none" />
                </div>
            </div>

            <div className="overflow-x-auto flex-1 custom-scrollbar">
                <table className="w-full text-sm text-left whitespace-nowrap">
                    <thead className="text-xs uppercase bg-zinc-50 dark:bg-zinc-900 text-zinc-500 dark:text-zinc-400 border-b border-zinc-200 dark:border-zinc-800 sticky top-0 z-10 shadow-sm">
                        <tr>
                            <th className="px-6 py-4 font-semibold w-12 text-center">#</th>
                            <th className="px-6 py-4 font-semibold">Column Name</th>
                            <th className="px-6 py-4 font-semibold">Data Type</th>
                            <th className="px-6 py-4 font-semibold text-center">Nullable</th>
                            <th className="px-6 py-4 font-semibold">Classification / Comments</th>
                        </tr>
                    </thead>
                    <tbody className="divide-y divide-zinc-100 dark:divide-zinc-800/60">
                        {filteredColumns.map((col, idx) => {
                            const pk = isPrimaryKey(col.name);
                            return (
                                <tr key={col.name} className="hover:bg-zinc-50 dark:hover:bg-zinc-900/50 transition-colors group">
                                    <td className="px-6 py-3 text-center text-zinc-400 dark:text-zinc-500 font-mono text-xs">
                                        {col.ordinalPosition}
                                    </td>
                                    <td className="px-6 py-3 font-medium text-zinc-900 dark:text-zinc-100 flex items-center gap-2">
                                        {col.name}
                                        {pk && <Key className="w-3.5 h-3.5 text-amber-500 inline-block shrink-0" />}
                                    </td>
                                    <td className="px-6 py-3">
                                        <span className="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-md bg-zinc-100 dark:bg-zinc-800 text-zinc-800 dark:text-zinc-300 font-mono text-xs border border-zinc-200 dark:border-zinc-700">
                                            {getDataTypeIcon(col.dataType)}
                                            {col.dataType}
                                        </span>
                                    </td>
                                    <td className="px-6 py-3 text-center">
                                        {col.isNullable ? (
                                            <span className="text-zinc-400 font-medium">Yes</span>
                                        ) : (
                                            <span className="text-zinc-700 dark:text-zinc-300 font-semibold bg-zinc-100 dark:bg-zinc-800 px-2 py-0.5 rounded text-xs">No</span>
                                        )}
                                    </td>
                                    <td className="px-6 py-3 truncate max-w-xs text-zinc-600 dark:text-zinc-400" title={col.comment || ''}>
                                        {col.comment || <span className="opacity-50 italic">No description</span>}
                                    </td>
                                </tr>
                            );
                        })}

                        {filteredColumns.length === 0 && (
                            <tr>
                                <td colSpan={5} className="px-6 py-12 text-center text-zinc-500">
                                    No columns found matching "{search}"
                                </td>
                            </tr>
                        )}
                    </tbody>
                </table>
            </div>
        </div>
    );
}
