import React from 'react';
import { DataCatalogTable } from '@/types/catalog';
import { GitBranch, Database, GitMerge, ArrowRight, Table2 } from 'lucide-react';

export default function TableLineage({ data }: { data: DataCatalogTable }) {
    // In a real implementation with Snowflake OBJECT_DEPENDENCIES, 
    // upstream and downstream would be populated.
    // For now, we simulate the visualization structure
    const hasDependencies = data.upstream.length > 0 || data.downstream.length > 0;

    if (!hasDependencies) {
        return (
            <div className="flex flex-col items-center justify-center p-12 bg-white dark:bg-zinc-950 border border-zinc-200 dark:border-zinc-800 rounded-xl shadow-sm h-full">
                <GitBranch className="w-16 h-16 text-zinc-300 dark:text-zinc-700 mb-4" />
                <h3 className="text-xl font-bold text-zinc-700 dark:text-zinc-300">No Lineage Metadata</h3>
                <p className="text-zinc-500 dark:text-zinc-400 max-w-md text-center mt-2">
                    This table has no recorded upstream sources or downstream dependencies in the Snowflake dependency graph.
                </p>
            </div>
        );
    }

    return (
        <div className="space-y-8 bg-white dark:bg-zinc-950 border border-zinc-200 dark:border-zinc-800 rounded-xl shadow-sm p-8 min-h-[400px]">
            <div className="flex items-center gap-2 mb-8">
                <GitMerge className="w-6 h-6 text-indigo-500" />
                <h2 className="text-xl font-bold text-zinc-900 dark:text-zinc-100">πQLens Lineage Explorer</h2>
            </div>

            <div className="flex flex-col md:flex-row items-center justify-center gap-8 md:gap-4 relative w-full overflow-x-auto px-4 py-8">

                {/* Upstream */}
                <div className="flex flex-col gap-4 w-64 shrink-0">
                    <h4 className="text-sm font-semibold uppercase text-zinc-500 text-center mb-2">Upstream Sources ({data.upstream.length})</h4>
                    {data.upstream.map((src, i) => (
                        <div key={i} className="bg-zinc-50 dark:bg-zinc-900 border border-zinc-200 dark:border-zinc-700 rounded-lg p-3 text-sm font-medium flex items-center gap-2 shadow-sm relative z-10 transition-transform hover:-translate-y-1">
                            <Database className="w-4 h-4 text-blue-500 shrink-0" />
                            <span className="truncate" title={src}>{src}</span>
                        </div>
                    ))}
                    {data.upstream.length === 0 && (
                        <div className="text-sm text-center text-zinc-400 italic py-4 border-2 border-dashed border-zinc-200 dark:border-zinc-800 rounded-lg">
                            No Upstream Sources
                        </div>
                    )}
                </div>

                {/* Visual Connector */}
                <div className="hidden md:flex items-center text-zinc-300 dark:text-zinc-700 w-16 shrink-0 justify-center">
                    <ArrowRight className="w-6 h-6" />
                </div>

                {/* Current Table (Center) */}
                <div className="flex flex-col gap-2 w-72 shrink-0 z-10 relative">
                    <h4 className="text-sm font-bold uppercase text-blue-600 dark:text-blue-400 text-center mb-2">Current Dataset</h4>
                    <div className="bg-white dark:bg-zinc-950 border-2 border-blue-500 rounded-xl p-5 shadow-lg shadow-blue-500/10 flex flex-col items-center">
                        <Table2 className="w-10 h-10 text-blue-600 dark:text-blue-400 mb-3" />
                        <h3 className="font-bold text-zinc-900 dark:text-zinc-100 text-center break-all">{data.table}</h3>
                        <span className="text-xs font-semibold text-zinc-500 mt-1 uppercase tracking-wider">{data.schema}</span>
                    </div>
                </div>

                {/* Visual Connector */}
                <div className="hidden md:flex items-center text-zinc-300 dark:text-zinc-700 w-16 shrink-0 justify-center">
                    <ArrowRight className="w-6 h-6" />
                </div>

                {/* Downstream */}
                <div className="flex flex-col gap-4 w-64 shrink-0">
                    <h4 className="text-sm font-semibold uppercase text-zinc-500 text-center mb-2">Downstream Impacts ({data.downstream.length})</h4>
                    {data.downstream.map((dest, i) => (
                        <div key={i} className="bg-zinc-50 dark:bg-zinc-900 border border-zinc-200 dark:border-zinc-700 rounded-lg p-3 text-sm font-medium flex items-center gap-2 shadow-sm relative z-10 transition-transform hover:-translate-y-1">
                            <Table2 className="w-4 h-4 text-emerald-500 shrink-0" />
                            <span className="truncate" title={dest}>{dest}</span>
                        </div>
                    ))}
                    {data.downstream.length === 0 && (
                        <div className="text-sm text-center text-zinc-400 italic py-4 border-2 border-dashed border-zinc-200 dark:border-zinc-800 rounded-lg">
                            No Downstream Impacts
                        </div>
                    )}
                </div>

            </div>
        </div>
    );
}
