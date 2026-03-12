import React from 'react';
import { DataCatalogTable } from '@/types/catalog';
import { Database, Clock, HardDrive, BarChart3, TrendingUp, Key, Tag, ShieldCheck } from 'lucide-react';

function formatBytes(bytes: number): string {
  if (bytes === 0) return '0 B';
  const k = 1024;
  const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(1)) + ' ' + sizes[i];
}

export default function TableOverview({ data }: { data: DataCatalogTable }) {
    return (
        <div className="space-y-4">
            {/* Two column layout: Properties + Governance side by side */}
            <div className="grid grid-cols-1 lg:grid-cols-5 gap-4">
                {/* Technical Properties — 3 cols */}
                <div className="lg:col-span-3 bg-white dark:bg-zinc-950 border border-zinc-200 dark:border-zinc-800 rounded-xl p-5 shadow-sm">
                    <h3 className="text-sm font-semibold text-zinc-900 dark:text-zinc-100 mb-3 flex items-center gap-2 uppercase tracking-wider">
                        <Database className="w-4 h-4 text-zinc-400" />
                        Technical Properties
                    </h3>
                    <div className="grid grid-cols-2 gap-y-3.5 gap-x-6">
                        <PropertyItem label="Fully Qualified Name" value={`${data.database}.${data.schema}.${data.table}`} />
                        <PropertyItem label="Owner Role" value={data.owner} />
                        <PropertyItem label="Created" value={new Date(data.createdAt).toLocaleDateString()} />
                        <PropertyItem label="Last Modified" value={new Date(data.lastModified).toLocaleDateString()} />
                        <PropertyItem label="Primary Keys" value={data.constraints.filter(c => c.constraintType === 'PRIMARY KEY').map(c => c.columnName).join(', ') || 'None'} />
                        <PropertyItem label="Foreign Keys" value={data.constraints.filter(c => c.constraintType === 'FOREIGN KEY').length.toString()} />
                    </div>
                </div>

                {/* Governance & Classification — 2 cols */}
                <div className="lg:col-span-2 bg-white dark:bg-zinc-950 border border-zinc-200 dark:border-zinc-800 rounded-xl p-5 shadow-sm">
                    <h3 className="text-sm font-semibold text-zinc-900 dark:text-zinc-100 mb-3 flex items-center gap-2 uppercase tracking-wider">
                        <ShieldCheck className="w-4 h-4 text-zinc-400" />
                        Governance
                    </h3>
                    <div className="space-y-3.5">
                        <div>
                            <span className="text-xs font-medium text-zinc-400 block mb-1">Business Domain</span>
                            <span className="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-md bg-zinc-100 text-zinc-700 text-xs font-semibold border border-zinc-200">
                                <Tag className="w-3 h-3" />
                                {data.businessDomain}
                            </span>
                        </div>
                        <div>
                            <span className="text-xs font-medium text-zinc-400 block mb-1">Usage Activity</span>
                            <span className={`inline-flex items-center gap-1.5 px-2.5 py-1 rounded-md text-xs font-semibold ${
                                data.usageClassification === 'High' ? 'bg-blue-100 text-blue-700' :
                                data.usageClassification === 'Medium' ? 'bg-indigo-100 text-indigo-700' :
                                'bg-zinc-100 text-zinc-500'
                            }`}>
                                <TrendingUp className="w-3 h-3" />
                                {data.usageClassification}
                            </span>
                        </div>
                        <div>
                            <span className="text-xs font-medium text-zinc-400 block mb-1">Tags</span>
                            {Object.keys(data.tags).length > 0 ? (
                                <div className="flex flex-wrap gap-1.5">
                                    {Object.entries(data.tags).map(([k, v]) => (
                                        <span key={k} className="px-2 py-0.5 rounded text-[10px] font-bold bg-indigo-100 text-indigo-700 border border-indigo-200">
                                            {k}: {v}
                                        </span>
                                    ))}
                                </div>
                            ) : (
                                <span className="text-xs text-zinc-400 italic">No tags applied</span>
                            )}
                        </div>
                    </div>
                </div>
            </div>
        </div>
    );
}

function PropertyItem({ label, value }: { label: string, value: string }) {
    return (
        <div>
            <h4 className="text-xs font-medium text-zinc-400 mb-0.5">{label}</h4>
            <p className="text-sm font-semibold text-zinc-800 dark:text-zinc-100 truncate" title={value}>{value}</p>
        </div>
    );
}
