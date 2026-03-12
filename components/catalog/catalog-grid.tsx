import React from 'react';
import Link from 'next/link';
import { CatalogListEntry } from '@/types/catalog';
import { Database, ShieldCheck, AlertTriangle, TrendingUp, Clock, HardDrive, BarChart2 } from 'lucide-react';
import { formatBytes } from '@/app/catalog/catalog-client';

export default function CatalogGrid({ data }: { data: CatalogListEntry[] }) {
  return (
    <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-6">
      {data.map(item => (
        <CatalogCard key={item.id} item={item} />
      ))}
    </div>
  );
}

function CatalogCard({ item }: { item: CatalogListEntry }) {
  // Determine DQ Score color
  const dqScore = item.dqScore;
  let scoreColor = 'text-zinc-500 bg-zinc-100 dark:bg-zinc-800';
  if (dqScore !== null) {
    if (dqScore >= 95) scoreColor = 'text-emerald-700 bg-emerald-100 dark:text-emerald-400 dark:bg-emerald-900/30';
    else if (dqScore >= 80) scoreColor = 'text-amber-700 bg-amber-100 dark:text-amber-400 dark:bg-amber-900/30';
    else scoreColor = 'text-red-700 bg-red-100 dark:text-red-400 dark:bg-red-900/30';
  }

  // Determine usage color
  const usageClass = item.usageClassification;
  let usageColor = 'text-zinc-500 bg-zinc-100 dark:bg-zinc-800';
  if (usageClass === 'High') usageColor = 'text-blue-700 bg-blue-100 dark:bg-blue-900/30 dark:text-blue-400';
  else if (usageClass === 'Medium') usageColor = 'text-indigo-700 bg-indigo-100 dark:bg-indigo-900/30 dark:text-indigo-400';

  return (
    <Link href={`/catalog/${item.database}/${item.schema}/${item.table}`} className="block group h-full">
      <div className="bg-white dark:bg-zinc-950 border border-zinc-200 dark:border-zinc-800 rounded-xl p-5 hover:shadow-lg hover:border-blue-400 dark:hover:border-blue-500 transition-all duration-200 h-full flex flex-col cursor-pointer relative overflow-hidden">

        {/* Decorative Top Accent */}
        <div className="absolute top-0 left-0 right-0 h-1 bg-gradient-to-r from-blue-400 to-indigo-500 opacity-0 group-hover:opacity-100 transition-opacity" />

        <div className="flex justify-between items-start mb-4">
          <div className="flex items-center gap-2 max-w-[70%]">
            <div className="p-2 bg-blue-50 dark:bg-blue-900/20 rounded-lg shrink-0">
              <Database className="w-5 h-5 text-blue-600 dark:text-blue-400" />
            </div>
            <div className="truncate">
              <h3 className="font-semibold text-zinc-900 dark:text-zinc-100 truncate text-base" title={item.table}>
                {item.table}
              </h3>
              <p className="text-xs text-zinc-500 dark:text-zinc-400 truncate flex items-center gap-1.5 mt-0.5" title={`${item.database} • ${item.schema}`}>
                <span>{item.database}</span>
                <span className="w-1 h-1 rounded-full bg-zinc-300 dark:bg-zinc-700"></span>
                <span>{item.schema}</span>
              </p>
            </div>
          </div>

          <div className={`flex items-center gap-1.5 px-2.5 py-1 rounded-md font-bold text-sm shrink-0 transition-colors ${scoreColor}`}>
            {dqScore !== null ? (
              <>
                <ShieldCheck className="w-4 h-4" />
                {dqScore}%
              </>
            ) : (
              <span className="text-xs font-semibold px-1">Unscored</span>
            )}
          </div>
        </div>

        <div className="flex-1"></div>

        {/* Metrics Grid */}
        <div className="grid grid-cols-2 gap-y-3 gap-x-2 mt-5 mb-4 px-2 py-3 bg-zinc-50 dark:bg-zinc-900/40 rounded-lg border border-zinc-100 dark:border-zinc-800/80">
          <div className="flex items-center gap-2">
            <HardDrive className="w-4 h-4 text-zinc-400 dark:text-zinc-500 shrink-0" />
            <div className="flex flex-col">
              <span className="text-[10px] uppercase text-zinc-500 font-semibold tracking-wider">Size</span>
              <span className="text-sm font-medium text-zinc-800 dark:text-zinc-200">{formatBytes(item.sizeBytes, 0)}</span>
            </div>
          </div>
          <div className="flex items-center gap-2">
            <BarChart2 className="w-4 h-4 text-zinc-400 dark:text-zinc-500 shrink-0" />
            <div className="flex flex-col">
              <span className="text-[10px] uppercase text-zinc-500 font-semibold tracking-wider">Storage</span>
              <span className="text-sm font-medium text-zinc-800 dark:text-zinc-200">
                {item.rowCount > 1000000 ? `${(item.rowCount / 1000000).toFixed(1)}M` : (item.rowCount > 1000 ? `${(item.rowCount / 1000).toFixed(1)}K` : item.rowCount)} rows
              </span>
            </div>
          </div>
        </div>

        {/* Footer info labels */}
        <div className="flex items-center justify-between mt-auto pt-3 border-t border-zinc-100 dark:border-zinc-800/60">
          <div className="flex gap-2">
            <span className={`text-xs font-medium px-2 py-1 rounded-md flex items-center gap-1 ${usageColor}`}>
              <TrendingUp className="w-3 h-3" />
              {item.usageClassification}
            </span>
            {item.trustLevel && (
              <span className="text-xs font-medium px-2 py-1 rounded-md bg-purple-100 text-purple-700 dark:bg-purple-900/30 dark:text-purple-400">
                {item.trustLevel}
              </span>
            )}
          </div>
          <div className="flex items-center gap-1 text-xs text-zinc-400" title={`Last modified: ${new Date(item.lastModified).toLocaleString()}`}>
            <Clock className="w-3.5 h-3.5" />
            {new Date(item.lastModified).toLocaleDateString()}
          </div>
        </div>
      </div>
    </Link>
  );
}
