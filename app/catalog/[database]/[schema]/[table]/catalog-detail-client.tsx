"use client";

import React, { useState, useEffect, useCallback } from 'react';
import Link from 'next/link';
import { ArrowLeft, Loader2, AlertCircle, Database, ShieldCheck, AlignLeft, GitBranch, Activity, Clock, HardDrive, BarChart3 } from 'lucide-react';
import { DataCatalogTable } from '@/types/catalog';
import TableOverview from '@/components/catalog/TableOverview';
import TableSchema from '@/components/catalog/TableSchema';
import TableDataQuality from '@/components/catalog/TableDataQuality';
import TableLineage from '@/components/catalog/TableLineage';

function formatBytes(bytes: number): string {
  if (bytes === 0) return '0 B';
  const k = 1024;
  const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(1)) + ' ' + sizes[i];
}

function timeAgo(dateStr: string): string {
  const diff = Date.now() - new Date(dateStr).getTime();
  const hours = Math.floor(diff / 3600000);
  if (hours < 1) return 'Just now';
  if (hours < 24) return `${hours}h ago`;
  const days = Math.floor(hours / 24);
  return `${days}d ago`;
}

export default function CatalogDetailClient({
  database,
  schema,
  table,
}: {
  database: string;
  schema: string;
  table: string;
}) {
  const [data, setData] = useState<DataCatalogTable | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [activeTab, setActiveTab] = useState<'overview' | 'schema' | 'quality' | 'lineage'>('overview');

  const fetchDetails = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const response = await fetch(`/api/catalog/${database}/${schema}/${table}`);
      if (!response.ok) {
        if (response.status === 404) throw new Error('Dataset not found in the catalog.');
        throw new Error('Failed to fetch dataset metadata.');
      }
      const result = await response.json();
      setData(result);
    } catch (err: any) {
      setError(err.message || 'An error occurred while fetching details.');
    } finally {
      setLoading(false);
    }
  }, [database, schema, table]);

  useEffect(() => {
    fetchDetails();
  }, [fetchDetails]);

  if (loading) {
    return (
      <div className="flex w-full h-full items-center justify-center bg-zinc-50 dark:bg-zinc-900/50">
        <div className="flex flex-col items-center space-y-3 text-blue-600 dark:text-blue-400">
          <Loader2 className="w-8 h-8 animate-spin" />
          <p className="text-sm font-medium animate-pulse">Loading dataset…</p>
        </div>
      </div>
    );
  }

  if (error || !data) {
    return (
      <div className="flex w-full h-full items-center justify-center p-8 bg-zinc-50 dark:bg-zinc-900/50">
        <div className="w-full max-w-md bg-white dark:bg-zinc-900 border border-zinc-200 dark:border-zinc-800 rounded-xl p-6 flex flex-col items-center text-center shadow-sm">
          <div className="w-12 h-12 bg-red-100 dark:bg-red-900/30 rounded-full flex items-center justify-center mb-4">
            <AlertCircle className="w-6 h-6 text-red-600 dark:text-red-400" />
          </div>
          <h2 className="text-lg font-bold text-zinc-900 dark:text-zinc-100 mb-1">Dataset Unreachable</h2>
          <p className="text-sm text-zinc-500 dark:text-zinc-400 mb-6">{error || 'The requested dataset could not be found or loaded.'}</p>
          <div className="flex gap-3">
            <button onClick={fetchDetails} className="px-4 py-2 bg-blue-600 hover:bg-blue-700 text-white rounded-lg text-sm font-medium transition-colors">
              Retry
            </button>
            <Link href="/catalog" className="px-4 py-2 bg-zinc-100 hover:bg-zinc-200 dark:bg-zinc-800 dark:hover:bg-zinc-700 text-zinc-700 dark:text-zinc-300 rounded-lg text-sm font-medium transition-colors">
              Back to Catalog
            </Link>
          </div>
        </div>
      </div>
    );
  }

  const dqScoreColor = data.dqScore !== null
    ? data.dqScore >= 95
      ? 'bg-emerald-50 text-emerald-700 border-emerald-200'
      : data.dqScore >= 80
        ? 'bg-amber-50 text-amber-700 border-amber-200'
        : 'bg-red-50 text-red-700 border-red-200'
    : 'bg-slate-50 text-slate-500 border-slate-200';

  return (
    <div className="flex flex-col h-full w-full bg-zinc-50 dark:bg-zinc-900/30">
      {/* Compact Header */}
      <div className="bg-white dark:bg-zinc-950 border-b border-zinc-200 dark:border-zinc-800 px-8 pt-3 pb-0 shrink-0">
        <div className="max-w-[1400px] mx-auto w-full">
          {/* Back link */}
          <Link href="/catalog" className="inline-flex items-center gap-1.5 text-xs text-zinc-400 hover:text-blue-600 transition-colors mb-2 group font-medium">
            <ArrowLeft className="w-3.5 h-3.5 group-hover:-translate-x-0.5 transition-transform" />
            Back to Catalog
          </Link>

          {/* Main header row */}
          <div className="flex items-center justify-between mb-3">
            <div className="flex items-center gap-3">
              <div className="p-2.5 bg-gradient-to-br from-blue-50 to-indigo-50 rounded-lg border border-blue-100/80 shadow-sm">
                <Database className="w-5 h-5 text-blue-600" />
              </div>
              <div>
                <div className="flex items-center gap-2">
                  <h1 className="text-lg font-bold text-zinc-900 dark:text-zinc-50 tracking-tight">{data.table}</h1>
                  <span className="text-xs font-medium text-zinc-400 bg-zinc-100 px-2 py-0.5 rounded border border-zinc-200">{data.database}</span>
                  <span className="text-zinc-300">·</span>
                  <span className="text-xs font-medium text-zinc-400 bg-zinc-100 px-2 py-0.5 rounded border border-zinc-200">{data.schema}</span>
                </div>
                <div className="flex items-center gap-1 mt-0.5 text-xs text-zinc-500">
                  Owned by <span className="font-semibold text-zinc-700">{data.owner}</span>
                </div>
              </div>
            </div>

            {/* Right: Inline KPIs + DQ Badge */}
            <div className="flex items-center gap-4">
              <div className="flex items-center gap-5 text-center">
                <div>
                  <p className="text-[11px] font-medium text-zinc-400 uppercase tracking-wider">Rows</p>
                  <p className="text-sm font-bold text-zinc-800">{data.rowCount.toLocaleString()}</p>
                </div>
                <div className="w-px h-7 bg-zinc-200"></div>
                <div>
                  <p className="text-[11px] font-medium text-zinc-400 uppercase tracking-wider">Size</p>
                  <p className="text-sm font-bold text-zinc-800">{formatBytes(data.sizeBytes)}</p>
                </div>
                <div className="w-px h-7 bg-zinc-200"></div>
                <div>
                  <p className="text-[11px] font-medium text-zinc-400 uppercase tracking-wider">Queries</p>
                  <p className="text-sm font-bold text-zinc-800">{data.queryCount30d.toLocaleString()}</p>
                </div>
                <div className="w-px h-7 bg-zinc-200"></div>
                <div>
                  <p className="text-[11px] font-medium text-zinc-400 uppercase tracking-wider">Last Access</p>
                  <p className="text-sm font-bold text-zinc-800">{timeAgo(data.lastAccessed)}</p>
                </div>
              </div>

              <div className="w-px h-8 bg-zinc-200"></div>

              {/* DQ Score Badge */}
              <div className={`flex items-center gap-1.5 px-3 py-1.5 rounded-lg font-bold border shadow-sm text-sm ${dqScoreColor}`}>
                <ShieldCheck className="w-4 h-4" />
                {data.dqScore !== null ? (
                  <span>{data.dqScore}%</span>
                ) : (
                  <span className="text-xs font-semibold">Pending Scan</span>
                )}
              </div>

              {data.trustLevel && (
                <div className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg font-bold border border-indigo-200 text-indigo-700 bg-indigo-50 shadow-sm text-xs uppercase tracking-wider">
                  <Activity className="w-3.5 h-3.5" />
                  {data.trustLevel}
                </div>
              )}
            </div>
          </div>

          {/* Tabs */}
          <div className="flex items-center gap-5">
            <TabButton active={activeTab === 'overview'} onClick={() => setActiveTab('overview')} icon={<AlignLeft className="w-3.5 h-3.5" />} label="Overview" />
            <TabButton active={activeTab === 'schema'} onClick={() => setActiveTab('schema')} icon={<Database className="w-3.5 h-3.5" />} label="Schema" count={data.columns.length} />
            <TabButton active={activeTab === 'quality'} onClick={() => setActiveTab('quality')} icon={<ShieldCheck className="w-3.5 h-3.5" />} label="Data Quality" />
            <TabButton active={activeTab === 'lineage'} onClick={() => setActiveTab('lineage')} icon={<GitBranch className="w-3.5 h-3.5" />} label="Lineage" />
          </div>
        </div>
      </div>

      {/* Content */}
      <div className="flex-1 overflow-y-auto w-full">
        <div className="max-w-[1400px] mx-auto w-full p-6">
          {activeTab === 'overview' && <TableOverview data={data} />}
          {activeTab === 'schema' && <TableSchema data={data} />}
          {activeTab === 'quality' && <TableDataQuality data={data} />}
          {activeTab === 'lineage' && <TableLineage data={data} />}
        </div>
      </div>
    </div>
  );
}

function TabButton({ active, onClick, icon, label, count }: { active: boolean; onClick: () => void; icon: React.ReactNode; label: string; count?: number }) {
  return (
    <button
      onClick={onClick}
      className={`flex items-center gap-1.5 pb-2.5 font-semibold text-[13px] transition-all relative border-b-2 ${active
        ? 'text-blue-600 border-blue-600'
        : 'text-zinc-400 border-transparent hover:text-zinc-700 hover:border-zinc-300'
        }`}
    >
      {icon}
      {label}
      {count !== undefined && (
        <span className={`ml-1 px-1.5 py-0.5 rounded-full text-[10px] font-bold ${active ? 'bg-blue-100 text-blue-700' : 'bg-zinc-100 text-zinc-500'}`}>
          {count}
        </span>
      )}
    </button>
  );
}
