"use client";

import React, { useState, useEffect, useMemo, useCallback } from 'react';
import { Search, Filter, Loader2, Database, AlertCircle, TrendingUp, Tags } from 'lucide-react';
import { CatalogListEntry } from '@/types/catalog';
import CatalogFilters from '@/components/catalog/catalog-filters';
import CatalogGrid from '@/components/catalog/catalog-grid';

// Helper to format bytes
export function formatBytes(bytes: number, decimals = 2) {
  if (!+bytes) return '0 Bytes';
  const k = 1024;
  const dm = decimals < 0 ? 0 : decimals;
  const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return `${parseFloat((bytes / Math.pow(k, i)).toFixed(dm))} ${sizes[i]}`;
}

export default function CatalogClient() {
  const [data, setData] = useState<CatalogListEntry[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  // Filter States
  const [searchTerm, setSearchTerm] = useState('');
  const [selectedDBs, setSelectedDBs] = useState<string[]>([]);
  const [selectedSchemas, setSelectedSchemas] = useState<string[]>([]);
  const [selectedDomains, setSelectedDomains] = useState<string[]>([]);
  const [selectedUsages, setSelectedUsages] = useState<string[]>([]);
  const [minDqScore, setMinDqScore] = useState<number | null>(null);

  // Pagination
  const [page, setPage] = useState(1);
  const ITEMS_PER_PAGE = 24;

  const fetchCatalog = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const response = await fetch('/api/catalog');
      if (!response.ok) throw new Error('Failed to fetch catalog');
      const result = await response.json();
      setData(result || []);
    } catch (err: any) {
      setError(err.message || 'An error occurred while fetching the catalog.');
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchCatalog();
  }, [fetchCatalog]);

  // Derived filter options based on data
  const dbs = useMemo(() => Array.from(new Set(data.map(d => d.database))).filter(Boolean).sort(), [data]);
  const schemas = useMemo(() => Array.from(new Set(data.map(d => d.schema))).filter(Boolean).sort(), [data]);
  const domains = useMemo(() => Array.from(new Set(data.map(d => d.businessDomain))).filter(Boolean).sort(), [data]);

  // Client-side filtering
  const filteredData = useMemo(() => {
    return data.filter(item => {
      // Search
      if (searchTerm) {
        const lowerSearch = searchTerm.toLowerCase();
        const matchesName = item.table.toLowerCase().includes(lowerSearch);
        const matchesDb = item.database.toLowerCase().includes(lowerSearch);
        const matchesSchema = item.schema.toLowerCase().includes(lowerSearch);
        if (!matchesName && !matchesDb && !matchesSchema) return false;
      }

      // DB
      if (selectedDBs.length > 0 && !selectedDBs.includes(item.database)) return false;

      // Schema
      if (selectedSchemas.length > 0 && !selectedSchemas.includes(item.schema)) return false;

      // Domain
      if (selectedDomains.length > 0 && !selectedDomains.includes(item.businessDomain)) return false;

      // Usage
      if (selectedUsages.length > 0 && !selectedUsages.includes(item.usageClassification)) return false;

      // DQ Score
      if (minDqScore !== null && (item.dqScore === null || item.dqScore < minDqScore)) return false;

      return true;
    });
  }, [data, searchTerm, selectedDBs, selectedSchemas, selectedDomains, selectedUsages, minDqScore]);

  // Pagination Slice
  const paginatedData = useMemo(() => {
    const startIndex = (page - 1) * ITEMS_PER_PAGE;
    return filteredData.slice(startIndex, startIndex + ITEMS_PER_PAGE);
  }, [filteredData, page]);

  const totalPages = Math.ceil(filteredData.length / ITEMS_PER_PAGE);

  return (
    <div className="flex h-full w-full">
      {/* Sidebar Filters */}
      <div className="w-80 shrink-0 border-r border-zinc-200 dark:border-zinc-800 bg-white dark:bg-zinc-950 flex flex-col h-full overflow-hidden shadow-sm z-10">
        <div className="p-5 border-b border-zinc-200 dark:border-zinc-800 shrink-0 bg-blue-50/50 dark:bg-blue-950/20">
          <h2 className="text-xl font-semibold mb-1 text-zinc-900 dark:text-zinc-100 flex items-center gap-2">
            <Filter className="w-5 h-5 text-blue-600 dark:text-blue-400" />
            Refine Catalog
          </h2>
          <p className="text-sm text-zinc-500 dark:text-zinc-400">
            {filteredData.length} {filteredData.length === 1 ? 'dataset' : 'datasets'} found
          </p>
        </div>

        <div className="flex-1 overflow-y-auto w-full custom-scrollbar p-5">
          <CatalogFilters
            dbs={dbs}
            schemas={schemas}
            domains={domains}
            selectedDBs={selectedDBs} setSelectedDBs={setSelectedDBs}
            selectedSchemas={selectedSchemas} setSelectedSchemas={setSelectedSchemas}
            selectedDomains={selectedDomains} setSelectedDomains={setSelectedDomains}
            selectedUsages={selectedUsages} setSelectedUsages={setSelectedUsages}
            minDqScore={minDqScore} setMinDqScore={setMinDqScore}
          />
        </div>
      </div>

      {/* Main Content Area */}
      <div className="flex-1 flex flex-col h-full overflow-hidden bg-zinc-50 dark:bg-zinc-900/50">
        {/* Header / Search bar */}
        <div className="py-6 px-10 border-b border-zinc-200 dark:border-zinc-800 bg-white dark:bg-zinc-950 shrink-0 flex items-center justify-between sticky top-0 z-10 shadow-sm">
          <div className="flex-1 max-w-2xl relative group">
            <div className="absolute inset-y-0 left-0 pl-4 flex items-center pointer-events-none">
              <Search className="h-5 w-5 text-zinc-400 group-focus-within:text-blue-500 transition-colors" />
            </div>
            <input
              type="text"
              placeholder="Search tables, columns, or exact matches..."
              className="block w-full pl-11 pr-3 py-3 border border-zinc-300 dark:border-zinc-700 rounded-xl leading-5 bg-zinc-50 dark:bg-zinc-900 text-zinc-900 dark:text-zinc-100 placeholder-zinc-500 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500 hover:border-blue-400 transition-all shadow-sm text-base"
              value={searchTerm}
              onChange={(e) => {
                setSearchTerm(e.target.value);
                setPage(1); // Reset pagination on search
              }}
            />
          </div>

          <div className="ml-6 flex items-center gap-4 text-sm font-medium">
            <div className="flex bg-white dark:bg-zinc-800 p-1.5 rounded-lg border border-zinc-200 dark:border-zinc-700 shadow-sm items-center">
              <span className="px-3 py-1 text-zinc-500 dark:text-zinc-400">Total Indexed</span>
              <span className="bg-blue-100 dark:bg-blue-900/40 text-blue-700 dark:text-blue-400 px-3 py-1 rounded-md ml-1 font-bold">
                {data.length.toLocaleString()}
              </span>
            </div>
          </div>
        </div>

        {/* Content View */}
        <div className="flex-1 overflow-y-auto p-10 custom-scrollbar">
          {loading ? (
            <div className="flex flex-col items-center justify-center h-full text-zinc-500 space-y-4">
              <Loader2 className="w-10 h-10 animate-spin text-blue-500" />
              <p className="text-lg font-medium animate-pulse">Scanning Enterprise Metadata...</p>
            </div>
          ) : error ? (
            <div className="flex flex-col items-center justify-center h-full text-red-500 space-y-4 bg-red-50/50 dark:bg-red-900/10 p-8 rounded-2xl border border-red-200 dark:border-red-900 max-w-2xl mx-auto">
              <AlertCircle className="w-12 h-12" />
              <p className="text-lg font-medium">{error}</p>
              <button
                onClick={fetchCatalog}
                className="px-6 py-2 bg-red-100 dark:bg-red-900/50 hover:bg-red-200 dark:hover:bg-red-900/80 rounded-lg text-red-700 dark:text-red-300 font-medium transition-colors"
              >
                Retry Extraction
              </button>
            </div>
          ) : filteredData.length === 0 ? (
            <div className="flex flex-col items-center justify-center h-full text-zinc-500 space-y-4">
              <Database className="w-16 h-16 text-zinc-300 dark:text-zinc-700 mb-2" />
              <p className="text-xl font-semibold text-zinc-700 dark:text-zinc-300">No Datasets Found</p>
              <p className="max-w-md text-center">Try adjusting your filters or search terms to find what you're looking for across the catalog.</p>
              <button
                onClick={() => {
                  setSearchTerm(''); setSelectedDBs([]); setSelectedSchemas([]);
                  setSelectedDomains([]); setSelectedUsages([]); setMinDqScore(null);
                }}
                className="mt-4 px-5 py-2 hover:bg-zinc-100 dark:hover:bg-zinc-800 rounded-lg text-zinc-600 dark:text-zinc-400 border border-zinc-200 dark:border-zinc-700 transition-colors"
              >
                Clear all filters
              </button>
            </div>
          ) : (
            <div className="flex flex-col h-full max-w-7xl mx-auto w-full">
              <CatalogGrid data={paginatedData} />

              {/* Pagination Controls */}
              {totalPages > 1 && (
                <div className="mt-8 mb-4 flex items-center justify-between border-t border-zinc-200 dark:border-zinc-800 pt-6">
                  <p className="text-sm text-zinc-600 dark:text-zinc-400 font-medium">
                    Showing <span className="font-semibold text-zinc-900 dark:text-zinc-100">{((page - 1) * ITEMS_PER_PAGE) + 1}</span> to <span className="font-semibold text-zinc-900 dark:text-zinc-100">{Math.min(page * ITEMS_PER_PAGE, filteredData.length)}</span> of <span className="font-semibold text-zinc-900 dark:text-zinc-100">{filteredData.length}</span> datasets
                  </p>
                  <div className="flex gap-2">
                    <button
                      onClick={() => setPage(p => Math.max(1, p - 1))}
                      disabled={page === 1}
                      className="px-4 py-2 border border-zinc-200 dark:border-zinc-700 rounded-lg hover:bg-zinc-50 dark:hover:bg-zinc-800 disabled:opacity-50 disabled:cursor-not-allowed text-sm font-medium transition-colors bg-white dark:bg-zinc-950 shadow-sm"
                    >
                      Previous
                    </button>
                    <div className="flex items-center px-4 font-medium text-sm text-zinc-700 dark:text-zinc-300">
                      Page {page} of {totalPages}
                    </div>
                    <button
                      onClick={() => setPage(p => Math.min(totalPages, p + 1))}
                      disabled={page === totalPages}
                      className="px-4 py-2 border border-zinc-200 dark:border-zinc-700 rounded-lg hover:bg-zinc-50 dark:hover:bg-zinc-800 disabled:opacity-50 disabled:cursor-not-allowed text-sm font-medium transition-colors bg-white dark:bg-zinc-950 shadow-sm"
                    >
                      Next
                    </button>
                  </div>
                </div>
              )}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
