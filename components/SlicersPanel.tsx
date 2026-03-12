'use client';

import { useState, useEffect } from 'react';
import { useRouter } from 'next/navigation';
import { Calendar, Database, Filter, ChevronRight, ChevronDown, Snowflake, Layers, Table } from 'lucide-react';
import { useFilters } from '@/lib/filter-store';
import { useAppStore } from '@/lib/store';

interface SlicersPanelProps {
  onTimeRangeChange?: (range: string) => void;
  onDatasetsChange?: (datasets: string[]) => void;
}

const TIME_RANGES = [
  'Last 7 Days',
  'Last 30 Days',
  'Last 90 Days',
  'Last 6 Months',
  'Last Year',
  'Custom Range',
];

interface DatabaseItem {
  name: string;
  isDefault: boolean;
  isCurrent: boolean;
}

interface SchemaItem {
  name: string;
  databaseName: string;
}

interface TableItem {
  name: string;
  schemaName: string;
  kind: string;
}

export function SlicersPanel({ onTimeRangeChange, onDatasetsChange }: SlicersPanelProps) {
  const { filters, setTimeRange: setFilterTimeRange } = useFilters();
  const { isConnected } = useAppStore();
  const router = useRouter();
  
  const [databases, setDatabases] = useState<DatabaseItem[]>([]);
  const [isLoadingDatabases, setIsLoadingDatabases] = useState(false);
  
  const [expandedDatabases, setExpandedDatabases] = useState<Set<string>>(new Set());
  const [expandedSchemas, setExpandedSchemas] = useState<Set<string>>(new Set());
  
  const [schemasCache, setSchemasCache] = useState<Record<string, SchemaItem[]>>({});
  const [tablesCache, setTablesCache] = useState<Record<string, TableItem[]>>({});
  
  const [loadingSchemas, setLoadingSchemas] = useState<Set<string>>(new Set());
  const [loadingTables, setLoadingTables] = useState<Set<string>>(new Set());
  
  const timeRange = filters.timeRange;

  // Fetch databases when connected
  useEffect(() => {
    if (!isConnected) {
      setDatabases([]);
      setExpandedDatabases(new Set());
      setExpandedSchemas(new Set());
      setSchemasCache({});
      setTablesCache({});
      return;
    }

    const fetchDatabases = async () => {
      try {
        setIsLoadingDatabases(true);
        const response = await fetch('/api/snowflake/databases');
        const result = await response.json();

        if (result.success && result.data) {
          setDatabases(result.data);
        }
      } catch (error) {
        console.error('Error fetching databases:', error);
      } finally {
        setIsLoadingDatabases(false);
      }
    };

    fetchDatabases();
  }, [isConnected]);

  const handleTimeRangeChange = (range: string) => {
    setFilterTimeRange(range);
    if (onTimeRangeChange) {
      onTimeRangeChange(range);
    }
  };

  const toggleDatabase = async (databaseName: string) => {
    const newExpanded = new Set(expandedDatabases);
    
    if (newExpanded.has(databaseName)) {
      // Collapse
      newExpanded.delete(databaseName);
      setExpandedDatabases(newExpanded);
    } else {
      // Expand and fetch schemas if not cached
      newExpanded.add(databaseName);
      setExpandedDatabases(newExpanded);
      
      if (!schemasCache[databaseName]) {
        await fetchSchemas(databaseName);
      }
    }
  };

  const fetchSchemas = async (databaseName: string) => {
    try {
      setLoadingSchemas(prev => new Set(prev).add(databaseName));
      const response = await fetch(`/api/snowflake/schemas?database=${encodeURIComponent(databaseName)}`);
      const result = await response.json();

      if (result.success && result.data) {
        setSchemasCache(prev => ({ ...prev, [databaseName]: result.data }));
      }
    } catch (error) {
      console.error('Error fetching schemas:', error);
    } finally {
      setLoadingSchemas(prev => {
        const newSet = new Set(prev);
        newSet.delete(databaseName);
        return newSet;
      });
    }
  };

  const toggleSchema = async (databaseName: string, schemaName: string) => {
    const schemaKey = `${databaseName}.${schemaName}`;
    const newExpanded = new Set(expandedSchemas);
    
    if (newExpanded.has(schemaKey)) {
      // Collapse
      newExpanded.delete(schemaKey);
      setExpandedSchemas(newExpanded);
    } else {
      // Expand and fetch tables if not cached
      newExpanded.add(schemaKey);
      setExpandedSchemas(newExpanded);
      
      if (!tablesCache[schemaKey]) {
        await fetchTables(databaseName, schemaName);
      }
    }
  };

  const fetchTables = async (databaseName: string, schemaName: string) => {
    const schemaKey = `${databaseName}.${schemaName}`;
    try {
      setLoadingTables(prev => new Set(prev).add(schemaKey));
      const response = await fetch(
        `/api/snowflake/tables?database=${encodeURIComponent(databaseName)}&schema=${encodeURIComponent(schemaName)}`
      );
      const result = await response.json();

      if (result.success && result.data) {
        setTablesCache(prev => ({ ...prev, [schemaKey]: result.data }));
      }
    } catch (error) {
      console.error('Error fetching tables:', error);
    } finally {
      setLoadingTables(prev => {
        const newSet = new Set(prev);
        newSet.delete(schemaKey);
        return newSet;
      });
    }
  };

  const handleTableClick = (databaseName: string, schemaName: string, tableName: string) => {
    // Navigate to table details page
    const dataset = `${databaseName}.${schemaName}`;
    router.push(`/datasets/${encodeURIComponent(dataset)}/tables/${encodeURIComponent(tableName)}`);
  };

  return (
    <div className="w-72 bg-white border-r border-slate-200 h-full overflow-y-auto">
      <div className="p-4">
        {/* Header */}
        <div className="flex items-center gap-2 mb-6">
          <Filter className="size-5 text-slate-700" />
          <h2 className="text-lg font-semibold text-slate-900">Filters</h2>
        </div>

        {/* Time Range Section */}
        <div className="mb-6">
          <div className="flex items-center gap-2 mb-3">
            <Calendar className="size-4 text-slate-600" />
            <label className="text-sm font-medium text-slate-700">Time Range</label>
          </div>
          <select
            value={timeRange}
            onChange={(e) => handleTimeRangeChange(e.target.value)}
            className="w-full px-3 py-2 border border-slate-300 rounded-md bg-white text-slate-900 text-sm focus:outline-none focus:ring-2 focus:ring-indigo-500 focus:border-indigo-500 appearance-none cursor-pointer"
            style={{
              backgroundImage: `url("data:image/svg+xml,%3csvg xmlns='http://www.w3.org/2000/svg' fill='none' viewBox='0 0 20 20'%3e%3cpath stroke='%236b7280' stroke-linecap='round' stroke-linejoin='round' stroke-width='1.5' d='M6 8l4 4 4-4'/%3e%3c/svg%3e")`,
              backgroundPosition: 'right 0.5rem center',
              backgroundRepeat: 'no-repeat',
              backgroundSize: '1.5em 1.5em',
              paddingRight: '2.5rem',
            }}
          >
            {TIME_RANGES.map((range) => (
              <option key={range} value={range}>
                {range}
              </option>
            ))}
          </select>
        </div>

        {/* Database Explorer Section */}
        <div>
          <div className="flex items-center gap-2 mb-3">
            <Database className="size-4 text-slate-600" />
            <label className="text-sm font-medium text-slate-700">Database Explorer</label>
          </div>

          {!isConnected ? (
            <div className="text-sm text-slate-500 py-4">Connect to Snowflake to view databases</div>
          ) : isLoadingDatabases ? (
            <div className="text-sm text-slate-500 py-4">Loading databases...</div>
          ) : databases.length === 0 ? (
            <div className="text-sm text-slate-500 py-4">No databases found</div>
          ) : (
            <div className="space-y-1">
              {databases.map((database) => (
                <div key={database.name}>
                  {/* Database Level */}
                  <div
                    onClick={() => toggleDatabase(database.name)}
                    className="flex items-center gap-2 cursor-pointer hover:bg-slate-50 p-2 rounded-md transition-colors group"
                  >
                    {expandedDatabases.has(database.name) ? (
                      <ChevronDown className="size-4 text-slate-400 shrink-0" />
                    ) : (
                      <ChevronRight className="size-4 text-slate-400 shrink-0" />
                    )}
                    <Snowflake className="size-4 text-blue-500 shrink-0" />
                    <span className="text-sm text-slate-900 font-medium truncate">{database.name}</span>
                  </div>

                  {/* Schemas Level */}
                  {expandedDatabases.has(database.name) && (
                    <div className="ml-6 mt-1 space-y-1">
                      {loadingSchemas.has(database.name) ? (
                        <div className="text-xs text-slate-400 py-2 pl-6">Loading schemas...</div>
                      ) : schemasCache[database.name]?.length === 0 ? (
                        <div className="text-xs text-slate-400 py-2 pl-6">No schemas found</div>
                      ) : (
                        schemasCache[database.name]?.map((schema) => {
                          const schemaKey = `${database.name}.${schema.name}`;
                          return (
                            <div key={schemaKey}>
                              {/* Schema Level */}
                              <div
                                onClick={() => toggleSchema(database.name, schema.name)}
                                className="flex items-center gap-2 cursor-pointer hover:bg-slate-50 p-2 rounded-md transition-colors group"
                              >
                                {expandedSchemas.has(schemaKey) ? (
                                  <ChevronDown className="size-3 text-slate-400 shrink-0" />
                                ) : (
                                  <ChevronRight className="size-3 text-slate-400 shrink-0" />
                                )}
                                <Layers className="size-3 text-purple-500 shrink-0" />
                                <span className="text-xs text-slate-700 truncate">{schema.name}</span>
                              </div>

                              {/* Tables Level */}
                              {expandedSchemas.has(schemaKey) && (
                                <div className="ml-5 mt-1 space-y-1">
                                  {loadingTables.has(schemaKey) ? (
                                    <div className="text-xs text-slate-400 py-1 pl-6">Loading tables...</div>
                                  ) : tablesCache[schemaKey]?.length === 0 ? (
                                    <div className="text-xs text-slate-400 py-1 pl-6">No tables found</div>
                                  ) : (
                                    tablesCache[schemaKey]?.map((table) => (
                                      <div
                                        key={table.name}
                                        onClick={() => handleTableClick(database.name, schema.name, table.name)}
                                        className="flex items-center gap-2 cursor-pointer hover:bg-slate-50 p-2 rounded-md transition-colors group"
                                      >
                                        <Table className="size-3 text-green-500 shrink-0" />
                                        <span className="text-xs text-slate-600 truncate">{table.name}</span>
                                        {table.kind === 'VIEW' && (
                                          <span className="text-[10px] text-slate-400 uppercase">view</span>
                                        )}
                                      </div>
                                    ))
                                  )}
                                </div>
                              )}
                            </div>
                          );
                        })
                      )}
                    </div>
                  )}
                </div>
              ))}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

