'use client';

import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { Play, RefreshCw } from 'lucide-react';

import { Button } from '@/components/ui/button';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';

type CustomSqlTabProps = {
  datasetId: string;
  database: string;
  schema: string;
  table: string;
};

type AppRole = 'ADMIN' | 'DATA_ENGINEER' | 'ANALYST' | 'VIEWER';

type CustomSqlPermissions = {
  canRunSql: boolean;
  canEditSql: boolean;
  canViewHistory: boolean;
  canConfigureDataset: boolean;
  canManageUsers: boolean;
  allowedCommands: string[];
};

type WarehouseInfo = {
  name: string;
  state: string | null;
  size: string | null;
  type: string | null;
};

type QueryExecutionResult = {
  audit_id: string;
  query_id: string | null;
  status: string;
  execution_time_ms: number;
  rows_returned: number;
  rows_updated: number;
  warehouse_used: string;
  database_used: string;
  schema_used: string;
  columns: string[];
  rows: unknown[][];
  truncated: boolean;
  applied_limit: number | null;
  command_type: string;
  notices: string[];
  app_role?: AppRole;
};

const defaultSql = `SELECT *\nFROM {{TABLE}}`;

function formatCell(value: unknown): string {
  if (value === null || value === undefined) return 'NULL';
  if (typeof value === 'object') return JSON.stringify(value);
  return String(value);
}

function statusClass(status: string): string {
  const normalized = status.toUpperCase();
  if (normalized === 'SUCCESS' || normalized === 'PASSED') return 'bg-emerald-100 text-emerald-700';
  if (normalized === 'BLOCKED' || normalized === 'FAILED' || normalized === 'ERROR') return 'bg-red-100 text-red-700';
  return 'bg-amber-100 text-amber-700';
}

export function CustomSqlTab({ datasetId, database, schema, table }: CustomSqlTabProps) {
  const [sqlText, setSqlText] = useState(defaultSql);
  const [selectedWarehouse, setSelectedWarehouse] = useState<string>('');

  const [appRole, setAppRole] = useState<AppRole>('VIEWER');
  const [permissions, setPermissions] = useState<CustomSqlPermissions>({
    canRunSql: false,
    canEditSql: false,
    canViewHistory: false,
    canConfigureDataset: false,
    canManageUsers: false,
    allowedCommands: [],
  });

  const [isExecuting, setIsExecuting] = useState(false);
  const [executionError, setExecutionError] = useState<string | null>(null);
  const [executionResult, setExecutionResult] = useState<QueryExecutionResult | null>(null);

  const [resultsView, setResultsView] = useState<'table' | 'json'>('table');
  const [pageSize, setPageSize] = useState(10);
  const [currentPage, setCurrentPage] = useState(1);

  const lastRunAtRef = useRef(0);

  const showRunButton = appRole === 'ADMIN' || appRole === 'DATA_ENGINEER';
  const editorDisabled = appRole === 'VIEWER' || !permissions.canEditSql;

  const fetchWorkspaceContext = useCallback(async () => {
    try {
      const response = await fetch('/api/snowflake/warehouses');
      const payload = await response.json();

      if (!response.ok || !payload.success) {
        throw new Error(payload.error || 'Failed to load workspace context');
      }

      const items = (payload.data?.warehouses || []) as WarehouseInfo[];
      const currentWarehouse = String(payload.data?.current_warehouse || '').toUpperCase();
      const fallbackWarehouse = items[0]?.name || '';
      setSelectedWarehouse(currentWarehouse || fallbackWarehouse);

      setAppRole((payload.data?.custom_sql_role || 'VIEWER') as AppRole);
      setPermissions(
        (payload.data?.custom_sql_permissions as CustomSqlPermissions) || {
          canRunSql: false,
          canEditSql: false,
          canViewHistory: false,
          canConfigureDataset: false,
          canManageUsers: false,
          allowedCommands: [],
        }
      );
    } catch (error: unknown) {
      const message = error instanceof Error ? error.message : 'Failed to load workspace context';
      setExecutionError(message);
    }
  }, []);

  useEffect(() => {
    fetchWorkspaceContext();
  }, [fetchWorkspaceContext]);

  useEffect(() => {
    setCurrentPage(1);
  }, [executionResult, pageSize]);

  const totalRows = executionResult?.rows.length || 0;
  const totalPages = Math.max(1, Math.ceil(totalRows / pageSize));

  const paginatedRows = useMemo(() => {
    if (!executionResult) return [];
    const start = (currentPage - 1) * pageSize;
    return executionResult.rows.slice(start, start + pageSize);
  }, [currentPage, executionResult, pageSize]);

  const paginatedJsonRows = useMemo(() => {
    if (!executionResult) return [];
    return paginatedRows.map((row) => {
      const mapped: Record<string, unknown> = {};
      executionResult.columns.forEach((column, index) => {
        mapped[column] = row[index];
      });
      return mapped;
    });
  }, [executionResult, paginatedRows]);

  const runQuery = useCallback(async () => {
    if (!showRunButton || !permissions.canRunSql) return;

    const now = Date.now();
    if (now - lastRunAtRef.current < 800) return;
    lastRunAtRef.current = now;

    try {
      setIsExecuting(true);
      setExecutionError(null);

      const response = await fetch(`/api/datasets/${encodeURIComponent(datasetId)}/custom-sql/execute`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          database,
          schema,
          table,
          sql: sqlText,
          warehouse: selectedWarehouse,
          mode: 'execute',
        }),
      });

      const payload = await response.json();
      if (!response.ok || !payload.success) {
        throw new Error(payload.error || 'Query execution failed');
      }

      setExecutionResult(payload.data as QueryExecutionResult);
    } catch (error: unknown) {
      const message = error instanceof Error ? error.message : 'Query execution failed';
      setExecutionError(message);
    } finally {
      setIsExecuting(false);
    }
  }, [database, datasetId, permissions.canRunSql, schema, selectedWarehouse, showRunButton, sqlText, table]);

  return (
    <div className="h-[calc(100vh-280px)] min-h-[520px] max-h-[calc(100vh-280px)] overflow-hidden rounded-md border border-slate-200 bg-white">
      <div className="grid h-full grid-rows-[42%_58%]">
        <section className="flex min-h-0 flex-col border-b border-slate-200">
          <div className="flex h-10 items-center justify-between border-b border-slate-200 bg-slate-50 px-3">
            <span className="text-xs font-semibold text-slate-800">Query</span>
            <div className="flex items-center gap-2">
              {showRunButton ? (
                <Button
                  className="h-7 bg-slate-900 px-2.5 text-xs text-white hover:bg-slate-800"
                  onClick={runQuery}
                  disabled={isExecuting || !selectedWarehouse}
                >
                  {isExecuting ? (
                    <RefreshCw className="mr-1 h-3.5 w-3.5 animate-spin" />
                  ) : (
                    <Play className="mr-1 h-3.5 w-3.5" />
                  )}
                  Run
                </Button>
              ) : (
                <span className="rounded bg-slate-200 px-2 py-0.5 text-[11px] font-medium text-slate-700">
                  {appRole === 'ANALYST' ? 'Read Only' : 'View Only'}
                </span>
              )}
            </div>
          </div>

          <textarea
            value={sqlText}
            onChange={(event) => setSqlText(event.target.value)}
            readOnly={editorDisabled}
            className="h-full w-full flex-1 resize-none border-0 bg-white p-3 font-mono text-[13px] leading-5 text-slate-900 outline-none"
            spellCheck={false}
          />

          {executionError && (
            <div className="border-t border-red-100 bg-red-50 px-3 py-2 text-xs text-red-700">{executionError}</div>
          )}
        </section>

        <section className="flex min-h-0 flex-col overflow-hidden">
          <div className="flex h-10 items-center justify-between border-b border-slate-200 bg-slate-50 px-3">
            <span className="text-xs font-semibold text-slate-800">Result</span>
            <div className="flex items-center gap-2">
              <div className="inline-flex rounded border border-slate-200 bg-white p-0.5">
                <button
                  type="button"
                  className={`px-2 py-1 text-[11px] ${resultsView === 'table' ? 'bg-slate-800 text-white' : 'text-slate-600'}`}
                  onClick={() => setResultsView('table')}
                >
                  Table
                </button>
                <button
                  type="button"
                  className={`px-2 py-1 text-[11px] ${resultsView === 'json' ? 'bg-slate-800 text-white' : 'text-slate-600'}`}
                  onClick={() => setResultsView('json')}
                >
                  JSON
                </button>
              </div>
              <Select value={String(pageSize)} onValueChange={(value) => setPageSize(Number(value))}>
                <SelectTrigger className="h-7 w-[74px] text-xs">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="10">10</SelectItem>
                  <SelectItem value="15">15</SelectItem>
                  <SelectItem value="20">20</SelectItem>
                </SelectContent>
              </Select>
            </div>
          </div>

          {!executionResult ? (
            <div className="flex flex-1 items-center justify-center text-sm text-slate-500">Run query to view results.</div>
          ) : (
            <>
              <div className="flex items-center gap-2 border-b border-slate-200 px-3 py-1.5 text-[11px] text-slate-600">
                <span className={`rounded px-2 py-0.5 font-semibold ${statusClass(executionResult.status)}`}>
                  {executionResult.status}
                </span>
                <span>{executionResult.rows_returned.toLocaleString()} rows</span>
                <span>{executionResult.execution_time_ms} ms</span>
              </div>

              <div className="min-h-0 flex-1 overflow-hidden">
                {resultsView === 'table' ? (
                  <table className="w-full table-fixed border-collapse text-xs">
                    <thead className="bg-slate-100">
                      <tr>
                        {executionResult.columns.map((column) => (
                          <th
                            key={column}
                            className="truncate border-b border-slate-200 px-2 py-1 text-left font-semibold text-slate-700"
                          >
                            {column}
                          </th>
                        ))}
                      </tr>
                    </thead>
                    <tbody>
                      {paginatedRows.map((row, rowIndex) => (
                        <tr key={`${currentPage}-${rowIndex}`} className="border-b border-slate-100">
                          {row.map((cell, cellIndex) => (
                            <td key={`${rowIndex}-${cellIndex}`} className="truncate px-2 py-1 text-slate-700">
                              {cell === null || cell === undefined ? (
                                <span className="italic text-slate-400">NULL</span>
                              ) : (
                                formatCell(cell)
                              )}
                            </td>
                          ))}
                        </tr>
                      ))}
                    </tbody>
                  </table>
                ) : (
                  <pre className="h-full overflow-hidden whitespace-pre-wrap p-3 font-mono text-[11px] text-slate-700">
                    {JSON.stringify(paginatedJsonRows, null, 2)}
                  </pre>
                )}
              </div>

              <div className="flex items-center justify-between border-t border-slate-200 px-3 py-1.5 text-[11px] text-slate-500">
                <span>
                  Showing {paginatedRows.length} of {totalRows} rows (Page {currentPage}/{totalPages})
                </span>
                <div className="flex items-center gap-2">
                  <Button
                    variant="outline"
                    className="h-7 px-2 text-xs"
                    onClick={() => setCurrentPage((value) => Math.max(1, value - 1))}
                    disabled={currentPage <= 1}
                  >
                    Prev
                  </Button>
                  <Button
                    variant="outline"
                    className="h-7 px-2 text-xs"
                    onClick={() => setCurrentPage((value) => Math.min(totalPages, value + 1))}
                    disabled={currentPage >= totalPages}
                  >
                    Next
                  </Button>
                </div>
              </div>
            </>
          )}
        </section>
      </div>
    </div>
  );
}
