"use client";

import React, { useEffect, useState } from 'react';
import { format } from 'date-fns';
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from '@/components/ui/table';
import { Badge } from '@/components/ui/badge';
import { Download, FileSpreadsheet, TableProperties, Loader2, RefreshCw } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { useToast } from '@/components/ui/toast';

interface ReportHistoryEntry {
  reportId: string;
  reportDate: string;
  generatedAt: string;
  generatedBy: string;
  format: string;
  status: string;
  totalDatasets: number;
  failedChecks: number;
  successRate: number;
  variant: string | null;
  mode: string | null;
  errorMessage: string | null;
}

function StatusChip({ status }: { status: string }) {
  if (status === 'COMPLETED') {
    return <span className="inline-flex h-6 items-center rounded-full bg-emerald-100 px-2.5 text-xs font-medium text-emerald-800">Completed</span>;
  }

  if (status === 'PENDING' || status === 'RUNNING') {
    return (
      <span className="inline-flex h-6 items-center gap-1.5 rounded-full bg-amber-100 px-2.5 text-xs font-medium text-amber-800">
        <Loader2 className="h-3 w-3 animate-spin" /> Generating
      </span>
    );
  }

  return <span className="inline-flex h-6 items-center rounded-full bg-red-100 px-2.5 text-xs font-medium text-red-800">Failed</span>;
}

export function ReportHistoryTab() {
  const [history, setHistory] = useState<ReportHistoryEntry[]>([]);
  const [isLoading, setIsLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const { showToast } = useToast();

  const fetchHistory = async () => {
    setIsLoading(true);
    setError(null);

    try {
      const res = await fetch('/api/reports/v2/history', { cache: 'no-store' });
      const data = await res.json();
      if (!res.ok || !data.success) {
        throw new Error(data.error || 'Failed to fetch report history');
      }

      setHistory(data.data || []);
    } catch (err: unknown) {
      setError(err instanceof Error ? err.message : 'Failed to fetch report history');
    } finally {
      setIsLoading(false);
    }
  };

  useEffect(() => {
    fetchHistory();
    const interval = setInterval(fetchHistory, 10000);
    return () => clearInterval(interval);
  }, []);

  const handleDownload = (reportId: string, status: string) => {
    if (status !== 'COMPLETED') {
      showToast('Report is not ready yet.', 'warning', 3000);
      return;
    }
    window.location.href = `/api/reports/v2/download/${reportId}`;
  };

  if (isLoading && history.length === 0) {
    return (
      <div className="flex flex-col items-center justify-center p-12 py-24 text-slate-500">
        <Loader2 className="mb-4 h-8 w-8 animate-spin text-indigo-500" />
        <p>Loading report history...</p>
      </div>
    );
  }

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <div>
          <h2 className="text-lg font-semibold text-slate-900">Report History</h2>
          <p className="text-sm text-slate-500">View and download previously generated reports.</p>
        </div>
        <Button variant="outline" size="sm" onClick={fetchHistory} disabled={isLoading}>
          <RefreshCw className={`mr-2 h-4 w-4 ${isLoading ? 'animate-spin' : ''}`} />
          Refresh
        </Button>
      </div>

      {error && <div className="rounded-md bg-red-50 p-4 text-sm text-red-700 border border-red-200">{error}</div>}

      {history.length === 0 && !isLoading && !error ? (
        <div className="rounded-xl border border-dashed border-slate-300 p-12 text-center text-slate-500">
          <FileSpreadsheet className="mx-auto mb-3 h-10 w-10 text-slate-300" />
          <h3 className="mb-1 text-sm font-semibold text-slate-900">No reports generated</h3>
          <p className="text-sm">Use download actions in dashboards to generate a new report.</p>
        </div>
      ) : (
        <div className="rounded-md border border-slate-200 bg-white">
          <Table>
            <TableHeader className="bg-slate-50">
              <TableRow>
                <TableHead>Report Date</TableHead>
                <TableHead>Execution Time</TableHead>
                <TableHead>Format</TableHead>
                <TableHead>Variant</TableHead>
                <TableHead>Datasets</TableHead>
                <TableHead>Success Rate</TableHead>
                <TableHead>Status</TableHead>
                <TableHead className="text-right">Action</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {history.map((entry) => (
                <TableRow key={entry.reportId}>
                  <TableCell className="font-medium text-slate-900">
                    {entry.reportDate
                      ? format(new Date(`${entry.reportDate.split('T')[0]}T00:00:00`), 'PP')
                      : '-'}
                  </TableCell>
                  <TableCell className="text-slate-500">
                    {entry.generatedAt ? format(new Date(`${entry.generatedAt}Z`), 'PP p') : '-'}
                  </TableCell>
                  <TableCell>
                    {entry.format === 'XLSX' ? (
                      <Badge variant="outline" className="border-indigo-200 bg-indigo-50 text-indigo-700">
                        <FileSpreadsheet className="mr-1 h-3 w-3" /> XLSX
                      </Badge>
                    ) : (
                      <Badge variant="outline" className="border-emerald-200 bg-emerald-50 text-emerald-700">
                        <TableProperties className="mr-1 h-3 w-3" /> CSV
                      </Badge>
                    )}
                  </TableCell>
                  <TableCell className="uppercase text-xs text-slate-600">{entry.variant || '-'}</TableCell>
                  <TableCell>{entry.totalDatasets || 0}</TableCell>
                  <TableCell>
                    <span className={`font-semibold ${entry.successRate >= 95 ? 'text-emerald-600' : entry.successRate >= 80 ? 'text-amber-600' : 'text-red-600'}`}>
                      {Number(entry.successRate || 0).toFixed(2)}%
                    </span>
                  </TableCell>
                  <TableCell>
                    <StatusChip status={entry.status} />
                  </TableCell>
                  <TableCell className="text-right">
                    <Button
                      variant="ghost"
                      size="sm"
                      onClick={() => handleDownload(entry.reportId, entry.status)}
                      disabled={entry.status !== 'COMPLETED'}
                      className="text-indigo-600 hover:text-indigo-800 hover:bg-indigo-50"
                      title={entry.status === 'FAILED' ? entry.errorMessage || 'Generation failed' : undefined}
                    >
                      <Download className="mr-2 h-4 w-4" /> Download
                    </Button>
                  </TableCell>
                </TableRow>
              ))}
            </TableBody>
          </Table>
        </div>
      )}
    </div>
  );
}

