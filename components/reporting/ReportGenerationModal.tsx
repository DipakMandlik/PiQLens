"use client";

import React, { useState } from 'react';
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { Button } from "@/components/ui/button";
import { FileDown, TableProperties, Loader2, ArrowRight, FileSpreadsheet } from "lucide-react";
import { useToast } from '@/components/ui/toast';

interface ReportGenerationModalProps {
  isOpen: boolean;
  onClose: () => void;
  prefilledDate?: string;
  prefilledDataset?: string;
}

async function pollForCompletion(reportId: string, timeoutMs = 120000): Promise<string> {
  const start = Date.now();

  while (Date.now() - start < timeoutMs) {
    const res = await fetch(`/api/reports/v2/status/${reportId}`, { cache: 'no-store' });
    const payload = await res.json();

    if (!res.ok || !payload.success) {
      throw new Error(payload.error || 'Failed to check report status');
    }

    const status = payload.data?.status;
    if (status === 'COMPLETED' && payload.data?.downloadUrl) {
      return payload.data.downloadUrl as string;
    }

    if (status === 'FAILED') {
      throw new Error(payload.data?.error || 'Report generation failed');
    }

    await new Promise((resolve) => setTimeout(resolve, 2000));
  }

  throw new Error('Report generation timed out');
}

export function ReportGenerationModal({
  isOpen,
  onClose,
  prefilledDate,
  prefilledDataset,
}: ReportGenerationModalProps) {
  const [date, setDate] = useState<string>(prefilledDate || new Date().toISOString().split('T')[0]);
  const [format, setFormat] = useState<'csv' | 'xlsx'>('xlsx');
  const [variant, setVariant] = useState<'summary' | 'detailed'>('detailed');
  const [isGenerating, setIsGenerating] = useState(false);
  const { showToast } = useToast();

  const handleGenerate = async () => {
    setIsGenerating(true);

    try {
      const res = await fetch('/api/reports/v2/generate', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          date,
          format,
          variant,
          mode: 'date_aggregate',
          scope: prefilledDataset ? 'dataset' : 'platform',
          dataset: prefilledDataset,
          generatedBy: 'report-modal',
        }),
      });

      const data = await res.json();
      if (!res.ok || !data.success || !data.data?.reportId) {
        throw new Error(data.error || 'Failed to queue report generation');
      }

      showToast('Report queued. Generating now...', 'info', 2500);
      const downloadUrl = await pollForCompletion(data.data.reportId);
      showToast('Report generated successfully.', 'success', 3500);
      window.location.href = downloadUrl;
      onClose();
    } catch (error: unknown) {
      showToast(error instanceof Error ? error.message : 'Failed to generate report', 'error', 5000);
    } finally {
      setIsGenerating(false);
    }
  };

  React.useEffect(() => {
    if (isOpen) {
      setIsGenerating(false);
      if (prefilledDate) setDate(prefilledDate);
    }
  }, [isOpen, prefilledDate]);

  return (
    <Dialog
      open={isOpen}
      onOpenChange={(open) => {
        if (!open && !isGenerating) onClose();
      }}
    >
      <DialogContent className="sm:max-w-[520px]">
        <DialogHeader>
          <DialogTitle className="flex items-center gap-2 text-xl">
            <FileDown className="h-5 w-5 text-indigo-600" />
            Enterprise Report Export
          </DialogTitle>
          <DialogDescription>
            Generate structured data quality reports in CSV or Excel format.
          </DialogDescription>
        </DialogHeader>

        <div className="grid gap-6 py-4">
          <div className="grid gap-2">
            <label className="text-sm font-semibold text-slate-700">Reporting Date</label>
            <input
              type="date"
              value={date}
              onChange={(e) => setDate(e.target.value)}
              disabled={isGenerating}
              className="flex h-9 w-full rounded-md border border-slate-300 bg-white px-3 py-1 text-sm shadow-sm transition-colors focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-indigo-600 disabled:cursor-not-allowed disabled:opacity-50"
            />
          </div>

          <div className="grid gap-2">
            <label className="text-sm font-semibold text-slate-700">Format</label>
            <div className="grid grid-cols-2 gap-3">
              <button
                onClick={() => setFormat('csv')}
                disabled={isGenerating}
                className={`flex flex-col items-center justify-center gap-2 rounded-lg border p-4 transition-colors ${format === 'csv'
                  ? 'border-emerald-600 bg-emerald-50 text-emerald-700'
                  : 'border-slate-200 bg-white text-slate-600 hover:border-slate-300'
                  }`}
              >
                <TableProperties className={format === 'csv' ? 'text-emerald-600' : 'text-slate-400'} />
                <span className="font-medium">CSV</span>
              </button>
              <button
                onClick={() => setFormat('xlsx')}
                disabled={isGenerating}
                className={`flex flex-col items-center justify-center gap-2 rounded-lg border p-4 transition-colors ${format === 'xlsx'
                  ? 'border-indigo-600 bg-indigo-50 text-indigo-700'
                  : 'border-slate-200 bg-white text-slate-600 hover:border-slate-300'
                  }`}
              >
                <FileSpreadsheet className={format === 'xlsx' ? 'text-indigo-600' : 'text-slate-400'} />
                <span className="font-medium">Excel (.xlsx)</span>
              </button>
            </div>
          </div>

          <div className="grid gap-2">
            <label className="text-sm font-semibold text-slate-700">Variant</label>
            <div className="grid grid-cols-2 gap-3">
              <button
                onClick={() => setVariant('summary')}
                disabled={isGenerating}
                className={`rounded-lg border px-3 py-2 text-sm font-medium transition-colors ${variant === 'summary'
                  ? 'border-slate-700 bg-slate-100 text-slate-900'
                  : 'border-slate-200 bg-white text-slate-600 hover:border-slate-300'
                  }`}
              >
                Summary
              </button>
              <button
                onClick={() => setVariant('detailed')}
                disabled={isGenerating}
                className={`rounded-lg border px-3 py-2 text-sm font-medium transition-colors ${variant === 'detailed'
                  ? 'border-slate-700 bg-slate-100 text-slate-900'
                  : 'border-slate-200 bg-white text-slate-600 hover:border-slate-300'
                  }`}
              >
                Detailed
              </button>
            </div>
            <p className="text-[11px] text-slate-500">
              Detailed reports include failed check-level rows (up to 1000 entries).
            </p>
          </div>
        </div>

        <DialogFooter>
          <Button variant="ghost" onClick={onClose} disabled={isGenerating} className="text-slate-600">
            Cancel
          </Button>
          <Button
            onClick={handleGenerate}
            disabled={isGenerating}
            className="bg-indigo-600 text-white hover:bg-indigo-700"
          >
            {isGenerating ? (
              <>
                <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                Generating...
              </>
            ) : (
              <>
                Generate <ArrowRight className="ml-2 h-4 w-4" />
              </>
            )}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}

