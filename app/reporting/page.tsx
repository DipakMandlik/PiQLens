/**
 * Reporting Home Page
 */

'use client';

import { useState } from 'react';
import Link from 'next/link';
import { Calendar, FileText, TrendingUp } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { BackButton } from '@/components/back-button';
import { useToast } from '@/components/ui/toast';

async function pollReport(reportId: string, timeoutMs = 120000): Promise<string> {
  const start = Date.now();

  while (Date.now() - start < timeoutMs) {
    const statusRes = await fetch(`/api/reports/v2/status/${reportId}`, { cache: 'no-store' });
    const statusPayload = await statusRes.json();

    if (!statusRes.ok || !statusPayload.success) {
      throw new Error(statusPayload.error || 'Failed to check report status');
    }

    if (statusPayload.data?.status === 'COMPLETED' && statusPayload.data?.downloadUrl) {
      return statusPayload.data.downloadUrl;
    }

    if (statusPayload.data?.status === 'FAILED') {
      throw new Error(statusPayload.data?.error || 'Report generation failed');
    }

    await new Promise((resolve) => setTimeout(resolve, 2000));
  }

  throw new Error('Report generation timed out');
}

export default function ReportingPage() {
  const [isGenerating, setIsGenerating] = useState(false);
  const { showToast } = useToast();

  const handleQuickGenerate = async (scope: 'platform' | 'dataset', format: 'xlsx' | 'csv') => {
    setIsGenerating(true);

    try {
      const today = new Date().toISOString().split('T')[0];

      const response = await fetch('/api/reports/v2/generate', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          scope,
          mode: 'date_aggregate',
          date: today,
          format,
          variant: 'detailed',
          generatedBy: 'reporting-page',
        }),
      });

      const result = await response.json();
      if (!response.ok || !result.success || !result.data?.reportId) {
        throw new Error(result.error || 'Failed to queue report generation');
      }

      showToast('Report queued. Generating now...', 'info', 2500);
      const downloadUrl = await pollReport(result.data.reportId as string);
      showToast('Report generated successfully.', 'success', 3500);
      window.location.href = downloadUrl;
    } catch (error: unknown) {
      showToast(error instanceof Error ? error.message : 'Failed to generate report', 'error', 5000);
    } finally {
      setIsGenerating(false);
    }
  };

  return (
    <div className="min-h-screen bg-gray-50 p-8">
      <div className="max-w-7xl mx-auto">
        <div className="mb-8">
          <div className="mb-4">
            <BackButton href="/" label="Back to Home" />
          </div>
          <h1 className="text-3xl font-bold text-gray-900 mb-2">Reporting</h1>
          <p className="text-gray-600">Generate, view, and manage data quality reports.</p>
        </div>

        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-6 mb-8">
          <Card className="hover:shadow-lg transition-shadow cursor-pointer">
            <CardHeader>
              <CardTitle className="flex items-center gap-2 text-lg">
                <FileText className="w-5 h-5 text-blue-600" />
                Platform Report
              </CardTitle>
            </CardHeader>
            <CardContent>
              <p className="text-sm text-gray-600 mb-4">Overall data quality for today</p>
              <div className="flex gap-2">
                <Button size="sm" onClick={() => handleQuickGenerate('platform', 'xlsx')} disabled={isGenerating}>
                  XLSX
                </Button>
                <Button size="sm" variant="outline" onClick={() => handleQuickGenerate('platform', 'csv')} disabled={isGenerating}>
                  CSV
                </Button>
              </div>
            </CardContent>
          </Card>

          <Card className="hover:shadow-lg transition-shadow">
            <CardHeader>
              <CardTitle className="flex items-center gap-2 text-lg">
                <FileText className="w-5 h-5 text-indigo-600" />
                Daily Intelligence
              </CardTitle>
            </CardHeader>
            <CardContent>
              <p className="text-sm text-gray-600 mb-4">Governance-grade offline reporting</p>
              <Link href="/reports">
                <Button size="sm" variant="default" className="w-full bg-indigo-600 hover:bg-indigo-700">
                  Manage Reports
                </Button>
              </Link>
            </CardContent>
          </Card>

          <Card className="hover:shadow-lg transition-shadow">
            <CardHeader>
              <CardTitle className="flex items-center gap-2 text-lg">
                <Calendar className="w-5 h-5 text-purple-600" />
                Scheduled Reports
              </CardTitle>
            </CardHeader>
            <CardContent>
              <p className="text-sm text-gray-600 mb-4">Manage automated report delivery</p>
              <Link href="/reporting/scheduled">
                <Button size="sm" variant="outline" className="w-full">
                  Manage Schedules
                </Button>
              </Link>
            </CardContent>
          </Card>

          <Card className="hover:shadow-lg transition-shadow">
            <CardHeader>
              <CardTitle className="flex items-center gap-2 text-lg">
                <TrendingUp className="w-5 h-5 text-orange-600" />
                Custom Report
              </CardTitle>
            </CardHeader>
            <CardContent>
              <p className="text-sm text-gray-600 mb-4">Generate custom reports</p>
              <Link href="/reporting/generate">
                <Button size="sm" variant="outline" className="w-full">
                  Create Report
                </Button>
              </Link>
            </CardContent>
          </Card>
        </div>

        <div className="bg-white rounded-lg shadow p-6 mb-8">
          <h2 className="text-xl font-semibold mb-4">Export Formats</h2>

          <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
            <div className="border rounded-lg p-4">
              <h3 className="font-semibold mb-2">PDF</h3>
              <p className="text-sm text-gray-600">
                Business-friendly format for leadership, audits, and presentations.
              </p>
              <span className="text-xs text-orange-600 mt-2 block">Coming Soon</span>
            </div>

            <div className="border rounded-lg p-4">
              <h3 className="font-semibold mb-2">CSV</h3>
              <p className="text-sm text-gray-600">Tabular format for analysis in spreadsheet tools.</p>
              <span className="text-xs text-green-600 mt-2 block">Available</span>
            </div>

            <div className="border rounded-lg p-4">
              <h3 className="font-semibold mb-2">XLSX</h3>
              <p className="text-sm text-gray-600">Enterprise-friendly workbook export with formatted sheets.</p>
              <span className="text-xs text-green-600 mt-2 block">Available</span>
            </div>
          </div>
        </div>

        <div className="bg-white rounded-lg shadow p-6">
          <h2 className="text-xl font-semibold mb-4">Enterprise Notes</h2>
          <ul className="space-y-2 text-sm text-gray-700">
            <li>Reports are generated asynchronously for reliability on large datasets.</li>
            <li>Detailed exports include failed checks up to 1000 rows.</li>
            <li>Timestamps are standardized in IST for consistency.</li>
          </ul>
        </div>
      </div>
    </div>
  );
}

