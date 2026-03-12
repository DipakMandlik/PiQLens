'use client';

import { useState, useEffect } from 'react';
import Link from 'next/link';
import { Calendar as CalendarIcon, Clock, Loader2, Gauge, ShieldCheck, AlertTriangle, ChevronDown, FileText, Play, Zap, Search as SearchIcon, Download } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Card, CardContent } from '@/components/ui/card';
import { Calendar } from '@/components/ui/calendar';
import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from "@/components/ui/popover";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
  DropdownMenuSeparator,
} from "@/components/ui/dropdown-menu";
import { CircularMetricCard } from '@/components/dashboard/CircularMetricCard';
import { Sidebar } from '@/components/layout/Sidebar';
import { TopNav } from '@/components/layout/TopNav';
import { ScanTimeline } from '@/components/dashboard/ScanTimeline';
import { useAppStore } from '@/lib/store';
import { getTodayIST, isTodayIST, formatDateReadable } from '@/lib/timezone-utils';
import { useToast } from '@/components/ui/toast';

// Helper functions for quality classification
function calculateQualityGrade(score: number): 'A+' | 'A' | 'B' | 'C' | 'D' | 'F' {
  if (score >= 97) return 'A+';
  if (score >= 93) return 'A';
  if (score >= 85) return 'B';
  if (score >= 70) return 'C';
  if (score >= 60) return 'D';
  return 'F';
}

function calculateTrustLevel(score: number): 'HIGH' | 'MEDIUM' | 'LOW' {
  if (score >= 90) return 'HIGH';
  if (score >= 70) return 'MEDIUM';
  return 'LOW';
}

export default function HomePage() {
  const { isConnected } = useAppStore();
  const { showToast } = useToast();

  // Date selector state
  const [selectedDate, setSelectedDate] = useState<Date>(new Date());
  const [availableDates, setAvailableDates] = useState<string[]>([]);
  const [calendarOpen, setCalendarOpen] = useState(false);

  // Scan state
  const [isScanning, setIsScanning] = useState(false);
  const [scanMode, setScanMode] = useState<'incremental' | 'full'>('incremental');
  const [currentRunId, setCurrentRunId] = useState<string | null>(null);

  // UI state
  const [lastScanTime, setLastScanTime] = useState('Not executed');
  const [refreshKey, setRefreshKey] = useState(0);
  const [hasData, setHasData] = useState(true);
  const [isLoading, setIsLoading] = useState(true);
  const [scanModeUsed, setScanModeUsed] = useState<string>('Unknown');
  const [datasetsScanned, setDatasetsScanned] = useState(0);
  const [rowsValidated, setRowsValidated] = useState(0);

  // Metrics state
  // Metrics state
  const [qualityScore, setQualityScore] = useState(0);
  const [qualityStatus, setQualityStatus] = useState('Unknown');

  // Counts (card subtitles)
  const [totalExecuted, setTotalExecuted] = useState(0);
  const [coverageExecuted, setCoverageExecuted] = useState(0);
  const [validityExecuted, setValidityExecuted] = useState(0);

  // Separate Scores
  const [coveragePercent, setCoveragePercent] = useState(0);
  const [validityPercent, setValidityPercent] = useState(0);
  const [coverageStrength, setCoverageStrength] = useState('Unknown');

  // Today vs Yesterday card deltas
  const [overallDelta, setOverallDelta] = useState<number | null>(null);
  const [coverageDelta, setCoverageDelta] = useState<number | null>(null);
  const [validityDelta, setValidityDelta] = useState<number | null>(null);
  const [overallInsight, setOverallInsight] = useState('No previous day data available.');
  const [coverageInsight, setCoverageInsight] = useState('No previous day data available.');
  const [validityInsight, setValidityInsight] = useState('No previous day data available.');

  const [activeChecks, setActiveChecks] = useState(0); // This is now unique checks from header API
  const [passedToday, setPassedToday] = useState(0);
  const [failedToday, setFailedToday] = useState(0);
  const [openAnomalies, setOpenAnomalies] = useState(0);
  const [slaBreaches, setSlaBreaches] = useState(0);
  const [riskLevel, setRiskLevel] = useState<'Low' | 'Medium' | 'High'>('Low');
  const [attentionItems, setAttentionItems] = useState<Array<{ severity: string; message: string; icon: string }>>([]);

  // Dimension Scores (for insights)
  const [todayDimensions, setTodayDimensions] = useState<any>(null);

  // Timeline State
  const [timelineData, setTimelineData] = useState<any[]>([]);

  // Download state
  const [isDownloading, setIsDownloading] = useState(false);

  // Fetch available dates for calendar
  useEffect(() => {
    if (!isConnected) return;

    const fetchAvailableDates = async () => {
      try {
        const response = await fetch('/api/dq/available-dates');
        const result = await response.json();

        if (result.success) {
          const dates = Array.isArray(result.data?.availableDates)
            ? result.data.availableDates
            : Array.isArray(result.data?.dates)
              ? result.data.dates
              : [];

          setAvailableDates(dates);
        } else {
          setAvailableDates([]);
        }
      } catch (error) {
        console.error('Error fetching available dates:', error);
      }
    };

    fetchAvailableDates();
  }, [isConnected, refreshKey]);

  // Fetch dashboard data
  useEffect(() => {
    if (!isConnected) {
      setIsLoading(false);
      return;
    }

    const fetchData = async () => {
      setIsLoading(true);
      try {
        const selectedDateStr = selectedDate.toLocaleDateString('en-CA', { timeZone: 'Asia/Kolkata' });
        const queryParam = selectedDateStr !== getTodayIST() ? `?date=${selectedDateStr}` : '';

        const [compareRes, checksRes, failedRes, slaRes, anomaliesRes, timelineRes] = await Promise.all([
          fetch(`/api/dq/card-comparison${queryParam}`),
          fetch(`/api/dq/total-checks${queryParam}`),
          fetch(`/api/dq/failed-checks${queryParam}`),
          fetch(`/api/dq/sla-compliance${queryParam}`),
          fetch(`/api/dq/critical-failed-records${queryParam}`),
          fetch(`/api/dq/todays-activity${queryParam}`),
        ]);

        const [compareData, checksData, failedData, slaData, anomaliesData, timelineData] = await Promise.all([
          compareRes.json(),
          checksRes.json(),
          failedRes.json(),
          slaRes.json(),
          anomaliesRes.json(),
          timelineRes.json()
        ]);

        const dataExists = (compareData.success && compareData.data?.hasData) || (checksData.success && checksData.data?.hasData);
        setHasData(!!dataExists);

        if (!dataExists) {
          resetMetrics();
          setTimelineData([]);
        } else {
          processMetrics(compareData, checksData, failedData, slaData, anomaliesData);
          if (timelineData.success && timelineData.data) {
            setTimelineData(timelineData.data);
            // Extract dimension scores from latest run
            if (timelineData.data.length > 0) {
              const latestRun = timelineData.data[0];
              setTodayDimensions(latestRun.dimensions);
            }
          } else {
            setTimelineData([]);
          }
        }
      } catch (error) {
        console.error('Error fetching dashboard data:', error);
      } finally {
        setIsLoading(false);
      }
    };

    fetchData();
  }, [isConnected, refreshKey, selectedDate]);

  // ... (Polling logic unchanged) ...

  const resetMetrics = () => {
    setQualityScore(0);
    setQualityStatus('Unknown');
    setTotalExecuted(0);
    setCoverageExecuted(0);
    setValidityExecuted(0);
    setActiveChecks(0);
    setFailedToday(0);
    setPassedToday(0);
    setCoveragePercent(0);
    setValidityPercent(0);
    setCoverageStrength('Unknown');
    setSlaBreaches(0);
    setOpenAnomalies(0);
    setAttentionItems([]);
    setLastScanTime('No scans executed on this date');
    setScanModeUsed('Unknown');
    setDatasetsScanned(0);
    setRowsValidated(0);
    setOverallDelta(null);
    setCoverageDelta(null);
    setValidityDelta(null);
    setOverallInsight('No previous day data available.');
    setCoverageInsight('No previous day data available.');
    setValidityInsight('No previous day data available.');
  };

  const processMetrics = (compareData: any, checksData: any, failedData: any, slaData: any, anomaliesData: any) => {
    // 1. Process top-card metrics from latest today vs latest yesterday
    if (compareData.success && compareData.data?.today) {
      const today = compareData.data.today;
      const deltas = compareData.data.deltas || {};
      const insights = compareData.data.microInsights || {};

      setQualityScore(Math.round(today.overallScore || 0));
      setTotalExecuted(today.totalChecks || 0);
      setFailedToday(today.failedChecks || 0);
      setPassedToday((today.totalChecks || 0) - (today.failedChecks || 0));

      if ((today.overallScore || 0) >= 90) setQualityStatus('Excellent');
      else if ((today.overallScore || 0) >= 75) setQualityStatus('Good');
      else if ((today.overallScore || 0) >= 60) setQualityStatus('Fair');
      else setQualityStatus('Poor');

      setCoveragePercent(Math.round(today.coverageScore || 0));
      setCoverageExecuted(today.totalChecks || 0);
      setValidityPercent(Math.round(today.validityScore || 0));
      setValidityExecuted(today.totalChecks || 0);

      if ((today.coverageScore || 0) >= 90) setCoverageStrength('Strong');
      else if ((today.coverageScore || 0) >= 75) setCoverageStrength('Moderate');
      else setCoverageStrength('Weak');

      setTodayDimensions(today.dimensions || null);

      setOverallDelta(typeof deltas.overall === 'number' ? deltas.overall : null);
      setCoverageDelta(typeof deltas.coverage === 'number' ? deltas.coverage : null);
      setValidityDelta(typeof deltas.validity === 'number' ? deltas.validity : null);

      setOverallInsight(insights.overall || 'No previous day data available.');
      setCoverageInsight(insights.coverage || 'No previous day data available.');
      setValidityInsight(insights.validity || 'No previous day data available.');
    }

    // 2. Process Unique Header Counts (Dataset/Rules)
    if (checksData.success && checksData.data) {
      setActiveChecks(checksData.data.totalChecks || 0); // UNIQUE CHECKS

      if (checksData.data.lastExecution) {
        const lastExecutionRaw = String(checksData.data.lastExecution).trim();
        setLastScanTime(lastExecutionRaw.replace('T', ' ').replace('Z', ''));
      } else {
        setLastScanTime('Unknown');
      }

      setDatasetsScanned(checksData.data.datasetsProcessed || 0);
      setScanModeUsed(checksData.data.runType || 'Unknown');
      setRowsValidated(checksData.data.rowsValidated || 0);
    }

    // 3. Process Failed Counts (for reference/legacy usage)
    if (failedData.success && failedData.data) {
      const failed = failedData.data.totalFailedChecks || 0;
      setFailedToday(failed);
      setPassedToday((checksData.data?.totalChecks || 0) - failed); // Note: This passed count might be mixed logic now, but less critical
    }

    // 4. SLA & Anomalies
    if (slaData.success && slaData.data) {
      const slaCompliance = slaData.data.slaCompliancePct || 100;
      if (slaCompliance < 100) {
        setSlaBreaches(Math.round((100 - slaCompliance) / 10));
      } else {
        setSlaBreaches(0);
      }
    }

    if (anomaliesData.success && anomaliesData.data) {
      const anomalies = anomaliesData.data.criticalFailedRecords || 0;
      setOpenAnomalies(anomalies);
      if (anomalies > 10 || (slaData.data?.slaCompliancePct || 100) < 80) {
        setRiskLevel('High');
      } else if (anomalies > 5 || (slaData.data?.slaCompliancePct || 100) < 90) {
        setRiskLevel('Medium');
      } else {
        setRiskLevel('Low');
      }
    }

    // 5. Build Attention Items
    const items = [];
    if (failedData.success && failedData.data && failedData.data.totalFailedChecks > 0) {
      items.push({
        severity: 'critical',
        message: `${failedData.data.totalFailedChecks} checks failing across datasets`,
        icon: '🔴'
      });
    }
    if (slaData.success && slaData.data && slaData.data.slaCompliancePct < 100) {
      items.push({
        severity: 'warning',
        message: `SLA compliance at ${Math.round(slaData.data.slaCompliancePct)}%`,
        icon: '🟠'
      });
    }
    if (anomaliesData.success && anomaliesData.data && anomaliesData.data.criticalFailedRecords > 0) {
      items.push({
        severity: 'info',
        message: `${anomaliesData.data.criticalFailedRecords} critical records need attention`,
        icon: '🔵'
      });
    }
    setAttentionItems(items);
  };

  // ... (Handlers unchanged) ...

  const handleRunScan = async (mode: 'incremental' | 'full') => {
    setIsScanning(true);
    setScanMode(mode);

    try {
      const response = await fetch('/api/dq/run-scan', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ scanType: mode }),
      });

      const result = await response.json();

      if (result.success) {
        setCurrentRunId(result.data.runId);
        setIsScanning(false);
        setRefreshKey((prev) => prev + 1);
      } else {
        alert('⚠️ Scan failed: ' + result.error);
        setIsScanning(false);
      }
    } catch (error) {
      console.error('Error running scan:', error);
      alert('❌ Failed to initiate scan. Please try again.');
      setIsScanning(false);
    }
  };

  const handleDownloadReport = async () => {
    setIsDownloading(true);
    try {
      const selectedDateStr = selectedDate.toLocaleDateString('en-CA', { timeZone: 'Asia/Kolkata' });

      const createResponse = await fetch('/api/reports/v2/generate', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          format: 'xlsx',
          variant: 'detailed',
          mode: 'date_aggregate',
          date: selectedDateStr,
          scope: 'platform',
          generatedBy: 'home-dashboard',
        }),
      });

      const createPayload = await createResponse.json();
      if (!createResponse.ok || !createPayload.success || !createPayload.data?.reportId) {
        throw new Error(createPayload.error || 'Failed to queue report generation');
      }

      showToast('Report queued. Generating now...', 'info', 2500);

      const reportId = createPayload.data.reportId as string;
      const started = Date.now();
      const timeoutMs = 120000;

      while (Date.now() - started < timeoutMs) {
        const statusResponse = await fetch(`/api/reports/v2/status/${reportId}`, { cache: 'no-store' });
        const statusPayload = await statusResponse.json();

        if (!statusResponse.ok || !statusPayload.success) {
          throw new Error(statusPayload.error || 'Failed to check report status');
        }

        const status = statusPayload.data?.status;
        if (status === 'COMPLETED' && statusPayload.data?.downloadUrl) {
          showToast('Report generated successfully.', 'success', 3500);
          window.location.href = statusPayload.data.downloadUrl;
          return;
        }

        if (status === 'FAILED') {
          throw new Error(statusPayload.data?.error || 'Report generation failed');
        }

        await new Promise((resolve) => setTimeout(resolve, 2000));
      }

      throw new Error('Report generation timed out');
    } catch (error: unknown) {
      console.error('Error generating report:', error);
      showToast(error instanceof Error ? error.message : 'Failed to generate report. Please try again.', 'error', 5000);
    } finally {
      setIsDownloading(false);
    }
  };

  const handleDateSelect = (date: Date | undefined) => {
    if (date) {
      setSelectedDate(date);
      setCalendarOpen(false);
    }
  };

  // Check if a date is in available dates list
  const isDateAvailable = (date: Date): boolean => {
    const dateStr = date.toLocaleDateString('en-CA', { timeZone: 'Asia/Kolkata' });

    // If date list is empty (API error/empty response), allow historical dates up to today.
    if (availableDates.length === 0) {
      return dateStr <= getTodayIST();
    }

    // Keep today selectable even when no checks were executed today.
    return availableDates.includes(dateStr) || dateStr === getTodayIST();
  };

  const selectedDateStr = selectedDate.toLocaleDateString('en-CA', { timeZone: 'Asia/Kolkata' });
  const isToday = isTodayIST(selectedDateStr);
  const canScan = isToday && !isScanning && isConnected;

  return (
    <div className="flex flex-col h-screen overflow-hidden bg-gray-50">
      <TopNav />

      <div className="flex flex-1 overflow-hidden">
        <Sidebar />

        <main className="flex-1 overflow-y-auto">
          <div className="px-8 py-6">
            {/* Enterprise Header V2.1 - Unified Row */}
            <div className="bg-white border border-slate-200 rounded-lg shadow-sm px-8 py-6 mb-8">
              <div className="grid grid-cols-1 xl:grid-cols-12 gap-6 items-center">

                {/* LEFT SECTION (Span 5): Title & Metadata */}
                <div className="col-span-12 xl:col-span-5 flex flex-col gap-2">
                  <h1 className="text-xl font-semibold text-slate-900 tracking-tight">
                    Daily Data Quality Overview
                  </h1>

                  {hasData ? (
                    <div className="flex flex-wrap items-center gap-x-4 gap-y-2 text-sm leading-relaxed">
                      {/* Last Scan */}
                      <div className="flex items-center gap-2 text-slate-500 font-medium bg-slate-50 px-3 py-1.5 rounded-md border border-slate-200 shadow-sm">
                        <Clock className="w-4 h-4 text-indigo-500" />
                        <span>Last:</span>
                        <span className="text-slate-800 font-bold">
                          {(() => {
                            if (!lastScanTime) return 'Never';
                            // lastScanTime is currently a string like "2026-02-25 17:59:22.631"
                            // If it doesn't have a 'Z' or timezone, JS might parse it as local time.
                            // To be safe, let's just show the string directly if it's already well-formatted
                            return lastScanTime.split('.')[0]; // Remove ms
                          })()}
                        </span>
                      </div>

                      <span className="hidden sm:inline text-slate-300">•</span>

                      {/* Mode Badge */}
                      <div className="flex items-center gap-1.5 px-2.5 py-0.5 rounded-full bg-slate-100 text-slate-700 border border-slate-200">
                        <span className="text-xs font-semibold uppercase tracking-wide">{scanModeUsed}</span>
                      </div>

                      <span className="hidden sm:inline text-slate-300">•</span>

                      {/* Metrics */}
                      <div className="flex items-center gap-3 text-slate-600">
                        <span title="Datasets Evaluated">
                          <strong className="text-slate-900 font-semibold">{datasetsScanned}</strong> Datasets Evaluated
                        </span>
                        <div className="h-1 w-1 rounded-full bg-slate-300"></div>
                        <span title="Rules Executed">
                          <strong className="text-slate-900 font-semibold">{activeChecks}</strong> Rules Executed
                        </span>
                      </div>
                    </div>
                  ) : (
                    <div className="flex items-center gap-2 text-sm text-slate-500">
                      <AlertTriangle className="w-4 h-4 text-amber-500" />
                      {isToday ? 'No scans executed today' : `No scan data for ${formatDateReadable(selectedDateStr)}`}
                    </div>
                  )}
                </div>

                {/* RIGHT SECTION (Span 7): Actions Row (Date -> Scan -> Download) */}
                <div className="col-span-12 xl:col-span-7 flex flex-col sm:flex-row items-center justify-end gap-3">

                  {/* 1. Date Selector */}
                  <div className="flex items-center gap-2 w-full sm:w-auto">
                    <Popover open={calendarOpen} onOpenChange={setCalendarOpen}>
                      <PopoverTrigger asChild>
                        <Button
                          variant="outline"
                          className="w-full sm:w-[240px] h-10 justify-start text-left font-normal border-slate-300 text-slate-700 hover:bg-slate-50 hover:text-slate-900 shadow-sm focus:ring-2 focus:ring-slate-200 transition-all"
                        >
                          <CalendarIcon className="mr-3 h-4 w-4 text-slate-500" />
                          <span className="flex-1 truncate">{formatDateReadable(selectedDateStr)}</span>
                          {isToday && (
                            <span className="ml-2 text-[10px] font-bold bg-indigo-50 text-indigo-700 px-2 py-0.5 rounded uppercase tracking-wide">Today</span>
                          )}
                          <ChevronDown className="ml-2 h-4 w-4 text-slate-400 opacity-50" />
                        </Button>
                      </PopoverTrigger>
                      <PopoverContent className="w-auto p-0" align="end">
                        <Calendar
                          mode="single"
                          selected={selectedDate}
                          onSelect={handleDateSelect}
                          disabled={(date) => !isDateAvailable(date)}
                          initialFocus
                          className="rounded-md border-0"
                        />
                      </PopoverContent>
                    </Popover>

                    {!isToday && (
                      <Button
                        variant="ghost"
                        size="icon"
                        onClick={() => handleDateSelect(new Date())}
                        title="Jump to Today"
                        className="h-10 w-10 text-slate-400 hover:text-indigo-600 hover:bg-indigo-50"
                      >
                        <Zap className="h-4 w-4 fill-current" />
                      </Button>
                    )}
                  </div>

                  {/* 2. Scan Button (Middle) */}
                  {canScan ? (
                    <DropdownMenu>
                      <DropdownMenuTrigger asChild>
                        <Button
                          className="w-full sm:w-auto h-10 px-6 bg-indigo-700 hover:bg-indigo-800 text-white font-medium shadow-sm transition-all"
                        >
                          {isScanning ? (
                            <>
                              <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                              Scanning
                            </>
                          ) : (
                            <>
                              <Play className="mr-2 h-4 w-4 fill-current" />
                              Scan Now
                              <ChevronDown className="ml-2 h-4 w-4 opacity-70" />
                            </>
                          )}
                        </Button>
                      </DropdownMenuTrigger>
                      <DropdownMenuContent align="end" className="w-64 p-1">
                        <DropdownMenuItem onClick={() => handleRunScan('incremental')} disabled={isScanning} className="cursor-pointer">
                          <div className="flex items-center gap-3 py-1">
                            <div className="p-1.5 bg-indigo-50 rounded-md text-indigo-600">
                              <Zap className="w-4 h-4" />
                            </div>
                            <div className="flex flex-col">
                              <span className="font-medium text-slate-900">Incremental Scan</span>
                              <span className="text-xs text-slate-500">Process changes only</span>
                            </div>
                          </div>
                        </DropdownMenuItem>
                        <DropdownMenuSeparator className="my-1" />
                        <DropdownMenuItem onClick={() => handleRunScan('full')} disabled={isScanning} className="cursor-pointer">
                          <div className="flex items-center gap-3 py-1">
                            <div className="p-1.5 bg-slate-100 rounded-md text-slate-600">
                              <SearchIcon className="w-4 h-4" />
                            </div>
                            <div className="flex flex-col">
                              <span className="font-medium text-slate-900">Full Refresh</span>
                              <span className="text-xs text-slate-500">Reprocess all data</span>
                            </div>
                          </div>
                        </DropdownMenuItem>
                      </DropdownMenuContent>
                    </DropdownMenu>
                  ) : (
                    !isToday ? (
                      <div className="hidden sm:flex h-10 items-center px-4 bg-slate-50 border border-slate-200 rounded-md text-xs font-medium text-slate-500">
                        Historical View
                      </div>
                    ) : (
                      <Button disabled variant="outline" className="h-10 px-6 border-dashed border-slate-300 text-slate-400 bg-slate-50">
                        Scan Disabled
                      </Button>
                    )
                  )}

                  {/* 3. Download Button (Last) */}
                  <Button
                    variant="outline"
                    className="w-full sm:w-auto h-10 px-4 font-medium border-slate-200 text-slate-600 hover:bg-slate-50 hover:text-slate-900 hover:border-slate-300 shadow-sm transition-all"
                    onClick={handleDownloadReport}
                    disabled={isDownloading || !hasData}
                  >
                    {isDownloading ? (
                      <Loader2 className="mr-2 h-4 w-4 animate-spin text-slate-400" />
                    ) : (
                      <Download className="mr-2 h-4 w-4 text-slate-400" />
                    )}
                    {isDownloading ? 'Exporting...' : 'Export Report'}
                  </Button>

                </div>
              </div>
            </div>

            {/* KPI Cards */}
            {!hasData ? (
              <div className="flex flex-col items-center justify-center p-12 bg-white border border-gray-200 rounded-lg mb-6">
                <div className="bg-gray-100 p-4 rounded-full mb-4">
                  <CalendarIcon className="h-8 w-8 text-gray-400" />
                </div>
                <h3 className="text-lg font-semibold text-gray-900 mb-1">
                  {isToday ? 'No Scans Executed Today' : 'No Data Available'}
                </h3>
                <p className="text-sm text-gray-600 mb-6 max-w-md text-center">
                  {isToday
                    ? 'Run your first scan to start monitoring data quality metrics.'
                    : `No scan results found for ${formatDateReadable(selectedDateStr)}.`
                  }
                </p>

                {isToday && canScan && (
                  <DropdownMenu>
                    <DropdownMenuTrigger asChild>
                      <Button className="h-10 px-6 bg-indigo-600 hover:bg-indigo-700 text-white font-semibold">
                        <Play className="mr-2 h-4 w-4" />
                        Run First Scan
                        <ChevronDown className="ml-2 h-4 w-4" />
                      </Button>
                    </DropdownMenuTrigger>
                    <DropdownMenuContent>
                      <DropdownMenuItem onClick={() => handleRunScan('incremental')}>
                        <Zap className="mr-2 h-4 w-4" />
                        Incremental Scan
                      </DropdownMenuItem>
                      <DropdownMenuItem onClick={() => handleRunScan('full')}>
                        <SearchIcon className="mr-2 h-4 w-4" />
                        Full Scan
                      </DropdownMenuItem>
                    </DropdownMenuContent>
                  </DropdownMenu>
                )}
              </div>
            ) : (
              <div className="grid grid-cols-3 gap-6 mb-6">
                <CircularMetricCard
                  title="Overall Quality Score"
                  type="overall"
                  icon={Gauge}
                  color={qualityScore >= 90 ? 'green' : qualityScore >= 80 ? 'blue' : qualityScore >= 70 ? 'amber' : 'red'}
                  todayScore={qualityScore}
                  todayChecks={totalExecuted}
                  todayFailed={failedToday}
                  qualityGrade={calculateQualityGrade(qualityScore)}
                  trustLevel={calculateTrustLevel(qualityScore)}
                  slaMet={qualityScore >= 90}
                  delta={overallDelta}
                  deltaTooltip="No previous day data available"
                  microInsight={overallInsight}
                />
                <CircularMetricCard
                  title="Coverage Score"
                  type="coverage"
                  icon={ShieldCheck}
                  color={coveragePercent >= 90 ? 'green' : coveragePercent >= 80 ? 'blue' : coveragePercent >= 70 ? 'amber' : 'red'}
                  todayScore={coveragePercent}
                  todayChecks={coverageExecuted}
                  todayFailed={failedToday}
                  qualityGrade={calculateQualityGrade(coveragePercent)}
                  trustLevel={calculateTrustLevel(coveragePercent)}
                  slaMet={coveragePercent >= 90}
                  delta={coverageDelta}
                  deltaTooltip="No previous day data available"
                  microInsight={coverageInsight}
                />
                <CircularMetricCard
                  title="Validity Score"
                  type="validity"
                  icon={AlertTriangle}
                  color={validityPercent >= 90 ? 'green' : validityPercent >= 80 ? 'blue' : validityPercent >= 70 ? 'amber' : 'red'}
                  todayScore={validityPercent}
                  todayChecks={validityExecuted}
                  todayFailed={failedToday}
                  qualityGrade={calculateQualityGrade(validityPercent)}
                  trustLevel={calculateTrustLevel(validityPercent)}
                  slaMet={validityPercent >= 90}
                  delta={validityDelta}
                  deltaTooltip="No previous day data available"
                  microInsight={validityInsight}
                />
              </div>
            )}

            {/* Attention Items */}
            <Card className="border border-gray-200 shadow-sm mb-8">
              <CardContent className="p-6">
                <div className="flex items-center justify-between mb-4">
                  <h2 className="text-lg font-semibold text-gray-900">What Needs Attention Today</h2>
                  <Link
                    href="/issues"
                    className="text-sm font-medium text-indigo-600 hover:text-indigo-700"
                  >
                    View all →
                  </Link>
                </div>

                <div className="space-y-2">
                  {attentionItems.map((item, index) => (
                    <button
                      key={index}
                      className="w-full flex items-center justify-between p-4 rounded-md border border-gray-200 hover:border-gray-300 hover:bg-gray-50 transition-colors text-left"
                    >
                      <div className="flex items-center gap-3">
                        <span className="text-lg">{item.icon}</span>
                        <span className="text-sm font-medium text-gray-800">{item.message}</span>
                      </div>
                      <ChevronDown className="h-5 w-5 text-gray-400 -rotate-90" />
                    </button>
                  ))}


                  {attentionItems.length === 0 && (
                    <div className="text-center py-8 bg-green-50 rounded-md border border-green-200">
                      <div className="text-4xl mb-2">✨</div>
                      <h3 className="text-sm font-semibold text-gray-900 mb-1">All Clear</h3>
                      <p className="text-xs text-gray-600">
                        No critical issues detected
                      </p>
                    </div>
                  )}
                </div>
              </CardContent>
            </Card>

            {/* Timeline */}
            {hasData && timelineData.length > 0 && (
              <ScanTimeline runs={timelineData} />
            )}

          </div>
        </main>
      </div>
    </div>
  );
}












