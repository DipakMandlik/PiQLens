"use client";

import Image from "next/image";
import { useParams, useRouter } from "next/navigation";
import { useState, useEffect, useMemo, useRef } from "react";
import {
  ChevronRight, ChevronDown, Database, Table as TableIcon, Settings,
  Activity, FileText, Eye, RefreshCw, PlayCircle,
  CheckCircle2, AlertOctagon, MoreVertical, X, Loader2,
  BarChart2, AlertTriangle, Hash, Calendar as CalendarIcon, Type, Clock,
  Plus, CheckCircle, Play, Pause, Trash2, ArrowRight, Search, Zap,
  UserRound, AlarmClock, Bot
} from "lucide-react";
import { FieldsTab } from '@/components/fields/FieldsTab';
import { ChecksTab } from '@/components/checks/ChecksTab';
import { CustomSqlTab } from '@/components/custom-sql/CustomSqlTab';
import { FailuresTab } from '@/components/failures/FailuresTab';
import { SmartDatePicker } from '@/components/SmartDatePicker';
import { AntigravityInsights } from '@/components/AntigravityInsights';
import { OverviewAdditiveSections } from '@/components/overview/OverviewAdditiveSections';
import { QualityTrendChart } from '@/components/charts/QualityTrendChart';
import { DataViewMode, modeLabel } from '@/lib/dq/data-view-mode';
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogDescription,
} from "@/components/ui/dialog";
import { Card } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { useToast } from "@/components/ui/toast";
import { BackButton } from "@/components/back-button";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Popover, PopoverContent, PopoverTrigger } from "@/components/ui/popover";
import {
  ResponsiveContainer,
  LineChart,
  Line,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
} from "recharts";

export default function TableDetailsPage() {
  const params = useParams();
  const router = useRouter();
  const dataset = decodeURIComponent(params.dataset as string);
  const tableName = decodeURIComponent(params.table as string);
  const isCustomSqlTabEnabled = process.env.NEXT_PUBLIC_ENABLE_CUSTOM_SQL_TAB !== 'false';

  // Legacy formatter used outside Activity views.
  const formatUtcToLocal = (timestamp: string | null | undefined) => {
    if (!timestamp) return "-";
    const trimmed = String(timestamp).trim();
    if (!trimmed) return "-";
    return trimmed.replace("T", " ").replace("Z", "");
  };

  // Activity timestamp formatter: no timezone conversion, no Date parsing.
  const formatRawActivityTimestamp = (timestamp: string | null | undefined) => {
    if (!timestamp) return "-";
    const trimmed = String(timestamp).trim();
    if (!trimmed) return "-";
    return trimmed.replace("T", " ").replace("Z", "");
  };

  const formatDateLabel = (dateValue: string): string => {
    if (!dateValue) return "Latest Available";
    const dt = new Date(`${dateValue}T00:00:00`);
    return dt.toLocaleDateString("en-IN", { day: "2-digit", month: "short", year: "numeric" });
  };

  const formatAnomalyFlag = (flag: string): string => {
    if (!flag) return "Anomaly";
    const normalized = flag.replace(/[_-]+/g, " ").toLowerCase();
    return normalized.replace(/\b\w/g, (char) => char.toUpperCase());
  };

  type TableSummaryResponse = {
    has_data: boolean;
    resolved_date: string | null;
    total_table_records: number;
    total_today_records: number;
    total_failed: number;
    total_checks: number;
    aggregated_score: number;
    run_count: number;
    mode?: DataViewMode;
    active_mode_value?: number;
  };

  type RunSummaryResponse = {
    run_id: string;
    run_type: string;
    execution_time: string | null;
    execution_duration: number;
    fail_rate: number;
    impacted_columns: string;
    failed_checks: number;
    checks_passed: number;
    checks_failed: number;
    rows_added_today: number;
    rows_failed: number;
    columns_impacted: number;
    most_failed_check: string;
    most_affected_column: string;
  };
  type ActivityRun = {
    run_id: string;
    run_type: string;
    execution_mode?: string | null;
    status?: string | null;
    run_status?: string | null;
    triggered_by?: string | null;
    started_at?: string | null;
    completed_at?: string | null;
    start_ts?: string | null;
    end_ts?: string | null;
    duration_seconds?: number | null;
    total_checks?: number | null;
    checks_executed?: number | null;
    passed_checks?: number | null;
    failed_checks?: number | null;
    failed?: number | null;
    warning_checks?: number | null;
    warnings?: number | null;
    skipped_checks?: number | null;
    scan_type?: string | null;
  };

  const formatDuration = (seconds: number | null | undefined) => {
    if (seconds === null || seconds === undefined || Number.isNaN(Number(seconds))) return '-';
    const safe = Math.max(0, Number(seconds));
    if (safe < 60) return `${safe.toFixed(1)}s`;
    const mins = Math.floor(safe / 60);
    const secs = Math.round(safe % 60);
    return `${mins}m ${secs}s`;
  };
  const normalizeActivityRunType = (runType?: string | null) => {
    const normalized = String(runType || "").toUpperCase();
    if (["INCREMENTAL", "INCREMENTAL_SCAN"].includes(normalized) || normalized.includes("INCR")) return "INCREMENTAL_SCAN";
    if (["CUSTOM", "CUSTOM_SCAN"].includes(normalized)) return "CUSTOM_SCAN";
    if (normalized === "PROFILING") return "PROFILING";
    return "FULL_SCAN";
  };

  // Execution mode describes trigger source (manual/scheduled/auto), not scan algorithm type.
  const normalizeExecutionMode = (executionMode?: string | null, triggeredBy?: string | null): "MANUAL" | "SCHEDULED" | "AUTO" => {
    const mode = String(executionMode || "").trim().toUpperCase();
    const source = String(triggeredBy || "").trim().toUpperCase();

    const resolveMode = (value: string): "MANUAL" | "SCHEDULED" | "AUTO" => {
      if (["A", "AUTO", "AUTOMATED", "BOT", "ETL_PIPELINE"].includes(value)) return "AUTO";
      if (["S", "SCHEDULED", "SYSTEM", "SCHEDULER"].includes(value)) return "SCHEDULED";
      if (["D", "MANUAL", "ADHOC"].includes(value)) return "MANUAL";
      return "MANUAL";
    };

    // execution_mode is authoritative when present; triggered_by is UI fallback only.
    if (mode) return resolveMode(mode);
    if (source) return resolveMode(source);
    return "MANUAL";
  };

  const getExecutionModeMeta = (executionMode?: string | null, triggeredBy?: string | null) => {
    const mode = normalizeExecutionMode(executionMode, triggeredBy);

    if (mode === "AUTO") {
      return {
        mode,
        label: "Auto",
        icon: <Bot size={12} className="text-violet-600" />,
        className: "bg-violet-50 text-violet-700 border border-violet-100",
      };
    }

    if (mode === "SCHEDULED") {
      return {
        mode,
        label: "Scheduled",
        icon: <AlarmClock size={12} className="text-amber-600" />,
        className: "bg-amber-50 text-amber-700 border border-amber-100",
      };
    }

    return {
      mode,
      label: "Manual",
      icon: <UserRound size={12} className="text-slate-600" />,
      className: "bg-slate-100 text-slate-700 border border-slate-200",
    };
  };

  const getRunTypeMeta = (run: ActivityRun) => {
    const normalizedType = normalizeActivityRunType(run.run_type);

    if (normalizedType === "INCREMENTAL_SCAN") {
      return {
        label: "Incremental",
        className: "bg-violet-100 text-violet-700",
        icon: <Zap size={12} />,
      };
    }

    if (normalizedType === "CUSTOM_SCAN") {
      return {
        label: "Custom Scan",
        className: "bg-purple-100 text-purple-700",
        icon: <Settings size={12} />,
      };
    }

    if (normalizedType === "PROFILING") {
      return {
        label: "Profiling",
        className: "bg-teal-100 text-teal-700",
        icon: <BarChart2 size={12} />,
      };
    }

    return {
      label: "Full Scan",
      className: "bg-blue-100 text-blue-700",
      icon: <Search size={12} />,
    };
  };


  const getRunStatusMeta = (run: ActivityRun) => {
    const rawStatus = String(run.run_status || run.status || "UNKNOWN").toUpperCase();
    const failed = Number(run.failed_checks ?? run.failed ?? 0);
    const warnings = Number(run.warning_checks ?? run.warnings ?? 0);
    const skipped = Number(run.skipped_checks ?? 0);

    if (rawStatus === "RUNNING") {
      return {
        label: "Running",
        className: "bg-indigo-100 text-indigo-700",
        icon: <Loader2 size={12} className="animate-spin" />,
      };
    }

    if (rawStatus === "FAILED") {
      return {
        label: "Failed",
        className: "bg-red-100 text-red-700",
        icon: <AlertOctagon size={12} />,
      };
    }

    if (rawStatus.includes("PARTIAL") || (rawStatus === "COMPLETED" && skipped > 0 && failed === 0)) {
      return {
        label: "Partial",
        className: "bg-orange-100 text-orange-700",
        icon: <AlertTriangle size={12} />,
      };
    }

    if (failed > 0 || rawStatus === "COMPLETED_WITH_FAILURES") {
      return {
        label: "Failed",
        className: "bg-red-100 text-red-700",
        icon: <AlertOctagon size={12} />,
      };
    }

    if (warnings > 0) {
      return {
        label: "Degraded",
        className: "bg-amber-100 text-amber-700",
        icon: <AlertTriangle size={12} />,
      };
    }

    return {
      label: "Healthy",
      className: "bg-emerald-100 text-emerald-700",
      icon: <CheckCircle2 size={12} />,
    };
  };

  const formatCount = (value: number | null | undefined) => {
    if (value === null || value === undefined || Number.isNaN(Number(value))) return "-";
    return Number(value).toLocaleString();
  };

  const [activeTab, setActiveTab] = useState("overview");
  const [previewData, setPreviewData] = useState<{
    columns: Array<{ name: string; type: string }>;
    rows: any[];
    rowCount: number;
  } | null>(null);
  const [isLoadingPreview, setIsLoadingPreview] = useState(false);
  const [previewError, setPreviewError] = useState<string | null>(null);
  const [tableRowCount, setTableRowCount] = useState<number | null>(null);
  const [isLoadingRowCount, setIsLoadingRowCount] = useState(false);
  const [qualityScore, setQualityScore] = useState<number | null>(null);
  const [isLoadingQualityScore, setIsLoadingQualityScore] = useState(false);
  const [completeness, setCompleteness] = useState<number | null>(null);
  const [uniqueness, setUniqueness] = useState<number | null>(null);
  const [consistency, setConsistency] = useState<number | null>(null);
  const [validity, setValidity] = useState<number | null>(null);
  // Initialize with empty date - will be set to first available date after fetching
  const getTodayIST = () => {
    const now = new Date();
    const istDate = new Date(now.toLocaleString('en-US', { timeZone: 'Asia/Kolkata' }));
    return istDate.toISOString().split('T')[0];
  };
  const [selectedExecutionDate, setSelectedExecutionDate] = useState<string>("");
  const [selectedDataViewMode, setSelectedDataViewMode] = useState<DataViewMode>("TODAY");
  const [availableExecutionDates, setAvailableExecutionDates] = useState<string[]>([]);
  const [isLoadingAvailableDates, setIsLoadingAvailableDates] = useState(false);
  const [overviewAnomalyFlags, setOverviewAnomalyFlags] = useState<string[]>([]);
  const [checksExecuted, setChecksExecuted] = useState<number | null>(null);
  const [failedRecordsCount, setFailedRecordsCount] = useState<number | null>(null);
  const [tableMetricsStatusMessage, setTableMetricsStatusMessage] = useState<string | null>(null);
  const [selectedScoreMetric, setSelectedScoreMetric] = useState<"completeness" | "uniqueness" | "consistency" | "validity">("completeness");
  const [tableSummary, setTableSummary] = useState<TableSummaryResponse | null>(null);
  const [isLoadingTableSummary, setIsLoadingTableSummary] = useState(false);
  const [runSummary, setRunSummary] = useState<RunSummaryResponse | null>(null);
  const [isLoadingRunSummary, setIsLoadingRunSummary] = useState(false);
  const [isRunMenuOpen, setIsRunMenuOpen] = useState(false);
  const [isCustomScanOpen, setIsCustomScanOpen] = useState(false);
  const [scanLoading, setScanLoading] = useState(false);
  const [rules, setRules] = useState<any[]>([]);
  const [selectedRuleIds, setSelectedRuleIds] = useState<number[]>([]);
  const [scope, setScope] = useState<"table" | "columns">("table");
  const [availableColumns, setAvailableColumns] = useState<string[]>([]);
  const [selectedColumns, setSelectedColumns] = useState<string[]>([]);
  const [execTasks, setExecTasks] = useState<Array<{
    id: string;
    ruleName: string;
    columnName: string | null;
    status: "pending" | "running" | "success" | "error";
    error?: string;
    runId?: string;
    spStatus?: string;
    passRate?: number;
    invalidRecords?: number;
    recordsProcessed?: number;
    durationSeconds?: number;
    datasetId?: string;
    totalChecks?: number;
    passed?: number;
    failed?: number;
    warnings?: number;
    skipped?: number;
  }>>([]);
  const [latestRunId, setLatestRunId] = useState<string | null>(null);
  const [isExecuting, setIsExecuting] = useState(false);
  const [resolvedDatasetId, setResolvedDatasetId] = useState<string | null>(null);
  const [activityData, setActivityData] = useState<ActivityRun[]>([]);
  const [isLoadingActivity, setIsLoadingActivity] = useState(false);
  const [activityError, setActivityError] = useState<string | null>(null);
  const [activityExecutionModeFilter, setActivityExecutionModeFilter] = useState<"ALL" | "MANUAL" | "SCHEDULED">("ALL");
  const [activityRunTypeFilter, setActivityRunTypeFilter] = useState<"ALL" | "FULL_SCAN" | "INCREMENTAL_SCAN" | "CUSTOM_SCAN" | "PROFILING">("ALL");
  const [selectedRunId, setSelectedRunId] = useState<string | null>(null);
  const [overviewDatasetId, setOverviewDatasetId] = useState<string | null>(null);
  const [overviewRunId, setOverviewRunId] = useState<string | null>(null);
  const [isResultsModalOpen, setIsResultsModalOpen] = useState(false);
  const [resultsModalData, setResultsModalData] = useState<{ summary: any; checks: any[] } | null>(null);
  const [isLoadingResults, setIsLoadingResults] = useState(false);
  const [isRerunning, setIsRerunning] = useState<string | null>(null);
  const [isRunningProfile, setIsRunningProfile] = useState(false);
  const { showToast } = useToast();

  // Observability state
  const [observabilityFreshness, setObservabilityFreshness] = useState<{
    lastAlteredAt: string | null;
    lastAlteredFormatted: string | null;
    createdAt: string | null;
    createdFormatted: string | null;
    freshnessDelayMinutes: number | null;
    freshnessDelayFormatted: string | null;
    slaStatus: string;
  } | null>(null);
  const [observabilityVolume, setObservabilityVolume] = useState<{
    rowCount: number | null;
    bytes: number | null;
    bytesFormatted: string | null;
  } | null>(null);
  const [observabilitySchema, setObservabilitySchema] = useState<{
    columnCount: number;
    nullableColumnCount: number;
    columns: Array<{
      name: string;
      dataType: string;
      isNullable: boolean;
      ordinalPosition: number;
    }>;
  } | null>(null);
  const [observabilityHealthScore, setObservabilityHealthScore] = useState<{
    overallScore: number;
    freshnessScore: number;
    volumeScore: number;
    schemaScore: number;
  } | null>(null);
  const [observabilityLoads, setObservabilityLoads] = useState<{
    totalLoads: number;
    successfulLoads: number;
    failedLoads: number;
    successRate: number | null;
    lastLoadTime: string | null;
    lastLoadTimeFormatted: string | null;
    rowCount: number;
    bytes: number;
    sizeFormatted: string;
    loadHistoryAvailable: boolean;
  } | null>(null);
  const [observabilityLineage, setObservabilityLineage] = useState<{
    node: {
      name: string;
      shortName: string;
      healthStatus: string;
      freshnessMinutes: number;
    };
    upstream: Array<{
      name: string;
      shortName: string;
      type: string;
      schema?: string;
      transformationType?: string;
      sourceCode?: string;
      database?: string;
    }>;
    downstream: Array<{
      name: string;
      shortName: string;
      type: string;
      schema?: string;
      transformationType?: string;
      sourceCode?: string;
      database?: string;
    }>;
    impact: {
      level: string;
      summary: string;
      downstreamCount: number;
      upstreamCount: number;
    };
    lineageAvailable: boolean;
  } | null>(null);
  const [isLoadingObservability, setIsLoadingObservability] = useState(false);
  const [observabilityError, setObservabilityError] = useState<string | null>(null);



  // Anomalies state
  const [anomaliesData, setAnomaliesData] = useState<{
    summary: {
      activeAnomalies: number;
      criticalAnomalies: number;
      resolvedAnomalies: number;
      totalAnomalies: number;
      lastScanTime: string | null;
      lastScanTimeFormatted: string | null;
    };
    anomalies: Array<{
      anomalyId: string;
      metric: string;
      scope: string;
      target: string;
      severity: string;
      baseline: number;
      current: number;
      deviationPct: number;
      detectedAt: string | null;
      detectedAtFormatted: string | null;
      status: string;
      description: string;
    }>;
  } | null>(null);
  const [isLoadingAnomalies, setIsLoadingAnomalies] = useState(false);
  const [anomaliesError, setAnomaliesError] = useState<string | null>(null);

  // Schedules state
  const [schedulesData, setSchedulesData] = useState<Array<{
    scheduleId: string;
    scanType: string;
    scheduleType: string;
    scheduleTime: string | null;
    timezone: string;
    status: string;
    nextRunAt: string | null;
    nextRunFormatted: string | null;
  }>>([]);
  const [isLoadingSchedules, setIsLoadingSchedules] = useState(false);
  const [showScheduleModal, setShowScheduleModal] = useState(false);
  const [scheduleWizardStep, setScheduleWizardStep] = useState(1);
  const [scheduleFromRun, setScheduleFromRun] = useState<any>(null); // Pre-fill context
  const [newSchedule, setNewSchedule] = useState({
    // Step 1: Scan Configuration
    scanType: "profiling" as string,
    // Step 2: Schedule Type
    isRecurring: true,
    // Step 3: Date & Time
    scheduleType: "daily" as string,
    scheduleTime: "02:00",
    scheduleDays: [] as string[],
    timezone: "Asia/Kolkata",
    runDate: "", // For one-time
    // Step 4: Schedule Window
    startDate: "",
    endDate: "",
    skipIfRunning: false,
    // Step 5: Failure Controls
    onFailureAction: "continue" as string,
    maxFailures: 3,
    notifyOnFailure: false,
    notifyOnSuccess: false,
    // Step 1.5: Custom Rule Selection
    customRules: [] as string[],
    scope: "table" as "table" | "columns",
    selectedColumns: [] as string[],
  });

  const groupedRules = useMemo(() => {
    const g: Record<string, any[]> = {};
    for (const r of rules) {
      const key = r.rule_type || "Uncategorized";
      if (!g[key]) g[key] = [];
      g[key].push(r);
    }
    return g;
  }, [rules]);


  // Extract schema from dataset (format: "DATABASE.SCHEMA")
  const [databaseName, schemaName] = dataset.split(".");

  // Quality score history state - DQ_SCORE read directly from DQ_DAILY_SUMMARY
  const [qualityScoreHistory, setQualityScoreHistory] = useState<Array<{ scan_date: string; dq_score: number }>>([]);
  const [isLoadingQualityHistory, setIsLoadingQualityHistory] = useState(false);
  const tableMetricsRequestSeq = useRef(0);
  const qualityHistoryRequestSeq = useRef(0);
  const tableSummaryRequestSeq = useRef(0);
  const runSummaryRequestSeq = useRef(0);

  // Fetch table preview data when Data Preview tab is active
  useEffect(() => {
    if (activeTab === "data-preview" && !previewData && !isLoadingPreview) {
      fetchPreviewData();
    }

    if (activeTab === "anomalies" && !anomaliesData && !isLoadingAnomalies) {
      fetchAnomaliesData();
    }
    if (activeTab === "activity" && schedulesData.length === 0 && !isLoadingSchedules) {
      fetchSchedules();
    }
  }, [activeTab]);

  useEffect(() => {
    if (activeTab !== "activity") return;
    setSelectedRunId(null);
    fetchActivity();
  }, [activeTab, activityExecutionModeFilter, activityRunTypeFilter]);

  // Ensure rules/columns are loaded when Schedule Modal Step 1 involves Custom/Columns
  useEffect(() => {
    if (showScheduleModal) {
      if ((newSchedule.scanType === "custom" || newSchedule.scanType === "checks") && rules.length === 0) {
        fetchRuleConfiguration();
      }
      if (newSchedule.scope === "columns" && availableColumns.length === 0) {
        fetchRuleConfiguration();
      }
    }
  }, [showScheduleModal, newSchedule.scanType, newSchedule.scope]);


  // Poll for updates (Activity & Schedules) every 30 seconds.
  // Scheduler execution is backend-controlled; polling refreshes persisted state.
  useEffect(() => {
    if (activeTab !== "activity") return;

    const pollUpdates = () => {
      fetchActivity();
      fetchSchedules();
    };

    const interval = setInterval(pollUpdates, 30000);
    return () => clearInterval(interval);
  }, [activeTab, activityExecutionModeFilter, activityRunTypeFilter]);

  // Resolve dataset id for overview-linked widgets.
  useEffect(() => {
    let cancelled = false;

    const resolveOverviewDataset = async () => {
      try {
        const datasetRes = await fetch(
          `/api/dq/dataset-by-table?database=${encodeURIComponent(databaseName)}&schema=${encodeURIComponent(schemaName)}&table=${encodeURIComponent(tableName)}`
        );
        const datasetJson = await datasetRes.json();

        if (!cancelled && datasetJson?.success && datasetJson.data?.dataset_id) {
          setOverviewDatasetId(datasetJson.data.dataset_id);
        }
      } catch {
        if (!cancelled) {
          setOverviewDatasetId(null);
        }
      }
    };

    resolveOverviewDataset();

    return () => {
      cancelled = true;
    };
  }, [databaseName, schemaName, tableName]);

  // Fetch available execution dates for the table.
  useEffect(() => {
    fetchAvailableExecutionDates(selectedDataViewMode);
  }, [tableName, selectedDataViewMode]);

  // Date + data-view controls are authoritative for metrics.
  // If date is empty, APIs resolve latest available context.
  useEffect(() => {
    fetchTableMetrics(selectedExecutionDate || undefined, selectedDataViewMode);
    fetchTableSummary(selectedExecutionDate || undefined, selectedDataViewMode);
    fetchQualityScoreHistory(selectedDataViewMode);
  }, [selectedExecutionDate, selectedDataViewMode]);

  // Fetch observability data when Observability tab is activated.
  useEffect(() => {
    if (activeTab === "observability") {
      fetchObservabilityData();
    }
  }, [activeTab]);

  // Keep dimensions and anomaly summaries run-aware.
  useEffect(() => {
    if (!overviewRunId) {
      setCompleteness(null);
      setUniqueness(null);
      setConsistency(null);
      setValidity(null);
      setOverviewAnomalyFlags([]);
      return;
    }

    const fetchRunMetrics = async () => {
      try {
        const response = await fetch(`/api/metrics/run/${encodeURIComponent(overviewRunId)}`);
        const result = await response.json();

        if (result.success && result.data) {
          const dimensionScores = result.data.dimension_scores || {};
          setCompleteness(dimensionScores.COMPLETENESS !== undefined ? Number(dimensionScores.COMPLETENESS) : null);
          setUniqueness(dimensionScores.UNIQUENESS !== undefined ? Number(dimensionScores.UNIQUENESS) : null);
          setConsistency(dimensionScores.CONSISTENCY !== undefined ? Number(dimensionScores.CONSISTENCY) : null);
          setValidity(dimensionScores.VALIDITY !== undefined ? Number(dimensionScores.VALIDITY) : null);
          setOverviewAnomalyFlags(Array.isArray(result.data.anomaly_flags) ? result.data.anomaly_flags : []);
          return;
        }

        setCompleteness(null);
        setUniqueness(null);
        setConsistency(null);
        setValidity(null);
        setOverviewAnomalyFlags([]);
      } catch {
        setCompleteness(null);
        setUniqueness(null);
        setConsistency(null);
        setValidity(null);
        setOverviewAnomalyFlags([]);
      }
    };

    fetchRunMetrics();
  }, [overviewRunId]);
  useEffect(() => {
    if (!overviewRunId) {
      setRunSummary(null);
      return;
    }

    fetchRunSummary(overviewRunId);
  }, [overviewRunId]);

  const fetchAvailableExecutionDates = async (mode: DataViewMode) => {
    try {
      setIsLoadingAvailableDates(true);
      const response = await fetch(
        `/api/dq/available-dates?table=${encodeURIComponent(tableName)}&mode=${encodeURIComponent(mode)}&days=90`
      );
      const result = await response.json();

      if (result.success && result.data && Array.isArray(result.data.dates)) {
        setAvailableExecutionDates(result.data.dates);

        // Auto-select first available date if none selected or selected date unavailable
        if (result.data.dates.length > 0 && !selectedExecutionDate) {
          setSelectedExecutionDate(result.data.dates[0]);
        } else if (result.data.dates.length > 0 && !result.data.dates.includes(selectedExecutionDate)) {
          setSelectedExecutionDate(result.data.dates[0]);
        }
      } else {
        setAvailableExecutionDates([]);
        setSelectedExecutionDate("");
      }
    } catch (error: any) {
      console.error("Error fetching available dates:", error);
      setAvailableExecutionDates([]);
      setSelectedExecutionDate("");
    } finally {
      setIsLoadingAvailableDates(false);
    }
  };

  const fetchQualityScoreHistory = async (mode: DataViewMode) => {
    const requestId = ++qualityHistoryRequestSeq.current;
    try {
      setIsLoadingQualityHistory(true);
      const response = await fetch(
        `/api/dq/quality-score-history?database=${encodeURIComponent(databaseName)}&schema=${encodeURIComponent(schemaName)}&table=${encodeURIComponent(tableName)}&mode=${encodeURIComponent(mode)}&days=30`
      );
      const result = await response.json();

      if (requestId !== qualityHistoryRequestSeq.current) return;

      if (result.last_5_days) {
        // Data arrives ASC from backend (oldest → newest), ready for graph
        setQualityScoreHistory(result.last_5_days);
      } else {
        setQualityScoreHistory([]);
      }
    } catch (error: any) {
      if (requestId !== qualityHistoryRequestSeq.current) return;
      console.error("Error fetching quality score history:", error);
      setQualityScoreHistory([]);
    } finally {
      if (requestId === qualityHistoryRequestSeq.current) {
        setIsLoadingQualityHistory(false);
      }
    }
  };

  const fetchTableMetrics = async (dateValue: string | undefined, dataViewMode: DataViewMode) => {
    const requestId = ++tableMetricsRequestSeq.current;
    try {
      setTableRowCount(null);
      setQualityScore(null);
      setChecksExecuted(null);
      setFailedRecordsCount(null);
      setCompleteness(null);
      setUniqueness(null);
      setConsistency(null);
      setValidity(null);
      setOverviewAnomalyFlags([]);
      setTableMetricsStatusMessage(null);
      setIsLoadingRowCount(true);
      setIsLoadingQualityScore(true);

      const params = new URLSearchParams({
        table: tableName,
        mode: dataViewMode,
      });

      if (dateValue) {
        params.set("date", dateValue);
      }

      const response = await fetch(`/api/dq/table-metrics?${params.toString()}`);
      const result = await response.json();

      if (requestId !== tableMetricsRequestSeq.current) return;

      if (result.success && result.data) {
        const resolvedDate = typeof result.data.resolvedDate === "string" ? result.data.resolvedDate.slice(0, 10) : null;
        if (!selectedExecutionDate && resolvedDate) {
          setSelectedExecutionDate(resolvedDate);
        }

        if (result.data.status === "ready") {
          setTableRowCount(result.data.recordsEvaluated ?? null);
          setQualityScore(result.data.qualityScore ?? null);
          setChecksExecuted(result.data.rulesExecuted ?? null);
          setFailedRecordsCount(result.data.failedRecords ?? null);
          setOverviewRunId(result.data.runId ?? null);
          setTableMetricsStatusMessage(result.data.statusMessage ?? null);
        } else {
          setTableMetricsStatusMessage(result.data.statusMessage || "No run executed for selected configuration.");
          setOverviewRunId(null);
        }
      } else {
        setTableMetricsStatusMessage("Failed to load table metrics.");
        setOverviewRunId(null);
      }
    } catch (error: any) {
      if (requestId !== tableMetricsRequestSeq.current) return;
      console.error("Error fetching table metrics:", error);
      setTableMetricsStatusMessage("Failed to load table execution metrics.");
      setOverviewRunId(null);
    } finally {
      if (requestId === tableMetricsRequestSeq.current) {
        setIsLoadingRowCount(false);
        setIsLoadingQualityScore(false);
      }
    }
  };
  const fetchTableSummary = async (dateValue: string | undefined, dataViewMode: DataViewMode) => {
    const requestId = ++tableSummaryRequestSeq.current;
    try {
      setIsLoadingTableSummary(true);

      const params = new URLSearchParams({
        database: databaseName,
        schema: schemaName,
        table: tableName,
        mode: dataViewMode,
      });

      if (dateValue) {
        params.set("date", dateValue);
      }

      const response = await fetch(`/api/metrics/table-summary?${params.toString()}`);
      const result = await response.json();

      if (requestId !== tableSummaryRequestSeq.current) return;

      if (result.success && result.data) {
        const resolvedDate = typeof result.data.resolved_date === "string" ? result.data.resolved_date.slice(0, 10) : null;
        if (!selectedExecutionDate && resolvedDate) {
          setSelectedExecutionDate(resolvedDate);
        }
        setTableSummary(result.data);
      } else {
        setTableSummary(null);
      }
    } catch (error: any) {
      if (requestId !== tableSummaryRequestSeq.current) return;
      console.error("Error fetching table summary:", error);
      setTableSummary(null);
    } finally {
      if (requestId === tableSummaryRequestSeq.current) {
        setIsLoadingTableSummary(false);
      }
    }
  };

  const fetchRunSummary = async (runId: string) => {
    const requestId = ++runSummaryRequestSeq.current;
    try {
      setIsLoadingRunSummary(true);

      const response = await fetch(`/api/metrics/run-summary?run_id=${encodeURIComponent(runId)}`);
      const result = await response.json();

      if (requestId !== runSummaryRequestSeq.current) return;

      if (result.success && result.data) {
        setRunSummary(result.data);
      } else {
        setRunSummary(null);
      }
    } catch (error: any) {
      if (requestId !== runSummaryRequestSeq.current) return;
      console.error("Error fetching run summary:", error);
      setRunSummary(null);
    } finally {
      if (requestId === runSummaryRequestSeq.current) {
        setIsLoadingRunSummary(false);
      }
    }
  };
  const fetchTableRowCount = async () => {
    try {
      setIsLoadingRowCount(true);
      const response = await fetch(
        `/api/snowflake/table-stats?database=${encodeURIComponent(databaseName)}&schema=${encodeURIComponent(schemaName)}&table=${encodeURIComponent(tableName)}`
      );
      const result = await response.json();

      if (result.success && result.data) {
        setTableRowCount(result.data.rowCount);
      }
    } catch (error: any) {
      console.error("Error fetching table row count:", error);
    } finally {
      setIsLoadingRowCount(false);
    }
  };

  const fetchQualityScore = async () => {
    try {
      setIsLoadingQualityScore(true);
      const response = await fetch(
        `/api/snowflake/table-quality-score?database=${encodeURIComponent(databaseName)}&schema=${encodeURIComponent(schemaName)}&table=${encodeURIComponent(tableName)}`
      );
      const result = await response.json();

      if (result.success && result.data) {
        if (result.data.qualityScore !== null) {
          setQualityScore(result.data.qualityScore);
        }
        if (result.data.completeness !== null) {
          setCompleteness(result.data.completeness);
        }
        if (result.data.uniqueness !== null) {
          setUniqueness(result.data.uniqueness);
        }
        if (result.data.consistency !== null) {
          setConsistency(result.data.consistency);
        }
        if (result.data.validity !== null) {
          setValidity(result.data.validity);
        }
      }
    } catch (error: any) {
      console.error("Error fetching quality score:", error);
    } finally {
      setIsLoadingQualityScore(false);
    }
  };

  const fetchObservabilityData = async () => {
    try {
      setIsLoadingObservability(true);
      setObservabilityError(null);

      // Fetch table metadata
      const metadataRes = await fetch(
        `/api/snowflake/table-metadata?database=${encodeURIComponent(databaseName)}&schema=${encodeURIComponent(schemaName)}&table=${encodeURIComponent(tableName)}`
      );
      const metadataData = await metadataRes.json();

      if (metadataData.success && metadataData.data) {
        // Set Volume data
        setObservabilityVolume({
          rowCount: metadataData.data.rowCount,
          bytes: metadataData.data.bytes,
          bytesFormatted: metadataData.data.bytesFormatted,
        });

        // Set Schema data
        setObservabilitySchema({
          columnCount: metadataData.data.columnCount,
          nullableColumnCount: metadataData.data.nullableColumnCount,
          columns: metadataData.data.columns,
        });
      }

      // Fetch freshness data
      const freshnessRes = await fetch(
        `/api/snowflake/table-freshness?database=${encodeURIComponent(databaseName)}&schema=${encodeURIComponent(schemaName)}&table=${encodeURIComponent(tableName)}`
      );
      const freshnessData = await freshnessRes.json();

      if (freshnessData.success && freshnessData.data) {
        setObservabilityFreshness({
          lastAlteredAt: freshnessData.data.lastUpdated,
          lastAlteredFormatted: new Date(freshnessData.data.lastUpdated).toLocaleDateString("en-US", {
            year: "numeric",
            month: "short",
            day: "numeric",
            hour: "2-digit",
            minute: "2-digit",
          }),
          createdAt: freshnessData.data.created,
          createdFormatted: freshnessData.data.createdFormatted,
          freshnessDelayMinutes: freshnessData.data.hoursSinceUpdate * 60,
          freshnessDelayFormatted: freshnessData.data.freshnessDelayFormatted,
          slaStatus: freshnessData.data.slaStatus,
        });
      }

      // Fetch health score
      const healthRes = await fetch(
        `/api/snowflake/table-health-score?database=${encodeURIComponent(databaseName)}&schema=${encodeURIComponent(schemaName)}&table=${encodeURIComponent(tableName)}`
      );
      const healthData = await healthRes.json();

      if (healthData.success && healthData.data) {
        setObservabilityHealthScore({
          overallScore: healthData.data.overallScore,
          freshnessScore: healthData.data.freshnessScore,
          volumeScore: healthData.data.volumeScore,
          schemaScore: healthData.data.schemaScore,
        });
      }

      // Fetch load history
      const loadHistoryRes = await fetch(
        `/api/snowflake/table-load-history?database=${encodeURIComponent(databaseName)}&schema=${encodeURIComponent(schemaName)}&table=${encodeURIComponent(tableName)}&days=30`
      );
      const loadHistoryData = await loadHistoryRes.json();

      if (loadHistoryData.success && loadHistoryData.data) {
        setObservabilityLoads({
          totalLoads: loadHistoryData.data.totalLoads,
          successfulLoads: loadHistoryData.data.successfulLoads,
          failedLoads: loadHistoryData.data.failedLoads,
          successRate: loadHistoryData.data.successRate,
          lastLoadTime: loadHistoryData.data.lastLoadTime,
          lastLoadTimeFormatted: loadHistoryData.data.lastLoadTimeFormatted,
          rowCount: loadHistoryData.data.rowCount,
          bytes: loadHistoryData.data.bytes,
          sizeFormatted: loadHistoryData.data.sizeFormatted,
          loadHistoryAvailable: loadHistoryData.data.loadHistoryAvailable,
        });
      } else {
        // Fallback to unavailable state
        setObservabilityLoads({
          totalLoads: 0,
          successfulLoads: 0,
          failedLoads: 0,
          successRate: null,
          lastLoadTime: null,
          lastLoadTimeFormatted: null,
          rowCount: 0,
          bytes: 0,
          sizeFormatted: "0 B",
          loadHistoryAvailable: false,
        });
      }

      // Fetch lineage data
      const lineageRes = await fetch(
        `/api/snowflake/table-lineage?database=${encodeURIComponent(databaseName)}&schema=${encodeURIComponent(schemaName)}&table=${encodeURIComponent(tableName)}`
      );
      const lineageData = await lineageRes.json();

      if (lineageData.success && lineageData.data) {
        setObservabilityLineage(lineageData.data);
      } else {
        // Fallback or empty state if fails
        setObservabilityLineage(null);
      }
    } catch (error: any) {
      console.error("Error fetching observability data:", error);
      setObservabilityError(
        error.message || "Failed to load observability metrics"
      );
    } finally {
      setIsLoadingObservability(false);
    }
  };

  const runFullScan = async () => {
    try {
      setScanLoading(true);
      showToast("Starting full table scan...", "info", 3000);

      const response = await fetch(`/api/dq/run-scan`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          dataset_id: overviewDatasetId || resolvedDatasetId,
          scan_type: "FULL",
          table_name: tableName,
          database: databaseName,
          schema: schemaName,
        }),
      });

      const result = await response.json();

      if (result.success) {
        showToast("Full scan completed successfully", "success", 5000);

        // Refresh metrics after a short delay to allow data to be written
        setTimeout(() => {
          fetchTableMetrics(selectedExecutionDate || undefined, selectedDataViewMode);
          fetchTableSummary(selectedExecutionDate || undefined, selectedDataViewMode);
          fetchQualityScoreHistory(selectedDataViewMode);
          fetchAvailableExecutionDates(selectedDataViewMode);
        }, 1000);
      } else {
        showToast(result.error || "Full scan failed", "error", 5000);
      }
    } catch (error: any) {
      console.error("Error running full scan:", error);
      showToast("Failed to run full scan: " + (error.message || "Unknown error"), "error", 5000);
    } finally {
      setScanLoading(false);
    }
  };

  const fetchPreviewData = async () => {
    try {
      setIsLoadingPreview(true);
      setPreviewError(null);

      const response = await fetch(
        `/api/snowflake/table-preview?database=${encodeURIComponent(databaseName)}&schema=${encodeURIComponent(schemaName)}&table=${encodeURIComponent(tableName)}`
      );
      const result = await response.json();

      if (result.success && result.data) {
        setPreviewData(result.data);
      } else {
        setPreviewError(result.error || "Failed to fetch table preview");
      }
    } catch (error: any) {
      console.error("Error fetching table preview:", error);
      setPreviewError("Failed to fetch table preview");
    } finally {
      setIsLoadingPreview(false);
    }
  };

  const fetchActivity = async () => {
    try {
      setIsLoadingActivity(true);
      setActivityError(null);

      const activityParams = new URLSearchParams({
        database: databaseName,
        schema: schemaName,
        table: tableName,
      });

      if (activityExecutionModeFilter !== "ALL") {
        activityParams.set("execution_mode", activityExecutionModeFilter);
      }
      if (activityRunTypeFilter !== "ALL") {
        activityParams.set("run_type", activityRunTypeFilter);
      }

      const response = await fetch(
        `/api/dq/activity?${activityParams.toString()}`,
        {
          cache: "no-store",
          headers: {
            "Cache-Control": "no-cache",
          },
        }
      );
      const result = await response.json();

      if (result.success && result.data) {
        setActivityData(result.data as ActivityRun[]);
      } else {
        setActivityError(result.error || "Failed to fetch activity");
      }
    } catch (error: any) {
      console.error("Error fetching activity:", error);
      setActivityError("Failed to fetch activity");
    } finally {
      setIsLoadingActivity(false);
    }
  };



  const fetchProfileData = async () => {
    // This is a placeholder for fetching profile data if needed by other components
    // For now, it mainly triggers a refresh of other data sources since profiling 
    // updates statistics that other tabs might use.
    console.log("Fetching profile data...");

    // Refresh basic table stats since profile might update row counts
    await fetchTableRowCount();

    // If the Fields tab is active, we might want to signal it to refresh, 
    // but typically it fetches its own data when a column is selected.
  };

  const fetchAnomaliesData = async () => {
    try {
      setIsLoadingAnomalies(true);
      setAnomaliesError(null);

      const response = await fetch(
        `/api/dq/table-anomalies?database=${encodeURIComponent(databaseName)}&schema=${encodeURIComponent(schemaName)}&table=${encodeURIComponent(tableName)}`
      );
      const result = await response.json();

      if (result.success) {
        setAnomaliesData(result.data);
      } else {
        setAnomaliesError(result.error || "Failed to fetch anomalies");
      }
    } catch (error: any) {
      console.error("Error fetching anomalies data:", error);
      setAnomaliesError("Failed to fetch anomalies data");
    } finally {
      setIsLoadingAnomalies(false);
    }
  };

  const fetchSchedules = async () => {
    try {
      setIsLoadingSchedules(true);
      const response = await fetch(
        `/api/schedules?database=${encodeURIComponent(databaseName)}&schema=${encodeURIComponent(schemaName)}&table=${encodeURIComponent(tableName)}`
      );
      const result = await response.json();
      if (result.success) {
        setSchedulesData(result.data.schedules || []);
      }
    } catch (error: any) {
      console.error("Error fetching schedules:", error);
    } finally {
      setIsLoadingSchedules(false);
    }
  };

  const createSchedule = async () => {
    try {
      // Calculate initial next run time client-side to ensure timezone correctness
      let initialNextRunAt = null;
      if (newSchedule.scheduleTime) {
        const [hours, minutes] = newSchedule.scheduleTime.split(":").map(Number);
        const now = new Date();
        const target = new Date(now);
        target.setHours(hours, minutes, 0, 0);

        // If target time is in the past, schedule for tomorrow
        if (target <= now) {
          target.setDate(target.getDate() + 1);
        }
        initialNextRunAt = target.toISOString();
      }

      const response = await fetch("/api/schedules", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          ...newSchedule,
          datasetId: decodeURIComponent(params.dataset as string),
          database: databaseName,
          schema: schemaName,
          table: tableName,
          initialNextRunAt // Send the calculated UTC time
        }),
      });
      const result = await response.json();
      if (result.success) {
        showToast("Schedule created successfully", "success", 3000);
        setShowScheduleModal(false);
        fetchSchedules();
      } else {
        showToast(result.error || "Failed to create schedule", "error", 3000);
      }
    } catch (error: any) {
      showToast("Failed to create schedule", "error", 3000);
    }
  };

  const deleteSchedule = async (scheduleId: string) => {
    try {
      const response = await fetch(`/api/schedules?scheduleId=${scheduleId}`, {
        method: "DELETE",
      });
      const result = await response.json();
      if (result.success) {
        showToast("Schedule deleted", "success", 2000);
        fetchSchedules();
      }
    } catch (error: any) {
      showToast("Failed to delete schedule", "error", 3000);
    }
  };

  const toggleSchedule = async (scheduleId: string, currentStatus: string) => {
    try {
      const newStatus = currentStatus === "active" ? "paused" : "active";
      const response = await fetch("/api/schedules", {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ scheduleId, status: newStatus }),
      });
      const result = await response.json();
      if (result.success) {
        showToast(`Schedule ${newStatus}`, "success", 2000);
        fetchSchedules();
      }
    } catch (error: any) {
      showToast("Failed to update schedule", "error", 3000);
    }
  };

  const runScheduleNow = async (scheduleId: string) => {
    try {
      showToast("Running scheduled scan...", "info", 2000);

      const response = await fetch("/api/scheduler/run", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ scheduleId }),
      });
      const result = await response.json();

      if (result.success && result.data?.executed > 0) {
        showToast("Scheduled scan executed successfully", "success", 3000);
      } else {
        showToast(result?.data?.message || "Schedule queued for execution", "info", 2500);
      }

      fetchSchedules();
      fetchActivity();
    } catch (error: any) {
      showToast("Failed to run schedule", "error", 3000);
    }
  };

  const runProfiling = async () => {
    try {
      setIsRunningProfile(true);
      showToast("Running profiling...", "info", 2000);

      const response = await fetch("/api/dq/run-profiling", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          database: databaseName,
          schema: schemaName,
          table: tableName,
          profile_level: "BASIC",
        }),
      });
      const result = await response.json();

      if (result.success) {
        showToast(
          `Profiling complete: ${result.data.columns_profiled} columns analyzed in ${result.data.duration_seconds?.toFixed(2)}s`,
          "success",
          4000
        );
        // Refresh profile data
        await fetchProfileData();
        // Refresh activity list if on activity tab
        if (activeTab === "activity") {
          await fetchActivity();
        }
      } else {
        showToast(result.error || "Profiling failed", "error", 4000);
      }
    } catch (error: any) {
      console.error("Error running profiling:", error);
      showToast("Profiling failed: " + (error.message || "Unknown error"), "error", 4000);
    } finally {
      setIsRunningProfile(false);
    }
  };

  const handleViewResults = async (runId: string) => {
    try {
      setIsLoadingResults(true);
      setResultsModalData(null);
      setIsResultsModalOpen(true);

      const response = await fetch(`/api/dq/run-details?run_id=${encodeURIComponent(runId)}`);
      const result = await response.json();

      if (result.success && result.data) {
        setResultsModalData(result.data);
      } else {
        showToast(result.error || "Failed to load run details", "error", 4000);
        setIsResultsModalOpen(false);
      }
    } catch (error: any) {
      console.error("Error fetching run details:", error);
      showToast("Failed to load run details", "error", 4000);
      setIsResultsModalOpen(false);
    } finally {
      setIsLoadingResults(false);
    }
  };

  const handleRerun = async (runId: string) => {
    try {
      setIsRerunning(runId);

      const response = await fetch("/api/dq/rerun", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ run_id: runId }),
      });
      const result = await response.json();

      if (result.success) {
        showToast(
          `Rerun completed: ${result.data.checks_executed || 1} check(s) executed`,
          "success",
          4000
        );
        // Refresh activity list
        await fetchActivity();
      } else {
        showToast(result.error || "Rerun failed", "error", 4000);
      }
    } catch (error: any) {
      console.error("Error rerunning scan:", error);
      showToast("Rerun failed: " + (error.message || "Unknown error"), "error", 4000);
    } finally {
      setIsRerunning(null);
    }
  };

  const handleAction = (action: string) => {
    console.log(`${action} action triggered for table: ${tableName}`);
  };

  const fetchRuleConfiguration = async () => {
    try {
      // Fetch configured rules
      const rulesRes = await fetch(
        `/api/dq/rules?database=${encodeURIComponent(databaseName)}&schema=${encodeURIComponent(schemaName)}&table=${encodeURIComponent(tableName)}`
      );
      const rulesJson = await rulesRes.json();
      if (rulesJson?.success && Array.isArray(rulesJson.data)) {
        setRules(rulesJson.data);
      } else {
        setRules([]);
      }

      // Fetch actual table columns
      const colsRes = await fetch(
        `/api/snowflake/table-columns?database=${encodeURIComponent(databaseName)}&schema=${encodeURIComponent(schemaName)}&table=${encodeURIComponent(tableName)}`
      );
      const colsJson = await colsRes.json();
      if (colsJson?.success && Array.isArray(colsJson.data)) {
        const cols = colsJson.data.map((c: any) => c.name);
        setAvailableColumns(cols);
      } else {
        setAvailableColumns([]);
      }
    } catch (e) {
      console.error("Failed to load rules/columns", e);
      setRules([]);
      setAvailableColumns([]);
    }
  };

  const openCustomScan = async () => {
    try {
      setIsCustomScanOpen(true);
      setScanLoading(false);
      setSelectedRuleIds([]);
      setScope("table");
      setSelectedColumns([]);
      setIsExecuting(false);
      setExecTasks([]);
      setResolvedDatasetId(null);

      // Fetch the actual dataset_id from DATASETS table
      const datasetRes = await fetch(
        `/api/dq/dataset-by-table?database=${encodeURIComponent(databaseName)}&schema=${encodeURIComponent(schemaName)}&table=${encodeURIComponent(tableName)}`
      );
      const datasetJson = await datasetRes.json();
      if (datasetJson?.success && datasetJson.data?.dataset_id) {
        setResolvedDatasetId(datasetJson.data.dataset_id);
        console.log("Resolved dataset_id:", datasetJson.data.dataset_id);
      } else {
        console.warn("Could not resolve dataset_id:", datasetJson?.error);
        showToast(`Warning: ${datasetJson?.error || "Could not resolve dataset_id"}`, "warning", 5000);
      }

      // Load Rules & Columns
      await fetchRuleConfiguration();

    } catch (e) {
      console.error("Failed to open custom scan", e);
    }
  };

  const hasSelectedDateData = availableExecutionDates.includes(selectedExecutionDate);
  const isResolvingExecutionContext =
    isLoadingAvailableDates || isLoadingRowCount || isLoadingQualityScore || isLoadingTableSummary;
  const recentRunDates = availableExecutionDates.slice(0, 5);
  const executionHintText = (() => {
    const normalizedStatus = (tableMetricsStatusMessage || "").toLowerCase();

    if (normalizedStatus.includes("latest available run")) {
      return "Using latest available full run";
    }

    if (
      normalizedStatus.includes("no run executed") ||
      normalizedStatus.includes("no data inserted") ||
      normalizedStatus.includes("no full run")
    ) {
      return "No run executed";
    }
    if (availableExecutionDates.length === 0) {
      return "No run executed";
    }

    return hasSelectedDateData ? "Data available" : "No run executed";
  })();
  const executionStatusInline = executionHintText === "Data available"
    ? "Data Available"
    : executionHintText === "No run executed"
      ? "No Run Executed"
      : "Using Latest Available Full Run";

  const recordsMetrics: Array<{ key: DataViewMode; label: string; value: number | null }> = [
    {
      key: "TABLE",
      label: "Total Records",
      value: tableSummary ? tableSummary.total_table_records : null,
    },
    {
      key: "TODAY",
      label: "Today's Records",
      value: tableSummary ? tableSummary.total_today_records : null,
    },
  ];

  const activeRecordsMetric = recordsMetrics.find((entry) => entry.key === selectedDataViewMode) || recordsMetrics[0];

  return (
    <div className="min-h-screen bg-slate-50">
      {/* Header */}
      <div className="bg-white border-b border-slate-200 sticky top-0 z-20">
        <div className="px-6 py-3">
          <div className="mb-2">
            <BackButton href="/" label="Back to Dashboard" className="px-3 py-1.5 text-xs" />
          </div>

          {/* Breadcrumb */}
          <div className="flex items-center gap-2 text-[11px] text-slate-400 mb-2">
            <button onClick={() => router.push("/")} className="hover:text-indigo-600 transition-colors">Dashboard</button>
            <ChevronRight size={12} />
            <span>{databaseName}</span>
            <ChevronRight size={12} />
            <span className="text-slate-700 font-medium">{tableName}</span>
          </div>

          <div className="space-y-1">
            <div className="flex items-center justify-between gap-6">
              <div className="flex min-w-0 items-center gap-3">
                <div className="rounded-md border border-slate-200 bg-slate-50 p-2">
                  <TableIcon className="text-slate-600" size={20} />
                </div>
                <div className="min-w-0">
                  <h1 className="truncate text-[22px] font-semibold leading-tight tracking-normal text-slate-900">{tableName}</h1>
                  <div className="mt-0.5 flex items-center gap-2.5 text-[13px] text-slate-500">
                    <span className="flex items-center gap-1">
                      <Database size={13} /> {databaseName}.{schemaName}
                    </span>
                    <span className="h-1 w-1 rounded-full bg-slate-300"></span>
                    <span className="rounded-full border border-slate-200 bg-slate-100 px-1.5 py-0.5 text-[11px] font-medium text-slate-600">
                      Active
                    </span>
                  </div>
                </div>
              </div>

              {/* Controls in right-side free space */}
              <div className="flex shrink-0 items-center gap-3">
                <div className="flex items-center gap-2">
                  <span className="shrink-0 text-[11px] font-semibold uppercase tracking-wide text-slate-500">Execution Date</span>
                  <SmartDatePicker
                    value={selectedExecutionDate}
                    onChange={(date) => setSelectedExecutionDate(date)}
                    availableDates={availableExecutionDates}
                    disabled={isLoadingAvailableDates}
                    placeholder="Select date"
                  />
                </div>

                <div className="h-6 w-px bg-slate-200" />

                <div className="flex items-center gap-2">
                  <span className="text-[11px] font-semibold uppercase tracking-wide text-slate-500">Data View</span>
                  <div className="inline-flex h-9 items-center rounded-md border border-slate-300 bg-white p-0.5">
                    {(["TABLE", "TODAY"] as DataViewMode[]).map((mode) => (
                      <button
                        key={mode}
                        onClick={() => setSelectedDataViewMode(mode)}
                        className={`h-7 w-[102px] rounded-[5px] text-sm font-medium transition-colors ${selectedDataViewMode === mode
                          ? "bg-slate-100 text-slate-900"
                          : "bg-transparent text-slate-600 hover:text-slate-800"
                          }`}
                      >
                        {modeLabel(mode)}
                      </button>
                    ))}
                  </div>
                </div>

                <div className="h-6 w-px bg-slate-200" />

                <Button
                  variant="outline"
                  className="h-9 rounded-md border-slate-300 px-4 text-slate-700 hover:bg-slate-100 hover:text-slate-900"
                  onClick={runFullScan}
                  disabled={scanLoading}
                >
                  {scanLoading ? "Scanning..." : "Scan Now"}
                </Button>
                <Button
                  variant="outline"
                  className="h-9 rounded-md border-slate-300 px-4 text-slate-700 hover:bg-slate-100 hover:text-slate-900"
                  onClick={() => {
                    setScheduleFromRun(null);
                    setShowScheduleModal(true);
                    fetchRuleConfiguration();
                  }}
                >
                  Schedule
                </Button>
                <Popover open={isRunMenuOpen} onOpenChange={setIsRunMenuOpen}>
                  <PopoverTrigger asChild>
                    <Button className="h-9 rounded-md bg-slate-900 px-4 hover:bg-slate-800">
                      Run
                      <ChevronDown size={14} className="ml-1" />
                    </Button>
                  </PopoverTrigger>
                  <PopoverContent align="end" className="w-40 p-2">
                    <div className="flex flex-col gap-1 text-sm text-slate-700">
                      <button
                        className="flex items-center gap-2 rounded px-2 py-2 text-left hover:bg-slate-100"
                        onClick={() => {
                          openCustomScan();
                          setIsRunMenuOpen(false);
                        }}
                      >
                        <Activity size={16} className="text-indigo-600" /> Scan
                      </button>
                      <button
                        className="flex items-center gap-2 rounded px-2 py-2 text-left hover:bg-slate-100"
                        onClick={() => {
                          runProfiling();
                          setIsRunMenuOpen(false);
                        }}
                      >
                        <BarChart2 size={16} className="text-indigo-600" /> Profile
                      </button>
                      <div className="h-px bg-slate-200 my-1" />
                      <button
                        className="flex items-center gap-2 rounded px-2 py-2 text-left hover:bg-slate-100"
                        onClick={() => {
                          setActiveTab("custom-sql");
                          setIsRunMenuOpen(false);
                        }}
                      >
                        <Database size={16} className="text-indigo-600" /> Open SQL Editor
                      </button>
                    </div>
                  </PopoverContent>
                </Popover>
              </div>
            </div>

            <div className="flex items-center gap-2 border-b border-slate-200 pb-2 pl-10 text-xs text-slate-500">
              <span>{executionStatusInline}</span>
              <span className="text-slate-300">|</span>
              <span>{availableExecutionDates.length} Runs</span>
              {isResolvingExecutionContext && (
                <Loader2 size={12} className="animate-spin text-slate-400" />
              )}
              {recentRunDates.length > 0 && (
                <>
                  <span className="text-slate-300">|</span>
                  <span className="text-slate-500">Recent:</span>
                  {recentRunDates.slice(0, 2).map((date, idx) => (
                    <span key={date} className="flex items-center gap-2">
                      <button
                        onClick={() => setSelectedExecutionDate(date)}
                        className="text-slate-600 hover:text-slate-900"
                      >
                        {new Date(`${date}T00:00:00`).toLocaleDateString("en-IN", {
                          day: "2-digit",
                          month: "short",
                        })}
                      </button>
                      {idx < Math.min(recentRunDates.length, 2) - 1 && (
                        <span className="text-slate-300">|</span>
                      )}
                    </span>
                  ))}
                </>
              )}
            </div>
          </div>
        </div>
      </div>

      {isCustomScanOpen && (
        <div className="fixed inset-0 z-50">
          <div className="absolute inset-0 bg-black/50" onClick={() => {
            if (!scanLoading) {
              setIsCustomScanOpen(false);
              setIsExecuting(false);
              setExecTasks([]);
            }
          }}></div>
          <div className="absolute left-1/2 top-1/2 -translate-x-1/2 -translate-y-1/2 bg-white rounded-xl shadow-xl w-170 max-w-[95vw] border border-slate-200">
            <div className="px-5 py-4 border-b border-slate-200">
              <h2 className="text-lg font-semibold text-slate-900">{isExecuting ? "Scan Results" : "Run Custom Data Quality Scan"}</h2>
              <p className="text-xs text-slate-500 mt-1">{tableName} in {databaseName}.{schemaName}</p>
            </div>
            {!isExecuting ? (
              <>
                <div className="p-5 space-y-5 max-h-[70vh] overflow-auto">
                  <div className="space-y-2">
                    <div className="font-medium text-slate-800">1. Select rules</div>
                    <div className="border rounded-md overflow-hidden">
                      {rules.length === 0 && (
                        <div className="text-sm text-slate-500 p-3 bg-slate-50">No rules configured.</div>
                      )}
                      {rules.length > 0 && (
                        (Object.entries(groupedRules).map(([category, categoryRules]: any) => (
                          <div key={category} className="border-b border-slate-100 last:border-0">
                            <details className="group">
                              <summary className="flex items-center gap-2 px-3 py-2.5 cursor-pointer hover:bg-slate-50 font-medium text-sm text-slate-700 bg-slate-50">
                                <span className="text-indigo-600 transition-transform group-open:rotate-90">
                                  <ChevronRight size={16} />
                                </span>
                                {category}
                                <span className="text-xs text-slate-500 ml-auto">({(categoryRules as any[]).length})</span>
                              </summary>
                              <div className="divide-y divide-slate-100 bg-white">
                                {(categoryRules as any[]).map((r: any) => (
                                  <label key={r.rule_id} className="flex items-center gap-3 px-4 py-2.5 text-sm cursor-pointer hover:bg-indigo-50 transition-colors">
                                    <input
                                      type="checkbox"
                                      className="h-4 w-4 accent-indigo-600"
                                      checked={selectedRuleIds.includes(r.rule_id)}
                                      onChange={(e) => {
                                        const checked = e.target.checked;
                                        setSelectedRuleIds((prev) =>
                                          checked ? Array.from(new Set([...prev, r.rule_id])) : prev.filter((id) => id !== r.rule_id)
                                        );
                                      }}
                                    />
                                    <div className="flex-1">
                                      <span className="text-slate-800 font-medium">{r.rule_name}</span>
                                    </div>
                                  </label>
                                ))}
                              </div>
                            </details>
                          </div>
                        )))
                      )}
                    </div>
                    {selectedRuleIds.length > 0 && (
                      <div className="text-xs text-indigo-600 bg-indigo-50 px-3 py-2 rounded border border-indigo-100">
                        {selectedRuleIds.length} rule{selectedRuleIds.length !== 1 ? "s" : ""} selected
                      </div>
                    )}
                  </div>

                  <div className="space-y-2">
                    <div className="font-medium text-slate-800">2. Scope</div>
                    <div className="flex items-center gap-6 text-sm">
                      <label className="flex items-center gap-2 cursor-pointer">
                        <input
                          type="radio"
                          name="scope"
                          value="table"
                          checked={scope === "table"}
                          onChange={() => setScope("table")}
                        />
                        Entire table
                      </label>
                      <label className="flex items-center gap-2 cursor-pointer">
                        <input
                          type="radio"
                          name="scope"
                          value="columns"
                          checked={scope === "columns"}
                          onChange={() => setScope("columns")}
                        />
                        Selected columns
                      </label>
                    </div>
                  </div>

                  {scope === "columns" && (
                    <div className="space-y-2">
                      <div className="font-medium text-slate-800">3. Select columns</div>
                      <div className="border rounded-md p-3 bg-slate-50 max-h-48 overflow-auto">
                        {availableColumns.length === 0 && (
                          <div className="text-sm text-slate-500">No columns available.</div>
                        )}
                        {availableColumns.length > 0 && (
                          <div className="grid grid-cols-2 gap-2">
                            {availableColumns.map((c) => (
                              <label key={c} className="flex items-center gap-2 p-2 rounded hover:bg-white cursor-pointer text-sm">
                                <input
                                  type="checkbox"
                                  className="h-4 w-4 accent-indigo-600"
                                  checked={selectedColumns.includes(c)}
                                  onChange={(e) => {
                                    const checked = e.target.checked;
                                    setSelectedColumns((prev) => (checked ? Array.from(new Set([...prev, c])) : prev.filter((x) => x !== c)));
                                  }}
                                />
                                <span className="text-slate-700">{c}</span>
                              </label>
                            ))}
                          </div>
                        )}
                      </div>
                      {selectedColumns.length > 0 && (
                        <div className="text-xs text-indigo-600 bg-indigo-50 px-3 py-2 rounded border border-indigo-100">
                          {selectedColumns.length} column{selectedColumns.length !== 1 ? "s" : ""} selected
                        </div>
                      )}
                    </div>
                  )}
                </div>
                <div className="px-5 py-4 border-t border-slate-200 flex items-center justify-end gap-2">
                  <Button variant="outline" onClick={() => setIsCustomScanOpen(false)} disabled={scanLoading}>Cancel</Button>
                  <Button
                    onClick={async () => {
                      if (selectedRuleIds.length === 0) return;
                      if (!resolvedDatasetId) {
                        showToast("Cannot run scan: Dataset not registered in DQ system. Please ensure the table is configured in DATA_QUALITY_DB.DQ_CONFIG.DATASETS", "error", 6000);
                        return;
                      }
                      // Build execution tasks: each rule x (columns or table)
                      const selectedRuleNames = rules
                        .filter((r: any) => selectedRuleIds.includes(r.rule_id))
                        .map((r: any) => r.rule_name);

                      const tasks: Array<{ id: string; ruleName: string; columnName: string | null; status: "pending" | "running" | "success" | "error"; error?: string }> = [];
                      if (scope === "table") {
                        for (const rn of selectedRuleNames) {
                          tasks.push({ id: `${rn}::ALL`, ruleName: rn, columnName: null, status: "pending" });
                        }
                      } else {
                        for (const rn of selectedRuleNames) {
                          for (const col of selectedColumns) {
                            tasks.push({ id: `${rn}::${col}`, ruleName: rn, columnName: col, status: "pending" });
                          }
                        }
                      }
                      setExecTasks(tasks);
                      setIsExecuting(true);

                      try {
                        setScanLoading(true);
                        for (let i = 0; i < tasks.length; i++) {
                          const t = tasks[i];
                          setExecTasks((prev) => prev.map((x, idx) => idx === i ? { ...x, status: "running" } : x));
                          try {
                            const res = await fetch("/api/dq/run-custom-rule", {
                              method: "POST",
                              headers: { "Content-Type": "application/json" },
                              body: JSON.stringify({
                                dataset_id: resolvedDatasetId,
                                rule_name: t.ruleName,
                                column_name: t.columnName ? t.columnName.toLowerCase() : null,
                                threshold: null,
                                run_mode: "ADHOC",
                              }),
                            });
                            const json = await res.json();
                            if (!json?.success) throw new Error(json?.error || "Failed");

                            // Parse SP response: data[0].SP_RUN_CUSTOM_RULE is a JSON string
                            let runMeta: any = {};
                            try {
                              if (json.data && json.data[0]) {
                                // Check if SP_RUN_CUSTOM_RULE exists and is a string
                                if (json.data[0].SP_RUN_CUSTOM_RULE) {
                                  runMeta = JSON.parse(json.data[0].SP_RUN_CUSTOM_RULE);
                                } else {
                                  // Otherwise use the data object directly
                                  runMeta = json.data[0];
                                }
                                console.log("Parsed run metadata:", runMeta);
                              }
                            } catch (parseErr) {
                              console.warn("Failed to parse SP response JSON", parseErr, json.data);
                            }

                            const runId = runMeta.run_id || runMeta.RUN_ID || runMeta.RUN_NUMBER;
                            const spStatus = runMeta.status || runMeta.STATUS || "COMPLETED";
                            const passRate = runMeta.pass_rate || runMeta.PASS_RATE || runMeta.PASS_PERCENTAGE;
                            const invalidRecords = runMeta.invalid_records || runMeta.INVALID_RECORDS || runMeta.FAILED_RECORDS || 0;
                            const recordsProcessed = runMeta.records_processed || runMeta.RECORDS_PROCESSED || runMeta.TOTAL_RECORDS || 0;
                            const durationSeconds = runMeta.duration_seconds || runMeta.DURATION_SECONDS || runMeta.EXECUTION_TIME || 0;
                            const datasetId = runMeta.dataset_id || runMeta.DATASET_ID || `${databaseName}.${schemaName}`;
                            const totalChecks = runMeta.total_checks || runMeta.TOTAL_CHECKS || 0;
                            const passed = runMeta.passed || runMeta.PASSED || 0;
                            const failed = runMeta.failed || runMeta.FAILED || 0;
                            const warnings = runMeta.warnings || runMeta.WARNINGS || 0;
                            const skipped = runMeta.skipped || runMeta.SKIPPED || 0;

                            setExecTasks((prev) => prev.map((x, idx) => idx === i ? {
                              ...x,
                              status: "success",
                              runId,
                              spStatus,
                              passRate,
                              invalidRecords,
                              recordsProcessed,
                              durationSeconds,
                              datasetId,
                              totalChecks,
                              passed,
                              failed,
                              warnings,
                              skipped
                            } : x));

                            // Store the latest run_id for filtering failed records
                            if (runId) {
                              setLatestRunId(runId);
                            }

                            // Show success toast with status badge
                            const statusBadge = spStatus === "COMPLETED_WITH_FAILURES" ? "WARNING FAILED" : spStatus === "COMPLETED" ? "PASSED" : "INFO " + spStatus;
                            const toastMsg = `Custom scan completed: ${t.ruleName}${t.columnName ? " on " + t.columnName : ""} - ${statusBadge}`;
                            const toastType = spStatus === "COMPLETED_WITH_FAILURES" ? "warning" : "success";
                            showToast(toastMsg, toastType, 6000);

                            // Refresh metrics immediately
                            await fetchTableMetrics(selectedExecutionDate || undefined, selectedDataViewMode);
                            await fetchTableSummary(selectedExecutionDate || undefined, selectedDataViewMode);
                            await fetchQualityScoreHistory(selectedDataViewMode);
                          } catch (err: any) {
                            setExecTasks((prev) => prev.map((x, idx) => idx === i ? { ...x, status: "error", error: err?.message || "Error" } : x));
                            showToast(`Failed: ${t.ruleName}${t.columnName ? " on " + t.columnName : ""} - ${err?.message || "Error"}`, "error", 5000);
                          }
                        }
                        // After all tasks, refresh failed records if on that tab
                        // Final summary toast
                        const totalSuccess = tasks.filter((_t, idx) => execTasks[idx]?.status === "success").length;
                        const totalFailed = tasks.filter((_t, idx) => execTasks[idx]?.status === "error").length;
                        showToast(`Scan complete: ${totalSuccess} succeeded, ${totalFailed} failed`, totalFailed > 0 ? "warning" : "success", 7000);
                      } finally {
                        setScanLoading(false);
                        // Keep isExecuting true so results stay visible
                      }
                    }}
                    disabled={scanLoading || selectedRuleIds.length === 0 || (scope === "columns" && selectedColumns.length === 0) || !resolvedDatasetId}
                  >
                    {scanLoading ? "Running..." : "Run Scan"}
                  </Button>
                  {!resolvedDatasetId && (
                    <div className="text-xs text-red-500 ml-2">
                      Dataset not registered
                    </div>
                  )}
                </div>
              </>
            ) : (
              /* Results View Only */
              <div className="p-5 space-y-4 max-h-[70vh] overflow-auto">
                <div className="text-sm font-medium text-slate-700 mb-2">Execution Progress</div>
                <div className="space-y-2 max-h-64 overflow-auto">
                  {execTasks.map((t, idx) => {
                    const isFailure = t.spStatus === "COMPLETED_WITH_FAILURES";
                    return (
                      <div
                        key={t.id}
                        className={`flex items-center justify-between p-3 rounded border text-sm transition-all ${isFailure
                          ? "bg-red-50 border-red-300"
                          : t.status === "success"
                            ? "bg-emerald-50 border-emerald-200"
                            : t.status === "error"
                              ? "bg-red-50 border-red-200"
                              : t.status === "running"
                                ? "bg-indigo-50 border-indigo-200"
                                : "bg-white border-slate-200"
                          }`}
                      >
                        <div className="flex-1">
                          <div className="flex items-center gap-2">
                            <span className={`font-medium ${isFailure ? "text-red-800" : "text-slate-800"}`}>
                              {t.ruleName}
                            </span>
                            {t.columnName && (
                              <>
                                <span className="text-slate-400">|</span>
                                <span className={`text-xs ${isFailure ? "text-red-700" : "text-slate-600"}`}>
                                  {t.columnName}
                                </span>
                              </>
                            )}
                          </div>
                          {t.status === "success" && t.recordsProcessed !== undefined && (
                            <div className="text-xs text-slate-500 mt-1">
                              {t.recordsProcessed.toLocaleString()} records | {t.invalidRecords || 0} failures
                              {t.passRate !== undefined && ` | ${t.passRate.toFixed(1)}% pass rate`}
                            </div>
                          )}
                          {t.error && (
                            <div className="text-xs text-red-600 mt-1">{t.error}</div>
                          )}
                        </div>
                        <div className="ml-3">
                          {t.status === "pending" && <span className="text-slate-400 text-xs">Pending</span>}
                          {t.status === "running" && <span className="text-indigo-600 text-xs font-medium">Running...</span>}
                          {t.status === "success" && !isFailure && (
                            <span className="px-2 py-1 rounded bg-emerald-100 text-emerald-700 text-xs font-semibold">Passed</span>
                          )}
                          {t.status === "success" && isFailure && (
                            <span className="px-2 py-1 rounded bg-red-100 text-red-700 text-xs font-semibold">Failed</span>
                          )}
                          {t.status === "error" && (
                            <span className="px-2 py-1 rounded bg-red-100 text-red-700 text-xs font-semibold">Error</span>
                          )}
                        </div>
                      </div>
                    );
                  })}
                </div>

                {/* Detailed Results Card for Completed Tasks */}
                {execTasks.some(t => t.status === "success" && t.runId) && (
                  <div className="mt-4 space-y-3">
                    <div className="text-sm font-medium text-slate-700">Scan Results</div>
                    {execTasks.filter(t => t.status === "success" && t.runId).map((t) => (
                      <Card key={t.runId} className="p-4 bg-gradient-to-br from-slate-50 to-white border border-slate-200">
                        <div className="space-y-3">
                          {/* Header with status */}
                          <div className="flex items-center justify-between pb-2 border-b border-slate-200">
                            <div>
                              <div className="font-semibold text-slate-900">{t.ruleName}</div>
                              <div className="text-xs text-slate-500 mt-0.5">
                                {t.columnName ? `Column: ${t.columnName}` : "Table-level check"} | {t.datasetId}
                              </div>
                            </div>
                            <div className={`px-3 py-1 rounded-full text-xs font-bold ${t.spStatus === "COMPLETED_WITH_FAILURES"
                              ? "bg-red-100 text-red-700"
                              : "bg-emerald-100 text-emerald-700"
                              }`}>
                              {t.spStatus === "COMPLETED_WITH_FAILURES" ? "Failed" : "Passed"}
                            </div>
                          </div>

                          {/* Metrics Grid */}
                          <div className="grid grid-cols-3 gap-3">
                            <div className="bg-white rounded-lg p-3 border border-slate-100">
                              <div className="text-xs text-slate-500 mb-1">Pass Rate</div>
                              <div className={`text-2xl font-bold ${(t.passRate || 0) >= 90 ? "text-emerald-600" :
                                (t.passRate || 0) >= 70 ? "text-yellow-600" : "text-red-600"
                                }`}>
                                {t.passRate !== undefined && t.passRate !== null ? t.passRate.toFixed(1) : "0.0"}%
                              </div>
                            </div>

                            <div className="bg-white rounded-lg p-3 border border-slate-100">
                              <div className="text-xs text-slate-500 mb-1">Records</div>
                              <div className="text-2xl font-bold text-slate-900">
                                {t.recordsProcessed !== undefined && t.recordsProcessed !== null ? t.recordsProcessed.toLocaleString() : "0"}
                              </div>
                              <div className="text-xs text-slate-500 mt-1">
                                {t.invalidRecords !== undefined && t.invalidRecords !== null ? t.invalidRecords.toLocaleString() : "0"} invalid
                              </div>
                            </div>

                            <div className="bg-white rounded-lg p-3 border border-slate-100">
                              <div className="text-xs text-slate-500 mb-1">Duration</div>
                              <div className="text-2xl font-bold text-slate-900">
                                {t.durationSeconds !== undefined && t.durationSeconds !== null ? t.durationSeconds.toFixed(2) : "0.00"}s
                              </div>
                            </div>
                          </div>

                          {/* Checks Breakdown */}
                          <div className="bg-white rounded-lg p-3 border border-slate-100">
                            <div className="text-xs text-slate-500 mb-2">Checks Breakdown</div>
                            <div className="flex items-center gap-4 text-sm">
                              <div className="flex items-center gap-1.5">
                                <div className="w-2 h-2 rounded-full bg-emerald-500"></div>
                                <span className="text-slate-600">Passed:</span>
                                <span className="font-semibold text-emerald-700">{t.passed || 0}</span>
                              </div>
                              <div className="flex items-center gap-1.5">
                                <div className="w-2 h-2 rounded-full bg-red-500"></div>
                                <span className="text-slate-600">Failed:</span>
                                <span className="font-semibold text-red-700">{t.failed || 0}</span>
                              </div>
                              <div className="flex items-center gap-1.5">
                                <div className="w-2 h-2 rounded-full bg-yellow-500"></div>
                                <span className="text-slate-600">Warnings:</span>
                                <span className="font-semibold text-yellow-700">{t.warnings || 0}</span>
                              </div>
                              <div className="flex items-center gap-1.5">
                                <div className="w-2 h-2 rounded-full bg-slate-400"></div>
                                <span className="text-slate-600">Skipped:</span>
                                <span className="font-semibold text-slate-600">{t.skipped || 0}</span>
                              </div>
                              <div className="ml-auto text-slate-500">
                                Total: <span className="font-semibold text-slate-900">{t.totalChecks || 0}</span>
                              </div>
                            </div>
                          </div>

                          {/* Run ID */}
                          <div className="text-xs text-slate-400 font-mono pt-2 border-t border-slate-100">
                            Run ID: {t.runId}
                          </div>
                        </div>
                      </Card>
                    ))}
                  </div>
                )}

                {/* Action Buttons */}
                <div className="mt-6 flex justify-between border-t border-slate-200 pt-4">
                  <Button
                    variant="outline"
                    onClick={() => {
                      setIsExecuting(false);
                      setExecTasks([]);
                      setSelectedRuleIds([]);
                      setSelectedColumns([]);
                    }}
                  >
                    New Scan
                  </Button>
                  <Button
                    onClick={() => {
                      setIsCustomScanOpen(false);
                      setIsExecuting(false);
                      setExecTasks([]);
                    }}
                  >
                    Close
                  </Button>
                </div>
              </div>
            )}
          </div>
        </div>
      )}

      {/* Tabs */}
      <div className="border-b border-slate-200 bg-white">
        <div className="px-6 py-3">
          <div className="flex w-full items-stretch gap-2 overflow-x-auto pb-1">
            {[
              {
                key: "overview",
                label: "Summary",
                icon: BarChart2,
                title: "Table overview and key metrics",
              },
              {
                key: "activity",
                label: "History",
                icon: Clock,
                title: "Scan execution history",
              },
              {
                key: "fields",
                label: "Columns",
                icon: FileText,
                title: "Column details and statistics",
              },
              {
                key: "observability",
                label: "Health",
                icon: Activity,
                title: "Data freshness and lineage",
              },
              {
                key: "checks",
                label: "Rules",
                icon: CheckCircle2,
                title: "Quality rule results",
              },
              {
                key: "anomalies",
                label: "Alerts",
                icon: AlertOctagon,
                title: "Detected anomalies and alerts",
              },
              {
                key: "data-preview",
                label: "Data",
                icon: Eye,
                title: "Sample data records",
              },
              {
                key: "failed-records",
                label: "Failures",
                icon: X,
                title: "Records that failed validation",
              },
            ].map((tab) => {
              const Icon = tab.icon;
              const isActive = activeTab === tab.key;

              return (
                <button
                  key={tab.key}
                  onClick={() => setActiveTab(tab.key)}
                  className={`inline-flex min-w-[132px] flex-1 items-center justify-center gap-2.5 rounded-lg border px-4 py-3.5 text-sm font-semibold transition-all duration-200 whitespace-nowrap ${isActive
                    ? "border-indigo-600 bg-indigo-600 text-white shadow-sm shadow-indigo-200"
                    : "border-slate-200 bg-white text-slate-600 hover:border-indigo-200 hover:bg-indigo-50 hover:text-indigo-600"
                    }`}
                  title={tab.title}
                >
                  <Icon size={18} />
                  <span>{tab.label}</span>
                </button>
              );
            })}
          </div>
        </div>
      </div>

      {/* Content Area */}
      <div className="px-6 py-6 min-h-125">
        {/* Overview Tab */}
        {activeTab === "overview" && (
          <div className="space-y-6">
            {tableMetricsStatusMessage && (
              <div className="rounded-lg border border-amber-200 bg-amber-50 px-4 py-2 text-sm text-amber-800">
                {tableMetricsStatusMessage}
              </div>
            )}

            <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
              <Card className="p-4 bg-gradient-to-br from-indigo-50 to-white shadow-sm border border-indigo-100">
                <div className="flex items-center justify-between gap-3 mb-3">
                  <div className="flex items-center gap-2">
                    <FileText className="text-indigo-600" size={20} />
                    <p className="text-base font-semibold text-slate-800">Records</p>
                  </div>
                </div>

                {(isLoadingRowCount || isLoadingTableSummary) ? (
                  <p className="text-2xl font-bold text-slate-400">Loading...</p>
                ) : (
                  <div className="space-y-2.5">
                    <p className="text-2xl font-bold text-slate-900">
                      {activeRecordsMetric.value !== null ? activeRecordsMetric.value.toLocaleString() : "0"}
                    </p>
                    {recordsMetrics.map((metric) => (
                      <div key={metric.key} className="flex items-center justify-between text-sm">
                        <span className={`${selectedDataViewMode === metric.key ? "text-slate-700 font-medium" : "text-slate-500"}`}>
                          {metric.label}
                        </span>
                        <span className={`${selectedDataViewMode === metric.key ? "text-slate-900 font-semibold" : "text-slate-700"}`}>
                          {metric.value !== null ? metric.value.toLocaleString() : "0"}
                        </span>
                      </div>
                    ))}
                  </div>
                )}
              </Card>

              <Card className="p-4 bg-gradient-to-br from-emerald-50 to-white shadow-sm border border-emerald-100">
                <div className="flex items-center gap-3 mb-3">
                  <div className="p-2 bg-emerald-100 rounded-lg">
                    <CheckCircle2 className="text-emerald-600" size={20} />
                  </div>
                  <p className="text-base font-semibold text-slate-800">Quality Score</p>
                </div>
                {isLoadingQualityScore ? (
                  <p className="text-2xl font-bold text-slate-400">Loading...</p>
                ) : (
                  <>
                    <p className="text-2xl font-bold text-slate-900">
                      {qualityScore !== null ? `${qualityScore.toFixed(1)}%` : "-"}
                    </p>
                    {qualityScore !== null && (
                      <p className="text-base text-emerald-600 mt-1">
                        {qualityScore >= 90 ? "Excellent" : qualityScore >= 70 ? "Good" : qualityScore >= 50 ? "Fair" : "Poor"}
                      </p>
                    )}
                  </>
                )}
              </Card>

              <Card className="p-4 bg-gradient-to-br from-blue-50 to-white shadow-sm border border-blue-100">
                <div className="flex items-center justify-between mb-3">
                  <div className="flex items-center gap-2">
                    <div className="p-2 bg-blue-100 rounded-lg">
                      <CheckCircle2 className="text-blue-600" size={20} />
                    </div>
                    <p className="text-base font-semibold text-slate-800">Data Quality</p>
                  </div>
                  <Select
                    value={selectedScoreMetric}
                    onValueChange={(value: "completeness" | "uniqueness" | "consistency" | "validity") => setSelectedScoreMetric(value)}
                  >
                    <SelectTrigger className="w-32 h-8 text-xs">
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent side="bottom" align="end">
                      <SelectItem value="completeness">Completeness</SelectItem>
                      <SelectItem value="uniqueness">Uniqueness</SelectItem>
                      <SelectItem value="consistency">Consistency</SelectItem>
                      <SelectItem value="validity">Validity</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
                {isLoadingQualityScore ? (
                  <p className="text-2xl font-bold text-slate-400">Loading...</p>
                ) : (
                  <>
                    <p className="text-2xl font-bold text-slate-900">
                      {selectedScoreMetric === "completeness"
                        ? completeness !== null ? `${completeness.toFixed(1)}%` : "-"
                        : selectedScoreMetric === "uniqueness"
                          ? uniqueness !== null ? `${uniqueness.toFixed(1)}%` : "-"
                          : selectedScoreMetric === "consistency"
                            ? consistency !== null ? `${consistency.toFixed(1)}%` : "-"
                            : validity !== null ? `${validity.toFixed(1)}%` : "-"}
                    </p>
                    {((selectedScoreMetric === "completeness" && completeness !== null) ||
                      (selectedScoreMetric === "uniqueness" && uniqueness !== null) ||
                      (selectedScoreMetric === "consistency" && consistency !== null) ||
                      (selectedScoreMetric === "validity" && validity !== null)) && (
                        <p className="text-base text-blue-600 mt-1">
                          {(() => {
                            const score = selectedScoreMetric === "completeness" ? completeness : selectedScoreMetric === "uniqueness" ? uniqueness : selectedScoreMetric === "consistency" ? consistency : validity;
                            return score! >= 95 ? "Excellent" : score! >= 85 ? "Good" : score! >= 70 ? "Fair" : "Poor";
                          })()}
                        </p>
                      )}
                  </>
                )}
              </Card>

              <Card className="p-4 bg-gradient-to-br from-purple-50 to-white shadow-sm border border-purple-100">
                <div className="flex items-center gap-3 mb-3">
                  <div className="p-2 bg-purple-100 rounded-lg">
                    <CheckCircle2 className="text-purple-600" size={20} />
                  </div>
                  <p className="text-base font-semibold text-slate-800">Checks Executed</p>
                </div>
                {isLoadingQualityScore ? (
                  <p className="text-2xl font-bold text-slate-400">Loading...</p>
                ) : (
                  <>
                    <p className="text-2xl font-bold text-slate-900">
                      {checksExecuted !== null ? checksExecuted.toLocaleString() : "-"}
                    </p>
                    {failedRecordsCount !== null && (
                      <p className="text-sm text-red-600 mt-1">Failed Records: {failedRecordsCount.toLocaleString()}</p>
                    )}
                  </>
                )}
              </Card>
            </div>

            <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
              <Card className="col-span-2 p-6 shadow-sm border border-slate-200">
                <div className="flex items-center justify-between mb-4">
                  <h3 className="font-bold text-slate-800">Quality Score History</h3>
                  {qualityScoreHistory.length > 0 && (
                    <span className="text-xs text-slate-400 font-medium">Last {qualityScoreHistory.length} days</span>
                  )}
                </div>

                {isLoadingQualityHistory ? (
                  <div className="grid grid-cols-2 sm:grid-cols-3 gap-3">
                    {Array.from({ length: 6 }).map((_, i) => (
                      <div key={i} className="animate-pulse bg-slate-100 rounded-lg h-24" />
                    ))}
                  </div>
                ) : qualityScoreHistory.length === 0 ? (
                  <div className="flex flex-col items-center justify-center h-48 text-slate-400">
                    <svg className="w-10 h-10 mb-2 text-slate-300" fill="none" viewBox="0 0 24 24" stroke="currentColor"><path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z" /></svg>
                    <p className="text-sm font-medium">No execution history found</p>
                    <p className="text-xs mt-1">Run a scan to start tracking quality scores</p>
                  </div>
                ) : (
                  <div className="h-full min-h-[260px] flex items-center justify-center">
                    <QualityTrendChart data={qualityScoreHistory} />
                  </div>
                )}
              </Card>
              <Card className="p-6 shadow-sm border border-slate-200">
                <h3 className="font-bold text-slate-800 mb-4">Recent Anomalies</h3>
                <div className="space-y-2">
                  <div className="flex items-start gap-3 p-3 bg-amber-50 rounded-lg border border-amber-100">
                    <AlertOctagon className="text-amber-600 shrink-0 mt-0.5" size={18} />
                    <div>
                      <p className="text-sm font-medium text-amber-900">
                        {overviewAnomalyFlags.length > 0 ? `${formatAnomalyFlag(overviewAnomalyFlags[0])} Detected` : "No Run-Scoped Anomaly"}
                      </p>
                      <p className="text-xs text-amber-700 mt-1">
                        {overviewAnomalyFlags.length > 0
                          ? `Triggered in selected ${selectedDataViewMode.toLowerCase()} run on ${formatDateLabel(selectedExecutionDate)}.`
                          : overviewRunId
                            ? "No anomalies triggered in the selected run."
                            : "No run executed for selected configuration."}
                      </p>
                    </div>
                  </div>
                  <div className="text-center text-sm text-slate-500 pt-2">
                    {overviewAnomalyFlags.length > 1
                      ? `${overviewAnomalyFlags.length - 1} additional anomaly signal(s) in selected run.`
                      : "No other recent anomalies."}
                  </div>
                </div>
              </Card>
            </div>

            <OverviewAdditiveSections
              datasetId={overviewDatasetId}
              selectedRunId={overviewRunId}
              selectedDate={selectedExecutionDate}
              selectedMode={selectedDataViewMode}
              onDrillToFailedRecords={() => setActiveTab("failed-records")}
            />
          </div>
        )}

        {/* Fields Tab */}
        {activeTab === "fields" && (
          <FieldsTab
            database={databaseName}
            schema={schemaName}
            table={tableName}
          />
        )}

        {/* Observability Tab */}
        {activeTab === "observability" && (
          <div className="space-y-6">
            {/* Loading State */}
            {isLoadingObservability && (
              <Card className="p-6 bg-white shadow-sm border border-slate-200">
                <div className="flex items-center justify-center h-64">
                  <div className="text-center">
                    <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-indigo-500 mx-auto mb-4"></div>
                    <p className="text-sm text-slate-500">Loading observability metrics...</p>
                  </div>
                </div>
              </Card>
            )}

            {/* Error State */}
            {observabilityError && !isLoadingObservability && (
              <Card className="p-6 bg-white shadow-sm border border-red-200">
                <div className="flex items-center justify-center h-32 text-red-500">
                  <div className="text-center">
                    <AlertTriangle className="h-8 w-8 mx-auto mb-2" />
                    <p className="text-sm">{observabilityError}</p>
                    <Button
                      variant="outline"
                      className="mt-4"
                      onClick={() => fetchObservabilityData()}
                    >
                      <RefreshCw className="h-4 w-4 mr-2" /> Retry
                    </Button>
                  </div>
                </div>
              </Card>
            )}

            {/* Metrics Content */}
            {!isLoadingObservability && !observabilityError && (
              <>
                {/* Antigravity AI Insights */}
                <AntigravityInsights
                  database={databaseName}
                  schema={schemaName}
                  table={tableName}
                />

                {/* Primary KPI Cards */}
                <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
                  {/* Freshness Card */}
                  <Card className="p-4 bg-white shadow-sm border-l-4 border-l-emerald-500">
                    <div className="flex items-center gap-3 mb-2">
                      <div className="p-2 bg-emerald-50 rounded-lg">
                        <Clock className="h-5 w-5 text-emerald-600" />
                      </div>
                      <h3 className="text-sm font-semibold text-slate-700">Freshness</h3>
                    </div>
                    <div className="space-y-1">
                      <div className={`inline-flex items-center px-2 py-0.5 rounded-full text-xs font-bold ${observabilityFreshness?.slaStatus === "On-time"
                        ? "bg-emerald-100 text-emerald-800"
                        : observabilityFreshness?.slaStatus === "Delayed"
                          ? "bg-amber-100 text-amber-800"
                          : "bg-rose-100 text-rose-800"
                        }`}>
                        {observabilityFreshness?.slaStatus || "Unknown"}
                      </div>
                      <p className="text-xs text-slate-500">
                        {observabilityFreshness?.freshnessDelayFormatted ?? "-"}
                      </p>
                    </div>
                  </Card>

                  {/* Volume Card */}
                  <Card className="p-4 bg-white shadow-sm border-l-4 border-l-blue-500">
                    <div className="flex items-center gap-3 mb-2">
                      <div className="p-2 bg-blue-50 rounded-lg">
                        <BarChart2 className="h-5 w-5 text-blue-600" />
                      </div>
                      <h3 className="text-sm font-semibold text-slate-700">Volume</h3>
                    </div>
                    <div className="space-y-1">
                      <p className="text-2xl font-bold text-slate-900">
                        {observabilityVolume?.rowCount?.toLocaleString() ?? "-"}
                      </p>
                      <p className="text-xs text-slate-500">
                        {observabilityVolume?.bytesFormatted ?? "-"} storage
                      </p>
                    </div>
                  </Card>

                  {/* Schema Card */}
                  <Card className="p-4 bg-white shadow-sm border-l-4 border-l-purple-500">
                    <div className="flex items-center gap-3 mb-2">
                      <div className="p-2 bg-purple-50 rounded-lg">
                        <Type className="h-5 w-5 text-purple-600" />
                      </div>
                      <h3 className="text-sm font-semibold text-slate-700">Schema</h3>
                    </div>
                    <div className="space-y-1">
                      <p className="text-2xl font-bold text-slate-900">
                        {observabilitySchema?.columnCount ?? "-"}
                      </p>
                      <p className="text-xs text-slate-500">
                        {observabilitySchema?.nullableColumnCount ?? 0} nullable columns
                      </p>
                    </div>
                  </Card>

                  {/* Health Score Card */}
                  <Card className="p-4 bg-white shadow-sm border-l-4 border-l-indigo-500">
                    <div className="flex items-center gap-3 mb-2">
                      <div className="p-2 bg-indigo-50 rounded-lg">
                        <Activity className="h-5 w-5 text-indigo-600" />
                      </div>
                      <h3 className="text-sm font-semibold text-slate-700">Health Score</h3>
                    </div>
                    <div className="space-y-1">
                      <p className={`text-2xl font-bold ${(observabilityHealthScore?.overallScore ?? 0) >= 90
                        ? "text-emerald-600"
                        : (observabilityHealthScore?.overallScore ?? 0) >= 75
                          ? "text-amber-600"
                          : "text-rose-600"
                        }`}>
                        {observabilityHealthScore?.overallScore ?? "-"}%
                      </p>
                      <p className="text-xs text-slate-500">Based on metadata analysis</p>
                    </div>
                  </Card>
                </div>

                {/* Secondary Metrics Row */}
                <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                  {/* Load Reliability Card */}
                  <Card className="p-4 bg-white shadow-sm border border-slate-200">
                    <div className="flex items-center gap-3 mb-3">
                      <div className="p-2 bg-cyan-50 rounded-lg">
                        <RefreshCw className="h-5 w-5 text-cyan-600" />
                      </div>
                      <h3 className="text-sm font-semibold text-slate-700">Load Reliability</h3>
                    </div>
                    {observabilityLoads?.loadHistoryAvailable ? (
                      <div className="space-y-2">
                        <div className="flex items-baseline gap-2">
                          <span className={`text-2xl font-bold ${(observabilityLoads.successRate ?? 0) >= 95
                            ? "text-emerald-600"
                            : (observabilityLoads.successRate ?? 0) >= 80
                              ? "text-amber-600"
                              : "text-rose-600"
                            }`}>
                            {observabilityLoads.successRate ?? "-"}%
                          </span>
                          <span className="text-xs text-slate-500">success rate</span>
                        </div>
                        <div className="flex gap-4 text-xs text-slate-500">
                          <span className="flex items-center gap-1">
                            <CheckCircle2 className="h-3 w-3 text-emerald-500" />
                            {observabilityLoads.successfulLoads} succeeded
                          </span>
                          {observabilityLoads.failedLoads > 0 && (
                            <span className="flex items-center gap-1">
                              <AlertTriangle className="h-3 w-3 text-rose-500" />
                              {observabilityLoads.failedLoads} failed
                            </span>
                          )}
                        </div>
                      </div>
                    ) : (
                      <div className="text-sm text-slate-400">
                        Load history not available
                      </div>
                    )}
                  </Card>

                  {/* Size & Growth Card */}
                  <Card className="p-4 bg-white shadow-sm border border-slate-200">
                    <div className="flex items-center gap-3 mb-3">
                      <div className="p-2 bg-amber-50 rounded-lg">
                        <Database className="h-5 w-5 text-amber-600" />
                      </div>
                      <h3 className="text-sm font-semibold text-slate-700">Size & Growth</h3>
                    </div>
                    <div className="space-y-2">
                      <div className="flex items-baseline gap-2">
                        <span className="text-2xl font-bold text-slate-900">
                          {observabilityLoads?.sizeFormatted ?? observabilityVolume?.bytesFormatted ?? "-"}
                        </span>
                      </div>
                      <p className="text-xs text-slate-500">
                        {observabilityVolume?.rowCount?.toLocaleString() ?? "-"} rows
                      </p>
                    </div>
                  </Card>

                  {/* Score Breakdown Card */}
                  <Card className="p-4 bg-white shadow-sm border border-slate-200">
                    <div className="flex items-center gap-3 mb-3">
                      <div className="p-2 bg-violet-50 rounded-lg">
                        <BarChart2 className="h-5 w-5 text-violet-600" />
                      </div>
                      <h3 className="text-sm font-semibold text-slate-700">Score Breakdown</h3>
                    </div>
                    <div className="space-y-2">
                      <div className="flex justify-between items-center">
                        <span className="text-xs text-slate-500">Freshness</span>
                        <span className={`text-xs font-bold ${(observabilityHealthScore?.freshnessScore ?? 0) >= 90 ? "text-emerald-600" :
                          (observabilityHealthScore?.freshnessScore ?? 0) >= 75 ? "text-amber-600" : "text-rose-600"
                          }`}>{observabilityHealthScore?.freshnessScore ?? "-"}%</span>
                      </div>
                      <div className="flex justify-between items-center">
                        <span className="text-xs text-slate-500">Volume</span>
                        <span className={`text-xs font-bold ${(observabilityHealthScore?.volumeScore ?? 0) >= 90 ? "text-emerald-600" :
                          (observabilityHealthScore?.volumeScore ?? 0) >= 75 ? "text-amber-600" : "text-rose-600"
                          }`}>{observabilityHealthScore?.volumeScore ?? "-"}%</span>
                      </div>
                      <div className="flex justify-between items-center">
                        <span className="text-xs text-slate-500">Schema</span>
                        <span className={`text-xs font-bold ${(observabilityHealthScore?.schemaScore ?? 0) >= 90 ? "text-emerald-600" :
                          (observabilityHealthScore?.schemaScore ?? 0) >= 75 ? "text-amber-600" : "text-rose-600"
                          }`}>{observabilityHealthScore?.schemaScore ?? "-"}%</span>
                      </div>
                    </div>
                  </Card>
                </div>

                {/* Metadata Details */}
                <Card className="p-6 bg-white shadow-sm border border-slate-200">
                  <h3 className="text-lg font-semibold text-slate-800 mb-4">Table Metadata</h3>
                  <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
                    <div className="bg-slate-50 rounded-lg p-4">
                      <p className="text-xs text-slate-500 uppercase font-medium mb-1">Created</p>
                      <p className="text-sm font-medium text-slate-700">
                        {observabilityFreshness?.createdFormatted ?? "-"}
                      </p>
                    </div>
                    <div className="bg-slate-50 rounded-lg p-4">
                      <p className="text-xs text-slate-500 uppercase font-medium mb-1">Last Altered</p>
                      <p className="text-sm font-medium text-slate-700">
                        {observabilityFreshness?.lastAlteredFormatted ?? "-"}
                      </p>
                    </div>
                    <div className="bg-slate-50 rounded-lg p-4">
                      <p className="text-xs text-slate-500 uppercase font-medium mb-1">Storage Size</p>
                      <p className="text-sm font-medium text-slate-700">
                        {observabilityVolume?.bytesFormatted ?? "-"}
                      </p>
                    </div>
                  </div>
                </Card>

                {/* Impact & Lineage Section */}
                <Card className="p-6 bg-white shadow-sm border border-slate-200">
                  <div className="flex items-center justify-between mb-4">
                    <div>
                      <h3 className="text-lg font-semibold text-slate-800">Impact & Lineage</h3>
                      <p className="text-xs text-slate-500 mt-1">
                        {observabilityLineage?.impact
                          ? `${observabilityLineage.impact.upstreamCount} upstream | ${observabilityLineage.impact.downstreamCount} downstream`
                          : "Dependency tracking unavailable"}
                      </p>
                    </div>
                    {observabilityLineage?.node?.healthStatus && (
                      <div className={`px-3 py-1 rounded-full text-xs font-bold ${observabilityLineage.node.healthStatus.toLowerCase() === "healthy"
                        ? "bg-emerald-100 text-emerald-800"
                        : observabilityLineage.node.healthStatus.toLowerCase() === "delayed"
                          ? "bg-amber-100 text-amber-800"
                          : "bg-rose-100 text-rose-800"
                        }`}>
                        {observabilityLineage.node.healthStatus.toUpperCase()}
                      </div>
                    )}
                  </div>

                  {/* Lineage Flow Visualization */}
                  <div className="flex items-center justify-center gap-4 py-8 bg-slate-50 rounded-lg mb-4 overflow-x-auto">

                    {/* Upstream Sources */}
                    <div className="flex flex-col items-end gap-2 min-w-[140px]">
                      {observabilityLineage?.upstream && observabilityLineage.upstream.length > 0 ? (
                        observabilityLineage.upstream.map((up, i) => (
                          <div key={i} className="px-4 py-2 bg-white border border-slate-200 rounded-md shadow-sm text-center min-w-[140px] group relative hover:border-indigo-300 transition-colors cursor-help" title={up.sourceCode || "No source code available"}>
                            <p className="text-xs font-bold text-slate-700">{up.name}</p>
                            <p className="text-[10px] text-slate-500 mb-1">{up.schema}</p>
                            {up.transformationType && (
                              <span className={`text-[9px] px-1.5 py-0.5 rounded uppercase font-medium tracking-wider ${up.transformationType === 'DIRECT' ? 'bg-emerald-50 text-emerald-600' :
                                up.transformationType === 'JOIN' ? 'bg-blue-50 text-blue-600' :
                                  'bg-slate-100 text-slate-500'
                                }`}>
                                {up.transformationType}
                              </span>
                            )}
                          </div>
                        ))
                      ) : (
                        <div className="px-4 py-2 bg-slate-100 border border-slate-200 rounded-md text-slate-400 text-xs italic min-w-[140px] text-center">
                          No upstream
                        </div>
                      )}
                    </div>

                    {/* Arrow */}
                    <div className="text-slate-300">
                      <ArrowRight size={24} />
                    </div>

                    {/* Current Node */}
                    <div className="px-6 py-3 bg-indigo-50 border-2 border-indigo-500 rounded-lg shadow-md text-center min-w-[160px] relative z-10">
                      <p className="text-sm font-bold text-indigo-900">{observabilityLineage?.node?.name || tableName}</p>
                      <p className="text-xs text-indigo-600">{observabilityLineage?.node?.shortName || schemaName}</p>
                    </div>

                    {/* Arrow */}
                    <div className="text-slate-300">
                      <ArrowRight size={24} />
                    </div>

                    {/* Downstream Targets */}
                    <div className="flex flex-col items-start gap-2 min-w-[140px]">
                      {observabilityLineage?.downstream && observabilityLineage.downstream.length > 0 ? (
                        observabilityLineage.downstream.map((down, i) => (
                          <div key={i} className="px-4 py-2 bg-white border border-slate-200 rounded-md shadow-sm text-center min-w-[140px] group relative hover:border-indigo-300 transition-colors cursor-help" title={down.sourceCode || "No source code available"}>
                            <p className="text-xs font-bold text-slate-700">{down.name}</p>
                            <p className="text-[10px] text-slate-500 mb-1">{down.schema}</p>
                            {down.transformationType && (
                              <span className={`text-[9px] px-1.5 py-0.5 rounded uppercase font-medium tracking-wider ${down.transformationType === 'DIRECT' ? 'bg-emerald-50 text-emerald-600' :
                                down.transformationType === 'JOIN' ? 'bg-blue-50 text-blue-600' :
                                  'bg-slate-100 text-slate-500'
                                }`}>
                                {down.transformationType}
                              </span>
                            )}
                          </div>
                        ))
                      ) : (
                        <div className="px-4 py-2 bg-slate-100 border border-slate-200 rounded-md text-slate-400 text-xs italic min-w-[140px] text-center">
                          No downstream
                        </div>
                      )}
                    </div>

                  </div>
                  {/* Business Impact Summary */}
                  {observabilityLineage?.impact?.summary && (
                    <div className={`p-4 rounded-lg border ${observabilityLineage.impact.level === "high"
                      ? "bg-rose-50 border-rose-200"
                      : observabilityLineage.impact.level === "medium"
                        ? "bg-amber-50 border-amber-200"
                        : "bg-emerald-50 border-emerald-200"
                      }`}>
                      <div className="flex items-start gap-3">
                        <div className={`p-1.5 rounded-full ${observabilityLineage.impact.level === "high"
                          ? "bg-rose-100"
                          : observabilityLineage.impact.level === "medium"
                            ? "bg-amber-100"
                            : "bg-emerald-100"
                          }`}>
                          {observabilityLineage.impact.level === "high" ? (
                            <AlertTriangle className={`h-4 w-4 text-rose-600`} />
                          ) : observabilityLineage.impact.level === "medium" ? (
                            <AlertTriangle className={`h-4 w-4 text-amber-600`} />
                          ) : (
                            <CheckCircle2 className={`h-4 w-4 text-emerald-600`} />
                          )}
                        </div>
                        <div>
                          <p className="text-sm font-medium text-slate-700">
                            Business Impact: <span className={`font-bold ${observabilityLineage.impact.level === "high"
                              ? "text-rose-700"
                              : observabilityLineage.impact.level === "medium"
                                ? "text-amber-700"
                                : "text-emerald-700"
                              }`}>{observabilityLineage.impact.level.toUpperCase()}</span>
                          </p>
                          <p className="text-sm text-slate-600 mt-1">
                            {observabilityLineage.impact.summary}
                          </p>
                        </div>
                      </div>
                    </div>
                  )}
                </Card>
              </>
            )}
          </div>
        )}

        {/* Checks Tab */}
        {activeTab === "checks" && (
          <ChecksTab database={databaseName} schema={schemaName} table={tableName} />
        )}

        {/* Anomalies Tab */}
        {
          activeTab === "anomalies" && (
            <div className="space-y-6">
              {/* Loading State */}
              {isLoadingAnomalies && (
                <Card className="p-6 bg-white shadow-sm border border-slate-200">
                  <div className="flex items-center justify-center h-64">
                    <div className="text-center">
                      <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-purple-500 mx-auto mb-4"></div>
                      <p className="text-sm text-slate-500">Detecting anomalies...</p>
                    </div>
                  </div>
                </Card>
              )}

              {/* Error State */}
              {anomaliesError && !isLoadingAnomalies && (
                <Card className="p-6 bg-white shadow-sm border border-red-200">
                  <div className="flex items-center justify-center h-32 text-red-500">
                    <div className="text-center">
                      <AlertTriangle className="h-8 w-8 mx-auto mb-2" />
                      <p className="text-sm">{anomaliesError}</p>
                      <Button
                        variant="outline"
                        className="mt-4"
                        onClick={() => fetchAnomaliesData()}
                      >
                        <RefreshCw className="h-4 w-4 mr-2" /> Retry
                      </Button>
                    </div>
                  </div>
                </Card>
              )}

              {/* Anomalies Content */}
              {!isLoadingAnomalies && !anomaliesError && (
                <>
                  {/* Summary Cards */}
                  <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                    {/* Active Anomalies */}
                    <Card className="p-4 bg-white shadow-sm border-l-4 border-l-rose-500">
                      <div className="text-xs text-slate-500 uppercase font-medium mb-1">Active</div>
                      <div className="text-2xl font-bold text-rose-600">
                        {anomaliesData?.summary.activeAnomalies ?? 0}
                      </div>
                    </Card>

                    {/* Critical Anomalies */}
                    <Card className="p-4 bg-white shadow-sm border-l-4 border-l-purple-500">
                      <div className="text-xs text-slate-500 uppercase font-medium mb-1">Critical</div>
                      <div className="text-2xl font-bold text-purple-600">
                        {anomaliesData?.summary.criticalAnomalies ?? 0}
                      </div>
                    </Card>

                    {/* Resolved Anomalies */}
                    <Card className="p-4 bg-white shadow-sm border-l-4 border-l-emerald-500">
                      <div className="text-xs text-slate-500 uppercase font-medium mb-1">Resolved</div>
                      <div className="text-2xl font-bold text-emerald-600">
                        {anomaliesData?.summary.resolvedAnomalies ?? 0}
                      </div>
                    </Card>

                    {/* Last Scan */}
                    <Card className="p-4 bg-white shadow-sm border border-slate-200">
                      <div className="text-xs text-slate-500 uppercase font-medium mb-1">Last Scan</div>
                      <div className="text-sm font-medium text-slate-700">
                        {anomaliesData?.summary.lastScanTimeFormatted ?? "-"}
                      </div>
                    </Card>
                  </div>

                  {/* Anomalies Table */}
                  <Card className="overflow-hidden bg-white shadow-sm border border-slate-200">
                    <div className="p-4 border-b border-slate-200 flex justify-between items-center">
                      <div>
                        <h3 className="text-lg font-semibold text-slate-800">Detected Anomalies</h3>
                        <p className="text-xs text-slate-500 mt-1">
                          Statistical deviations from historical baselines
                        </p>
                      </div>
                      <Button
                        variant="outline"
                        size="sm"
                        onClick={() => fetchAnomaliesData()}
                      >
                        <RefreshCw className="h-4 w-4 mr-2" /> Scan Now
                      </Button>
                    </div>

                    {anomaliesData?.anomalies && anomaliesData.anomalies.length > 0 ? (
                      <div className="overflow-x-auto">
                        <table className="w-full text-sm">
                          <thead className="bg-slate-50">
                            <tr>
                              <th className="text-left px-4 py-3 text-xs text-slate-600 font-semibold">Metric</th>
                              <th className="text-left px-4 py-3 text-xs text-slate-600 font-semibold">Scope</th>
                              <th className="text-left px-4 py-3 text-xs text-slate-600 font-semibold">Target</th>
                              <th className="text-left px-4 py-3 text-xs text-slate-600 font-semibold">Severity</th>
                              <th className="text-left px-4 py-3 text-xs text-slate-600 font-semibold">Baseline</th>
                              <th className="text-left px-4 py-3 text-xs text-slate-600 font-semibold">Current</th>
                              <th className="text-left px-4 py-3 text-xs text-slate-600 font-semibold">Deviation</th>
                              <th className="text-left px-4 py-3 text-xs text-slate-600 font-semibold">Status</th>
                            </tr>
                          </thead>
                          <tbody>
                            {anomaliesData.anomalies.map((anomaly, idx) => (
                              <tr key={anomaly.anomalyId || idx} className="border-t border-slate-100 hover:bg-slate-50">
                                <td className="px-4 py-3 font-medium text-slate-800">{anomaly.metric}</td>
                                <td className="px-4 py-3">
                                  <span className={`px-2 py-0.5 rounded text-xs font-medium ${anomaly.scope === "Column"
                                    ? "bg-purple-100 text-purple-700"
                                    : "bg-blue-100 text-blue-700"
                                    }`}>
                                    {anomaly.scope}
                                  </span>
                                </td>
                                <td className="px-4 py-3 text-slate-600 font-mono text-xs">{anomaly.target}</td>
                                <td className="px-4 py-3">
                                  <span className={`px-2 py-0.5 rounded-full text-xs font-bold ${anomaly.severity === "Critical"
                                    ? "bg-rose-100 text-rose-800"
                                    : anomaly.severity === "High"
                                      ? "bg-amber-100 text-amber-800"
                                      : anomaly.severity === "Medium"
                                        ? "bg-yellow-100 text-yellow-800"
                                        : "bg-slate-100 text-slate-600"
                                    }`}>
                                    {anomaly.severity}
                                  </span>
                                </td>
                                <td className="px-4 py-3 text-slate-600">{anomaly.baseline?.toLocaleString()}</td>
                                <td className="px-4 py-3 text-slate-600">{anomaly.current?.toLocaleString()}</td>
                                <td className="px-4 py-3">
                                  <span className={`text-sm font-bold ${anomaly.deviationPct > 0
                                    ? "text-amber-600"
                                    : "text-rose-600"
                                    }`}>
                                    {anomaly.deviationPct > 0 ? "+" : ""}{anomaly.deviationPct}%
                                  </span>
                                </td>
                                <td className="px-4 py-3">
                                  <span className={`inline-flex items-center px-2 py-0.5 rounded-full text-xs font-bold ${anomaly.status === "Active"
                                    ? "bg-rose-100 text-rose-800"
                                    : "bg-emerald-100 text-emerald-800"
                                    }`}>
                                    {anomaly.status}
                                  </span>
                                </td>
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </div>
                    ) : (
                      <div className="flex items-center justify-center h-48 text-slate-400">
                        <div className="text-center">
                          <CheckCircle2 className="h-12 w-12 mx-auto mb-3 text-emerald-300" />
                          <p className="text-sm font-medium text-emerald-600">No anomalies detected</p>
                          <p className="text-xs mt-1 text-slate-400">All metrics are within normal ranges</p>
                        </div>
                      </div>
                    )}
                  </Card>
                </>
              )}
            </div>
          )
        }

        {/* Data Preview Tab */}
        {
          activeTab === "data-preview" && (
            <Card className="overflow-hidden border border-slate-200 shadow-md">
              {isLoadingPreview ? (
                <div className="flex items-center justify-center h-64 p-6">
                  <div className="text-center">
                    <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-indigo-500 mx-auto mb-4"></div>
                    <p className="text-sm text-slate-500">Loading table data...</p>
                  </div>
                </div>
              ) : previewError ? (
                <div className="flex items-center justify-center h-64 p-6">
                  <div className="text-center">
                    <p className="text-sm text-red-500 mb-4">{previewError}</p>
                    <Button onClick={fetchPreviewData} variant="outline" size="sm">
                      Retry
                    </Button>
                  </div>
                </div>
              ) : !previewData ? (
                <div className="flex items-center justify-center h-64 text-slate-400 p-6">
                  <p className="text-sm">Click to load table preview</p>
                </div>
              ) : (
                <>
                  <div className="p-4 border-b border-slate-100 bg-slate-50 flex justify-between items-center">
                    <h3 className="font-bold text-slate-800 flex items-center gap-2">
                      <TableIcon size={16} /> Live Data Preview
                    </h3>
                    <div className="flex gap-2">
                      <Button
                        onClick={fetchPreviewData}
                        variant="ghost"
                        className="text-xs h-8 hover:bg-slate-200"
                      >
                        <RefreshCw size={14} className="mr-1" /> Refresh
                      </Button>
                    </div>
                  </div>

                  <div className="overflow-x-auto custom-scrollbar max-h-150">
                    <table className="w-full text-base text-left border-collapse">
                      <thead className="bg-slate-100 sticky top-0 z-10 text-sm font-bold text-slate-700 shadow-sm">
                        <tr>
                          {previewData.columns.map((column, idx) => (
                            <th
                              key={idx}
                              className="px-6 py-4 border-r border-slate-200 last:border-0 whitespace-nowrap bg-slate-100"
                            >
                              <div className="font-bold text-slate-900 text-sm">
                                {column.name}
                              </div>
                              <div className="text-xs font-normal text-slate-600 mt-1">
                                {column.type}
                              </div>
                            </th>
                          ))}
                        </tr>
                      </thead>
                      <tbody className="divide-y divide-slate-100">
                        {previewData.rows.map((row, rowIndex) => (
                          <tr key={rowIndex} className="hover:bg-indigo-50/30 transition-colors">
                            {previewData.columns.map((column, colIndex) => (
                              <td
                                key={colIndex}
                                className="px-6 py-3 border-r border-slate-100 last:border-0 text-slate-600 font-mono text-sm"
                              >
                                {row[column.name] === null ? (
                                  <span className="text-slate-400 italic">NULL</span>
                                ) : typeof row[column.name] === "object" ? (
                                  <span className="text-slate-600">
                                    {JSON.stringify(row[column.name])}
                                  </span>
                                ) : (
                                  <span className="text-slate-900">
                                    {String(row[column.name])}
                                  </span>
                                )}
                              </td>
                            ))}
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>

                  <div className="p-3 bg-slate-50 border-t border-slate-200 text-xs text-slate-500 flex justify-center">
                    Showing {previewData.rows.length} rows (Limit 100)
                  </div>
                </>
              )}
            </Card>
          )
        }

        {/* Failed Records Tab */}
        {
          activeTab === "failed-records" && (
            <FailuresTab
              database={databaseName}
              schema={schemaName}
              table={tableName}
              executionTimeSeconds={runSummary?.execution_duration ?? null}
              dqScore={tableSummary?.aggregated_score ?? null}
              onOpenDataView={() => setActiveTab("data-preview")}
            />
          )
        }
        {/* Custom SQL Tab */}
        {
          isCustomSqlTabEnabled && activeTab === "custom-sql" && (
            <CustomSqlTab
              datasetId={dataset}
              database={databaseName}
              schema={schemaName}
              table={tableName}
            />
          )
        }

        {/* Activity Tab */}
        {
          activeTab === "activity" && (
            <>
              <Card className="overflow-hidden border border-slate-200 shadow-md">
                {isLoadingActivity ? (
                  <div className="flex items-center justify-center h-64 p-6">
                    <div className="text-center">
                      <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-indigo-500 mx-auto mb-4"></div>
                      <p className="text-sm text-slate-500">Loading activity...</p>
                    </div>
                  </div>
                ) : activityError ? (
                  <div className="flex items-center justify-center h-64 p-6">
                    <div className="text-center">
                      <p className="text-sm text-red-500 mb-4">{activityError}</p>
                      <Button onClick={fetchActivity} variant="outline" size="sm">
                        Retry
                      </Button>
                    </div>
                  </div>
                ) : activityData.length === 0 ? (
                  <div className="flex items-center justify-center h-64 p-6">
                    <div className="text-center">
                      <Activity size={48} className="text-slate-300 mx-auto mb-4" />
                      <p className="text-lg font-semibold text-slate-700">No Activity Yet</p>
                      <p className="text-sm text-slate-500 mt-2">Run a scan to see activity here.</p>
                    </div>
                  </div>
                ) : (
                  <>
                    <div className="p-4 border-b border-slate-200 bg-slate-100 flex flex-wrap gap-3 justify-between items-center">
                      <h3 className="font-bold text-slate-800 flex items-center gap-2">
                        <Activity size={16} className="text-indigo-600" /> Execution History
                        <span className="text-xs font-normal text-slate-500 ml-2">
                          ({activityData.length} runs)
                        </span>
                      </h3>
                      <div className="flex flex-wrap items-center gap-2">
                        <div className="inline-flex items-center rounded-md border border-slate-200 bg-white p-1">
                          {([
                            { key: "ALL", label: "All Runs" },
                            { key: "MANUAL", label: "Manual" },
                            { key: "SCHEDULED", label: "Scheduled" },
                          ] as const).map((option) => (
                            <button
                              key={option.key}
                              type="button"
                              onClick={() => setActivityExecutionModeFilter(option.key)}
                              className={`px-2.5 py-1 text-xs rounded-sm font-medium transition-colors ${activityExecutionModeFilter === option.key
                                ? "bg-slate-800 text-white"
                                : "text-slate-600 hover:bg-slate-100"
                                }`}
                            >
                              {option.label}
                            </button>
                          ))}
                        </div>
                        <div className="inline-flex items-center rounded-md border border-slate-200 bg-white p-1">
                          {([
                            { key: "ALL", label: "All Types" },
                            { key: "FULL_SCAN", label: "Full Scan" },
                            { key: "INCREMENTAL_SCAN", label: "Incremental" },
                            { key: "CUSTOM_SCAN", label: "Custom" },
                            { key: "PROFILING", label: "Profiling" },
                          ] as const).map((option) => (
                            <button
                              key={option.key}
                              type="button"
                              onClick={() => setActivityRunTypeFilter(option.key)}
                              className={`px-2.5 py-1 text-xs rounded-sm font-medium transition-colors ${activityRunTypeFilter === option.key
                                ? "bg-slate-800 text-white"
                                : "text-slate-600 hover:bg-slate-100"
                                }`}
                            >
                              {option.label}
                            </button>
                          ))}
                        </div>
                        <Button
                          onClick={fetchActivity}
                          variant="ghost"
                          className="text-xs h-8 hover:bg-slate-200"
                        >
                          <RefreshCw size={14} className="mr-1" /> Refresh
                        </Button>
                      </div>
                    </div>

                    <div className="overflow-x-auto custom-scrollbar">
                      <table className="w-full text-sm text-left border-collapse">
                        <thead className="bg-slate-200 sticky top-0 z-10 text-xs font-semibold text-slate-700">
                          <tr>
                            <th className="px-4 py-3 whitespace-nowrap">Run Type</th>
                            <th className="px-4 py-3 whitespace-nowrap">Execution Mode</th>
                            <th className="px-4 py-3 whitespace-nowrap">Status</th>
                            <th className="px-4 py-3 whitespace-nowrap border-l border-slate-300">Started At</th>
                            <th className="px-4 py-3 whitespace-nowrap">Duration</th>
                            <th className="px-4 py-3 whitespace-nowrap text-right border-l border-slate-300">Checks Executed</th>
                            <th className="px-4 py-3 whitespace-nowrap text-right">Failed Checks</th>
                            <th className="px-4 py-3 whitespace-nowrap text-right">Warnings</th>
                            <th className="px-4 py-3 whitespace-nowrap text-center w-16">Actions</th>
                          </tr>
                        </thead>
                        <tbody className="divide-y divide-slate-100">
                          {activityData.map((run: ActivityRun) => {
                            const runTypeMeta = getRunTypeMeta(run);
                            const executionModeMeta = getExecutionModeMeta(run.execution_mode, run.triggered_by);
                            const statusMeta = getRunStatusMeta(run);
                            const failedCount = Number(run.failed_checks ?? run.failed ?? 0);
                            const warningCount = Number(run.warning_checks ?? run.warnings ?? 0);

                            return (
                              <tr
                                key={run.run_id}
                                className={`hover:bg-indigo-50/50 transition-colors cursor-pointer ${selectedRunId === run.run_id ? "bg-indigo-50" : ""}`}
                                onClick={() => setSelectedRunId(run.run_id)}
                              >
                                <td className="px-4 py-3">
                                  <span className={`inline-flex items-center gap-1.5 px-2.5 py-1 rounded-md text-xs font-semibold ${runTypeMeta.className}`}>
                                    {runTypeMeta.icon}
                                    {runTypeMeta.label}
                                  </span>
                                </td>
                                <td className="px-4 py-3">
                                  <span className={`inline-flex items-center gap-1.5 px-2.5 py-1 rounded-md text-xs font-semibold ${executionModeMeta.className}`}>
                                    {executionModeMeta.icon}
                                    {executionModeMeta.label}
                                  </span>
                                </td>
                                <td className="px-4 py-3">
                                  <span className={`inline-flex items-center gap-1.5 px-2.5 py-1 rounded-md text-xs font-semibold ${statusMeta.className}`}>
                                    {statusMeta.icon}
                                    {statusMeta.label}
                                  </span>
                                </td>
                                <td className="px-4 py-3 text-slate-600 border-l border-slate-100">
                                  {formatRawActivityTimestamp(run.started_at ?? run.start_ts ?? null)}
                                </td>
                                <td className="px-4 py-3 text-slate-600 font-mono">
                                  {formatDuration(run.duration_seconds)}
                                </td>
                                <td className="px-4 py-3 text-right font-semibold text-slate-900 border-l border-slate-100 tabular-nums">
                                  {formatCount(run.checks_executed ?? run.total_checks ?? null)}
                                </td>
                                <td className="px-4 py-3 text-right tabular-nums">
                                  <span className={`font-semibold ${failedCount > 0 ? "text-red-600" : "text-slate-500"}`}>
                                    {formatCount(failedCount)}
                                  </span>
                                </td>
                                <td className="px-4 py-3 text-right tabular-nums">
                                  <span className={`font-semibold ${warningCount > 0 ? "text-amber-600" : "text-slate-500"}`}>
                                    {formatCount(warningCount)}
                                  </span>
                                </td>
                                <td className="px-4 py-3 text-center">
                                  <DropdownMenu>
                                    <DropdownMenuTrigger asChild>
                                      <Button
                                        variant="ghost"
                                        size="sm"
                                        className="h-8 w-8 p-0 hover:bg-slate-200"
                                        onClick={(e) => e.stopPropagation()}
                                      >
                                        {isRerunning === run.run_id ? (
                                          <Loader2 className="h-4 w-4 animate-spin" />
                                        ) : (
                                          <MoreVertical className="h-4 w-4" />
                                        )}
                                      </Button>
                                    </DropdownMenuTrigger>
                                    <DropdownMenuContent align="end" className="w-40 bg-white">
                                      <DropdownMenuItem
                                        className="cursor-pointer flex items-center gap-2"
                                        onClick={(e) => {
                                          e.stopPropagation();
                                          handleViewResults(run.run_id);
                                        }}
                                      >
                                        <Eye className="h-4 w-4" />
                                        View Results
                                      </DropdownMenuItem>
                                      <DropdownMenuItem
                                        className="cursor-pointer flex items-center gap-2"
                                        disabled={isRerunning !== null}
                                        onClick={(e) => {
                                          e.stopPropagation();
                                          handleRerun(run.run_id);
                                        }}
                                      >
                                        <RefreshCw className={`h-4 w-4 ${isRerunning === run.run_id ? "animate-spin" : ""}`} />
                                        Rerun
                                      </DropdownMenuItem>
                                      <DropdownMenuItem
                                        className="cursor-pointer flex items-center gap-2"
                                        onClick={(e) => {
                                          e.stopPropagation();
                                          setNewSchedule(prev => ({
                                            ...prev,
                                            scanType: run.scan_type || "profiling",
                                          }));
                                          setScheduleFromRun(run);
                                          setShowScheduleModal(true);
                                        }}
                                      >
                                        <CalendarIcon className="h-4 w-4" />
                                        Schedule
                                      </DropdownMenuItem>
                                    </DropdownMenuContent>
                                  </DropdownMenu>
                                </td>
                              </tr>
                            );
                          })}
                        </tbody>
                      </table>
                    </div>

                    {selectedRunId && (
                      <div className="p-3 bg-indigo-50 border-t border-indigo-100 text-xs text-indigo-700 flex items-center justify-between">
                        <span className="font-mono">Selected Run: {selectedRunId}</span>
                        <Button
                          variant="ghost"
                          size="sm"
                          className="text-xs h-7 text-indigo-700 hover:bg-indigo-100"
                          onClick={() => setSelectedRunId(null)}
                        >
                          Clear Selection
                        </Button>
                      </div>
                    )}

                    <div className="p-3 bg-slate-50 border-t border-slate-200 text-xs text-slate-500 flex justify-center">
                      Showing {activityData.length} runs (Limit 20)
                    </div>
                  </>
                )}
              </Card>

              {/* Scheduled Scans Panel */}
              <Card className="overflow-hidden border border-slate-200 shadow-md mt-4">
                <div className="p-4 border-b border-slate-100 bg-slate-50 flex justify-between items-center">
                  <h3 className="font-bold text-slate-800 flex items-center gap-2">
                    <CalendarIcon size={16} className="text-indigo-600" /> Scheduled Scans
                    <span className="text-xs font-normal text-slate-500 ml-2">
                      ({schedulesData.filter(s => s.status === 'active').length} active)
                    </span>
                  </h3>
                  <Button
                    size="sm"
                    onClick={() => setShowScheduleModal(true)}
                    className="bg-indigo-600 hover:bg-indigo-700 text-white"
                  >
                    <Plus size={14} className="mr-1" /> New Schedule
                  </Button>
                </div>

                {isLoadingSchedules ? (
                  <div className="flex items-center justify-center h-32 p-6">
                    <div className="text-center">
                      <div className="animate-spin rounded-full h-6 w-6 border-b-2 border-indigo-500 mx-auto mb-3"></div>
                      <p className="text-sm text-slate-500">Loading schedules...</p>
                    </div>
                  </div>
                ) : schedulesData.length === 0 ? (
                  <div className="flex items-center justify-center h-32 p-6">
                    <div className="text-center">
                      <CalendarIcon size={32} className="text-slate-300 mx-auto mb-3" />
                      <p className="text-sm font-medium text-slate-700">No Scheduled Scans</p>
                      <p className="text-xs text-slate-500 mt-1">Create a schedule to automate your scans</p>
                    </div>
                  </div>
                ) : (
                  <div className="overflow-x-auto">
                    <table className="w-full text-sm">
                      <thead className="bg-slate-50 text-slate-600 text-xs uppercase">
                        <tr>
                          <th className="px-4 py-3 text-left font-semibold">Scan Type</th>
                          <th className="px-4 py-3 text-left font-semibold">Frequency</th>
                          <th className="px-4 py-3 text-left font-semibold">Next Run</th>
                          <th className="px-4 py-3 text-left font-semibold">Last Run</th>
                          <th className="px-4 py-3 text-left font-semibold">Status</th>
                          <th className="px-4 py-3 text-center font-semibold">Actions</th>
                        </tr>
                      </thead>
                      <tbody className="divide-y divide-slate-100">
                        {schedulesData.map((schedule: any) => (
                          <tr key={schedule.scheduleId} className="hover:bg-slate-50 transition-colors">
                            <td className="px-4 py-3">
                              <span className="inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-xs font-medium bg-indigo-50 text-indigo-700">
                                {schedule.scanType === 'profiling' && <Database size={12} />}
                                {schedule.scanType === 'checks' && <CheckCircle size={12} />}
                                {schedule.scanType === 'anomalies' && <Activity size={12} />}
                                {schedule.scanType === 'full' && <Play size={12} />}
                                {schedule.scanType.charAt(0).toUpperCase() + schedule.scanType.slice(1)}
                              </span>
                            </td>
                            <td className="px-4 py-3 text-slate-700">
                              <div className="flex items-center gap-1.5">
                                <Clock size={12} className="text-slate-400" />
                                {schedule.scheduleType === 'hourly' && 'Every hour'}
                                {schedule.scheduleType === 'daily' && `Daily at ${schedule.scheduleTime || '00:00'}`}
                                {schedule.scheduleType === 'weekly' && `Weekly on ${(Array.isArray(schedule.scheduleDays) && schedule.scheduleDays.length > 0)
                                  ? schedule.scheduleDays.join(', ')
                                  : (schedule.scheduleDayOfWeek || 'Mon')}`}
                                {schedule.scheduleType === 'once' && 'One-time'}
                              </div>
                            </td>
                            <td className="px-4 py-3 text-slate-600 text-xs">
                              {schedule.nextRunAt ? formatUtcToLocal(schedule.nextRunAt) : '-'}
                            </td>
                            <td className="px-4 py-3 text-slate-600 text-xs">
                              {schedule.lastRunAt ? formatUtcToLocal(schedule.lastRunAt) : 'Never'}
                            </td>
                            <td className="px-4 py-3">
                              <span className={`inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-xs font-medium ${schedule.status === 'active'
                                ? 'bg-green-50 text-green-700'
                                : schedule.status === 'paused'
                                  ? 'bg-amber-50 text-amber-700'
                                  : 'bg-slate-100 text-slate-500'
                                }`}>
                                {schedule.status === 'active' && <CheckCircle size={10} />}
                                {schedule.status === 'paused' && <Pause size={10} />}
                                {schedule.status.charAt(0).toUpperCase() + schedule.status.slice(1)}
                              </span>
                            </td>
                            <td className="px-4 py-3">
                              <div className="flex items-center justify-center gap-1">
                                <Button
                                  size="sm"
                                  variant="ghost"
                                  className="h-7 px-2 text-green-600 hover:text-green-700 hover:bg-green-50"
                                  onClick={() => runScheduleNow(schedule.scheduleId)}
                                  title="Run Now"
                                >
                                  <PlayCircle size={14} />
                                </Button>
                                <Button
                                  size="sm"
                                  variant="ghost"
                                  className="h-7 px-2 text-slate-500 hover:text-slate-700 hover:bg-slate-100"
                                  onClick={() => toggleSchedule(schedule.scheduleId, schedule.status)}
                                  title={schedule.status === 'active' ? 'Pause' : 'Resume'}
                                >
                                  {schedule.status === 'active' ? <Pause size={14} /> : <Play size={14} />}
                                </Button>
                                <Button
                                  size="sm"
                                  variant="ghost"
                                  className="h-7 px-2 text-red-500 hover:text-red-700 hover:bg-red-50"
                                  onClick={() => deleteSchedule(schedule.scheduleId)}
                                  title="Delete"
                                >
                                  <Trash2 size={14} />
                                </Button>
                              </div>
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                )}
              </Card>
            </>
          )
        }
      </div >
      {/* Results Modal */}
      < Dialog open={isResultsModalOpen} onOpenChange={setIsResultsModalOpen} >
        <DialogContent className="max-w-4xl max-h-[85vh] overflow-hidden flex flex-col bg-white">
          <DialogHeader>
            <DialogTitle className="flex items-center gap-2">
              <FileText className="h-5 w-5 text-indigo-600" />
              Run Results
              {resultsModalData?.summary?.run_id && (
                <span className="text-sm font-mono text-slate-500 ml-2">
                  {resultsModalData.summary.run_id}
                </span>
              )}
            </DialogTitle>
            <DialogDescription>
              Detailed check results for this data quality run
            </DialogDescription>
          </DialogHeader>

          {isLoadingResults ? (
            <div className="flex items-center justify-center h-48">
              <div className="text-center">
                <Loader2 className="h-8 w-8 animate-spin text-indigo-500 mx-auto mb-4" />
                <p className="text-sm text-slate-500">Loading run details...</p>
              </div>
            </div>
          ) : resultsModalData ? (
            <div className="flex-1 overflow-y-auto">
              {/* Summary KPIs */}
              <div className="grid grid-cols-2 md:grid-cols-4 gap-3 p-4 bg-slate-50 rounded-lg mb-4">
                <div className="text-center p-3 bg-white rounded-lg border border-slate-200">
                  <p className="text-xs text-slate-500 mb-1">Status</p>
                  {(() => {
                    const statusMeta = getRunStatusMeta(resultsModalData.summary as ActivityRun);
                    return (
                      <span className={`inline-flex items-center gap-1.5 px-2.5 py-1 rounded-md text-xs font-semibold ${statusMeta.className}`}>
                        {statusMeta.icon}
                        {statusMeta.label}
                      </span>
                    );
                  })()}
                </div>
                <div className="text-center p-3 bg-white rounded-lg border border-slate-200">
                  <p className="text-xs text-slate-500 mb-1">Duration</p>
                  <p className="text-lg font-bold text-slate-900">
                    {resultsModalData.summary.duration_seconds?.toFixed(2) ?? "-"}s
                  </p>
                </div>
                <div className="text-center p-3 bg-white rounded-lg border border-slate-200">
                  <p className="text-xs text-slate-500 mb-1">Checks Executed</p>
                  <p className="text-lg font-bold text-slate-900">
                    {resultsModalData.summary.checks_executed ?? resultsModalData.summary.total_checks ?? 0}
                  </p>
                </div>
                <div className="text-center p-3 bg-white rounded-lg border border-slate-200">
                  <p className="text-xs text-slate-500 mb-1">Passed / Failed</p>
                  <p className="text-lg font-bold">
                    <span className="text-emerald-600">{resultsModalData.summary.passed_checks ?? 0}</span>
                    <span className="text-slate-400 mx-1">/</span>
                    <span className="text-red-600">{resultsModalData.summary.failed_checks ?? 0}</span>
                  </p>
                </div>
              </div>

              {/* Run Info */}
              <div className="flex gap-4 text-xs text-slate-500 mb-4 px-1">
                <span><strong>Started:</strong> {formatRawActivityTimestamp(resultsModalData.summary.started_at ?? resultsModalData.summary.start_ts ?? null)}</span>
                {(() => {
                  const executionMeta = getExecutionModeMeta(resultsModalData.summary.execution_mode, resultsModalData.summary.triggered_by);
                  return (
                    <span className="inline-flex items-center gap-1.5">
                      <strong>Execution Mode:</strong>
                      {executionMeta.icon}
                      <span>{executionMeta.label}</span>
                    </span>
                  );
                })()}
              </div>

              {/* Checks Table */}
              <div className="border border-slate-200 rounded-lg overflow-hidden">
                <div className="p-3 bg-slate-100 border-b border-slate-200">
                  <h4 className="font-semibold text-sm text-slate-800">Check Results ({resultsModalData.checks?.length ?? 0})</h4>
                </div>
                <div className="max-h-[300px] overflow-y-auto">
                  <table className="w-full text-sm">
                    <thead className="bg-slate-50 sticky top-0 text-xs font-semibold text-slate-600">
                      <tr>
                        <th className="px-3 py-2 text-left">Rule Name</th>
                        <th className="px-3 py-2 text-left">Rule Type</th>
                        <th className="px-3 py-2 text-left">Column</th>
                        <th className="px-3 py-2 text-center">Status</th>
                        <th className="px-3 py-2 text-right">Pass Rate</th>
                        <th className="px-3 py-2 text-right">Threshold</th>
                        <th className="px-3 py-2 text-right">Total</th>
                        <th className="px-3 py-2 text-right">Invalid</th>
                      </tr>
                    </thead>
                    <tbody className="divide-y divide-slate-100">
                      {resultsModalData.checks?.map((check: any, idx: number) => (
                        <tr key={idx} className="hover:bg-slate-50">
                          <td className="px-3 py-2 font-medium text-slate-900">
                            {check.rule_name}
                          </td>
                          <td className="px-3 py-2 text-slate-600">
                            <span className="px-2 py-0.5 rounded-full text-xs bg-indigo-100 text-indigo-700">
                              {check.rule_type}
                            </span>
                          </td>
                          <td className="px-3 py-2 text-slate-600 font-mono text-xs">
                            {check.column_name || "-"}
                          </td>
                          <td className="px-3 py-2 text-center">
                            <span className={`inline-flex items-center gap-1 px-2 py-0.5 rounded-full text-xs font-semibold ${check.check_status === "PASSED"
                              ? "bg-emerald-100 text-emerald-700"
                              : check.check_status === "FAILED"
                                ? "bg-red-100 text-red-700"
                                : check.check_status === "WARNING"
                                  ? "bg-yellow-100 text-yellow-700"
                                  : "bg-slate-100 text-slate-700"
                              }`}>
                              {check.check_status === "PASSED" && <CheckCircle2 size={10} />}
                              {check.check_status === "FAILED" && <AlertOctagon size={10} />}
                              {check.check_status}
                            </span>
                          </td>
                          <td className="px-3 py-2 text-right font-mono">
                            <span className={`font-semibold ${(check.pass_rate ?? 100) >= (check.threshold ?? 0)
                              ? "text-emerald-600"
                              : "text-red-600"
                              }`}>
                              {check.pass_rate !== null && check.pass_rate !== undefined
                                ? `${check.pass_rate.toFixed(2)}%`
                                : "-"}
                            </span>
                          </td>
                          <td className="px-3 py-2 text-right font-mono text-slate-600">
                            {check.threshold !== null && check.threshold !== undefined
                              ? `${check.threshold}%`
                              : "-"}
                          </td>
                          <td className="px-3 py-2 text-right font-mono text-slate-600">
                            {check.total_records?.toLocaleString() ?? "-"}
                          </td>
                          <td className="px-3 py-2 text-right font-mono">
                            <span className={(check.invalid_records ?? 0) > 0 ? "text-red-600 font-semibold" : "text-slate-500"}>
                              {check.invalid_records?.toLocaleString() ?? "-"}
                            </span>
                          </td>
                        </tr>
                      ))}
                      {(!resultsModalData.checks || resultsModalData.checks.length === 0) && (
                        <tr>
                          <td colSpan={8} className="px-3 py-8 text-center text-slate-500">
                            No check results found for this run.
                          </td>
                        </tr>
                      )}
                    </tbody>
                  </table>
                </div>
              </div>
            </div>
          ) : (
            <div className="flex items-center justify-center h-48">
              <p className="text-sm text-slate-500">No data available</p>
            </div>
          )}
        </DialogContent>
      </Dialog >

      {/* Schedule Creation Modal - Enterprise Wizard */}
      < Dialog open={showScheduleModal} onOpenChange={(open) => {
        setShowScheduleModal(open);
        if (!open) setScheduleWizardStep(1);
      }}>
        <DialogContent className="max-w-lg bg-white">
          <DialogHeader>
            <DialogTitle className="flex items-center gap-2">
              <CalendarIcon className="h-5 w-5 text-indigo-600" />
              {scheduleFromRun ? "Schedule This Scan" : "Create Scan Schedule"}
            </DialogTitle>
            <DialogDescription>
              Step {scheduleWizardStep} of 6
            </DialogDescription>
          </DialogHeader>

          {/* Step Indicator */}
          <div className="flex items-center justify-between px-2 py-3 border-b border-slate-100">
            {[1, 2, 3, 4, 5, 6].map((step) => (
              <div key={step} className="flex items-center">
                <div className={`w-8 h-8 rounded-full flex items-center justify-center text-sm font-medium transition-colors ${step < scheduleWizardStep ? "bg-green-500 text-white" :
                  step === scheduleWizardStep ? "bg-indigo-600 text-white" :
                    "bg-slate-200 text-slate-500"
                  }`}>
                  {step < scheduleWizardStep ? <CheckCircle size={16} /> : step}
                </div>
                {step < 6 && <div className={`w-8 h-0.5 mx-1 ${step < scheduleWizardStep ? "bg-green-500" : "bg-slate-200"}`} />}
              </div>
            ))}
          </div>

          {/* Step 1: Scan Configuration */}
          {scheduleWizardStep === 1 && (
            <div className="space-y-4 py-4">
              <h4 className="font-semibold text-slate-800">Scan Configuration</h4>

              <div>
                <label className="block text-xs font-semibold text-slate-500 uppercase tracking-wider mb-2">Scan Type</label>
                <div className="flex items-center gap-2 overflow-x-auto pb-1">
                  {["profiling", "custom", "checks", "anomalies", "full"].map((type) => (
                    <button
                      key={type}
                      type="button"
                      onClick={() => setNewSchedule(prev => ({ ...prev, scanType: type }))}
                      className={`flex-1 px-3 py-2 rounded-lg border text-sm font-medium whitespace-nowrap transition-all ${newSchedule.scanType === type
                        ? "border-indigo-600 bg-indigo-50 text-indigo-700 shadow-sm ring-1 ring-indigo-600"
                        : "border-slate-200 bg-white text-slate-600 hover:bg-slate-50 hover:border-slate-300"
                        }`}
                    >
                      {type === "full" ? "Full Scan" : type.charAt(0).toUpperCase() + type.slice(1)}
                    </button>
                  ))}
                </div>
              </div>

              <div className="bg-slate-50 rounded-lg p-2.5 text-xs flex items-center flex-wrap gap-2 border border-slate-100 mt-2">
                <span className="text-slate-500 font-medium uppercase tracking-wider">Target:</span>
                <div className="flex items-center gap-1 font-mono text-slate-700">
                  <span>{databaseName}</span>
                  <span className="text-slate-300">/</span>
                  <span>{schemaName}</span>
                  <span className="text-slate-300">/</span>
                  <span className="font-bold text-indigo-700">{tableName}</span>
                </div>
              </div>

              {/* Custom Rule Selection (Only for Custom/Checks) */}
              {(newSchedule.scanType === "custom" || newSchedule.scanType === "checks") && (
                <div className="space-y-3 pt-2 border-t border-slate-100">
                  <div className="flex items-center justify-between">
                    <label className="block text-sm font-medium text-slate-700">Select Rules to Run</label>
                    <div className="text-xs text-slate-500">
                      {newSchedule.customRules.length} selected
                    </div>
                  </div>

                  {/* Scope Toggle */}
                  <div className="flex items-center gap-2 p-1 bg-slate-100 rounded-lg w-fit">
                    <button
                      onClick={() => setNewSchedule(prev => ({ ...prev, scope: "table" }))}
                      className={`px-3 py-1.5 rounded-md text-xs font-medium transition-all ${newSchedule.scope === "table"
                        ? "bg-white text-indigo-700 shadow-sm"
                        : "text-slate-500 hover:text-slate-700"
                        }`}
                    >
                      All Rules
                    </button>
                    <button
                      onClick={() => setNewSchedule(prev => ({ ...prev, scope: "columns" }))}
                      className={`px-3 py-1.5 rounded-md text-xs font-medium transition-all ${newSchedule.scope === "columns"
                        ? "bg-white text-indigo-700 shadow-sm"
                        : "text-slate-500 hover:text-slate-700"
                        }`}
                    >
                      Specific Columns
                    </button>
                  </div>

                  {/* Selection List (Rules or Columns) */}
                  <div className="border rounded-lg max-h-60 overflow-y-auto bg-white">
                    {newSchedule.scope === "columns" ? (
                      // COLUMN SELECTION
                      availableColumns.length === 0 ? (
                        <div className="p-8 text-center text-slate-500 text-sm">
                          No columns found for this table.
                        </div>
                      ) : (
                        <div className="divide-y divide-slate-50">
                          {availableColumns.map((colName) => (
                            <label key={colName} className="flex items-center gap-3 p-3 hover:bg-slate-50 cursor-pointer">
                              <input
                                type="checkbox"
                                checked={newSchedule.selectedColumns.includes(colName)}
                                onChange={(e) => {
                                  if (e.target.checked) {
                                    setNewSchedule(prev => ({ ...prev, selectedColumns: [...prev.selectedColumns, colName] }));
                                  } else {
                                    setNewSchedule(prev => ({ ...prev, selectedColumns: prev.selectedColumns.filter(c => c !== colName) }));
                                  }
                                }}
                                className="w-4 h-4 text-indigo-600 rounded border-slate-300"
                              />
                              <span className="text-sm font-medium text-slate-700 font-mono">{colName}</span>
                            </label>
                          ))}
                        </div>
                      )
                    ) : (
                      // RULE SELECTION
                      Object.entries(groupedRules).length === 0 ? (
                        <div className="p-8 text-center text-slate-500 text-sm">
                          No rules configured. Please add rules in the Checks tab first.
                        </div>
                      ) : (
                        Object.entries(groupedRules).map(([type, typeRules]) => (
                          <div key={type} className="border-b border-slate-50 last:border-0">
                            <div className="px-3 py-2 bg-slate-50 text-xs font-semibold text-slate-600 sticky top-0">
                              {type}
                            </div>
                            <div className="divide-y divide-slate-50">
                              {typeRules.map((rule: any) => (
                                <label key={rule.rule_id} className="flex items-start gap-3 p-3 hover:bg-slate-50 cursor-pointer">
                                  <input
                                    type="checkbox"
                                    checked={newSchedule.customRules.includes(rule.rule_id)}
                                    onChange={(e) => {
                                      if (e.target.checked) {
                                        setNewSchedule(prev => ({ ...prev, customRules: [...prev.customRules, rule.rule_id] }));
                                      } else {
                                        setNewSchedule(prev => ({ ...prev, customRules: prev.customRules.filter(id => id !== rule.rule_id) }));
                                      }
                                    }}
                                    className="mt-0.5 w-4 h-4 text-indigo-600 rounded border-slate-300"
                                  />
                                  <div>
                                    <div className="text-sm font-medium text-slate-800">{rule.rule_name}</div>
                                    <div className="text-xs text-slate-500">
                                      {rule.column_name ? `Column: ${rule.column_name}` : "Table Level"}
                                      {rule.threshold_value && ` | Threshold: ${rule.threshold_value}`}
                                    </div>
                                  </div>
                                </label>
                              ))}
                            </div>
                          </div>
                        ))
                      ))}
                  </div>
                  {/* Select All / Clear */}
                  <div className="flex items-center gap-2 text-xs">
                    <button
                      className="text-indigo-600 hover:text-indigo-800"
                      onClick={() => setNewSchedule(prev => ({ ...prev, customRules: rules.map(r => r.rule_id) }))}
                    >
                      Select All
                    </button>
                    <span className="text-slate-300">|</span>
                    <button
                      className="text-slate-500 hover:text-slate-700"
                      onClick={() => setNewSchedule(prev => ({ ...prev, customRules: [] }))}
                    >
                      Clear Selection
                    </button>
                  </div>
                </div>
              )}
            </div>
          )}

          {/* Step 2: Schedule Type */}
          {scheduleWizardStep === 2 && (
            <div className="space-y-4 py-4">
              <h4 className="font-semibold text-slate-800">Schedule Type</h4>

              <div className="space-y-3">
                <label className={`flex items-center gap-3 p-4 rounded-lg border cursor-pointer transition-colors ${!newSchedule.isRecurring ? "border-indigo-500 bg-indigo-50" : "border-slate-200 hover:bg-slate-50"
                  }`}>
                  <input
                    type="radio"
                    name="isRecurring"
                    checked={!newSchedule.isRecurring}
                    onChange={() => setNewSchedule(prev => ({ ...prev, isRecurring: false }))}
                    className="w-4 h-4 text-indigo-600"
                  />
                  <div>
                    <p className="font-medium">One-time</p>
                    <p className="text-sm text-slate-500">Run once at a specific date and time</p>
                  </div>
                </label>

                <label className={`flex items-center gap-3 p-4 rounded-lg border cursor-pointer transition-colors ${newSchedule.isRecurring ? "border-indigo-500 bg-indigo-50" : "border-slate-200 hover:bg-slate-50"
                  }`}>
                  <input
                    type="radio"
                    name="isRecurring"
                    checked={newSchedule.isRecurring}
                    onChange={() => setNewSchedule(prev => ({ ...prev, isRecurring: true }))}
                    className="w-4 h-4 text-indigo-600"
                  />
                  <div>
                    <p className="font-medium">Recurring</p>
                    <p className="text-sm text-slate-500">Run automatically on a schedule</p>
                  </div>
                </label>
              </div>
            </div>
          )}

          {/* Step 3: Date & Time */}
          {scheduleWizardStep === 3 && (
            <div className="space-y-4 py-4">
              <h4 className="font-semibold text-slate-800">Date & Time</h4>

              {!newSchedule.isRecurring ? (
                // One-time configuration
                <div className="space-y-2">
                  <div>
                    <label className="block text-sm font-medium text-slate-700 mb-1.5">Run Date</label>
                    <input
                      type="date"
                      className="w-full px-3 py-2 border border-slate-300 rounded-md text-sm focus:outline-none focus:ring-2 focus:ring-indigo-500"
                      value={newSchedule.runDate}
                      onChange={(e) => setNewSchedule(prev => ({ ...prev, runDate: e.target.value }))}
                    />
                  </div>
                  <div>
                    <label className="block text-sm font-medium text-slate-700 mb-1.5">Run Time</label>
                    <input
                      type="time"
                      className="w-full px-3 py-2 border border-slate-300 rounded-md text-sm focus:outline-none focus:ring-2 focus:ring-indigo-500"
                      value={newSchedule.scheduleTime}
                      onChange={(e) => setNewSchedule(prev => ({ ...prev, scheduleTime: e.target.value }))}
                    />
                  </div>
                </div>
              ) : (
                // Recurring configuration
                <div className="space-y-2">
                  <div>
                    <label className="block text-sm font-medium text-slate-700 mb-1.5">Frequency</label>
                    <Select
                      value={newSchedule.scheduleType}
                      onValueChange={(value) => setNewSchedule(prev => ({ ...prev, scheduleType: value }))}
                    >
                      <SelectTrigger className="w-full">
                        <SelectValue />
                      </SelectTrigger>
                      <SelectContent>
                        <SelectItem value="hourly">Hourly</SelectItem>
                        <SelectItem value="daily">Daily</SelectItem>
                        <SelectItem value="weekly">Weekly</SelectItem>
                      </SelectContent>
                    </Select>
                  </div>

                  {newSchedule.scheduleType === "weekly" && (
                    <div>
                      <label className="block text-sm font-medium text-slate-700 mb-1.5">Days</label>
                      <div className="flex flex-wrap gap-2">
                        {["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"].map((day) => (
                          <button
                            key={day}
                            type="button"
                            onClick={() => {
                              const days = newSchedule.scheduleDays.includes(day)
                                ? newSchedule.scheduleDays.filter(d => d !== day)
                                : [...newSchedule.scheduleDays, day];
                              setNewSchedule(prev => ({ ...prev, scheduleDays: days }));
                            }}
                            className={`px-3 py-1.5 rounded-md text-sm font-medium transition-colors ${newSchedule.scheduleDays.includes(day)
                              ? "bg-indigo-600 text-white"
                              : "bg-slate-100 text-slate-700 hover:bg-slate-200"
                              }`}
                          >
                            {day}
                          </button>
                        ))}
                      </div>
                    </div>
                  )}

                  {newSchedule.scheduleType !== "hourly" && (
                    <div>
                      <label className="block text-sm font-medium text-slate-700 mb-1.5">Time</label>
                      <input
                        type="time"
                        className="w-full px-3 py-2 border border-slate-300 rounded-md text-sm focus:outline-none focus:ring-2 focus:ring-indigo-500"
                        value={newSchedule.scheduleTime}
                        onChange={(e) => setNewSchedule(prev => ({ ...prev, scheduleTime: e.target.value }))}
                      />
                    </div>
                  )}
                </div>
              )}

              <div>
                <label className="block text-sm font-medium text-slate-700 mb-1.5">Timezone</label>
                <Select
                  value={newSchedule.timezone}
                  onValueChange={(value) => setNewSchedule(prev => ({ ...prev, timezone: value }))}
                >
                  <SelectTrigger className="w-full">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="Asia/Kolkata">India (IST)</SelectItem>
                    <SelectItem value="UTC">UTC</SelectItem>
                    <SelectItem value="America/New_York">Eastern Time (ET)</SelectItem>
                    <SelectItem value="America/Chicago">Central Time (CT)</SelectItem>
                    <SelectItem value="America/Los_Angeles">Pacific Time (PT)</SelectItem>
                    <SelectItem value="Europe/London">London (GMT)</SelectItem>
                  </SelectContent>
                </Select>
              </div>
            </div>
          )}

          {/* Step 4: Schedule Window */}
          {scheduleWizardStep === 4 && (
            <div className="space-y-4 py-4">
              <h4 className="font-semibold text-slate-800">Schedule Window</h4>

              <div className="grid grid-cols-2 gap-4">
                <div>
                  <label className="block text-sm font-medium text-slate-700 mb-1.5">Start Date</label>
                  <input
                    type="date"
                    className="w-full px-3 py-2 border border-slate-300 rounded-md text-sm focus:outline-none focus:ring-2 focus:ring-indigo-500"
                    value={newSchedule.startDate}
                    onChange={(e) => setNewSchedule(prev => ({ ...prev, startDate: e.target.value }))}
                  />
                </div>
                <div>
                  <label className="block text-sm font-medium text-slate-700 mb-1.5">End Date (optional)</label>
                  <input
                    type="date"
                    className="w-full px-3 py-2 border border-slate-300 rounded-md text-sm focus:outline-none focus:ring-2 focus:ring-indigo-500"
                    value={newSchedule.endDate}
                    onChange={(e) => setNewSchedule(prev => ({ ...prev, endDate: e.target.value }))}
                  />
                </div>
              </div>

              <div className="pt-2">
                <label className="flex items-center gap-3 cursor-pointer">
                  <input
                    type="checkbox"
                    checked={newSchedule.skipIfRunning}
                    onChange={(e) => setNewSchedule(prev => ({ ...prev, skipIfRunning: e.target.checked }))}
                    className="w-4 h-4 text-indigo-600 rounded"
                  />
                  <div>
                    <p className="text-sm font-medium text-slate-700">Skip if previous run still active</p>
                    <p className="text-xs text-slate-500">Prevents overlapping scans in production</p>
                  </div>
                </label>
              </div>
            </div>
          )}

          {/* Step 5: Failure Controls */}
          {scheduleWizardStep === 5 && (
            <div className="space-y-4 py-4">
              <h4 className="font-semibold text-slate-800">Failure & Notifications</h4>

              <div>
                <label className="block text-sm font-medium text-slate-700 mb-1.5">On Failure</label>
                <Select
                  value={newSchedule.onFailureAction}
                  onValueChange={(value) => setNewSchedule(prev => ({ ...prev, onFailureAction: value }))}
                >
                  <SelectTrigger className="w-full">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    <SelectItem value="continue">Continue next scheduled run</SelectItem>
                    <SelectItem value="pause">Pause after failures</SelectItem>
                  </SelectContent>
                </Select>
              </div>

              {newSchedule.onFailureAction === "pause" && (
                <div>
                  <label className="block text-sm font-medium text-slate-700 mb-1.5">
                    Pause after how many failures?
                  </label>
                  <input
                    type="number"
                    min="1"
                    max="10"
                    className="w-24 px-3 py-2 border border-slate-300 rounded-md text-sm focus:outline-none focus:ring-2 focus:ring-indigo-500"
                    value={newSchedule.maxFailures}
                    onChange={(e) => setNewSchedule(prev => ({ ...prev, maxFailures: parseInt(e.target.value) || 3 }))}
                  />
                </div>
              )}

              <div className="space-y-3 pt-2">
                <label className="flex items-center gap-3 cursor-pointer">
                  <input
                    type="checkbox"
                    checked={newSchedule.notifyOnFailure}
                    onChange={(e) => setNewSchedule(prev => ({ ...prev, notifyOnFailure: e.target.checked }))}
                    className="w-4 h-4 text-indigo-600 rounded"
                  />
                  <span className="text-sm text-slate-700">Notify on failure</span>
                </label>
                <label className="flex items-center gap-3 cursor-pointer">
                  <input
                    type="checkbox"
                    checked={newSchedule.notifyOnSuccess}
                    onChange={(e) => setNewSchedule(prev => ({ ...prev, notifyOnSuccess: e.target.checked }))}
                    className="w-4 h-4 text-indigo-600 rounded"
                  />
                  <span className="text-sm text-slate-700">Notify on success</span>
                </label>
              </div>
            </div>
          )}

          {/* Step 6: Summary */}
          {scheduleWizardStep === 6 && (
            <div className="space-y-4 py-4">
              <h4 className="font-semibold text-slate-800">Review & Confirm</h4>

              <div className="bg-gradient-to-r from-indigo-50 to-purple-50 rounded-lg p-4 border border-indigo-100">
                <p className="text-slate-800 leading-relaxed">
                  {newSchedule.isRecurring ? (
                    <>
                      This <span className="font-semibold">{newSchedule.scanType}</span> scan will run{" "}
                      <span className="font-semibold">
                        {newSchedule.scheduleType === "hourly" ? "every hour" :
                          newSchedule.scheduleType === "daily" ? `every day at ${newSchedule.scheduleTime}` :
                            `every ${newSchedule.scheduleDays.join(", ")} at ${newSchedule.scheduleTime}`}
                      </span>{" "}
                      <span className="font-semibold">{newSchedule.timezone}</span>
                      {newSchedule.startDate && <>, starting <span className="font-semibold">{newSchedule.startDate}</span></>}
                      {newSchedule.endDate ? <>, until <span className="font-semibold">{newSchedule.endDate}</span></> : ", with no end date"}.
                    </>
                  ) : (
                    <>
                      This <span className="font-semibold">{newSchedule.scanType}</span> scan will run{" "}
                      <span className="font-semibold">once</span> on{" "}
                      <span className="font-semibold">{newSchedule.runDate}</span> at{" "}
                      <span className="font-semibold">{newSchedule.scheduleTime} {newSchedule.timezone}</span>.
                    </>
                  )}
                </p>
              </div>

              <div className="grid grid-cols-2 gap-3 text-sm">
                <div className="bg-slate-50 rounded-lg p-3">
                  <p className="text-slate-500 text-xs mb-1">Target</p>
                  <p className="font-mono text-slate-800">{tableName}</p>
                </div>
                <div className="bg-slate-50 rounded-lg p-3">
                  <p className="text-slate-500 text-xs mb-1">Failure Policy</p>
                  <p className="text-slate-800">{newSchedule.onFailureAction === "continue" ? "Continue" : `Pause after ${newSchedule.maxFailures}`}</p>
                </div>
              </div>
            </div>
          )}

          {/* Navigation Buttons */}
          <div className="flex justify-between pt-4 border-t border-slate-100">
            <Button
              variant="outline"
              onClick={() => {
                if (scheduleWizardStep === 1) {
                  setShowScheduleModal(false);
                } else {
                  setScheduleWizardStep(prev => prev - 1);
                }
              }}
            >
              {scheduleWizardStep === 1 ? "Cancel" : "Back"}
            </Button>

            {scheduleWizardStep < 6 ? (
              <Button
                className="bg-indigo-600 hover:bg-indigo-700 text-white"
                onClick={() => setScheduleWizardStep(prev => prev + 1)}
              >
                Next
              </Button>
            ) : (
              <Button
                className="bg-green-600 hover:bg-green-700 text-white"
                onClick={() => {
                  createSchedule();
                  setScheduleWizardStep(1);
                }}
              >
                <CheckCircle size={14} className="mr-1" /> Create Schedule
              </Button>
            )}
          </div>
        </DialogContent>
      </Dialog >
    </div >
  );
}




















