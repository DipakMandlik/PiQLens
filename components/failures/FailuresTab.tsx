'use client';

import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import {
  AlertTriangle,
  CheckCircle2,
  Info,
  ChevronDown,
  ChevronLeft,
  ChevronRight,
  ChevronUp,
  Clock,
  ExternalLink,
  Filter,
  RefreshCw,
  Search,
  Shield,
  ShieldAlert,
  ShieldCheck,
  X,
  XCircle,
} from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Card } from '@/components/ui/card';
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';

/* ─── types ─── */

type FailuresTabProps = {
  database: string;
  schema: string;
  table: string;
  executionTimeSeconds?: number | null;
  dqScore?: number | null;
  onOpenDataView?: () => void;
};

type FailureSummary = {
  failed_checks: number;
  failed_records: number;
  most_critical_rule: string | null;
  last_failure_ts: string | null;
};

type FailureRuleGroup = {
  rule_id: number | null;
  rule_name: string | null;
  rule_type: string | null;
  column_name: string | null;
  threshold: number | null;
  pass_rate: number | null;
  severity: string;
  failed_records: number;
  last_failure_ts: string | null;
};

type FailureRecordItem = {
  failure_id: number;
  run_id: string;
  primary_key_column: string;
  failed_record_pk: string;
  column_name: string;
  failed_column_value: string;
  failure_reason: string;
  detected_ts: string;
};

type Pagination = {
  page: number;
  page_size: number;
  total_rows: number;
  total_pages: number;
};

type FailureRecordsResponse = {
  records: FailureRecordItem[];
  pagination: Pagination;
};

/* ─── helpers ─── */

function formatTs(value: string | null): string {
  if (!value) return '—';
  const dt = new Date(value);
  if (Number.isNaN(dt.getTime())) return value;
  return dt.toLocaleString('en-IN', { dateStyle: 'medium', timeStyle: 'short' });
}

function relativeTime(value: string | null): string {
  if (!value) return '';
  const dt = new Date(value);
  if (Number.isNaN(dt.getTime())) return '';
  const diff = Date.now() - dt.getTime();
  const mins = Math.floor(diff / 60_000);
  if (mins < 1) return 'just now';
  if (mins < 60) return `${mins}m ago`;
  const hrs = Math.floor(mins / 60);
  if (hrs < 24) return `${hrs}h ago`;
  return `${Math.floor(hrs / 24)}d ago`;
}

type SeverityLevel = 'HIGH' | 'MEDIUM' | 'LOW';

function normalizeSeverity(severity: string): SeverityLevel {
  const s = String(severity || '').toUpperCase();
  if (s === 'HIGH') return 'HIGH';
  if (s === 'MEDIUM') return 'MEDIUM';
  return 'LOW';
}

const SEVERITY_CONFIG: Record<SeverityLevel, {
  bg: string; text: string; dot: string; border: string; icon: typeof ShieldAlert;
}> = {
  HIGH: {
    bg: 'bg-red-50', text: 'text-red-700', dot: 'bg-red-500',
    border: 'border-red-200', icon: ShieldAlert,
  },
  MEDIUM: {
    bg: 'bg-amber-50', text: 'text-amber-700', dot: 'bg-amber-500',
    border: 'border-amber-200', icon: Shield,
  },
  LOW: {
    bg: 'bg-slate-50', text: 'text-slate-600', dot: 'bg-slate-400',
    border: 'border-slate-200', icon: ShieldCheck,
  },
};

function useDebounce<T>(value: T, delay: number): T {
  const [debounced, setDebounced] = useState(value);
  useEffect(() => {
    const id = setTimeout(() => setDebounced(value), delay);
    return () => clearTimeout(id);
  }, [value, delay]);
  return debounced;
}

/* ─── component ─── */

export function FailuresTab({
  database,
  schema,
  table,
  executionTimeSeconds,
  dqScore,
  onOpenDataView,
}: FailuresTabProps) {
  /* summary state */
  const [summary, setSummary] = useState<FailureSummary | null>(null);
  const [rules, setRules] = useState<FailureRuleGroup[]>([]);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [dataSource, setDataSource] = useState<'failed_records' | 'check_results' | null>(null);

  /* filter state */
  const [isFilterOpen, setIsFilterOpen] = useState(false);
  const [ruleTypeFilter, setRuleTypeFilter] = useState<string>('ALL');
  const [severityFilter, setSeverityFilter] = useState<string>('ALL');
  const [columnFilter, setColumnFilter] = useState<string>('');
  const [recordIdInput, setRecordIdInput] = useState<string>('');
  const debouncedRecordId = useDebounce(recordIdInput, 500);

  /* expansion state */
  const [expandedRules, setExpandedRules] = useState<Record<string, boolean>>({});
  const [ruleRecords, setRuleRecords] = useState<Record<string, FailureRecordsResponse>>({});
  const [ruleLoading, setRuleLoading] = useState<Record<string, boolean>>({});
  const [ruleErrors, setRuleErrors] = useState<Record<string, string | null>>({});

  /* record detail state */
  const [selectedRecord, setSelectedRecord] = useState<FailureRecordItem | null>(null);
  const [recordDetail, setRecordDetail] = useState<Record<string, unknown> | null>(null);
  const [recordDetailLoading, setRecordDetailLoading] = useState(false);
  const [recordDetailError, setRecordDetailError] = useState<string | null>(null);

  /* animation ref for shimmer */
  const summaryRef = useRef<HTMLDivElement>(null);

  const hasActiveFilters = ruleTypeFilter !== 'ALL' || severityFilter !== 'ALL' ||
    columnFilter.trim() !== '' || debouncedRecordId.trim() !== '';

  /* compose query params */
  const filterParams = useMemo(() => {
    const params = new URLSearchParams({ database, schema, table, window_hours: '24' });
    if (ruleTypeFilter !== 'ALL') params.set('rule_type', ruleTypeFilter);
    if (severityFilter !== 'ALL') params.set('severity', severityFilter);
    if (columnFilter.trim()) params.set('column', columnFilter.trim());
    if (debouncedRecordId.trim()) params.set('record_id', debouncedRecordId.trim());
    return params;
  }, [database, schema, table, ruleTypeFilter, severityFilter, columnFilter, debouncedRecordId]);

  /* fetch summary + rules */
  const fetchSummary = useCallback(async () => {
    try {
      setIsLoading(true);
      setError(null);
      const response = await fetch(`/api/dq/failures/summary?${filterParams.toString()}`);
      const payload = await response.json();
      if (!response.ok || !payload.success) {
        throw new Error(payload.error || 'Failed to fetch failure summary');
      }
      setSummary(payload.data.summary as FailureSummary);
      setRules((payload.data.rules || []) as FailureRuleGroup[]);
      setDataSource(payload.data.source || 'failed_records');
    } catch (err: unknown) {
      const message = err instanceof Error ? err.message : 'Failed to fetch failure summary';
      setError(message);
      setSummary(null);
      setRules([]);
    } finally {
      setIsLoading(false);
    }
  }, [filterParams]);

  useEffect(() => { fetchSummary(); }, [fetchSummary]);

  /* fetch paginated records for a rule */
  const loadRuleRecords = useCallback(
    async (ruleId: number, page = 1) => {
      const key = String(ruleId);
      try {
        setRuleLoading((prev) => ({ ...prev, [key]: true }));
        setRuleErrors((prev) => ({ ...prev, [key]: null }));

        const params = new URLSearchParams({
          database, schema, table,
          window_hours: '24',
          page: String(page),
          page_size: '50',
        });
        if (debouncedRecordId.trim()) params.set('record_id', debouncedRecordId.trim());

        const response = await fetch(`/api/dq/failures/rules/${ruleId}/records?${params.toString()}`);
        const payload = await response.json();
        if (!response.ok || !payload.success) {
          throw new Error(payload.error || 'Failed to fetch rule records');
        }
        setRuleRecords((prev) => ({ ...prev, [key]: payload.data as FailureRecordsResponse }));
      } catch (err: unknown) {
        const message = err instanceof Error ? err.message : 'Failed to fetch rule records';
        setRuleErrors((prev) => ({ ...prev, [key]: message }));
      } finally {
        setRuleLoading((prev) => ({ ...prev, [key]: false }));
      }
    },
    [database, schema, table, debouncedRecordId]
  );

  /* toggle rule expansion */
  const toggleRule = useCallback(
    (rule: FailureRuleGroup) => {
      if (!rule.rule_id) return;
      const key = String(rule.rule_id);
      setExpandedRules((prev) => {
        const next = !prev[key];
        if (next && !ruleRecords[key]) {
          loadRuleRecords(rule.rule_id!, 1);
        }
        return { ...prev, [key]: next };
      });
    },
    [loadRuleRecords, ruleRecords]
  );

  /* open record detail dialog */
  const openRecordDrawer = useCallback(
    async (record: FailureRecordItem) => {
      setSelectedRecord(record);
      setRecordDetail(null);
      setRecordDetailError(null);
      setRecordDetailLoading(true);

      try {
        const params = new URLSearchParams({
          database, schema, table,
          pk_column: record.primary_key_column,
          pk_value: record.failed_record_pk,
        });
        const response = await fetch(`/api/dq/failures/record-detail?${params.toString()}`);
        const payload = await response.json();
        if (!response.ok || !payload.success) {
          throw new Error(payload.error || 'Failed to fetch record detail');
        }
        setRecordDetail(payload.data.record || null);
      } catch (err: unknown) {
        const msg = err instanceof Error ? err.message : 'Failed to fetch record detail';
        setRecordDetailError(msg);
      } finally {
        setRecordDetailLoading(false);
      }
    },
    [database, schema, table]
  );

  /* clear all filters */
  const clearAllFilters = useCallback(() => {
    setRuleTypeFilter('ALL');
    setSeverityFilter('ALL');
    setColumnFilter('');
    setRecordIdInput('');
  }, []);

  /* unique rule types from loaded data */
  const uniqueRuleTypes = useMemo(() => {
    const set = new Set<string>();
    rules.forEach((r) => { if (r.rule_type) set.add(r.rule_type); });
    return Array.from(set).sort();
  }, [rules]);

  const failedRecordCount = summary?.failed_records ?? 0;
  const hasFailures = failedRecordCount > 0;

  /* ─── RENDER ─── */
  return (
    <div className="space-y-3">
      {/* ═══════════════════════════════════════════════
          SUMMARY HEADER
         ═══════════════════════════════════════════════ */}
      <div ref={summaryRef}>
        {isLoading ? (
          /* shimmer skeleton */
          <div className="grid grid-cols-1 gap-2 md:grid-cols-4">
            {[1, 2, 3, 4].map((i) => (
              <div key={i} className="h-16 animate-pulse rounded-lg bg-slate-100" />
            ))}
          </div>
        ) : error ? (
          /* error state */
          <Card className="border-red-200 bg-red-50/50 p-3">
            <div className="flex items-center justify-between gap-2">
              <div className="flex items-center gap-2 text-sm text-red-700">
                <XCircle className="h-4 w-4 shrink-0" />
                <span>{error}</span>
              </div>
              <Button
                variant="outline"
                className="h-8 shrink-0 border-red-200 text-xs text-red-700 hover:bg-red-100"
                onClick={fetchSummary}
              >
                <RefreshCw className="mr-1.5 h-3.5 w-3.5" /> Retry
              </Button>
            </div>
          </Card>
        ) : !hasFailures ? (
          /* ── ALL-PASS EMPTY STATE ── */
          <Card className="overflow-hidden border-emerald-200">
            <div
              className="px-4 py-5"
              style={{
                background:
                  'linear-gradient(135deg, rgba(16,185,129,0.08) 0%, rgba(52,211,153,0.04) 100%)',
              }}
            >
              <div className="flex items-center gap-3">
                <div className="flex h-10 w-10 items-center justify-center rounded-full bg-emerald-100">
                  <CheckCircle2 className="h-5 w-5 text-emerald-600" />
                </div>
                <div>
                  <div className="text-sm font-semibold text-emerald-800">
                    All checks passed for this execution window
                  </div>
                  <div className="mt-0.5 flex items-center gap-3 text-xs text-emerald-600/80">
                    {executionTimeSeconds != null && (
                      <span className="flex items-center gap-1">
                        <Clock className="h-3 w-3" />
                        {executionTimeSeconds.toFixed(1)}s
                      </span>
                    )}
                    {dqScore != null && (
                      <span className="flex items-center gap-1">
                        <ShieldCheck className="h-3 w-3" />
                        DQ Score: {dqScore}%
                      </span>
                    )}
                  </div>
                </div>
              </div>
            </div>
          </Card>
        ) : (
          /* ── FAILURE SUMMARY CARDS ── */
          <div className="grid grid-cols-1 gap-2 md:grid-cols-4">
            {/* Failed Checks */}
            <div
              className="rounded-lg border border-red-200 px-3 py-2.5"
              style={{
                background:
                  'linear-gradient(135deg, rgba(239,68,68,0.06) 0%, rgba(252,165,165,0.03) 100%)',
              }}
            >
              <div className="text-[11px] font-medium uppercase tracking-wide text-red-400">
                Failed Checks
              </div>
              <div className="mt-0.5 text-xl font-bold text-red-700">
                {summary?.failed_checks ?? 0}
              </div>
            </div>

            {/* Failed Records */}
            <div
              className="rounded-lg border border-rose-200 px-3 py-2.5"
              style={{
                background:
                  'linear-gradient(135deg, rgba(244,63,94,0.06) 0%, rgba(251,113,133,0.03) 100%)',
              }}
            >
              <div className="text-[11px] font-medium uppercase tracking-wide text-rose-400">
                Failed Records
              </div>
              <div className="mt-0.5 flex items-baseline gap-1.5">
                <span className="text-xl font-bold text-rose-700">
                  {failedRecordCount.toLocaleString()}
                </span>
                {failedRecordCount > 100 && (
                  <span className="text-[10px] text-rose-400">(sampled)</span>
                )}
              </div>
            </div>

            {/* Most Critical Rule */}
            <div className="rounded-lg border border-slate-200 bg-white px-3 py-2.5">
              <div className="text-[11px] font-medium uppercase tracking-wide text-slate-400">
                Most Critical Rule
              </div>
              <div className="mt-0.5 truncate text-sm font-semibold text-slate-800">
                {summary?.most_critical_rule || '—'}
              </div>
            </div>

            {/* Last Failure */}
            <div className="rounded-lg border border-slate-200 bg-white px-3 py-2.5">
              <div className="text-[11px] font-medium uppercase tracking-wide text-slate-400">
                Last Failure
              </div>
              <div className="mt-0.5 text-sm font-semibold text-slate-800">
                {formatTs(summary?.last_failure_ts || null)}
              </div>
              {summary?.last_failure_ts && (
                <div className="text-[10px] text-slate-400">
                  {relativeTime(summary.last_failure_ts)}
                </div>
              )}
            </div>
          </div>
        )}
      </div>

      {/* ═══════════════════════════════════════════════
          CHECK_RESULTS FALLBACK BANNER
         ═══════════════════════════════════════════════ */}
      {dataSource === 'check_results' && hasFailures && (
        <div className="flex items-start gap-2 rounded-lg border border-blue-200 bg-blue-50/60 px-3 py-2 text-xs text-blue-700">
          <Info className="mt-0.5 h-3.5 w-3.5 shrink-0" />
          <div>
            <span className="font-semibold">Rule-level summary only.</span> Row-level failed records
            will be available after the next scan run. Click <strong>Scan Now</strong> to capture detailed failure data.
          </div>
        </div>
      )}

      {/* ═══════════════════════════════════════════════
          FILTERS + RULE CARDS
         ═══════════════════════════════════════════════ */}
      <Card className="overflow-hidden border border-slate-200">
        {/* toolbar */}
        <div className="flex items-center justify-between border-b border-slate-100 px-3 py-1.5">
          <button
            type="button"
            onClick={() => setIsFilterOpen((v) => !v)}
            className="inline-flex items-center gap-1.5 rounded-md px-2 py-1 text-xs font-medium text-slate-600 transition-colors hover:bg-slate-100"
          >
            <Filter className="h-3.5 w-3.5" />
            Filters
            {hasActiveFilters && (
              <span className="flex h-4 w-4 items-center justify-center rounded-full bg-indigo-600 text-[9px] font-bold text-white">
                !
              </span>
            )}
            {isFilterOpen ? (
              <ChevronUp className="h-3 w-3 text-slate-400" />
            ) : (
              <ChevronDown className="h-3 w-3 text-slate-400" />
            )}
          </button>

          <div className="flex items-center gap-1.5">
            {hasActiveFilters && (
              <button
                type="button"
                onClick={clearAllFilters}
                className="inline-flex items-center gap-1 rounded-md px-2 py-1 text-[11px] text-slate-500 transition-colors hover:bg-slate-100 hover:text-slate-700"
              >
                <X className="h-3 w-3" /> Clear All
              </button>
            )}
            <Button
              variant="ghost"
              className="h-7 text-xs text-slate-500"
              onClick={fetchSummary}
            >
              <RefreshCw className="mr-1 h-3 w-3" /> Refresh
            </Button>
          </div>
        </div>

        {/* filter panel */}
        {isFilterOpen && (
          <div className="grid grid-cols-1 gap-2 border-b border-slate-100 bg-slate-50/50 p-3 md:grid-cols-4">
            <Select value={ruleTypeFilter} onValueChange={setRuleTypeFilter}>
              <SelectTrigger className="h-8 border-slate-200 bg-white text-xs">
                <SelectValue placeholder="Rule Type" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="ALL">All Rule Types</SelectItem>
                {uniqueRuleTypes.map((type) => (
                  <SelectItem key={type} value={type}>{type}</SelectItem>
                ))}
              </SelectContent>
            </Select>

            <Select value={severityFilter} onValueChange={setSeverityFilter}>
              <SelectTrigger className="h-8 border-slate-200 bg-white text-xs">
                <SelectValue placeholder="Severity" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="ALL">All Severity</SelectItem>
                <SelectItem value="HIGH">
                  <span className="flex items-center gap-1.5">
                    <span className="h-1.5 w-1.5 rounded-full bg-red-500" /> HIGH
                  </span>
                </SelectItem>
                <SelectItem value="MEDIUM">
                  <span className="flex items-center gap-1.5">
                    <span className="h-1.5 w-1.5 rounded-full bg-amber-500" /> MEDIUM
                  </span>
                </SelectItem>
                <SelectItem value="LOW">
                  <span className="flex items-center gap-1.5">
                    <span className="h-1.5 w-1.5 rounded-full bg-slate-400" /> LOW
                  </span>
                </SelectItem>
              </SelectContent>
            </Select>

            <input
              value={columnFilter}
              onChange={(e) => setColumnFilter(e.target.value)}
              placeholder="Filter by column"
              className="h-8 rounded-md border border-slate-200 bg-white px-2.5 text-xs outline-none transition-colors focus:border-indigo-300 focus:ring-1 focus:ring-indigo-200"
            />

            <div className="relative">
              <Search className="absolute left-2 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-slate-400" />
              <input
                value={recordIdInput}
                onChange={(e) => setRecordIdInput(e.target.value)}
                placeholder="Search Record ID…"
                className="h-8 w-full rounded-md border border-slate-200 bg-white pl-7 pr-2 text-xs outline-none transition-colors focus:border-indigo-300 focus:ring-1 focus:ring-indigo-200"
              />
            </div>
          </div>
        )}

        {/* ── rule cards ── */}
        <div className="space-y-0 divide-y divide-slate-100">
          {rules.map((rule) => {
            const ruleKey = String(
              rule.rule_id || `${rule.rule_name || 'RULE'}-${rule.column_name || 'TABLE'}`
            );
            const isExpanded = expandedRules[ruleKey];
            const recordsPayload = ruleRecords[ruleKey];
            const records = recordsPayload?.records || [];
            const pagination = recordsPayload?.pagination;
            const sev = normalizeSeverity(rule.severity);
            const sevCfg = SEVERITY_CONFIG[sev];
            const SevIcon = sevCfg.icon;
            const passRate = rule.pass_rate != null ? Number(rule.pass_rate) : null;

            return (
              <div key={ruleKey} className="bg-white">
                {/* card header */}
                <button
                  type="button"
                  className={`flex w-full items-center gap-3 px-3 py-2.5 text-left transition-colors ${dataSource === 'failed_records'
                    ? 'hover:bg-slate-50/80 cursor-pointer'
                    : 'cursor-default'
                    }`}
                  onClick={() => dataSource === 'failed_records' && toggleRule(rule)}
                >
                  {/* severity icon */}
                  <div
                    className={`flex h-8 w-8 shrink-0 items-center justify-center rounded-lg ${sevCfg.bg}`}
                  >
                    <SevIcon className={`h-4 w-4 ${sevCfg.text}`} />
                  </div>

                  {/* info block */}
                  <div className="min-w-0 flex-1">
                    <div className="flex items-center gap-2">
                      <span className="truncate text-sm font-semibold text-slate-900">
                        {rule.rule_name || 'UNKNOWN'}
                      </span>
                      {rule.column_name && (
                        <span className="truncate text-xs text-slate-400">
                          → {rule.column_name}
                        </span>
                      )}
                    </div>

                    <div className="mt-1 flex flex-wrap items-center gap-x-3 gap-y-1 text-[11px] text-slate-500">
                      {/* pass rate bar */}
                      {passRate != null && (
                        <span className="flex items-center gap-1.5">
                          <span className="text-slate-400">Pass:</span>
                          <span className="relative inline-block h-1.5 w-16 overflow-hidden rounded-full bg-slate-200">
                            <span
                              className="absolute left-0 top-0 h-full rounded-full transition-all duration-500"
                              style={{
                                width: `${Math.min(100, Math.max(0, passRate))}%`,
                                background:
                                  passRate >= 90
                                    ? '#10b981'
                                    : passRate >= 70
                                      ? '#f59e0b'
                                      : '#ef4444',
                              }}
                            />
                          </span>
                          <span className="font-medium text-slate-600">
                            {passRate.toFixed(1)}%
                          </span>
                        </span>
                      )}

                      <span>
                        <span className="text-slate-400">Failed:</span>{' '}
                        <span className="font-semibold text-red-600">{rule.failed_records}</span>
                      </span>

                      {rule.threshold != null && (
                        <span>
                          <span className="text-slate-400">Threshold:</span> {rule.threshold}%
                        </span>
                      )}

                      {rule.rule_type && (
                        <span className="rounded bg-slate-100 px-1.5 py-0.5 text-[10px] font-medium text-slate-500">
                          {rule.rule_type}
                        </span>
                      )}

                      {/* severity badge */}
                      <span
                        className={`inline-flex items-center gap-1 rounded px-1.5 py-0.5 text-[10px] font-bold ${sevCfg.bg} ${sevCfg.text}`}
                      >
                        <span className={`h-1.5 w-1.5 rounded-full ${sevCfg.dot}`} />
                        {sev}
                      </span>
                    </div>
                  </div>

                  {/* chevron */}
                  {dataSource === 'failed_records' && (
                    <div className="shrink-0 text-slate-400">
                      {isExpanded ? (
                        <ChevronUp className="h-4 w-4" />
                      ) : (
                        <ChevronDown className="h-4 w-4" />
                      )}
                    </div>
                  )}
                </button>

                {/* ── expanded: records grid ── */}
                {isExpanded && (
                  <div className="border-t border-slate-100 bg-slate-50/40 px-3 py-2">
                    {ruleLoading[ruleKey] ? (
                      <div className="space-y-1.5 py-3">
                        {[1, 2, 3].map((i) => (
                          <div key={i} className="h-6 animate-pulse rounded bg-slate-100" />
                        ))}
                      </div>
                    ) : ruleErrors[ruleKey] ? (
                      <div className="flex items-center gap-2 py-3 text-sm text-red-600">
                        <AlertTriangle className="h-4 w-4 shrink-0" />
                        {ruleErrors[ruleKey]}
                      </div>
                    ) : records.length === 0 ? (
                      <div className="py-3 text-center text-xs text-slate-400">
                        No failed records found for this rule.
                      </div>
                    ) : (
                      <>
                        <div className="overflow-auto rounded-md border border-slate-200 bg-white shadow-sm">
                          <table className="w-full text-xs">
                            <thead>
                              <tr className="bg-slate-100/80">
                                <th className="sticky top-0 bg-slate-100/80 px-2.5 py-1.5 text-left text-[11px] font-semibold uppercase tracking-wide text-slate-500">
                                  Record ID
                                </th>
                                <th className="sticky top-0 bg-slate-100/80 px-2.5 py-1.5 text-left text-[11px] font-semibold uppercase tracking-wide text-slate-500">
                                  Column
                                </th>
                                <th className="sticky top-0 bg-slate-100/80 px-2.5 py-1.5 text-left text-[11px] font-semibold uppercase tracking-wide text-slate-500">
                                  Failed Value
                                </th>
                                <th className="sticky top-0 bg-slate-100/80 px-2.5 py-1.5 text-left text-[11px] font-semibold uppercase tracking-wide text-slate-500">
                                  Failure Reason
                                </th>
                                <th className="sticky top-0 bg-slate-100/80 px-2.5 py-1.5 text-left text-[11px] font-semibold uppercase tracking-wide text-slate-500">
                                  Failed At
                                </th>
                              </tr>
                            </thead>
                            <tbody>
                              {records.map((record, idx) => {
                                const isBlank =
                                  !record.failed_column_value ||
                                  record.failed_column_value === 'NULL' ||
                                  record.failed_column_value.trim() === '';
                                return (
                                  <tr
                                    key={record.failure_id}
                                    className={`border-t border-slate-50 transition-colors hover:bg-indigo-50/40 ${idx % 2 === 1 ? 'bg-slate-50/50' : ''
                                      }`}
                                  >
                                    <td className="px-2.5 py-1.5">
                                      <button
                                        type="button"
                                        onClick={() => openRecordDrawer(record)}
                                        className="font-mono text-xs font-medium text-indigo-600 transition-colors hover:text-indigo-800 hover:underline"
                                      >
                                        {record.failed_record_pk || '—'}
                                      </button>
                                    </td>
                                    <td className="px-2.5 py-1.5 text-slate-700">
                                      {record.column_name || '—'}
                                    </td>
                                    <td className="px-2.5 py-1.5">
                                      {isBlank ? (
                                        <span className="italic text-rose-500">
                                          {record.failed_column_value === 'NULL' ? 'NULL' : '(empty)'}
                                        </span>
                                      ) : (
                                        <span className="font-mono text-slate-700">
                                          {record.failed_column_value}
                                        </span>
                                      )}
                                    </td>
                                    <td className="max-w-[200px] truncate px-2.5 py-1.5 text-slate-600">
                                      {record.failure_reason || '—'}
                                    </td>
                                    <td className="whitespace-nowrap px-2.5 py-1.5 text-slate-500">
                                      {formatTs(record.detected_ts)}
                                    </td>
                                  </tr>
                                );
                              })}
                            </tbody>
                          </table>
                        </div>

                        {/* pagination */}
                        {pagination && pagination.total_pages > 1 && (
                          <div className="mt-2 flex items-center justify-between text-[11px]">
                            <span className="text-slate-400">
                              Page {pagination.page} of {pagination.total_pages}
                              <span className="mx-1.5">·</span>
                              {pagination.total_rows.toLocaleString()} records
                            </span>
                            <div className="flex items-center gap-1">
                              <Button
                                variant="outline"
                                size="sm"
                                className="h-6 w-6 p-0"
                                disabled={pagination.page <= 1}
                                onClick={() => loadRuleRecords(rule.rule_id!, pagination.page - 1)}
                              >
                                <ChevronLeft className="h-3 w-3" />
                              </Button>
                              <Button
                                variant="outline"
                                size="sm"
                                className="h-6 w-6 p-0"
                                disabled={pagination.page >= pagination.total_pages}
                                onClick={() => loadRuleRecords(rule.rule_id!, pagination.page + 1)}
                              >
                                <ChevronRight className="h-3 w-3" />
                              </Button>
                            </div>
                          </div>
                        )}
                      </>
                    )}
                  </div>
                )}
              </div>
            );
          })}

          {/* no rules placeholder */}
          {!isLoading && !error && rules.length === 0 && hasFailures && (
            <div className="px-3 py-4 text-center text-xs text-slate-400">
              No failed rule groups match current filters.
            </div>
          )}
        </div>
      </Card>

      {/* ═══════════════════════════════════════════════
          RECORD DETAIL DIALOG (side-sheet style)
         ═══════════════════════════════════════════════ */}
      <Dialog
        open={Boolean(selectedRecord)}
        onOpenChange={(open) => !open && setSelectedRecord(null)}
      >
        <DialogContent className="fixed right-0 top-0 h-full max-h-full w-full max-w-lg translate-x-0 rounded-l-xl rounded-r-none border-l border-slate-200 bg-white shadow-2xl sm:max-w-lg data-[state=open]:slide-in-from-right data-[state=closed]:slide-out-to-right">
          <DialogHeader className="border-b border-slate-100 pb-3">
            <DialogTitle className="flex items-center gap-2 text-sm font-semibold text-slate-800">
              <ExternalLink className="h-4 w-4 text-indigo-500" />
              Record Detail
            </DialogTitle>
          </DialogHeader>

          {!selectedRecord ? null : recordDetailLoading ? (
            <div className="space-y-2 py-6">
              {[1, 2, 3, 4, 5].map((i) => (
                <div key={i} className="h-8 animate-pulse rounded bg-slate-100" />
              ))}
            </div>
          ) : recordDetailError ? (
            <div className="flex items-center gap-2 py-4 text-sm text-red-600">
              <XCircle className="h-4 w-4 shrink-0" />
              {recordDetailError}
            </div>
          ) : (
            <div className="space-y-4">
              {/* record meta */}
              <div className="rounded-lg bg-slate-50 px-3 py-2">
                <div className="flex items-center gap-4 text-xs">
                  <span className="text-slate-400">Record ID</span>
                  <span className="font-mono font-semibold text-slate-800">
                    {selectedRecord.failed_record_pk || '—'}
                  </span>
                </div>
                <div className="mt-1 flex items-center gap-4 text-xs">
                  <span className="text-slate-400">PK Column</span>
                  <span className="font-medium text-slate-700">
                    {selectedRecord.primary_key_column || '—'}
                  </span>
                </div>
                <div className="mt-1 flex items-center gap-4 text-xs">
                  <span className="text-slate-400">Failure</span>
                  <span className="text-red-600">
                    {selectedRecord.failure_reason || '—'}
                  </span>
                </div>
              </div>

              {/* key-value table */}
              <div className="max-h-[55vh] overflow-auto rounded-lg border border-slate-200">
                <table className="w-full text-xs">
                  <tbody>
                    {Object.entries(recordDetail || {}).map(([key, value]) => {
                      const isFailingCol =
                        key.toUpperCase() === selectedRecord.column_name?.toUpperCase();

                      return (
                        <tr
                          key={key}
                          className={`border-t border-slate-50 ${isFailingCol
                            ? 'bg-red-50/60'
                            : ''
                            }`}
                        >
                          <td
                            className={`w-2/5 px-2.5 py-1.5 font-semibold ${isFailingCol
                              ? 'text-red-700'
                              : 'bg-slate-50/80 text-slate-600'
                              }`}
                          >
                            {key}
                            {isFailingCol && (
                              <span className="ml-1.5 rounded bg-red-100 px-1 py-0.5 text-[9px] font-bold text-red-600">
                                FAILED
                              </span>
                            )}
                          </td>
                          <td
                            className={`px-2.5 py-1.5 ${isFailingCol ? 'font-semibold text-red-700' : 'text-slate-700'
                              }`}
                          >
                            {value === null || value === undefined ? (
                              <span className="italic text-slate-300">NULL</span>
                            ) : typeof value === 'object' ? (
                              <code className="text-[10px]">{JSON.stringify(value)}</code>
                            ) : (
                              String(value)
                            )}
                          </td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>

              {/* actions */}
              <div className="flex items-center justify-end gap-2 border-t border-slate-100 pt-3">
                <Button
                  variant="outline"
                  className="h-8 text-xs"
                  onClick={() => {
                    setSelectedRecord(null);
                    onOpenDataView?.();
                  }}
                >
                  <ExternalLink className="mr-1.5 h-3.5 w-3.5" />
                  Open in Data View
                </Button>
              </div>
            </div>
          )}
        </DialogContent>
      </Dialog>
    </div>
  );
}
