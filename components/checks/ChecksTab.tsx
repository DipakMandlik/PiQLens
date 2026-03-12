"use client";

import { useState, useEffect, useCallback } from "react";
import {
    CheckCircle2, AlertOctagon, AlertTriangle, RefreshCw,
    TrendingUp, TrendingDown, Minus, Clock, Hash,
    ArrowUpRight, ArrowDownRight, Shield, Activity,
    Filter, X, History,
    type LucideIcon,
} from "lucide-react";
import { Card } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { CheckHistoryDrawer } from "./CheckHistoryDrawer";

// ─── Types ─────────────────────────────────────────────────────────────
interface InventoryData {
    totalActive: number;
    columnLevel: number;
    tableLevel: number;
    critical: number;
    byType: Record<string, number>;
}

interface ExecutionData {
    executed: number;
    passed: number;
    failed: number;
    warning: number;
    skipped: number;
    error: number;
    other: number;
    lastRunTime: string | null;
}

interface ComparisonData {
    yesterdayExecuted: number;
    yesterdayFailed: number;
    trend: string;
}

interface CheckRow {
    checkId: string;
    runId: string;
    checkTimestamp: string | null;
    columnName: string | null;
    ruleName: string;
    ruleType: string;
    ruleLevel: string;
    totalRecords: number;
    validRecords: number;
    invalidRecords: number;
    passRate: number | null;
    threshold: number;
    checkStatus: string;
    executionTimeMs: number;
    failureReason: string | null;
    scope: string;
    target: string;
    totalRuns: number;
    failureCount: number;
    passCount: number;
    trend: "improved" | "degraded" | "stable" | "new";
    lastRunTimestamp: string | null;
}

interface Props {
    database: string;
    schema: string;
    table: string;
}

// ─── Status Utilities ─────────────────────────────────────────────────
const STATUS_CONFIG: Record<string, { bg: string; text: string; icon: LucideIcon; label: string }> = {
    PASSED: { bg: "bg-emerald-50", text: "text-emerald-700", icon: CheckCircle2, label: "Passed" },
    FAILED: { bg: "bg-rose-50", text: "text-rose-700", icon: AlertOctagon, label: "Failed" },
    WARNING: { bg: "bg-amber-50", text: "text-amber-700", icon: AlertTriangle, label: "Warning" },
    SKIPPED: { bg: "bg-blue-50", text: "text-blue-700", icon: Minus, label: "Skipped" },
    ERROR: { bg: "bg-purple-50", text: "text-purple-700", icon: AlertOctagon, label: "Error" },
    OTHER: { bg: "bg-slate-50", text: "text-slate-600", icon: Hash, label: "Other" },
};

function StatusBadge({ status }: { status: string }) {
    const cfg = STATUS_CONFIG[status] || STATUS_CONFIG.OTHER;
    const Icon = cfg.icon;
    return (
        <span className={`inline-flex items-center gap-1 px-2.5 py-1 rounded-full text-xs font-bold ${cfg.bg} ${cfg.text}`}>
            <Icon className="h-3 w-3" />
            {cfg.label}
        </span>
    );
}

function TrendIndicator({ trend }: { trend: string }) {
    if (trend === "improved") return <span className="text-emerald-500 flex items-center gap-0.5 text-xs font-medium"><TrendingUp className="h-3.5 w-3.5" />Improved</span>;
    if (trend === "degraded") return <span className="text-rose-500 flex items-center gap-0.5 text-xs font-medium"><TrendingDown className="h-3.5 w-3.5" />Degraded</span>;
    if (trend === "new") return <span className="text-blue-400 text-xs font-medium">New</span>;
    return <span className="text-slate-400 flex items-center gap-0.5 text-xs font-medium"><Minus className="h-3.5 w-3.5" />Stable</span>;
}

function formatTimestamp(ts: string | null): string {
    if (!ts) return "—";
    try {
        return new Date(ts).toLocaleString("en-US", { dateStyle: "medium", timeStyle: "short" });
    } catch {
        return "—";
    }
}

// ─── Main Component ───────────────────────────────────────────────────
export function ChecksTab({ database, schema, table }: Props) {
    const [mode, setMode] = useState<"today" | "all">("all");
    const [statusFilter, setStatusFilter] = useState<string | null>(null);
    const [isLoading, setIsLoading] = useState(false);
    const [error, setError] = useState<string | null>(null);

    // Summary data
    const [inventory, setInventory] = useState<InventoryData | null>(null);
    const [execution, setExecution] = useState<ExecutionData | null>(null);
    const [comparison, setComparison] = useState<ComparisonData | null>(null);

    // Checks list
    const [checks, setChecks] = useState<CheckRow[]>([]);

    // Drill-down
    const [selectedCheck, setSelectedCheck] = useState<CheckRow | null>(null);
    const [historyOpen, setHistoryOpen] = useState(false);

    // ─── Fetch ─────────────────────────────────────────────────────────
    const fetchData = useCallback(async () => {
        setIsLoading(true);
        setError(null);
        try {
            const executionParams = `database=${encodeURIComponent(database)}&schema=${encodeURIComponent(schema)}&table=${encodeURIComponent(table)}&mode=${mode}`;
            const inventoryParams = `database=${encodeURIComponent(database)}&schema=${encodeURIComponent(schema)}&table=${encodeURIComponent(table)}`;

            const [inventoryRes, summaryRes, checksRes] = await Promise.all([
                fetch(`/api/dq/checks/inventory?${inventoryParams}`).then(r => r.json()),
                fetch(`/api/dq/checks/summary?${executionParams}`).then(r => r.json()),
                fetch(`/api/dq/table-checks?${executionParams}`).then(r => r.json()),
            ]);

            if (inventoryRes.success) {
                const invData = inventoryRes.data || {};
                setInventory({
                    totalActive: Number(invData.total_active ?? 0),
                    columnLevel: Number(invData.column_level ?? 0),
                    tableLevel: Number(invData.table_level ?? 0),
                    critical: Number(invData.critical ?? 0),
                    byType: invData.by_type ?? {},
                });
            } else {
                setInventory(null);
            }

            if (summaryRes.success) {
                setExecution(summaryRes.data.execution);
                setComparison(summaryRes.data.comparison);
            }
            if (checksRes.success) {
                setChecks(checksRes.data.checks || []);
            } else {
                setError(checksRes.error || "Failed to load checks");
            }
        } catch (err: unknown) {
            console.error("ChecksTab fetch error:", err);
            setError("Failed to fetch checks data");
        } finally {
            setIsLoading(false);
        }
    }, [database, schema, table, mode]);

    useEffect(() => {
        fetchData();
    }, [fetchData]);

    // Filtered checks
    const filteredChecks = statusFilter
        ? checks.filter(c => c.checkStatus === statusFilter)
        : checks;

    const handleCheckClick = (check: CheckRow) => {
        setSelectedCheck(check);
        setHistoryOpen(true);
    };

    // ─── Render ────────────────────────────────────────────────────────
    if (isLoading) {
        return (
            <Card className="p-6 bg-white shadow-sm border border-slate-200">
                <div className="flex items-center justify-center h-64">
                    <div className="text-center">
                        <div className="animate-spin rounded-full h-8 w-8 border-b-2 border-indigo-500 mx-auto mb-4" />
                        <p className="text-sm text-slate-500">Loading quality checks...</p>
                    </div>
                </div>
            </Card>
        );
    }

    if (error) {
        return (
            <Card className="p-6 bg-white shadow-sm border border-red-200">
                <div className="flex items-center justify-center h-32 text-red-500">
                    <div className="text-center">
                        <AlertTriangle className="h-8 w-8 mx-auto mb-2" />
                        <p className="text-sm">{error}</p>
                        <Button variant="outline" className="mt-4" onClick={fetchData}>
                            <RefreshCw className="h-4 w-4 mr-2" /> Retry
                        </Button>
                    </div>
                </div>
            </Card>
        );
    }

    const exec = execution || { executed: 0, passed: 0, failed: 0, warning: 0, skipped: 0, error: 0, other: 0, lastRunTime: null };
    const inv = inventory || { totalActive: 0, columnLevel: 0, tableLevel: 0, critical: 0, byType: {} };
    const comp = comparison || { yesterdayExecuted: 0, yesterdayFailed: 0, trend: "stable" };

    return (
        <div className="space-y-6">
            {/* Top Bar: Mode Toggle + Refresh */}
            <div className="flex items-center justify-between">
                <div className="flex items-center gap-2">
                    <div className="inline-flex items-center bg-slate-100 rounded-lg p-0.5">
                        <button
                            onClick={() => setMode("today")}
                            className={`px-4 py-1.5 rounded-md text-sm font-medium transition-all ${mode === "today"
                                ? "bg-white shadow-sm text-indigo-700"
                                : "text-slate-500 hover:text-slate-700"
                                }`}
                        >
                            Today
                        </button>
                        <button
                            onClick={() => setMode("all")}
                            className={`px-4 py-1.5 rounded-md text-sm font-medium transition-all ${mode === "all"
                                ? "bg-white shadow-sm text-indigo-700"
                                : "text-slate-500 hover:text-slate-700"
                                }`}
                        >
                            All Time
                        </button>
                    </div>
                    {statusFilter && (
                        <button
                            onClick={() => setStatusFilter(null)}
                            className="inline-flex items-center gap-1 px-3 py-1.5 rounded-full bg-indigo-50 text-indigo-700 text-xs font-medium hover:bg-indigo-100 transition-colors"
                        >
                            <Filter className="h-3 w-3" />
                            {statusFilter}
                            <X className="h-3 w-3 ml-1" />
                        </button>
                    )}
                </div>
                <Button variant="outline" size="sm" onClick={fetchData}>
                    <RefreshCw className="h-4 w-4 mr-2" /> Refresh
                </Button>
            </div>

            {/* ─── Dual-Layer Summary Header ──────────────────────────── */}
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
                {/* LEFT: Check Inventory (Static) */}
                <Card className="p-5 bg-gradient-to-br from-slate-50 to-white border border-slate-200 shadow-sm relative overflow-hidden">
                    <div className="absolute top-0 left-0 w-1 h-full bg-gradient-to-b from-indigo-400 to-indigo-600" />
                    <div className="flex items-center gap-2 mb-4">
                        <Shield className="h-4 w-4 text-indigo-500" />
                        <h3 className="text-xs font-bold text-slate-500 uppercase tracking-wider">Check Inventory</h3>
                        <span className="text-[10px] text-slate-400 ml-auto">Static Configuration</span>
                    </div>

                    <div className="grid grid-cols-4 gap-3">
                        <div>
                            <div className="text-2xl font-bold text-slate-800">{inv.totalActive}</div>
                            <div className="text-[11px] text-slate-500 mt-0.5">Active Checks</div>
                        </div>
                        <div>
                            <div className="text-2xl font-bold text-violet-600">{inv.columnLevel}</div>
                            <div className="text-[11px] text-slate-500 mt-0.5">Column-Level</div>
                        </div>
                        <div>
                            <div className="text-2xl font-bold text-blue-600">{inv.tableLevel}</div>
                            <div className="text-[11px] text-slate-500 mt-0.5">Table-Level</div>
                        </div>
                        <div>
                            <div className="text-2xl font-bold text-rose-600">{inv.critical}</div>
                            <div className="text-[11px] text-slate-500 mt-0.5">Critical</div>
                        </div>
                    </div>

                    {Object.keys(inv.byType).length > 0 && (
                        <div className="mt-3 pt-3 border-t border-slate-100 flex flex-wrap gap-1.5">
                            {Object.entries(inv.byType).map(([type, count]) => (
                                <span key={type} className="inline-flex items-center px-2 py-0.5 rounded bg-slate-100 text-[10px] font-medium text-slate-600 uppercase">
                                    {type}: {count}
                                </span>
                            ))}
                        </div>
                    )}
                </Card>

                {/* RIGHT: Execution Metrics (Dynamic) */}
                <Card className="p-5 bg-gradient-to-br from-slate-50 to-white border border-slate-200 shadow-sm relative overflow-hidden">
                    <div className="absolute top-0 left-0 w-1 h-full bg-gradient-to-b from-emerald-400 to-emerald-600" />
                    <div className="flex items-center gap-2 mb-4">
                        <Activity className="h-4 w-4 text-emerald-500" />
                        <h3 className="text-xs font-bold text-slate-500 uppercase tracking-wider">Execution Metrics</h3>
                        <span className="text-[10px] text-slate-400 ml-auto">{mode === "today" ? "Today" : "All Time"}</span>
                    </div>

                    <div className="grid grid-cols-2 gap-3">
                        {/* Executed */}
                        <div className="col-span-2 flex items-baseline gap-2">
                            <span className="text-3xl font-bold text-slate-800">{exec.executed}</span>
                            <span className="text-sm text-slate-500">Checks Executed</span>
                            {mode === "today" && comp.yesterdayExecuted > 0 && (
                                <span className="text-[10px] text-slate-400 ml-auto">
                                    vs {comp.yesterdayExecuted} yesterday
                                </span>
                            )}
                        </div>

                        {/* Status breakdown — clickable for filtering */}
                        <div className="col-span-2 grid grid-cols-3 sm:grid-cols-6 gap-2">
                            {([
                                ["PASSED", exec.passed, "bg-emerald-50 border-emerald-200 text-emerald-700 hover:bg-emerald-100"],
                                ["FAILED", exec.failed, "bg-rose-50 border-rose-200 text-rose-700 hover:bg-rose-100"],
                                ["WARNING", exec.warning, "bg-amber-50 border-amber-200 text-amber-700 hover:bg-amber-100"],
                                ["SKIPPED", exec.skipped, "bg-blue-50 border-blue-200 text-blue-700 hover:bg-blue-100"],
                                ["ERROR", exec.error, "bg-purple-50 border-purple-200 text-purple-700 hover:bg-purple-100"],
                                ["OTHER", exec.other, "bg-slate-50 border-slate-200 text-slate-600 hover:bg-slate-100"],
                            ] as [string, number, string][]).map(([label, count, className]) => (
                                <button
                                    key={label}
                                    onClick={() => setStatusFilter(statusFilter === label ? null : label)}
                                    className={`p-2 rounded-lg border text-center transition-all cursor-pointer ${className} ${statusFilter === label ? "ring-2 ring-indigo-400 ring-offset-1" : ""
                                        }`}
                                >
                                    <div className="text-lg font-bold">{count}</div>
                                    <div className="text-[10px] font-medium uppercase">{label}</div>
                                </button>
                            ))}
                        </div>

                        {/* Trend indicator */}
                        {mode === "today" && (
                            <div className="col-span-2 flex items-center justify-between pt-2 border-t border-slate-100">
                                <span className="text-[11px] text-slate-400">
                                    <Clock className="h-3 w-3 inline mr-1" />
                                    Last run: {formatTimestamp(exec.lastRunTime)}
                                </span>
                                {comp.trend === "better" && (
                                    <span className="text-[11px] text-emerald-600 flex items-center gap-1">
                                        <ArrowDownRight className="h-3 w-3" /> Fewer failures vs yesterday
                                    </span>
                                )}
                                {comp.trend === "worse" && (
                                    <span className="text-[11px] text-rose-600 flex items-center gap-1">
                                        <ArrowUpRight className="h-3 w-3" /> More failures vs yesterday
                                    </span>
                                )}
                                {comp.trend === "stable" && (
                                    <span className="text-[11px] text-slate-400 flex items-center gap-1">
                                        <Minus className="h-3 w-3" /> No change vs yesterday
                                    </span>
                                )}
                            </div>
                        )}
                        {mode === "all" && exec.lastRunTime && (
                            <div className="col-span-2 pt-2 border-t border-slate-100">
                                <span className="text-[11px] text-slate-400">
                                    <Clock className="h-3 w-3 inline mr-1" />
                                    Last run: {formatTimestamp(exec.lastRunTime)}
                                </span>
                            </div>
                        )}
                    </div>
                </Card>
            </div>

            {/* ─── Checks Table ──────────────────────────────────────── */}
            <Card className="overflow-hidden bg-white shadow-sm border border-slate-200">
                <div className="p-4 border-b border-slate-200 flex justify-between items-center">
                    <div>
                        <h3 className="text-lg font-semibold text-slate-800">Quality Checks</h3>
                        <p className="text-xs text-slate-500 mt-0.5">
                            {filteredChecks.length} check{filteredChecks.length !== 1 ? "s" : ""}
                            {statusFilter ? ` (filtered by ${statusFilter})` : ""}
                            {" · "}Click a row for execution history
                        </p>
                    </div>
                </div>

                {filteredChecks.length > 0 ? (
                    <div className="overflow-x-auto">
                        <table className="w-full text-sm">
                            <thead className="bg-slate-50">
                                <tr>
                                    <th className="text-left px-4 py-3 text-xs text-slate-600 font-semibold">Check Name</th>
                                    <th className="text-left px-4 py-3 text-xs text-slate-600 font-semibold">Scope</th>
                                    <th className="text-left px-4 py-3 text-xs text-slate-600 font-semibold">Target</th>
                                    <th className="text-left px-4 py-3 text-xs text-slate-600 font-semibold">Severity</th>
                                    <th className="text-left px-4 py-3 text-xs text-slate-600 font-semibold">Status</th>
                                    <th className="text-left px-4 py-3 text-xs text-slate-600 font-semibold">Pass Rate</th>
                                    <th className="text-left px-4 py-3 text-xs text-slate-600 font-semibold">Last Run</th>
                                    <th className="text-right px-4 py-3 text-xs text-slate-600 font-semibold">Runs</th>
                                    <th className="text-right px-4 py-3 text-xs text-slate-600 font-semibold">Failures</th>
                                    <th className="text-left px-4 py-3 text-xs text-slate-600 font-semibold">Trend</th>
                                </tr>
                            </thead>
                            <tbody>
                                {filteredChecks.map((check, idx) => (
                                    <tr
                                        key={check.checkId || idx}
                                        onClick={() => handleCheckClick(check)}
                                        className="border-t border-slate-100 hover:bg-indigo-50/50 cursor-pointer transition-colors group"
                                    >
                                        <td className="px-4 py-3 font-medium text-slate-800 group-hover:text-indigo-700 transition-colors">
                                            <div className="flex items-center gap-2">
                                                {check.ruleName.replace(/_/g, " ").replace(/\b\w/g, c => c.toUpperCase())}
                                                <History className="h-3.5 w-3.5 text-slate-300 opacity-0 group-hover:opacity-100 transition-opacity" />
                                            </div>
                                        </td>
                                        <td className="px-4 py-3">
                                            <span className={`px-2 py-0.5 rounded text-xs font-medium ${check.scope === "Column"
                                                ? "bg-purple-100 text-purple-700"
                                                : "bg-blue-100 text-blue-700"
                                                }`}>
                                                {check.scope}
                                            </span>
                                        </td>
                                        <td className="px-4 py-3 text-slate-600 font-mono text-xs">{check.target}</td>
                                        <td className="px-4 py-3">
                                            <span className={`text-xs font-medium ${check.ruleLevel === "Critical" ? "text-rose-600"
                                                : check.ruleLevel === "High" ? "text-amber-600"
                                                    : "text-slate-500"
                                                }`}>
                                                {check.ruleLevel}
                                            </span>
                                        </td>
                                        <td className="px-4 py-3">
                                            <StatusBadge status={check.checkStatus} />
                                        </td>
                                        <td className="px-4 py-3">
                                            <span className={`text-sm font-bold ${(check.passRate ?? 0) >= 99 ? "text-emerald-600"
                                                : (check.passRate ?? 0) >= 95 ? "text-amber-600"
                                                    : "text-rose-600"
                                                }`}>
                                                {check.passRate != null ? `${check.passRate.toFixed(1)}%` : "—"}
                                            </span>
                                        </td>
                                        <td className="px-4 py-3 text-xs text-slate-500">
                                            {formatTimestamp(check.checkTimestamp)}
                                        </td>
                                        <td className="px-4 py-3 text-right text-sm font-medium text-slate-700">
                                            {check.totalRuns}
                                        </td>
                                        <td className="px-4 py-3 text-right">
                                            <span className={`text-sm font-medium ${check.failureCount > 0 ? "text-rose-600" : "text-slate-400"}`}>
                                                {check.failureCount}
                                            </span>
                                        </td>
                                        <td className="px-4 py-3">
                                            <TrendIndicator trend={check.trend} />
                                        </td>
                                    </tr>
                                ))}
                            </tbody>
                        </table>
                    </div>
                ) : (
                    <div className="flex items-center justify-center h-48 text-slate-400">
                        <div className="text-center">
                            <CheckCircle2 className="h-12 w-12 mx-auto mb-3 text-slate-300" />
                            <p className="text-sm">
                                {statusFilter
                                    ? `No checks with status "${statusFilter}" found`
                                    : "No quality checks found for this table"}
                            </p>
                            <p className="text-xs mt-1">
                                {statusFilter
                                    ? "Try clearing the filter"
                                    : "Run profiling or add rules to see checks here"}
                            </p>
                        </div>
                    </div>
                )}
            </Card>

            {/* ─── History Drawer ────────────────────────────────────── */}
            {selectedCheck && (
                <CheckHistoryDrawer
                    open={historyOpen}
                    onClose={() => { setHistoryOpen(false); setSelectedCheck(null); }}
                    database={database}
                    schema={schema}
                    table={table}
                    ruleName={selectedCheck.ruleName}
                    column={selectedCheck.columnName}
                    checkName={selectedCheck.ruleName.replace(/_/g, " ").replace(/\b\w/g, c => c.toUpperCase())}
                    scope={selectedCheck.scope}
                    target={selectedCheck.target}
                />
            )}
        </div>
    );
}






