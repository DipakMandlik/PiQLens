"use client";

import { useState, useEffect } from "react";
import {
    CheckCircle2, AlertOctagon, AlertTriangle, X,
    Minus, Clock, Hash, Activity
} from "lucide-react";
import { Card } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import {
    Dialog,
    DialogContent,
    DialogHeader,
    DialogTitle,
    DialogDescription,
} from "@/components/ui/dialog";

// ─── Types ─────────────────────────────────────────────────────────────
interface HistoryEntry {
    runId: string;
    timestamp: string | null;
    status: string;
    passRate: number | null;
    totalRecords: number;
    validRecords: number;
    invalidRecords: number;
    threshold: number;
    executionTimeMs: number;
    failureReason: string | null;
    scanScope: string;
}

interface Props {
    open: boolean;
    onClose: () => void;
    database: string;
    schema: string;
    table: string;
    ruleName: string;
    column: string | null;
    checkName: string;
    scope: string;
    target: string;
}

// ─── Status Config ─────────────────────────────────────────────────────
const STATUS_STYLES: Record<string, { dot: string; bg: string; text: string }> = {
    PASSED: { dot: "bg-emerald-500", bg: "bg-emerald-50", text: "text-emerald-700" },
    FAILED: { dot: "bg-rose-500", bg: "bg-rose-50", text: "text-rose-700" },
    WARNING: { dot: "bg-amber-500", bg: "bg-amber-50", text: "text-amber-700" },
    SKIPPED: { dot: "bg-blue-500", bg: "bg-blue-50", text: "text-blue-700" },
    ERROR: { dot: "bg-purple-500", bg: "bg-purple-50", text: "text-purple-700" },
    OTHER: { dot: "bg-slate-400", bg: "bg-slate-50", text: "text-slate-600" },
};

function StatusDot({ status }: { status: string }) {
    const style = STATUS_STYLES[status] || STATUS_STYLES.OTHER;
    return <span className={`inline-block w-2.5 h-2.5 rounded-full ${style.dot}`} />;
}

function formatTs(ts: string | null): string {
    if (!ts) return "—";
    try {
        return new Date(ts).toLocaleString("en-US", { dateStyle: "medium", timeStyle: "short" });
    } catch { return "—"; }
}

function formatDuration(ms: number): string {
    if (ms < 1000) return `${ms}ms`;
    return `${(ms / 1000).toFixed(1)}s`;
}

// ─── Component ─────────────────────────────────────────────────────────
export function CheckHistoryDrawer({
    open, onClose, database, schema, table,
    ruleName, column, checkName, scope, target
}: Props) {
    const [history, setHistory] = useState<HistoryEntry[]>([]);
    const [isLoading, setIsLoading] = useState(false);
    const [error, setError] = useState<string | null>(null);

    useEffect(() => {
        if (!open) return;
        const fetchHistory = async () => {
            setIsLoading(true);
            setError(null);
            try {
                let url = `/api/dq/checks/history?database=${encodeURIComponent(database)}&schema=${encodeURIComponent(schema)}&table=${encodeURIComponent(table)}&ruleName=${encodeURIComponent(ruleName)}`;
                if (column) url += `&column=${encodeURIComponent(column)}`;

                const res = await fetch(url);
                const json = await res.json();
                if (json.success) {
                    setHistory(json.data || []);
                } else {
                    setError(json.error || "Failed to fetch history");
                }
            } catch (err: any) {
                setError("Failed to fetch check history");
            } finally {
                setIsLoading(false);
            }
        };
        fetchHistory();
    }, [open, database, schema, table, ruleName, column]);

    // Summary stats
    const totalRuns = history.length;
    const passedRuns = history.filter(h => h.status === "PASSED").length;
    const failedRuns = history.filter(h => h.status === "FAILED").length;
    const avgPassRate = history.length > 0
        ? (history.reduce((sum, h) => sum + (h.passRate ?? 0), 0) / history.length)
        : 0;

    return (
        <Dialog open={open} onOpenChange={(o) => { if (!o) onClose(); }}>
            <DialogContent className="max-w-3xl max-h-[85vh] overflow-hidden flex flex-col">
                <DialogHeader className="pb-3 border-b border-slate-200">
                    <DialogTitle className="text-lg font-bold text-slate-800 flex items-center gap-2">
                        <Activity className="h-5 w-5 text-indigo-500" />
                        {checkName}
                    </DialogTitle>
                    <DialogDescription className="text-xs text-slate-500 flex items-center gap-3 mt-1">
                        <span className={`px-2 py-0.5 rounded text-xs font-medium ${scope === "Column" ? "bg-purple-100 text-purple-700" : "bg-blue-100 text-blue-700"}`}>
                            {scope}
                        </span>
                        <span className="font-mono">{target}</span>
                        <span className="text-slate-300">·</span>
                        <span>Execution history (last {totalRuns} runs)</span>
                    </DialogDescription>
                </DialogHeader>

                <div className="flex-1 overflow-y-auto py-4 space-y-4">
                    {/* Mini Summary Row */}
                    <div className="grid grid-cols-4 gap-3">
                        <Card className="p-3 text-center bg-slate-50 border-slate-200">
                            <div className="text-xl font-bold text-slate-700">{totalRuns}</div>
                            <div className="text-[10px] text-slate-500 uppercase">Total Runs</div>
                        </Card>
                        <Card className="p-3 text-center bg-emerald-50 border-emerald-200">
                            <div className="text-xl font-bold text-emerald-700">{passedRuns}</div>
                            <div className="text-[10px] text-emerald-600 uppercase">Passed</div>
                        </Card>
                        <Card className="p-3 text-center bg-rose-50 border-rose-200">
                            <div className="text-xl font-bold text-rose-700">{failedRuns}</div>
                            <div className="text-[10px] text-rose-600 uppercase">Failed</div>
                        </Card>
                        <Card className="p-3 text-center bg-indigo-50 border-indigo-200">
                            <div className="text-xl font-bold text-indigo-700">{avgPassRate.toFixed(1)}%</div>
                            <div className="text-[10px] text-indigo-600 uppercase">Avg Pass Rate</div>
                        </Card>
                    </div>

                    {/* Loading */}
                    {isLoading && (
                        <div className="flex items-center justify-center py-12">
                            <div className="animate-spin rounded-full h-6 w-6 border-b-2 border-indigo-500" />
                        </div>
                    )}

                    {/* Error */}
                    {error && !isLoading && (
                        <div className="text-center py-8 text-rose-500 text-sm">
                            <AlertTriangle className="h-6 w-6 mx-auto mb-2" />
                            {error}
                        </div>
                    )}

                    {/* Timeline */}
                    {!isLoading && !error && history.length > 0 && (
                        <div className="relative">
                            {/* Vertical line */}
                            <div className="absolute left-[18px] top-0 bottom-0 w-px bg-slate-200" />

                            <div className="space-y-0">
                                {history.map((entry, idx) => {
                                    const style = STATUS_STYLES[entry.status] || STATUS_STYLES.OTHER;
                                    return (
                                        <div key={entry.runId || idx} className="relative pl-10 py-3 group">
                                            {/* Dot on timeline */}
                                            <div className={`absolute left-[13px] top-[18px] w-3 h-3 rounded-full border-2 border-white ${style.dot} shadow-sm z-10`} />

                                            <div className={`rounded-lg border p-3 transition-all ${style.bg} border-opacity-50 hover:shadow-sm`}>
                                                <div className="flex items-center justify-between mb-1.5">
                                                    <div className="flex items-center gap-2">
                                                        <span className={`inline-flex items-center px-2 py-0.5 rounded-full text-[11px] font-bold ${style.bg} ${style.text}`}>
                                                            {entry.status}
                                                        </span>
                                                        <span className="text-xs text-slate-400">
                                                            <Clock className="h-3 w-3 inline mr-0.5" />
                                                            {formatTs(entry.timestamp)}
                                                        </span>
                                                    </div>
                                                    <span className="text-[10px] text-slate-400 font-mono">
                                                        {entry.scanScope}
                                                    </span>
                                                </div>

                                                <div className="grid grid-cols-4 gap-3 text-xs">
                                                    <div>
                                                        <span className="text-slate-400">Pass Rate</span>
                                                        <div className={`font-bold ${(entry.passRate ?? 0) >= 99 ? "text-emerald-600" : (entry.passRate ?? 0) >= 95 ? "text-amber-600" : "text-rose-600"}`}>
                                                            {entry.passRate != null ? `${entry.passRate.toFixed(1)}%` : "—"}
                                                        </div>
                                                    </div>
                                                    <div>
                                                        <span className="text-slate-400">Records</span>
                                                        <div className="font-medium text-slate-700">
                                                            {entry.totalRecords.toLocaleString()}
                                                        </div>
                                                    </div>
                                                    <div>
                                                        <span className="text-slate-400">Invalid</span>
                                                        <div className={`font-medium ${entry.invalidRecords > 0 ? "text-rose-600" : "text-slate-400"}`}>
                                                            {entry.invalidRecords.toLocaleString()}
                                                        </div>
                                                    </div>
                                                    <div>
                                                        <span className="text-slate-400">Duration</span>
                                                        <div className="font-medium text-slate-700">
                                                            {formatDuration(entry.executionTimeMs)}
                                                        </div>
                                                    </div>
                                                </div>

                                                {entry.failureReason && (
                                                    <div className="mt-2 pt-2 border-t border-slate-200/50 text-[11px] text-rose-600 flex items-start gap-1">
                                                        <AlertOctagon className="h-3 w-3 mt-0.5 flex-shrink-0" />
                                                        <span className="line-clamp-2">{entry.failureReason}</span>
                                                    </div>
                                                )}
                                            </div>
                                        </div>
                                    );
                                })}
                            </div>
                        </div>
                    )}

                    {/* Empty */}
                    {!isLoading && !error && history.length === 0 && (
                        <div className="text-center py-12 text-slate-400">
                            <CheckCircle2 className="h-10 w-10 mx-auto mb-3 text-slate-300" />
                            <p className="text-sm">No execution history found</p>
                            <p className="text-xs mt-1">This check has not been executed yet</p>
                        </div>
                    )}
                </div>
            </DialogContent>
        </Dialog>
    );
}
