'use client';

import React, { useEffect, useState } from 'react';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { Loader2, Zap, HardDrive, Activity, Server, Database, TrendingUp } from 'lucide-react';
import { TopNav } from '@/components/layout/TopNav';
import { Sidebar } from '@/components/layout/Sidebar';

interface CacheMetrics {
    enabled: boolean;
    status: string;
    message?: string;
    memory?: {
        used_human: string;
        peak_human: string;
        fragmentation_ratio: string;
    };
    performance?: {
        hit_rate_pct: string;
        total_hits: number;
        total_misses: number;
        evicted_keys: number;
        expired_keys: number;
        instantaneous_ops_per_sec: number;
        total_connections_received: number;
    };
    storage?: {
        total_keys: number;
        keys_with_expiration: number;
    };
    clients?: {
        connected_clients: number;
        blocked_clients: number;
    };
}

export default function CacheAnalyticsPage() {
    const [metrics, setMetrics] = useState<CacheMetrics | null>(null);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState<string | null>(null);

    const fetchMetrics = async () => {
        setLoading(true);
        setError(null);
        try {
            const res = await fetch('/api/admin/cache');
            const result = await res.json();
            if (result.success) {
                if (!result.data && result.enabled === false) {
                    setMetrics({ enabled: false, status: 'Disabled', message: result.message });
                } else {
                    setMetrics(result.data);
                }
            } else {
                setError(result.error || 'Failed to fetch cache metrics.');
            }
        } catch (err: any) {
            setError(err.message || 'Network error occurred.');
        } finally {
            setLoading(false);
        }
    };

    useEffect(() => {
        fetchMetrics();
        // Auto-refresh every 30 seconds
        const interval = setInterval(fetchMetrics, 30000);
        return () => clearInterval(interval);
    }, []);

    if (loading && !metrics) {
        return (
            <div className="min-h-screen bg-neutral-900 border-neutral-800 flex items-center justify-center">
                <Loader2 className="h-8 w-8 text-indigo-500 animate-spin" />
            </div>
        );
    }

    return (
        <div className="flex min-h-screen bg-[#0A0A0B] text-foreground font-sans selection:bg-indigo-500/30">
            <Sidebar />

            <div className="flex-1 flex flex-col min-w-0 overflow-hidden relative border-l border-neutral-800/50">
                <TopNav />

                <main className="flex-1 overflow-y-auto overflow-x-hidden pt-24 px-4 sm:px-6 lg:px-8 pb-12 relative">
                    <div className="max-w-6xl mx-auto space-y-8">

                        <div className="flex items-center justify-between">
                            <div>
                                <h1 className="text-3xl font-light tracking-tight text-white mb-2 flex items-center gap-3">
                                    <Zap className="h-6 w-6 text-indigo-400" />
                                    Valkey Cache Engine
                                </h1>
                                <p className="text-sm text-neutral-400">
                                    Real-time observability and performance metrics for the PI_QLens distributed cache layer.
                                </p>
                            </div>
                            <div className="flex items-center gap-4">
                                <Badge variant={metrics?.enabled ? "default" : "destructive"} className="px-3 py-1 font-medium bg-indigo-500/10 text-indigo-400 border border-indigo-500/20">
                                    {metrics?.status || 'Unknown'}
                                </Badge>
                                <button
                                    onClick={fetchMetrics}
                                    className="px-4 py-2 bg-neutral-800 hover:bg-neutral-700 text-sm font-medium rounded-lg transition-colors border border-neutral-700 flex items-center gap-2"
                                    disabled={loading}
                                >
                                    {loading ? <Loader2 className="w-4 h-4 animate-spin" /> : <Activity className="w-4 h-4" />}
                                    Refresh
                                </button>
                            </div>
                        </div>

                        {error && (
                            <div className="bg-red-500/10 border border-red-500/20 text-red-400 p-4 rounded-xl text-sm">
                                {error}
                            </div>
                        )}

                        {!metrics?.enabled && metrics?.message && (
                            <div className="bg-yellow-500/10 border border-yellow-500/20 text-yellow-400 p-4 rounded-xl text-sm flex items-center gap-3">
                                <Database className="w-5 h-5" />
                                {metrics.message}
                            </div>
                        )}

                        {metrics?.enabled && metrics.performance && (
                            <>
                                {/* Core KPIs */}
                                <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
                                    <Card className="bg-neutral-900/50 border-neutral-800 backdrop-blur-sm">
                                        <CardHeader className="pb-2">
                                            <CardTitle className="text-sm font-medium text-neutral-400 flex justify-between">
                                                Hit Rate
                                                <TrendingUp className="w-4 h-4 text-emerald-400" />
                                            </CardTitle>
                                        </CardHeader>
                                        <CardContent>
                                            <div className="text-3xl font-light text-white">{metrics.performance.hit_rate_pct}%</div>
                                            <p className="text-xs text-neutral-500 mt-1">Global average efficiency</p>
                                        </CardContent>
                                    </Card>

                                    <Card className="bg-neutral-900/50 border-neutral-800 backdrop-blur-sm">
                                        <CardHeader className="pb-2">
                                            <CardTitle className="text-sm font-medium text-neutral-400 flex justify-between">
                                                Memory Used
                                                <HardDrive className="w-4 h-4 text-indigo-400" />
                                            </CardTitle>
                                        </CardHeader>
                                        <CardContent>
                                            <div className="text-3xl font-light text-white">{metrics.memory?.used_human}</div>
                                            <p className="text-xs text-neutral-500 mt-1">Peak: {metrics.memory?.peak_human}</p>
                                        </CardContent>
                                    </Card>

                                    <Card className="bg-neutral-900/50 border-neutral-800 backdrop-blur-sm">
                                        <CardHeader className="pb-2">
                                            <CardTitle className="text-sm font-medium text-neutral-400 flex justify-between">
                                                Total Keys
                                                <Database className="w-4 h-4 text-blue-400" />
                                            </CardTitle>
                                        </CardHeader>
                                        <CardContent>
                                            <div className="text-3xl font-light text-white">{metrics.storage?.total_keys.toLocaleString()}</div>
                                            <p className="text-xs text-neutral-500 mt-1">{metrics.storage?.keys_with_expiration.toLocaleString()} with TTL</p>
                                        </CardContent>
                                    </Card>

                                    <Card className="bg-neutral-900/50 border-neutral-800 backdrop-blur-sm">
                                        <CardHeader className="pb-2">
                                            <CardTitle className="text-sm font-medium text-neutral-400 flex justify-between">
                                                Active Clients
                                                <Server className="w-4 h-4 text-orange-400" />
                                            </CardTitle>
                                        </CardHeader>
                                        <CardContent>
                                            <div className="text-3xl font-light text-white">{metrics.clients?.connected_clients.toLocaleString()}</div>
                                            <p className="text-xs text-neutral-500 mt-1">Connections open</p>
                                        </CardContent>
                                    </Card>
                                </div>

                                {/* Detailed Analytics */}
                                <div className="grid grid-cols-1 md:grid-cols-2 gap-6 mt-8">
                                    {/* Traffic & Load */}
                                    <Card className="bg-neutral-900/30 border-neutral-800">
                                        <CardHeader>
                                            <CardTitle className="text-lg font-medium text-white">Traffic & Workload</CardTitle>
                                            <CardDescription>Throughput and historical request volume</CardDescription>
                                        </CardHeader>
                                        <CardContent className="space-y-4">
                                            <div className="flex justify-between items-center py-2 border-b border-neutral-800/50">
                                                <span className="text-sm text-neutral-400">Total Cache Hits</span>
                                                <span className="text-sm text-white font-medium">{metrics.performance.total_hits.toLocaleString()}</span>
                                            </div>
                                            <div className="flex justify-between items-center py-2 border-b border-neutral-800/50">
                                                <span className="text-sm text-neutral-400">Total Cache Misses</span>
                                                <span className="text-sm text-white font-medium">{metrics.performance.total_misses.toLocaleString()}</span>
                                            </div>
                                            <div className="flex justify-between items-center py-2 border-b border-neutral-800/50">
                                                <span className="text-sm text-neutral-400">Operations Per Second</span>
                                                <span className="text-sm text-white font-medium">{metrics.performance.instantaneous_ops_per_sec.toLocaleString()}</span>
                                            </div>
                                            <div className="flex justify-between items-center py-2">
                                                <span className="text-sm text-neutral-400">Total Connections Received</span>
                                                <span className="text-sm text-white font-medium">{metrics.performance.total_connections_received.toLocaleString()}</span>
                                            </div>
                                        </CardContent>
                                    </Card>

                                    {/* Lifecycle & Health */}
                                    <Card className="bg-neutral-900/30 border-neutral-800">
                                        <CardHeader>
                                            <CardTitle className="text-lg font-medium text-white">Lifecycle & Health</CardTitle>
                                            <CardDescription>Eviction policies and memory health</CardDescription>
                                        </CardHeader>
                                        <CardContent className="space-y-4">
                                            <div className="flex justify-between items-center py-2 border-b border-neutral-800/50">
                                                <span className="text-sm text-neutral-400">Keys Evicted (OOM)</span>
                                                <span className="text-sm text-white font-medium">{metrics.performance.evicted_keys.toLocaleString()}</span>
                                            </div>
                                            <div className="flex justify-between items-center py-2 border-b border-neutral-800/50">
                                                <span className="text-sm text-neutral-400">Keys Expired (TTL)</span>
                                                <span className="text-sm text-white font-medium">{metrics.performance.expired_keys.toLocaleString()}</span>
                                            </div>
                                            <div className="flex justify-between items-center py-2 border-b border-neutral-800/50">
                                                <span className="text-sm text-neutral-400">Blocked Clients</span>
                                                <span className="text-sm text-white font-medium">{metrics.clients?.blocked_clients.toLocaleString()}</span>
                                            </div>
                                            <div className="flex justify-between items-center py-2">
                                                <span className="text-sm text-neutral-400">Mem Fragmentation</span>
                                                <span className="text-sm text-white font-medium">{metrics.memory?.fragmentation_ratio}</span>
                                            </div>
                                        </CardContent>
                                    </Card>
                                </div>
                            </>
                        )}

                    </div>
                </main>
            </div>
        </div>
    );
}
