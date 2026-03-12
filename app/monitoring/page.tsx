'use client';

import { useEffect, useState } from 'react';
import Link from 'next/link';
import {
    Activity,
    Search,
    AlertTriangle,
    CheckCircle2,
    Clock,
    Database,
    Filter,
    RefreshCw,
    ArrowRight,
    ShieldAlert,
    BarChart2,
    Table as TableIcon
} from 'lucide-react';
import { Button } from '@/components/ui/button';
import { BackButton } from '@/components/back-button';
import { format } from 'date-fns';

// Types derived from API
interface Monitor {
    id: string;
    name: string;
    type: 'FRESHNESS' | 'VOLUME' | 'QUALITY' | 'SCHEMA';
    status: 'HEALTHY' | 'WARNING' | 'BREACHED';
    value: string;
    threshold: string;
    lastEvaluated: string | null;
    message: string;
    table: string;
}

export default function MonitoringPage() {
    const [monitors, setMonitors] = useState<Monitor[]>([]);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState<string | null>(null);
    const [selectedMonitor, setSelectedMonitor] = useState<Monitor | null>(null);
    const [search, setSearch] = useState('');
    const [filterType, setFilterType] = useState<string>('ALL');

    const fetchMonitors = async () => {
        setLoading(true);
        setError(null);
        try {
            const res = await fetch('/api/monitoring/list');
            const data = await res.json();
            if (data.success) {
                setMonitors(data.data);
                // Select first monitor by default if none selected
                if (!selectedMonitor && data.data.length > 0) {
                    setSelectedMonitor(data.data[0]);
                }
            } else {
                setError(data.error || 'Failed to load monitors');
            }
        } catch (e: any) {
            console.error('Failed to fetch monitors', e);
            setError(e.message || 'Network error');
        } finally {
            setLoading(false);
        }
    };

    useEffect(() => {
        fetchMonitors();
    }, []);

    // Error UI
    if (error) {
        return (
            <div className="h-[calc(100vh-64px)] flex items-center justify-center bg-gray-50">
                <div className="bg-white p-8 rounded-lg shadow-md max-w-md text-center">
                    <AlertTriangle className="h-12 w-12 text-red-500 mx-auto mb-4" />
                    <h2 className="text-xl font-bold text-gray-900 mb-2">Unavailable</h2>
                    <p className="text-gray-600 mb-6">{error}</p>
                    <Button onClick={fetchMonitors} variant="outline">
                        <RefreshCw className="h-4 w-4 mr-2" />
                        Retry
                    </Button>
                </div>
            </div>
        );
    }

    // Filter Logic
    const filteredMonitors = monitors.filter(m => {
        const matchesSearch = m.name.toLowerCase().includes(search.toLowerCase()) ||
            m.type.toLowerCase().includes(search.toLowerCase());
        const matchesType = filterType === 'ALL' || m.type === filterType;
        return matchesSearch && matchesType;
    });

    const getStatusColor = (status: string) => {
        switch (status) {
            case 'HEALTHY': return 'text-green-600 bg-green-50 border-green-200';
            case 'WARNING': return 'text-amber-600 bg-amber-50 border-amber-200';
            case 'BREACHED': return 'text-red-600 bg-red-50 border-red-200';
            default: return 'text-gray-600 bg-gray-50 border-gray-200';
        }
    };

    const getStatusIcon = (status: string) => {
        switch (status) {
            case 'HEALTHY': return <CheckCircle2 className="h-4 w-4" />;
            case 'WARNING': return <AlertTriangle className="h-4 w-4" />;
            case 'BREACHED': return <ShieldAlert className="h-4 w-4" />;
            default: return <Activity className="h-4 w-4" />;
        }
    };

    return (
        <div className="h-[calc(100vh-64px)] bg-gray-50 flex flex-col">
            {/* Header */}
            <div className="bg-white border-b border-gray-200 px-6 py-4 shrink-0">
                <div className="flex items-center justify-between mb-4">
                    <BackButton href="/" label="Back to Home" />
                </div>
                <div className="flex items-center justify-between">
                    <div>
                        <h1 className="text-xl font-bold text-gray-900 flex items-center gap-2">
                            <Activity className="h-6 w-6 text-blue-600" />
                            Monitoring
                        </h1>
                        <div className="flex items-center gap-3 mt-1 text-sm text-gray-500">
                            <span className="flex items-center gap-1.5">
                                <span className="relative flex h-2 w-2">
                                    <span className="animate-ping absolute inline-flex h-full w-full rounded-full bg-green-400 opacity-75"></span>
                                    <span className="relative inline-flex rounded-full h-2 w-2 bg-green-500"></span>
                                </span>
                                Active
                            </span>
                            <span>|</span>
                            <span>Last Refresh: {format(new Date(), 'HH:mm')} IST</span>
                        </div>
                    </div>
                    <Button variant="outline" size="sm" onClick={fetchMonitors}>
                        <RefreshCw className={`h-4 w-4 mr-2 ${loading ? 'animate-spin' : ''}`} />
                        Refresh
                    </Button>
                </div>
            </div>


            {/* Main Content - Split Pane */}
            <div className="flex-1 flex overflow-hidden">

                {/* LEFT PANEL - LIST */}
                <div className="w-[400px] bg-white border-r border-gray-200 flex flex-col">
                    <div className="p-4 border-b border-gray-200 space-y-3">
                        <div className="relative">
                            <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4 text-gray-400" />
                            <input
                                type="text"
                                placeholder="Search monitors..."
                                className="w-full pl-9 pr-4 py-2 text-sm border border-gray-300 rounded-lg outline-none focus:ring-2 focus:ring-blue-500"
                                value={search}
                                onChange={(e) => setSearch(e.target.value)}
                            />
                        </div>
                        <div className="flex gap-2 overflow-x-auto pb-1 no-scrollbar">
                            {['ALL', 'FRESHNESS', 'VOLUME', 'QUALITY'].map(type => (
                                <button
                                    key={type}
                                    onClick={() => setFilterType(type)}
                                    className={`
                           px-3 py-1 rounded-full text-xs font-medium whitespace-nowrap transition-colors
                           ${filterType === type
                                            ? 'bg-blue-100 text-blue-700'
                                            : 'bg-gray-100 text-gray-600 hover:bg-gray-200'}
                        `}
                                >
                                    {type}
                                </button>
                            ))}
                        </div>
                    </div>

                    <div className="flex-1 overflow-y-auto p-2 space-y-2">
                        {filteredMonitors.map(monitor => (
                            <div
                                key={monitor.id}
                                onClick={() => setSelectedMonitor(monitor)}
                                className={`
                         group p-3 rounded-lg cursor-pointer border transition-all
                         ${selectedMonitor?.id === monitor.id
                                        ? 'bg-blue-50 border-blue-200 shadow-sm'
                                        : 'bg-white border-transparent hover:bg-gray-50 border-gray-100'}
                      `}
                            >
                                <div className="flex items-start justify-between mb-1">
                                    <div className={`flex items-center gap-1.5 px-2 py-0.5 rounded text-[10px] font-bold uppercase tracking-wide border ${getStatusColor(monitor.status)}`}>
                                        {getStatusIcon(monitor.status)}
                                        {monitor.status}
                                    </div>
                                    <span className="text-[10px] text-gray-400 font-mono">{monitor.type}</span>
                                </div>
                                <h3 className="text-sm font-semibold text-gray-900 group-hover:text-blue-700 truncate">
                                    {monitor.name}
                                </h3>
                                <p className="text-xs text-gray-500 truncate mt-0.5">{monitor.message}</p>
                            </div>
                        ))}
                    </div>
                </div>

                {/* RIGHT PANEL - DETAIL */}
                <div className="flex-1 overflow-y-auto bg-gray-50 p-8">
                    {selectedMonitor ? (
                        <div className="max-w-3xl mx-auto space-y-6">
                            {/* Header Card */}
                            <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-6">
                                <div className="flex items-start justify-between mb-6">
                                    <div>
                                        <div className="flex items-center gap-2 mb-2">
                                            <h2 className="text-2xl font-bold text-gray-900">{selectedMonitor.name}</h2>
                                            <span className="px-2 py-1 bg-gray-100 text-gray-600 text-xs font-mono rounded">
                                                {selectedMonitor.id}
                                            </span>
                                        </div>
                                        <p className="text-gray-500 font-medium">{selectedMonitor.type} Monitor</p>
                                    </div>
                                    <div className={`flex items-center gap-2 px-4 py-2 rounded-lg border ${getStatusColor(selectedMonitor.status)}`}>
                                        {getStatusIcon(selectedMonitor.status)}
                                        <span className="font-bold">{selectedMonitor.status}</span>
                                    </div>
                                </div>

                                <div className="grid grid-cols-4 gap-6 pt-6 border-t border-gray-100">
                                    <div>
                                        <span className="text-xs text-gray-500 uppercase tracking-wider font-semibold block mb-1">Observed</span>
                                        <span className="text-xl font-mono font-medium text-gray-900">{selectedMonitor.value}</span>
                                    </div>
                                    <div>
                                        <span className="text-xs text-gray-500 uppercase tracking-wider font-semibold block mb-1">Threshold</span>
                                        <span className="text-xl font-mono font-medium text-gray-500">{selectedMonitor.threshold}</span>
                                    </div>
                                    <div className="col-span-2">
                                        <span className="text-xs text-gray-500 uppercase tracking-wider font-semibold block mb-1">Last Evaluated</span>
                                        <span className="text-lg text-gray-700 flex items-center gap-2">
                                            <Clock className="h-4 w-4 text-gray-400" />
                                            {selectedMonitor.lastEvaluated
                                                ? format(new Date(selectedMonitor.lastEvaluated), 'MMM d, HH:mm')
                                                : '-'}
                                        </span>
                                    </div>
                                </div>
                            </div>

                            {/* Impact & Actions Grid */}
                            <div className="grid grid-cols-2 gap-6">
                                <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-6">
                                    <h3 className="text-sm font-semibold text-gray-900 flex items-center gap-2 mb-4">
                                        <Database className="h-4 w-4 text-blue-600" />
                                        Impacted Assets
                                    </h3>
                                    <div className="bg-gray-50 rounded-lg p-3 border border-gray-100 flex items-center justify-between">
                                        <div className="flex items-center gap-3">
                                            <div className="p-2 bg-white rounded border border-gray-200">
                                                <TableIcon className="h-4 w-4 text-gray-500" />
                                            </div>
                                            <div>
                                                <p className="text-sm font-medium text-gray-900">{selectedMonitor.table}</p>
                                                <p className="text-xs text-gray-500">BANKING_DW.BRONZE</p>
                                            </div>
                                        </div>
                                        <Link href={`/datasets/${selectedMonitor.table}`}>
                                            <Button variant="ghost" size="sm" className="text-blue-600 hover:text-blue-700">
                                                View
                                                <ArrowRight className="h-3 w-3 ml-1" />
                                            </Button>
                                        </Link>
                                    </div>
                                </div>

                                <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-6">
                                    <h3 className="text-sm font-semibold text-gray-900 flex items-center gap-2 mb-4">
                                        <Clock className="h-4 w-4 text-purple-600" />
                                        Recent Events
                                    </h3>
                                    <div className="relative pl-4 border-l-2 border-gray-100 space-y-6">
                                        <div className="relative">
                                            <div className={`absolute -left-[21px] top-1 h-3 w-3 rounded-full border-2 border-white ${selectedMonitor.status === 'HEALTHY' ? 'bg-green-500' : 'bg-red-500'}`} />
                                            <p className="text-sm font-medium text-gray-900">
                                                Monitor Status: {selectedMonitor.status}
                                            </p>
                                            <span className="text-xs text-gray-500">
                                                {selectedMonitor.lastEvaluated ? format(new Date(selectedMonitor.lastEvaluated), 'HH:mm') : 'Now'}
                                            </span>
                                        </div>
                                        {selectedMonitor.type === 'FRESHNESS' && (
                                            <div className="relative">
                                                <div className="absolute -left-[21px] top-1 h-3 w-3 rounded-full border-2 border-white bg-gray-300" />
                                                <p className="text-sm text-gray-600">Last Data Load</p>
                                                <span className="text-xs text-gray-500">See Metadata</span>
                                            </div>
                                        )}
                                    </div>
                                </div>
                            </div>
                        </div>
                    ) : (
                        <div className="h-full flex flex-col items-center justify-center text-gray-400">
                            <Activity className="h-12 w-12 mb-4 opacity-20" />
                            <p>Select a monitor to view details</p>
                        </div>
                    )}
                </div>

            </div>
        </div>
    );
}
