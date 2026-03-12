'use client';

import { useEffect, useState, Suspense } from 'react';
import { useParams, useSearchParams } from 'next/navigation';
import Link from 'next/link';
import {
    ArrowLeft, Database, Table as TableIcon, Calendar, HardDrive, CheckCircle2, XCircle,
    Loader2, Columns, Activity, Info, Network, FileText, Shield, Hash, Search, Key, Users, BookOpen, FileEdit, Clock, Check, Edit2, AlertTriangle, ArrowRight, Save, UserCircle2, Server
} from 'lucide-react';
import { Button } from '@/components/ui/button';
import { formatIST } from '@/lib/timezone-utils';

interface Column {
    COLUMN_NAME: string;
    DATA_TYPE: string;
    IS_NULLABLE: string;
    ORDINAL_POSITION: number;
    COMMENT?: string;
}

interface DatasetDetails {
    name: string;
    database: string;
    schema: string;
    rowCount: number;
    bytes: number;
    comment: string | null;
    created: string;
    lastAltered: string;
    columns: Column[];
    isOnboarded: boolean;
    datasetId: string | null;
    latestDqScore: number | null;
}

const formatBytes = (bytes: number, decimals = 2) => {
    if (!+bytes) return '0 Bytes';
    const k = 1024;
    const dm = decimals < 0 ? 0 : decimals;
    const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return `${parseFloat((bytes / Math.pow(k, i)).toFixed(dm))} ${sizes[i]}`;
};

function OwnershipTab({ datasetName, dbParam, schemaParam }: { datasetName: string, dbParam: string | null, schemaParam: string | null }) {
    const [grants, setGrants] = useState<any[]>([]);
    const [loading, setLoading] = useState(true);

    useEffect(() => {
        let isMounted = true;
        const fetchGrants = async () => {
            let url = `/api/dq/datasets/${datasetName}/grants`;
            if (dbParam && schemaParam) url += `?database=${dbParam}&schema=${schemaParam}`;
            try {
                const res = await fetch(url);
                const data = await res.json();
                if (data.success && isMounted) setGrants(data.data);
            } catch (e) { console.error(e); }
            if (isMounted) setLoading(false);
        };
        fetchGrants();
        return () => { isMounted = false; };
    }, [datasetName, dbParam, schemaParam]);

    if (loading) return <div className="p-12 text-center"><Loader2 className="h-8 w-8 text-blue-600 animate-spin mx-auto" /></div>;

    const owners = grants.filter(g => g.privilege === 'OWNERSHIP');

    return (
        <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-6 animate-in fade-in">
            <h3 className="text-lg font-semibold text-gray-900 border-b pb-4 mb-4 flex items-center gap-2">
                <Shield className="h-5 w-5 text-gray-500" />
                Data Ownership & Stewardship
            </h3>
            <div className="space-y-6">
                <div className="grid grid-cols-2 gap-6">
                    <div className="bg-blue-50/50 p-4 rounded-lg border border-blue-100">
                        <span className="text-sm font-semibold text-gray-500 block mb-1">Technical Owner (Snowflake Role)</span>
                        <span className="text-lg font-bold text-gray-900">{owners[0]?.grantee_name || 'Unknown'}</span>
                    </div>
                    <div className="bg-gray-50 p-4 rounded-lg border border-gray-200">
                        <span className="text-sm font-semibold text-gray-500 block mb-1">Business Owner</span>
                        <div className="flex items-center justify-between">
                            <span className="text-gray-900">Unassigned</span>
                            <Button variant="outline" size="sm">Assign</Button>
                        </div>
                    </div>
                    <div className="bg-gray-50 p-4 rounded-lg border border-gray-200">
                        <span className="text-sm font-semibold text-gray-500 block mb-1">Data Steward</span>
                        <div className="flex items-center justify-between">
                            <span className="text-gray-900">Unassigned</span>
                            <Button variant="outline" size="sm">Assign</Button>
                        </div>
                    </div>
                    <div className="bg-gray-50 p-4 rounded-lg border border-gray-200">
                        <span className="text-sm font-semibold text-gray-500 block mb-1">Business Domain</span>
                        <div className="flex items-center justify-between">
                            <span className="text-gray-900">Core</span>
                            <Button variant="outline" size="sm">Change</Button>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    );
}

function UsageTab({ datasetName, dbParam, schemaParam }: { datasetName: string, dbParam: string | null, schemaParam: string | null }) {
    const [usage, setUsage] = useState<any[]>([]);
    const [loading, setLoading] = useState(true);

    useEffect(() => {
        let isMounted = true;
        const fetchUsage = async () => {
            let url = `/api/dq/datasets/${datasetName}/usage`;
            if (dbParam && schemaParam) url += `?database=${dbParam}&schema=${schemaParam}`;
            try {
                const res = await fetch(url);
                const data = await res.json();
                if (data.success && isMounted) setUsage(data.data);
            } catch (e) { console.error(e); }
            if (isMounted) setLoading(false);
        };
        fetchUsage();
        return () => { isMounted = false; };
    }, [datasetName, dbParam, schemaParam]);

    if (loading) return <div className="p-12 text-center"><Loader2 className="h-8 w-8 text-blue-600 animate-spin mx-auto" /></div>;

    return (
        <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-6 animate-in fade-in">
            <h3 className="text-lg font-semibold text-gray-900 border-b pb-4 mb-4 flex items-center gap-2">
                <Activity className="h-5 w-5 text-gray-500" />
                Query Activity (Last 30 Days)
            </h3>
            {usage.length === 0 ? (
                <p className="text-gray-500 text-center py-8">No usage metrics found for this dataset in the last 30 days.</p>
            ) : (
                <div className="space-y-6">
                    <div className="grid grid-cols-2 gap-4">
                        <div className="p-4 bg-gray-50 rounded-lg border border-gray-100">
                            <div className="text-sm text-gray-500">Most Active User</div>
                            <div className="text-xl font-bold mt-1">{usage[0]?.USER_NAME || 'N/A'}</div>
                        </div>
                        <div className="p-4 bg-gray-50 rounded-lg border border-gray-100">
                            <div className="text-sm text-gray-500">Total Queries</div>
                            <div className="text-xl font-bold mt-1">{usage.reduce((sum, u) => sum + parseInt(u.QUERY_COUNT), 0)}</div>
                        </div>
                    </div>
                    <table className="w-full text-left text-sm">
                        <thead className="bg-gray-50 text-gray-600 font-semibold border-b">
                            <tr>
                                <th className="px-4 py-2">User</th>
                                <th className="px-4 py-2">Query Count</th>
                                <th className="px-4 py-2">Avg Execution Time (ms)</th>
                                <th className="px-4 py-2">Last Accessed</th>
                            </tr>
                        </thead>
                        <tbody className="divide-y divide-gray-100">
                            {usage.map((u, i) => (
                                <tr key={i} className="hover:bg-blue-50/50">
                                    <td className="px-4 py-3 font-medium text-gray-900">{u.USER_NAME}</td>
                                    <td className="px-4 py-3">{u.QUERY_COUNT}</td>
                                    <td className="px-4 py-3">{Math.round(u.AVG_EXECUTION_TIME_MS)}</td>
                                    <td className="px-4 py-3">{u.LAST_ACCESSED ? new Date(u.LAST_ACCESSED).toLocaleString() : 'N/A'}</td>
                                </tr>
                            ))}
                        </tbody>
                    </table>
                </div>
            )}
        </div>
    );
}

function AccessTab({ datasetName, dbParam, schemaParam }: { datasetName: string, dbParam: string | null, schemaParam: string | null }) {
    const [grants, setGrants] = useState<any[]>([]);
    const [loading, setLoading] = useState(true);

    useEffect(() => {
        let isMounted = true;
        const fetchGrants = async () => {
            let url = `/api/dq/datasets/${datasetName}/grants`;
            if (dbParam && schemaParam) url += `?database=${dbParam}&schema=${schemaParam}`;
            try {
                const res = await fetch(url);
                const data = await res.json();
                if (data.success && isMounted) setGrants(data.data);
            } catch (e) { console.error(e); }
            if (isMounted) setLoading(false);
        };
        fetchGrants();
        return () => { isMounted = false; };
    }, [datasetName, dbParam, schemaParam]);

    if (loading) return <div className="p-12 text-center"><Loader2 className="h-8 w-8 text-blue-600 animate-spin mx-auto" /></div>;

    const nonOwners = grants.filter(g => g.privilege !== 'OWNERSHIP');

    return (
        <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-6 animate-in fade-in">
            <div className="flex items-center justify-between border-b pb-4 mb-4">
                <h3 className="text-lg font-semibold text-gray-900 flex items-center gap-2">
                    <Key className="h-5 w-5 text-gray-500" />
                    Security & Access Control
                </h3>
                <Button size="sm">Grant Access</Button>
            </div>
            {nonOwners.length === 0 ? (
                <p className="text-gray-500 text-center py-8">No specific grants found other than Ownership.</p>
            ) : (
                <table className="w-full text-left text-sm">
                    <thead className="bg-gray-50 text-gray-600 font-semibold border-b">
                        <tr>
                            <th className="px-4 py-2">Role / Grantee</th>
                            <th className="px-4 py-2">Privilege</th>
                            <th className="px-4 py-2">Granted On</th>
                            <th className="px-4 py-2 text-right">Actions</th>
                        </tr>
                    </thead>
                    <tbody className="divide-y divide-gray-100">
                        {nonOwners.map((g, i) => (
                            <tr key={i} className="hover:bg-blue-50/50">
                                <td className="px-4 py-3 font-medium text-gray-900">{g.grantee_name}</td>
                                <td className="px-4 py-3">
                                    <span className="px-2 py-1 bg-gray-100 rounded text-xs font-semibold uppercase">{g.privilege}</span>
                                </td>
                                <td className="px-4 py-3 text-gray-500">{new Date(g.created_on).toLocaleDateString()}</td>
                                <td className="px-4 py-3 text-right">
                                    <Button variant="ghost" size="sm" className="text-red-600 hover:text-red-700 hover:bg-red-50">Revoke</Button>
                                </td>
                            </tr>
                        ))}
                    </tbody>
                </table>
            )}
        </div>
    );
}

function LineageTab({ datasetName, dbParam, schemaParam }: { datasetName: string, dbParam: string | null, schemaParam: string | null }) {
    const [lineage, setLineage] = useState<any[]>([]);
    const [loading, setLoading] = useState(true);

    useEffect(() => {
        let isMounted = true;
        const fetchLineage = async () => {
            let url = `/api/dq/datasets/${datasetName}/lineage`;
            if (dbParam && schemaParam) url += `?database=${dbParam}&schema=${schemaParam}`;
            try {
                const res = await fetch(url);
                const data = await res.json();
                if (data.success && isMounted) setLineage(data.data);
            } catch (e) { console.error(e); }
            if (isMounted) setLoading(false);
        };
        fetchLineage();
        return () => { isMounted = false; };
    }, [datasetName, dbParam, schemaParam]);

    if (loading) return <div className="p-12 text-center"><Loader2 className="h-8 w-8 text-blue-600 animate-spin mx-auto" /></div>;

    const tableStr = datasetName.toUpperCase();
    const upstreams = lineage.filter(l => l.REFERENCING_OBJECT_NAME === tableStr);
    const downstreams = lineage.filter(l => l.REFERENCED_OBJECT_NAME === tableStr);

    return (
        <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-6 animate-in fade-in">
            <h3 className="text-lg font-semibold text-gray-900 border-b pb-4 mb-4 flex items-center gap-2">
                <Network className="h-5 w-5 text-gray-500" />
                Object Dependencies (Lineage)
            </h3>

            <div className="flex flex-col md:flex-row items-center justify-center gap-6 lg:gap-14 py-16 relative w-full overflow-hidden">
                {/* Background connecting line (horizontal for desktop) */}
                <div className="absolute top-1/2 left-10 right-10 h-0.5 bg-gray-200 -z-10 hidden md:block rounded-full"></div>
                {/* Background connecting line (vertical for mobile) */}
                <div className="absolute top-10 bottom-10 left-1/2 w-0.5 bg-gray-200 -z-10 md:hidden rounded-full"></div>

                {/* 1. Upstream Sources */}
                <div className="flex flex-col gap-4 w-full max-w-[320px] relative z-10">
                    <div className="text-center md:text-left mb-2 md:-mt-10">
                        <span className="text-xs font-bold text-gray-500 uppercase tracking-widest bg-white pr-3">Upstream Sources ({upstreams.length})</span>
                    </div>
                    <div className="flex flex-col gap-3">
                        {upstreams.length === 0 ? (
                            <div className="p-6 border-2 border-dashed border-gray-200 rounded-xl bg-gray-50/80 text-center text-gray-400 text-sm shadow-sm">
                                <span className="block mb-1">No upstream tables</span>
                                <span className="text-xs text-gray-400 font-light">This appears to be a source table.</span>
                            </div>
                        ) : upstreams.map((u, i) => (
                            <div key={i} className="group p-3.5 bg-white border border-gray-200 rounded-xl shadow-sm hover:shadow-md hover:border-indigo-300 transition-all flex items-center justify-between">
                                <div className="flex items-center gap-3 overflow-hidden">
                                    <div className="h-10 w-10 flex items-center justify-center rounded-lg bg-indigo-50 border border-indigo-100 shrink-0 text-indigo-500 group-hover:bg-indigo-500 group-hover:text-white transition-colors">
                                        <Database className="h-5 w-5" />
                                    </div>
                                    <div className="truncate">
                                        <p className="text-sm font-bold text-gray-900 truncate">{u.REFERENCED_OBJECT_NAME}</p>
                                        <p className="text-[11px] text-gray-500 truncate font-mono mt-0.5">{u.REFERENCED_DATABASE}.{u.REFERENCED_SCHEMA}</p>
                                    </div>
                                </div>
                                <ArrowRight className="h-4 w-4 text-gray-300 group-hover:text-indigo-500 shrink-0 transition-colors hidden md:block" />
                            </div>
                        ))}
                    </div>
                </div>

                {/* 2. Current Hub */}
                <div className="relative z-10 shrink-0 mx-auto my-8 md:my-0">
                    <div className="absolute -inset-4 bg-blue-500 opacity-10 blur-xl rounded-full animate-pulse"></div>
                    <div className="relative p-7 bg-white border-2 border-blue-500 rounded-2xl shadow-[0_8px_30px_rgb(59,130,246,0.15)] flex flex-col items-center justify-center min-w-[220px] hover:-translate-y-1 transition-transform">
                        <div className="absolute -top-3 -right-3 h-6 w-6 bg-blue-500 text-white rounded-full flex items-center justify-center text-xs font-bold shadow-sm">
                            <CheckCircle2 className="h-3.5 w-3.5" />
                        </div>
                        <div className="h-16 w-16 bg-gradient-to-br from-blue-50 to-blue-100 border border-blue-200 rounded-full flex items-center justify-center mb-4 shadow-inner">
                            <TableIcon className="h-8 w-8 text-blue-600" />
                        </div>
                        <span className="text-[10px] font-bold text-blue-600 uppercase tracking-widest mb-1.5 opacity-80">This Dataset</span>
                        <span className="font-bold text-gray-900 text-lg truncate px-2 w-full text-center max-w-[200px]" title={tableStr}>{tableStr}</span>
                        <div className="flex items-center gap-1 mt-3">
                            <span className="text-[10px] text-gray-500 font-medium bg-gray-100 px-2 py-0.5 rounded-l-full border border-r-0 border-gray-200">{upstreams.length} IN</span>
                            <span className="text-[10px] text-gray-500 font-medium bg-gray-100 px-2 py-0.5 rounded-r-full border border-l-0 border-gray-200">{downstreams.length} OUT</span>
                        </div>
                    </div>
                </div>

                {/* 3. Downstream Consumers */}
                <div className="flex flex-col gap-4 w-full max-w-[320px] relative z-10">
                    <div className="text-center md:text-right mb-2 md:-mt-10">
                        <span className="text-xs font-bold text-gray-500 uppercase tracking-widest bg-white pl-3">Downstream Consumers ({downstreams.length})</span>
                    </div>
                    <div className="flex flex-col gap-3">
                        {downstreams.length === 0 ? (
                            <div className="p-6 border-2 border-dashed border-gray-200 rounded-xl bg-gray-50/80 text-center text-gray-400 text-sm shadow-sm">
                                <span className="block mb-1">No downstream usage</span>
                                <span className="text-xs text-gray-400 font-light">This appears to be a terminal table.</span>
                            </div>
                        ) : downstreams.map((d, i) => (
                            <div key={i} className="group p-3.5 bg-white border border-gray-200 rounded-xl shadow-sm hover:shadow-md hover:border-emerald-400 transition-all flex items-center justify-between">
                                <ArrowRight className="h-4 w-4 text-gray-300 group-hover:text-emerald-500 shrink-0 transition-colors hidden md:block" />
                                <div className="flex items-center gap-3 overflow-hidden text-left md:text-right w-full md:justify-end">
                                    <div className="truncate order-2 md:order-1">
                                        <p className="text-sm font-bold text-gray-900 truncate">{d.REFERENCING_OBJECT_NAME}</p>
                                        <p className="text-[11px] text-gray-500 truncate font-mono mt-0.5">{d.REFERENCING_DATABASE}.{d.REFERENCING_SCHEMA}</p>
                                    </div>
                                    <div className="h-10 w-10 flex items-center justify-center rounded-lg bg-emerald-50 border border-emerald-100 shrink-0 text-emerald-600 group-hover:bg-emerald-500 group-hover:text-white transition-colors order-1 md:order-2">
                                        <Activity className="h-5 w-5" />
                                    </div>
                                </div>
                            </div>
                        ))}
                    </div>
                </div>
            </div>
        </div>
    );
}

// Mocked user role for Phase 1 - ultimately wire to useAppStore() or AuthContext
type AppRole = 'PLATFORM_ADMIN' | 'DATA_OWNER' | 'DATA_STEWARD' | 'DATA_ANALYST' | 'DATA_VIEWER';

export default function DatasetDetailPage() {
    // Current user role mock (change to test visibility)
    const currentUserRole: AppRole = 'PLATFORM_ADMIN';

    const params = useParams();
    const searchParams = useSearchParams();

    // The new catalog passes db.schema.table as the URL parameter
    const fullDatasetPath = decodeURIComponent(params.dataset as string);
    const pathParts = fullDatasetPath.split('.');
    const datasetName = pathParts.length > 0 ? pathParts[pathParts.length - 1] : fullDatasetPath;

    // Use query params if available, otherwise parse from path
    const dbParam = searchParams.get('database') || (pathParts.length === 3 ? pathParts[0] : null);
    const schemaParam = searchParams.get('schema') || (pathParts.length === 3 ? pathParts[1] : null);

    const [details, setDetails] = useState<DatasetDetails | null>(null);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState<string | null>(null);
    const [activeTab, setActiveTab] = useState<'overview' | 'schema' | 'ownership' | 'usage' | 'lineage' | 'access'>('overview');

    // Editable Description state
    const [isEditingDesc, setIsEditingDesc] = useState(false);
    const [descBody, setDescBody] = useState('');
    const [savingDesc, setSavingDesc] = useState(false);

    useEffect(() => {
        const fetchDetails = async () => {
            setLoading(true);
            try {
                let url = `/api/dq/datasets/${datasetName}`;
                if (dbParam && schemaParam) url += `?database=${dbParam}&schema=${schemaParam}`;
                const res = await fetch(url);
                const data = await res.json();

                if (!data.success) {
                    throw new Error(data.error || 'Failed to load details');
                }

                setDetails(data.data);
                setDescBody(data.data.comment || '');
            } catch (err: any) {
                setError(err.message);
            } finally {
                setLoading(false);
            }
        };

        if (datasetName) {
            fetchDetails();
        }
    }, [datasetName, dbParam, schemaParam]);

    const handleSaveComment = async () => {
        setSavingDesc(true);
        try {
            const res = await fetch(`/api/dq/datasets/${datasetName}/comments`, {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    database: details?.database || dbParam,
                    schema: details?.schema || schemaParam,
                    comment: descBody
                })
            });
            const data = await res.json();
            if (data.success && details) {
                setDetails({ ...details, comment: descBody });
                setIsEditingDesc(false);
            } else {
                alert(data.error || 'Failed to update description');
            }
        } catch (e) {
            console.error(e);
            alert('A network error occurred');
        }
        setSavingDesc(false);
    };

    if (loading) {
        return (
            <div className="min-h-screen bg-gray-50 flex items-center justify-center">
                <Loader2 className="h-8 w-8 text-blue-600 animate-spin" />
            </div>
        );
    }

    if (error || !details) {
        return (
            <div className="min-h-screen bg-gray-50 p-8">
                <div className="max-w-4xl mx-auto bg-white p-8 rounded-lg shadow text-center">
                    <XCircle className="h-12 w-12 text-red-500 mx-auto mb-4" />
                    <h2 className="text-xl font-bold text-gray-900 mb-2">Error Loading Dataset</h2>
                    <p className="text-gray-600 mb-6">{error || 'Dataset not found'}</p>
                    <Link href="/data">
                        <Button variant="outline"><ArrowLeft className="h-4 w-4 mr-2" /> Back to Catalog</Button>
                    </Link>
                </div>
            </div>
        );
    }

    const dbToUse = details.database || dbParam || 'Unknown';
    const schemaToUse = details.schema || schemaParam || 'Unknown';

    return (
        <div className="bg-gray-50 font-sans" style={{ height: 'calc(100vh - 64px)', display: 'flex', flexDirection: 'column' }}>
            {/* Compact Header */}
            <div className="bg-white border-b border-gray-200 shadow-sm relative z-10 px-6 lg:px-10 pt-3 pb-0 shrink-0">
                <div className="max-w-[1600px] mx-auto">
                    {/* Breadcrumb */}
                    <Link href="/data" className="inline-flex items-center text-xs font-medium text-gray-400 hover:text-blue-600 mb-2 transition-colors">
                        <ArrowLeft className="h-3.5 w-3.5 mr-1" /> Back to Catalog
                    </Link>

                    <div className="flex items-center justify-between mb-3">
                        {/* Left: Identity */}
                        <div className="flex items-center gap-3">
                            <div className="p-2.5 bg-gradient-to-br from-blue-600 to-indigo-700 text-white rounded-xl shadow-md">
                                <TableIcon className="h-5 w-5" />
                            </div>
                            <div>
                                <div className="flex items-center gap-2">
                                    <h1 className="text-lg font-bold text-gray-900 tracking-tight">{details.name}</h1>
                                    <span className="px-2 py-0.5 bg-blue-50 text-blue-700 text-[10px] font-bold uppercase rounded border border-blue-200 tracking-widest">Base Table</span>
                                </div>
                                <div className="flex items-center gap-1.5 mt-0.5 text-xs text-gray-500">
                                    <Database className="h-3 w-3 text-gray-400" />
                                    <span className="font-medium">{dbToUse}</span>
                                    <span className="text-gray-300">/</span>
                                    <span className="font-medium">{schemaToUse}</span>
                                    <span className="text-gray-300">·</span>
                                    <span>Owner: <span className="font-semibold text-gray-700">{details.database || 'ACCOUNTADMIN'}</span></span>
                                </div>
                            </div>
                        </div>

                        {/* Right: Inline KPIs + DQ Badge */}
                        <div className="flex items-center gap-4">
                            <div className="flex items-center gap-5 text-center">
                                <div>
                                    <p className="text-[10px] font-bold text-gray-400 uppercase tracking-wider">Rows</p>
                                    <p className="text-sm font-bold text-gray-800">{details.rowCount > 1000000 ? (details.rowCount / 1000000).toFixed(1) + 'M' : details.rowCount.toLocaleString()}</p>
                                </div>
                                <div className="w-px h-7 bg-gray-200"></div>
                                <div>
                                    <p className="text-[10px] font-bold text-gray-400 uppercase tracking-wider">Size</p>
                                    <p className="text-sm font-bold text-gray-800">{formatBytes(details.bytes)}</p>
                                </div>
                                <div className="w-px h-7 bg-gray-200"></div>
                                <div>
                                    <p className="text-[10px] font-bold text-gray-400 uppercase tracking-wider">Modified</p>
                                    <p className="text-sm font-bold text-gray-800">{(() => { const diff = Date.now() - new Date(details.lastAltered).getTime(); const hours = Math.floor(diff / 3600000); if (hours < 1) return 'Just now'; if (hours < 24) return hours + 'h ago'; return Math.floor(hours / 24) + 'd ago'; })()}</p>
                                </div>
                            </div>

                            <div className="w-px h-8 bg-gray-200"></div>

                            {/* DQ Score Badge - all tables are under check */}
                            {details.latestDqScore !== null ? (
                                <div className={`flex items-center gap-1.5 px-3 py-1.5 rounded-lg font-bold border shadow-sm text-sm ${
                                    details.latestDqScore >= 95
                                        ? 'bg-emerald-50 text-emerald-700 border-emerald-200'
                                        : details.latestDqScore >= 80
                                            ? 'bg-amber-50 text-amber-700 border-amber-200'
                                            : 'bg-red-50 text-red-700 border-red-200'
                                }`}>
                                    <CheckCircle2 className="w-4 h-4" />
                                    {details.latestDqScore}%
                                </div>
                            ) : (
                                <div className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg font-semibold border border-slate-200 bg-slate-50 text-slate-500 shadow-sm text-xs">
                                    <Clock className="w-3.5 h-3.5" />
                                    Pending Scan
                                </div>
                            )}
                        </div>
                    </div>

                    {/* Tabs */}
                    <div className="flex items-center gap-1.5 bg-gray-50 p-1 rounded-lg border border-gray-200/60 max-w-max mb-2">
                        {[
                            { id: 'overview', label: 'Overview', icon: Info, count: null, requiredRoles: ['PLATFORM_ADMIN', 'DATA_OWNER', 'DATA_STEWARD', 'DATA_ANALYST', 'DATA_VIEWER'] },
                            { id: 'schema', label: 'Schema', icon: Columns, count: details.columns.length, requiredRoles: ['PLATFORM_ADMIN', 'DATA_OWNER', 'DATA_STEWARD', 'DATA_ANALYST', 'DATA_VIEWER'] },
                            { id: 'usage', label: 'Usage', icon: Activity, count: '30d', requiredRoles: ['PLATFORM_ADMIN', 'DATA_OWNER', 'DATA_ANALYST'] },
                            { id: 'lineage', label: 'Lineage', icon: Network, count: null, requiredRoles: ['PLATFORM_ADMIN', 'DATA_OWNER', 'DATA_ANALYST'] },
                            { id: 'access', label: 'Access', icon: Key, count: null, requiredRoles: ['PLATFORM_ADMIN'] },
                            { id: 'ownership', label: 'Ownership', icon: Users, count: null, requiredRoles: ['PLATFORM_ADMIN', 'DATA_OWNER'] },
                        ].filter(tab => tab.requiredRoles.includes(currentUserRole)).map((tab) => (
                            <button
                                key={tab.id}
                                onClick={() => setActiveTab(tab.id as any)}
                                className={`group flex items-center gap-1.5 px-3 py-2 text-[12px] font-semibold transition-all whitespace-nowrap rounded-md
                                    ${activeTab === tab.id
                                        ? 'bg-white text-blue-700 shadow-sm ring-1 ring-gray-900/5'
                                        : 'text-gray-500 hover:text-gray-800 hover:bg-gray-100/50'
                                    }`}
                            >
                                <tab.icon className={`h-3.5 w-3.5 transition-colors ${activeTab === tab.id ? 'text-blue-600' : 'text-gray-400'}`} />
                                {tab.label}
                                {tab.count !== null && (
                                    <span className={`px-1.5 py-0.5 rounded-full text-[10px] ml-0.5 ${activeTab === tab.id ? 'bg-blue-50 text-blue-700 font-bold' : 'bg-gray-100 text-gray-500'}`}>
                                        {tab.count}
                                    </span>
                                )}
                            </button>
                        ))}
                    </div>
                </div>
            </div>

            {/* Content Area */}
            <div className="flex-1 overflow-y-auto">
            <div className="max-w-[1600px] mx-auto px-6 py-5">

                {activeTab === 'overview' && (
                    <div className="grid grid-cols-1 lg:grid-cols-5 gap-4">

                        {/* Left — Governance + DQ Status (2 cols) */}
                        <div className="lg:col-span-2 space-y-4">
                            {/* Data Quality Status */}
                            <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-4">
                                <h3 className="text-[11px] font-bold text-gray-500 uppercase tracking-widest mb-3 flex items-center gap-1.5">
                                    <Shield className="h-3.5 w-3.5 text-emerald-500" /> Data Quality Status
                                </h3>
                                {details.isOnboarded ? (
                                    <Link href={`/datasets/${dbToUse}.${schemaToUse}/tables/${details.name}`} className="block group">
                                        <div className="flex items-center justify-between p-3 rounded-lg border border-emerald-200 bg-emerald-50 group-hover:bg-emerald-100 transition-colors">
                                            <div className="flex items-center gap-2.5">
                                                <div className="h-8 w-8 rounded-full bg-emerald-100 flex items-center justify-center border border-emerald-200">
                                                    <Activity className="h-4 w-4 text-emerald-600" />
                                                </div>
                                                <div>
                                                    <p className="text-sm font-bold text-emerald-800">
                                                        {details.latestDqScore !== null ? `${details.latestDqScore}% Score` : 'Monitored'}
                                                    </p>
                                                    <p className="text-[10px] uppercase text-emerald-600 font-semibold tracking-wider">View DQ Dashboard →</p>
                                                </div>
                                            </div>
                                        </div>
                                    </Link>
                                ) : (
                                    <div className="flex items-center gap-2.5 p-3 rounded-lg border border-blue-200 bg-blue-50 text-xs text-blue-800">
                                        <Clock className="h-4 w-4 text-blue-400 shrink-0" />
                                        Awaiting first DQ scan. All tables are under monitoring.
                                    </div>
                                )}
                            </div>

                            {/* Table Metadata */}
                            <div className="bg-white rounded-xl shadow-sm border border-gray-200 p-4">
                                <h3 className="text-[11px] font-bold text-gray-500 uppercase tracking-widest mb-3">Table Info</h3>
                                <div className="space-y-2.5">
                                    <div className="flex justify-between text-sm">
                                        <span className="text-gray-500">Created</span>
                                        <span className="font-medium text-gray-800">{new Date(details.created).toLocaleDateString()}</span>
                                    </div>
                                    <div className="flex justify-between text-sm">
                                        <span className="text-gray-500">Last Modified</span>
                                        <span className="font-medium text-gray-800">{new Date(details.lastAltered).toLocaleDateString()}</span>
                                    </div>
                                    <div className="flex justify-between text-sm">
                                        <span className="text-gray-500">Columns</span>
                                        <span className="font-medium text-gray-800">{details.columns.length}</span>
                                    </div>
                                    <div className="flex justify-between text-sm">
                                        <span className="text-gray-500">Dataset ID</span>
                                        <span className="font-mono text-xs text-gray-600">{details.datasetId || 'N/A'}</span>
                                    </div>
                                </div>
                            </div>
                        </div>

                        {/* Right — Business Context (3 cols) */}
                        <div className="lg:col-span-3 space-y-4">
                            {/* Business Context */}
                            <div className="bg-white rounded-xl shadow-sm border border-gray-200 overflow-hidden">
                                <div className="px-4 py-3 border-b border-gray-100 bg-gray-50/50 flex items-center justify-between">
                                    <h3 className="text-[11px] font-bold text-gray-500 uppercase tracking-widest flex items-center gap-1.5">
                                        <FileText className="h-3.5 w-3.5 text-blue-500" /> Business Context
                                    </h3>
                                    {['PLATFORM_ADMIN', 'DATA_OWNER', 'DATA_STEWARD'].includes(currentUserRole) && (
                                        <Button variant="outline" size="sm" onClick={() => setIsEditingDesc(!isEditingDesc)} className="h-6 text-[10px] px-2">
                                            <Edit2 className="h-3 w-3 mr-1" /> Edit
                                        </Button>
                                    )}
                                </div>
                                <div className="p-4">
                                    {isEditingDesc ? (
                                        <div className="bg-gray-50 p-3 rounded-lg border border-blue-200">
                                            <textarea
                                                className="w-full text-sm p-2.5 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 outline-none resize-y"
                                                rows={4}
                                                value={descBody}
                                                onChange={(e) => setDescBody(e.target.value)}
                                                placeholder="Document the business purpose..."
                                            />
                                            <div className="flex items-center gap-2 mt-2 justify-end">
                                                <Button size="sm" variant="ghost" onClick={() => setIsEditingDesc(false)} disabled={savingDesc}>Cancel</Button>
                                                <Button size="sm" className="gap-1.5 bg-blue-600 hover:bg-blue-700 h-7 text-xs" onClick={handleSaveComment} disabled={savingDesc}>
                                                    {savingDesc ? <Loader2 className="h-3 w-3 animate-spin" /> : <Save className="h-3 w-3" />} Save
                                                </Button>
                                            </div>
                                        </div>
                                    ) : (
                                        <div className="text-sm text-gray-700 leading-relaxed min-h-[80px]">
                                            {details.comment ? (
                                                <p>{details.comment}</p>
                                            ) : (
                                                <div className="flex flex-col items-center justify-center h-20 text-gray-400 gap-1">
                                                    <FileEdit className="h-6 w-6 text-gray-200" />
                                                    <p className="text-xs italic">No business context documented.</p>
                                                    {['PLATFORM_ADMIN', 'DATA_OWNER', 'DATA_STEWARD'].includes(currentUserRole) && (
                                                        <button onClick={() => setIsEditingDesc(true)} className="text-blue-600 hover:underline text-xs not-italic">Add description</button>
                                                    )}
                                                </div>
                                            )}
                                        </div>
                                    )}
                                </div>
                            </div>
                        </div>
                    </div>
                )}

                {activeTab === 'schema' && (
                    <div className="bg-white rounded-xl shadow-sm border border-gray-200 overflow-hidden animate-in fade-in duration-300">
                        <div className="p-6 border-b border-gray-200 flex items-center justify-between bg-gray-50/50">
                            <h3 className="text-lg font-semibold text-gray-900 flex items-center gap-2">
                                <Columns className="h-5 w-5 text-gray-500" />
                                Entity Schema ({details.columns.length} columns)
                            </h3>
                        </div>
                        <div className="overflow-x-auto">
                            <table className="w-full text-left text-sm">
                                <thead className="bg-gray-50 text-gray-600 font-semibold border-b border-gray-200 uppercase tracking-wider text-xs">
                                    <tr>
                                        <th className="px-6 py-4">Column Name</th>
                                        <th className="px-6 py-4">Data Type</th>
                                        <th className="px-6 py-4">Required</th>
                                        <th className="px-6 py-4">Description</th>
                                    </tr>
                                </thead>
                                <tbody className="divide-y divide-gray-100">
                                    {details.columns.map((col) => {
                                        const isPII = col.COLUMN_NAME.includes('EMAIL') || col.COLUMN_NAME.includes('PHONE') || col.COLUMN_NAME.includes('SSN') || col.COLUMN_NAME.includes('NAME');
                                        return (
                                            <tr key={col.COLUMN_NAME} className="hover:bg-blue-50/50 transition-colors group">
                                                <td className="px-6 py-4">
                                                    <span className="font-semibold text-gray-900 flex items-center gap-2">
                                                        {col.COLUMN_NAME}
                                                        {isPII && <span className="px-1.5 py-0.5 rounded bg-red-100 text-red-700 text-[10px] uppercase font-bold tracking-wider">PII</span>}
                                                    </span>
                                                </td>
                                                <td className="px-6 py-4">
                                                    <span className="font-mono text-xs text-blue-700 bg-blue-50 border border-blue-100 rounded px-2 py-1">
                                                        {col.DATA_TYPE}
                                                    </span>
                                                </td>
                                                <td className="px-6 py-4">
                                                    {col.IS_NULLABLE === 'YES' ? (
                                                        <span className="text-gray-400 text-xs">No</span>
                                                    ) : (
                                                        <span className="text-gray-800 font-medium text-xs">Yes (NOT NULL)</span>
                                                    )}
                                                </td>
                                                <td className="px-6 py-4 text-gray-500 text-sm group-hover:text-gray-900 transition-colors">
                                                    {/* Stub for editable column comment */}
                                                    <div className="flex items-center gap-2">
                                                        <span className={!col.COMMENT ? 'italic text-gray-300' : ''}>{col.COMMENT || 'Add description...'}</span>
                                                        <button className="opacity-0 group-hover:opacity-100 text-gray-400 hover:text-blue-600">
                                                            <Edit2 className="h-3.5 w-3.5" />
                                                        </button>
                                                    </div>
                                                </td>
                                            </tr>
                                        );
                                    })}
                                </tbody>
                            </table>
                        </div>
                    </div>
                )}

                {activeTab === 'ownership' && (
                    <OwnershipTab datasetName={details.name} dbParam={dbToUse} schemaParam={schemaToUse} />
                )}

                {activeTab === 'usage' && (
                    <UsageTab datasetName={details.name} dbParam={dbToUse} schemaParam={schemaToUse} />
                )}

                {activeTab === 'lineage' && (
                    <LineageTab datasetName={details.name} dbParam={dbToUse} schemaParam={schemaToUse} />
                )}

                {activeTab === 'access' && (
                    <AccessTab datasetName={details.name} dbParam={dbToUse} schemaParam={schemaToUse} />
                )}

            </div>
            </div>
        </div>
    );
}
