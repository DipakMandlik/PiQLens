'use client';

import { useEffect, useState } from 'react';
import Link from 'next/link';
import { useRouter } from 'next/navigation';
import {
    Database,
    Search,
    Table as TableIcon,
    Calendar,
    HardDrive,
    AlertCircle,
    RefreshCw,
    Loader2,
    ArrowLeft,
    ChevronDown,
    ExternalLink
} from 'lucide-react';
import { Button } from '@/components/ui/button';
import { format } from 'date-fns';


interface HierarchyTable {
    id: string;
    name: string;
    href: string;
}

interface HierarchySchema {
    id: string;
    name: string;
    tables: HierarchyTable[];
}

interface HierarchyDatabase {
    id: string;
    name: string;
    schemas: HierarchySchema[];
}

export default function DataCatalogPage() {
    const router = useRouter();
    const [databases, setDatabases] = useState<HierarchyDatabase[]>([]);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState<string | null>(null);
    const [search, setSearch] = useState('');
    const [expandedDbs, setExpandedDbs] = useState<Record<string, boolean>>({});
    const [expandedSchemas, setExpandedSchemas] = useState<Record<string, boolean>>({});

    const fetchHierarchy = async () => {
        setLoading(true);
        setError(null);
        try {
            const res = await fetch('/api/snowflake/database-hierarchy');
            const data = await res.json();

            if (data.success && data.data) {
                setDatabases(data.data);

                // Keep first DB expanded by default
                if (data.data.length > 0) {
                    setExpandedDbs({ [data.data[0].id]: true });
                }
            } else {
                setError(data.error?.message || 'Failed to load database hierarchy');
            }
        } catch (err: any) {
            console.error('Hierarchy fetch error:', err);
            setError(err.message || 'Snowflake connection failed');
        } finally {
            setLoading(false);
        }
    };

    useEffect(() => {
        fetchHierarchy();
    }, []);

    const toggleDb = (id: string) => {
        setExpandedDbs(prev => ({ ...prev, [id]: !prev[id] }));
    };

    const toggleSchema = (id: string) => {
        setExpandedSchemas(prev => ({ ...prev, [id]: !prev[id] }));
    };

    // Filter logic
    const filteredDatabases = databases.map(db => {
        const filteredSchemas = db.schemas.map(schema => {
            const searchLower = search.toLowerCase();
            const filteredTables = schema.tables.filter(table =>
                table.name.toLowerCase().includes(searchLower) ||
                schema.name.toLowerCase().includes(searchLower) ||
                db.name.toLowerCase().includes(searchLower)
            );
            return { ...schema, tables: filteredTables };
        }).filter(schema => schema.tables.length > 0);
        return { ...db, schemas: filteredSchemas };
    }).filter(db => db.schemas.length > 0);

    return (
        <div className="min-h-screen bg-gray-50">
            {/* Header */}
            <div className="bg-white border-b border-gray-200">
                <div className="max-w-[1600px] mx-auto px-6 py-8">
                    <div className="flex items-center justify-between mb-6">
                        <div className="flex items-center gap-4">
                            <Button
                                variant="outline"
                                size="sm"
                                onClick={() => router.push('/')}
                                className="flex items-center gap-2"
                            >
                                <ArrowLeft className="h-4 w-4" />
                                Back
                            </Button>
                            <div>
                                <h1 className="text-2xl font-bold text-gray-900 flex items-center gap-2">
                                    <Database className="h-6 w-6 text-blue-600" />
                                    Data Catalog
                                </h1>
                                <div className="flex items-center gap-2 mt-1">
                                    <span className="text-gray-500 text-sm">Organized by Database and Schema</span>
                                </div>
                            </div>
                        </div>
                        <div className="flex items-center gap-3">
                            <div className="flex items-center gap-2 px-3 py-1.5 bg-green-50 text-green-700 rounded-full text-sm font-medium border border-green-200">
                                <div className="h-2 w-2 bg-green-500 rounded-full animate-pulse" />
                                Snowflake Connected
                            </div>
                            <Button variant="outline" size="sm" onClick={fetchHierarchy}>
                                <RefreshCw className={`h-4 w-4 mr-2 ${loading ? 'animate-spin' : ''}`} />
                                Refresh
                            </Button>
                        </div>
                    </div>


                    <div className="relative max-w-lg">
                        <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-5 w-5 text-gray-400" />
                        <input
                            type="text"
                            placeholder="Search datasets..."
                            className="w-full pl-10 pr-4 py-2 border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500 outline-none transition-all"
                            value={search}
                            onChange={(e) => setSearch(e.target.value)}
                        />
                    </div>
                </div>
            </div>

            {/* Content */}
            <div className="max-w-[1600px] mx-auto px-6 py-8">

                {loading && databases.length === 0 ? (
                    <div className="flex flex-col items-center justify-center py-20">
                        <Loader2 className="h-10 w-10 text-blue-600 animate-spin mb-4" />
                        <p className="text-gray-500">Loading database hierarchy from Snowflake...</p>
                    </div>
                ) : error ? (
                    <div className="rounded-lg border border-red-200 bg-red-50 p-6 flex flex-col items-center justify-center text-center">
                        <AlertCircle className="h-10 w-10 text-red-500 mb-2" />
                        <h3 className="text-lg font-semibold text-red-700">Connection Error</h3>
                        <p className="text-red-600 mb-4">{error}</p>
                        <Button onClick={fetchHierarchy} variant="destructive">Try Again</Button>
                    </div>
                ) : filteredDatabases.length === 0 ? (
                    <div className="text-center py-20 text-gray-500">
                        No datasets found matching your search.
                    </div>
                ) : (
                    <div className="bg-white border border-gray-200 rounded-xl shadow-sm overflow-hidden">
                        {filteredDatabases.map((db, dbIndex) => (
                            <div key={db.id} className={`${dbIndex !== filteredDatabases.length - 1 ? 'border-b border-gray-100' : ''}`}>
                                {/* Database Header */}
                                <div
                                    className="flex items-center justify-between p-4 bg-gray-50/80 hover:bg-blue-50/50 cursor-pointer transition-colors"
                                    onClick={() => toggleDb(db.id)}
                                >
                                    <div className="flex items-center gap-3">
                                        <ChevronDown className={`h-5 w-5 text-gray-400 transition-transform ${expandedDbs[db.id] ? '' : '-rotate-90'}`} />
                                        <Database className="h-5 w-5 text-blue-600" />
                                        <h2 className="text-lg font-semibold text-gray-900 tracking-tight">{db.name}</h2>
                                    </div>
                                    <span className="text-xs font-medium text-gray-500 bg-gray-200/50 px-2.5 py-1 rounded-full">{db.schemas.length} Schemas</span>
                                </div>

                                {/* Schemas List (Expanded) */}
                                {expandedDbs[db.id] && (
                                    <div className="divide-y divide-gray-100">
                                        {db.schemas.map((schema) => (
                                            <div key={schema.id} className="bg-white">
                                                {/* Schema Header */}
                                                <div
                                                    className="flex items-center justify-between py-3 px-6 pl-12 hover:bg-gray-50 cursor-pointer transition-colors"
                                                    onClick={() => toggleSchema(schema.id)}
                                                >
                                                    <div className="flex items-center gap-2.5">
                                                        <ChevronDown className={`h-4 w-4 text-gray-400 transition-transform flex-shrink-0 ${expandedSchemas[schema.id] ? '' : '-rotate-90'}`} />
                                                        <span className="font-mono text-[15px] font-medium text-slate-700">{schema.name}</span>
                                                    </div>
                                                    <span className="text-sm text-gray-400">{schema.tables.length} Tables</span>
                                                </div>

                                                {/* Tables Grid (Expanded) */}
                                                {expandedSchemas[schema.id] && (
                                                    <div className="p-4 pl-16 pr-8 bg-slate-50/30">
                                                        <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-3">
                                                            {schema.tables.map(table => (
                                                                <button
                                                                    key={table.id}
                                                                    onClick={() => router.push(`/datasets/${db.name}.${schema.name}.${table.name}?database=${db.name}&schema=${schema.name}`)}
                                                                    className="group flex flex-col p-4 bg-white border border-gray-200 rounded-lg hover:border-blue-400 hover:shadow-sm hover:ring-1 hover:ring-blue-100 transition-all text-left"
                                                                >
                                                                    <div className="flex items-start justify-between w-full mb-1">
                                                                        <div className="flex items-center gap-2.5 mb-2">
                                                                            <TableIcon className="h-4 w-4 text-blue-500" />
                                                                            <span className="font-semibold text-gray-900 truncate" title={table.name}>{table.name}</span>
                                                                        </div>
                                                                        <ExternalLink className="h-3.5 w-3.5 text-gray-300 group-hover:text-blue-500 opacity-0 group-hover:opacity-100 transition-opacity" />
                                                                    </div>
                                                                    <span className="text-xs text-slate-500 font-mono truncate">{db.name}.{schema.name}</span>
                                                                </button>
                                                            ))}
                                                        </div>
                                                    </div>
                                                )}
                                            </div>
                                        ))}
                                    </div>
                                )}
                            </div>
                        ))}
                    </div>
                )}
            </div>
        </div>
    );
}
