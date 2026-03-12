'use client';

import { useState, useEffect } from 'react';
import {
    Shield,
    Users,
    FileText,
    Clock,
    History,
    Edit2,
    Plus,
    CheckCircle2,
    Save
} from 'lucide-react';
import { Button } from '@/components/ui/button';
import { BackButton } from '@/components/back-button';
import { format } from 'date-fns';

export default function GovernancePage() {
    const [activeTab, setActiveTab] = useState('OWNERSHIP');
    const [data, setData] = useState<any[]>([]);
    const [loading, setLoading] = useState(false);
    const [editingId, setEditingId] = useState<string | null>(null);
    const [editFormData, setEditFormData] = useState<any>({});

    // Fetch logic
    const fetchData = async () => {
        setLoading(true);
        try {
            let endpoint = '/api/governance/ownership';
            if (activeTab === 'SLA') endpoint = '/api/governance/slas';
            if (activeTab === 'POLICIES') endpoint = '/api/governance/policies';
            if (activeTab === 'AUDIT') endpoint = '/api/governance/audit';

            const res = await fetch(endpoint);
            const json = await res.json();
            if (json.success) setData(json.data || []);
            else setData([]);
        } catch (e) {
            console.error('Fetch error:', e);
        } finally {
            setLoading(false);
        }
    };

    useEffect(() => {
        fetchData();
    }, [activeTab]);

    // Handle Edit/Save for Ownership
    const handleEditOwnership = (row: any) => {
        setEditingId(row.TABLE_NAME);
        setEditFormData({
            dataset: row.TABLE_NAME,
            owner: row.DATA_OWNER || '',
            steward: row.DATA_STEWARD || '',
            criticality: row.CRITICALITY || 'MEDIUM',
            email: row.CONTACT_EMAIL || ''
        });
    };

    const handleSaveOwnership = async () => {
        try {
            await fetch('/api/governance/ownership', {
                method: 'POST',
                body: JSON.stringify(editFormData)
            });
            setEditingId(null);
            fetchData();
        } catch (e) {
            alert('Failed to save');
        }
    };

    const navItems = [
        { id: 'OWNERSHIP', label: 'Data Ownership', icon: Users },
        { id: 'RULES', label: 'Quality Rules', icon: CheckCircle2 },
        { id: 'SLA', label: 'SLA Management', icon: Clock },
        { id: 'POLICIES', label: 'Policies', icon: FileText },
        { id: 'AUDIT', label: 'Audit Log', icon: History },
    ];

    return (
        <div className="h-[calc(100vh-64px)] flex bg-gray-50">
            {/* Sidebar */}
            <div className="w-64 bg-white border-r border-gray-200 flex-shrink-0">
                <div className="p-6 border-b border-gray-200">
                    <div className="mb-4">
                        <BackButton href="/" label="Back to Home" />
                    </div>
                    <h1 className="text-xl font-bold text-gray-900 flex items-center gap-2">
                        <Shield className="h-6 w-6 text-purple-600" />
                        Governance
                    </h1>
                    <p className="text-xs text-gray-500 mt-1">Control Plane</p>
                </div>
                <nav className="p-4 space-y-1">
                    {navItems.map(item => (
                        <button
                            key={item.id}
                            onClick={() => setActiveTab(item.id)}
                            className={`
                  w-full flex items-center gap-3 px-3 py-2 text-sm font-medium rounded-md transition-colors
                  ${activeTab === item.id
                                    ? 'bg-purple-50 text-purple-700'
                                    : 'text-gray-700 hover:bg-gray-100'}
                `}
                        >
                            <item.icon className="h-4 w-4" />
                            {item.label}
                        </button>
                    ))}
                </nav>
            </div>

            {/* Main Content */}
            <div className="flex-1 overflow-auto p-8">
                <div className="max-w-5xl mx-auto">
                    <div className="mb-6">
                        <h2 className="text-2xl font-bold text-gray-900">
                            {navItems.find(i => i.id === activeTab)?.label}
                        </h2>
                        <p className="text-gray-500 text-sm">
                            {activeTab === 'OWNERSHIP' && 'Define who is accountable for each dataset.'}
                            {activeTab === 'RULES' && 'View defined quality rules (Read Only).'}
                            {activeTab === 'SLA' && 'Manage service level agreements.'}
                            {activeTab === 'POLICIES' && 'Organization-wide governance policies.'}
                        </p>
                    </div>

                    {/* Content Area */}
                    <div className="bg-white rounded-lg shadow-sm border border-gray-200 overflow-hidden">
                        {loading && <div className="p-8 text-center text-gray-500">Loading configuration...</div>}

                        {!loading && activeTab === 'OWNERSHIP' && (
                            <table className="w-full text-sm text-left">
                                <thead className="bg-gray-50 text-gray-500 font-medium border-b border-gray-200">
                                    <tr>
                                        <th className="px-6 py-3">Dataset</th>
                                        <th className="px-6 py-3">Owner</th>
                                        <th className="px-6 py-3">Steward</th>
                                        <th className="px-6 py-3">Criticality</th>
                                        <th className="px-6 py-3 text-right">Actions</th>
                                    </tr>
                                </thead>
                                <tbody className="divide-y divide-gray-100">
                                    {data.map((row: any) => (
                                        <tr key={row.TABLE_NAME}>
                                            <td className="px-6 py-3 font-medium text-gray-900">{row.TABLE_NAME}</td>

                                            {editingId === row.TABLE_NAME ? (
                                                <>
                                                    <td className="px-6 py-3">
                                                        <input
                                                            className="border rounded px-2 py-1 w-full"
                                                            value={editFormData.owner}
                                                            onChange={e => setEditFormData({ ...editFormData, owner: e.target.value })}
                                                        />
                                                    </td>
                                                    <td className="px-6 py-3">
                                                        <input
                                                            className="border rounded px-2 py-1 w-full"
                                                            value={editFormData.steward}
                                                            onChange={e => setEditFormData({ ...editFormData, steward: e.target.value })}
                                                        />
                                                    </td>
                                                    <td className="px-6 py-3">
                                                        <select
                                                            className="border rounded px-2 py-1 w-full"
                                                            value={editFormData.criticality}
                                                            onChange={e => setEditFormData({ ...editFormData, criticality: e.target.value })}
                                                        >
                                                            <option value="HIGH">HIGH</option>
                                                            <option value="MEDIUM">MEDIUM</option>
                                                            <option value="LOW">LOW</option>
                                                        </select>
                                                    </td>
                                                    <td className="px-6 py-3 text-right">
                                                        <Button size="sm" onClick={handleSaveOwnership} className="gap-2">
                                                            <Save className="h-3 w-3" /> Save
                                                        </Button>
                                                    </td>
                                                </>
                                            ) : (
                                                <>
                                                    <td className="px-6 py-3 text-gray-600">{row.DATA_OWNER || '-'}</td>
                                                    <td className="px-6 py-3 text-gray-600">{row.DATA_STEWARD || '-'}</td>
                                                    <td className="px-6 py-3">
                                                        <span className={`px-2 py-0.5 rounded text-xs font-bold ${row.CRITICALITY === 'HIGH' ? 'bg-red-100 text-red-700' :
                                                            row.CRITICALITY === 'LOW' ? 'bg-gray-100 text-gray-600' : 'bg-blue-50 text-blue-700'
                                                            }`}>
                                                            {row.CRITICALITY || 'MEDIUM'}
                                                        </span>
                                                    </td>
                                                    <td className="px-6 py-3 text-right">
                                                        <button onClick={() => handleEditOwnership(row)} className="text-blue-600 hover:text-blue-800">
                                                            <Edit2 className="h-4 w-4" />
                                                        </button>
                                                    </td>
                                                </>
                                            )}
                                        </tr>
                                    ))}
                                </tbody>
                            </table>
                        )}

                        {!loading && activeTab === 'RULES' && (
                            <div className="p-8 text-center text-gray-500">
                                <CheckCircle2 className="h-12 w-12 mx-auto mb-4 text-gray-300" />
                                <p>Quality Rules are defined in `DATASET_RULE_CONFIG`.</p>
                                <p className="text-sm mt-2">This view will be implemented in V1.1 to show read-only rules.</p>
                            </div>
                        )}

                        {!loading && activeTab === 'SLA' && (
                            <div className="p-6">
                                <div className="flex justify-end mb-4">
                                    <Button className="gap-2 bg-purple-600 hover:bg-purple-700">
                                        <Plus className="h-4 w-4" /> Define SLA
                                    </Button>
                                </div>
                                <table className="w-full text-sm text-left">
                                    <thead className="bg-gray-50 text-gray-500 font-medium border-b border-gray-200">
                                        <tr>
                                            <th className="px-6 py-3">Dataset</th>
                                            <th className="px-6 py-3">Type</th>
                                            <th className="px-6 py-3">Threshold</th>
                                            <th className="px-6 py-3">Window</th>
                                            <th className="px-6 py-3">Status</th>
                                        </tr>
                                    </thead>
                                    <tbody className="divide-y divide-gray-100">
                                        {data.length === 0 ? (
                                            <tr><td colSpan={5} className="px-6 py-8 text-center text-gray-400">No SLAs defined</td></tr>
                                        ) : (
                                            data.map((row: any) => (
                                                <tr key={row.SLA_ID}>
                                                    <td className="px-6 py-3 font-medium">{row.DATASET_NAME}</td>
                                                    <td className="px-6 py-3">{row.SLA_TYPE}</td>
                                                    <td className="px-6 py-3 font-mono text-xs bg-gray-50 rounded w-fit px-2">{row.THRESHOLD_VALUE}</td>
                                                    <td className="px-6 py-3">{row.WINDOW_HOURS}h</td>
                                                    <td className="px-6 py-3">
                                                        <span className={`px-2 py-0.5 rounded text-xs ${row.ENABLED ? 'bg-green-100 text-green-700' : 'bg-gray-100 text-gray-500'}`}>
                                                            {row.ENABLED ? 'Active' : 'Disabled'}
                                                        </span>
                                                    </td>
                                                </tr>
                                            ))
                                        )}
                                    </tbody>
                                </table>
                            </div>
                        )}

                        {!loading && activeTab === 'POLICIES' && (
                            <div className="divide-y divide-gray-100">
                                {data.map((policy: any) => (
                                    <div key={policy.POLICY_ID} className="p-6">
                                        <div className="flex items-start justify-between">
                                            <div>
                                                <h3 className="text-lg font-medium text-gray-900">{policy.POLICY_NAME}</h3>
                                                <p className="text-gray-600 mt-1">{policy.DESCRIPTION}</p>
                                            </div>
                                            <span className={`px-2 py-1 rounded text-xs font-bold ${policy.IS_ENFORCED ? 'bg-purple-100 text-purple-700' : 'bg-gray-100 text-gray-600'}`}>
                                                {policy.IS_ENFORCED ? 'Enforced' : 'Guideline'}
                                            </span>
                                        </div>
                                        <div className="mt-3 text-xs text-gray-400 font-mono">
                                            Scope: {policy.SCOPE}
                                        </div>
                                    </div>
                                ))}
                            </div>
                        )}

                        {!loading && activeTab === 'AUDIT' && (
                            <div className="p-6 space-y-6">
                                {data.map((log: any) => (
                                    <div key={log.AUDIT_ID} className="flex gap-4">
                                        <div className="flex-shrink-0 mt-1">
                                            <History className="h-4 w-4 text-gray-400" />
                                        </div>
                                        <div>
                                            <p className="text-sm text-gray-900">
                                                <span className="font-bold">{log.ACTION}</span> on {log.ENTITY_TYPE} ({log.ENTITY_ID})
                                            </p>
                                            <p className="text-xs text-gray-500 mt-0.5">
                                                by {log.CHANGED_BY} • {format(new Date(log.CHANGED_AT), 'MMM d, yyyy HH:mm')}
                                            </p>
                                            <pre className="mt-2 bg-gray-50 p-2 rounded text-[10px] text-gray-600 overflow-x-auto max-w-lg">
                                                {JSON.stringify(JSON.parse(log.NEW_VALUE || '{}'), null, 2)}
                                            </pre>
                                        </div>
                                    </div>
                                ))}
                            </div>
                        )}

                    </div>
                </div>
            </div>
        </div>
    );
}
