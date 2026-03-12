'use client';

import { useState, useEffect } from 'react';
import { Dialog, DialogContent, DialogHeader, DialogTitle, DialogFooter, DialogDescription } from '@/components/ui/dialog';
import { Button } from '@/components/ui/button';
import { Loader2, CheckCircle2, AlertCircle } from 'lucide-react';
import { Card } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';

interface Rule {
    rule_id: number;
    rule_name: string;
    rule_type: string;
    rule_level: string;
    description: string;
    threshold_value?: number;
}

interface RunCheckDialogProps {
    isOpen: boolean;
    onClose: () => void;
    database: string;
    schema: string;
    table: string;
    column: string;
    onSuccess?: (results: any[]) => void;
}

export function RunCheckDialog({ isOpen, onClose, database, schema, table, column, onSuccess }: RunCheckDialogProps) {
    const [rules, setRules] = useState<Rule[]>([]);
    const [selectedRules, setSelectedRules] = useState<number[]>([]);
    const [isLoading, setIsLoading] = useState(false);
    const [isScanning, setIsScanning] = useState(false);
    const [error, setError] = useState<string | null>(null);
    const [datasetId, setDatasetId] = useState<string | null>(null);

    // Fetch rules when dialog opens
    useEffect(() => {
        if (isOpen && column) {
            fetchColumnRules();
            setSelectedRules([]);
            setError(null);
        }
    }, [isOpen, column]);

    const fetchColumnRules = async () => {
        setIsLoading(true);
        try {
            // First resolve the dataset_id
            let resolvedDatasetId = `${database}.${schema}.${table}`;
            try {
                const datasetRes = await fetch(
                    `/api/dq/dataset-by-table?database=${encodeURIComponent(database)}&schema=${encodeURIComponent(schema)}&table=${encodeURIComponent(table)}`
                );
                const datasetJson = await datasetRes.json();
                if (datasetJson?.success && datasetJson.data?.dataset_id) {
                    resolvedDatasetId = datasetJson.data.dataset_id;
                }
            } catch (e) {
                console.warn('Could not resolve dataset_id, using default');
            }
            setDatasetId(resolvedDatasetId);

            // Fetch only rules assigned to this column via DATASET_RULE_CONFIG
            const res = await fetch(
                `/api/dq/rules?dqDatabase=DATA_QUALITY_DB&dqSchema=DQ_CONFIG&datasetId=${encodeURIComponent(resolvedDatasetId)}&column=${encodeURIComponent(column)}`
            );
            const json = await res.json();

            if (json.success) {
                setRules(json.data || []);
            } else {
                setError(json.error || 'Failed to load rules');
            }
        } catch (e) {
            setError('Failed to fetch rules');
        } finally {
            setIsLoading(false);
        }
    };

    const handleToggleRule = (id: number) => {
        setSelectedRules(prev =>
            prev.includes(id) ? prev.filter(r => r !== id) : [...prev, id]
        );
    };

    const handleSelectAll = () => {
        if (selectedRules.length === rules.length) {
            setSelectedRules([]);
        } else {
            setSelectedRules(rules.map(r => r.rule_id));
        }
    };

    const handleRunScan = async () => {
        if (selectedRules.length === 0) return;

        setIsScanning(true);
        setError(null);

        try {
            const selectedRuleNames = rules
                .filter(r => selectedRules.includes(r.rule_id))
                .map(r => r.rule_name);

            const resolvedDatasetId = datasetId || `${database}.${schema}.${table}`;

            // Execute rules one by one
            const results = [];
            for (const rule_name of selectedRuleNames) {
                try {
                    const res = await fetch('/api/dq/run-custom-rule', {
                        method: 'POST',
                        headers: { 'Content-Type': 'application/json' },
                        body: JSON.stringify({
                            dataset_id: resolvedDatasetId,
                            rule_name: rule_name,
                            column_name: column,
                            threshold: 0.0,
                            run_mode: 'ADHOC'
                        })
                    });

                    const json = await res.json();

                    if (json.success) {
                        let runMeta: any = {};
                        if (json.data && json.data[0]) {
                            if (json.data[0].SP_RUN_CUSTOM_RULE) {
                                runMeta = JSON.parse(json.data[0].SP_RUN_CUSTOM_RULE);
                            } else {
                                runMeta = json.data[0];
                            }
                        }

                        results.push({
                            rule: rule_name,
                            success: true,
                            daa: [runMeta]
                        });
                    } else {
                        results.push({
                            rule: rule_name,
                            success: false,
                            error: json.error
                        });
                    }
                } catch (ruleError: any) {
                    results.push({
                        rule: rule_name,
                        success: false,
                        error: ruleError.message
                    });
                }
            }

            if (onSuccess) onSuccess(results);
            onClose();
        } catch (e: any) {
            setError(e.message || 'Failed to execute scan');
        } finally {
            setIsScanning(false);
        }
    };

    return (
        <Dialog open={isOpen} onOpenChange={onClose}>
            <DialogContent className="sm:max-w-[600px] bg-white">
                <DialogHeader>
                    <DialogTitle>Run Checks on Column</DialogTitle>
                    <DialogDescription>
                        Showing rules assigned to column <span className="font-mono text-indigo-600 font-bold">{column}</span>
                    </DialogDescription>
                </DialogHeader>

                <div className="py-4">
                    {isLoading ? (
                        <div className="flex justify-center py-8">
                            <Loader2 className="w-8 h-8 animate-spin text-slate-400" />
                        </div>
                    ) : error ? (
                        <div className="flex items-center gap-2 p-4 text-red-600 bg-red-50 rounded-md">
                            <AlertCircle className="w-5 h-5" />
                            <p className="text-sm">{error}</p>
                        </div>
                    ) : rules.length === 0 ? (
                        <div className="text-center py-8">
                            <p className="text-slate-500 mb-2">No rules assigned to this column.</p>
                            <p className="text-xs text-slate-400">
                                Assign rules to this column via Dataset Rule Configuration.
                            </p>
                        </div>
                    ) : (
                        <>
                            {/* Select All */}
                            <div className="flex items-center justify-between mb-3 px-1">
                                <span className="text-xs text-slate-500">{rules.length} rule{rules.length !== 1 ? 's' : ''} assigned</span>
                                <button
                                    onClick={handleSelectAll}
                                    className="text-xs text-indigo-600 hover:text-indigo-800 font-medium"
                                >
                                    {selectedRules.length === rules.length ? 'Deselect All' : 'Select All'}
                                </button>
                            </div>

                            <div className="max-h-[300px] overflow-y-auto space-y-2 pr-2">
                                {rules.map(rule => (
                                    <div
                                        key={rule.rule_id}
                                        className={`
                                            flex items-start gap-3 p-3 rounded-lg border cursor-pointer transition-all
                                            ${selectedRules.includes(rule.rule_id)
                                                ? 'border-indigo-500 bg-indigo-50/50'
                                                : 'border-slate-200 hover:border-indigo-200 hover:bg-slate-50'}
                                        `}
                                        onClick={() => handleToggleRule(rule.rule_id)}
                                    >
                                        <div className={`mt-0.5 w-4 h-4 rounded border flex items-center justify-center transition-colors ${selectedRules.includes(rule.rule_id) ? 'bg-indigo-600 border-indigo-600' : 'border-slate-300 bg-white'}`}>
                                            {selectedRules.includes(rule.rule_id) && <CheckCircle2 className="w-3 h-3 text-white" />}
                                        </div>
                                        <div className="flex-1">
                                            <div className="flex items-center justify-between mb-1">
                                                <span className="font-medium text-sm text-slate-900">{rule.rule_name}</span>
                                                <div className="flex gap-1.5">
                                                    <Badge variant="outline" className="text-[10px] h-5">{rule.rule_type}</Badge>
                                                    <Badge variant="outline" className="text-[10px] h-5 bg-slate-50">{rule.rule_level}</Badge>
                                                </div>
                                            </div>
                                            <p className="text-xs text-slate-500">{rule.description}</p>
                                            {rule.threshold_value != null && (
                                                <p className="text-[10px] text-slate-400 mt-1">Threshold: {rule.threshold_value}%</p>
                                            )}
                                        </div>
                                    </div>
                                ))}
                            </div>
                        </>
                    )}
                </div>

                <DialogFooter className="flex justify-between items-center sm:justify-between">
                    <div className="text-xs text-slate-500">
                        {selectedRules.length} of {rules.length} selected
                    </div>
                    <div className="flex gap-2">
                        <Button variant="outline" onClick={onClose} disabled={isScanning}>Cancel</Button>
                        <Button
                            onClick={handleRunScan}
                            disabled={isScanning || selectedRules.length === 0}
                            className="bg-indigo-600 hover:bg-indigo-700 text-white"
                        >
                            {isScanning ? (
                                <>
                                    <Loader2 className="w-4 h-4 mr-2 animate-spin" />
                                    Running...
                                </>
                            ) : (
                                `Run ${selectedRules.length > 0 ? selectedRules.length : ''} Check${selectedRules.length !== 1 ? 's' : ''}`
                            )}
                        </Button>
                    </div>
                </DialogFooter>
            </DialogContent>
        </Dialog>
    );
}
