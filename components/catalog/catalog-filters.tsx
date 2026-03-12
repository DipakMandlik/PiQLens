import React from 'react';
import { Database, Folder, BarChart3, ShieldCheck, Tag } from 'lucide-react';

interface CatalogFiltersProps {
    dbs: string[];
    schemas: string[];
    domains: string[];
    selectedDBs: string[];
    setSelectedDBs: React.Dispatch<React.SetStateAction<string[]>>;
    selectedSchemas: string[];
    setSelectedSchemas: React.Dispatch<React.SetStateAction<string[]>>;
    selectedDomains: string[];
    setSelectedDomains: React.Dispatch<React.SetStateAction<string[]>>;
    selectedUsages: string[];
    setSelectedUsages: React.Dispatch<React.SetStateAction<string[]>>;
    minDqScore: number | null;
    setMinDqScore: React.Dispatch<React.SetStateAction<number | null>>;
}

export default function CatalogFilters({
    dbs, schemas, domains,
    selectedDBs, setSelectedDBs,
    selectedSchemas, setSelectedSchemas,
    selectedDomains, setSelectedDomains,
    selectedUsages, setSelectedUsages,
    minDqScore, setMinDqScore
}: CatalogFiltersProps) {

    const toggleFilter = (
        current: string[],
        setter: React.Dispatch<React.SetStateAction<string[]>>,
        value: string
    ) => {
        if (current.includes(value)) {
            setter(current.filter(item => item !== value));
        } else {
            setter([...current, value]);
        }
    };

    const usageLevels = ['High', 'Medium', 'Low', 'Unknown'];
    const dqThresholds = [
        { label: '> 90', value: 90 },
        { label: '> 80', value: 80 },
        { label: '> 70', value: 70 },
        { label: 'Any', value: null }
    ];

    return (
        <div className="space-y-8">

            {/* DB Filter */}
            {dbs.length > 0 && (
                <FilterSection title="Databases" icon={<Database className="w-4 h-4" />}>
                    {dbs.map(db => (
                        <CheckboxItem
                            key={db}
                            label={db}
                            checked={selectedDBs.includes(db)}
                            onChange={() => toggleFilter(selectedDBs, setSelectedDBs, db)}
                        />
                    ))}
                </FilterSection>
            )}

            {/* Domain Filter */}
            {domains.length > 0 && (
                <FilterSection title="Business Domains" icon={<Tag className="w-4 h-4" />}>
                    {domains.map(domain => (
                        <CheckboxItem
                            key={domain}
                            label={domain}
                            checked={selectedDomains.includes(domain)}
                            onChange={() => toggleFilter(selectedDomains, setSelectedDomains, domain)}
                        />
                    ))}
                </FilterSection>
            )}

            {/* Schema Filter */}
            {schemas.length > 0 && (
                <FilterSection title="Schemas" icon={<Folder className="w-4 h-4" />}>
                    <div className="max-h-40 overflow-y-auto custom-scrollbar pr-2 space-y-1">
                        {schemas.map(schema => (
                            <CheckboxItem
                                key={schema}
                                label={schema}
                                checked={selectedSchemas.includes(schema)}
                                onChange={() => toggleFilter(selectedSchemas, setSelectedSchemas, schema)}
                            />
                        ))}
                    </div>
                </FilterSection>
            )}

            {/* Usage Filter */}
            <FilterSection title="Usage Classification" icon={<BarChart3 className="w-4 h-4" />}>
                {usageLevels.map(level => (
                    <CheckboxItem
                        key={level}
                        label={level}
                        checked={selectedUsages.includes(level)}
                        onChange={() => toggleFilter(selectedUsages, setSelectedUsages, level)}
                    />
                ))}
            </FilterSection>

            {/* DQ Score Filter */}
            <FilterSection title="Minimum DQ Score" icon={<ShieldCheck className="w-4 h-4" />}>
                <div className="flex flex-wrap gap-2">
                    {dqThresholds.map(threshold => (
                        <button
                            key={threshold.label}
                            onClick={() => setMinDqScore(threshold.value)}
                            className={`px-3 py-1.5 text-sm font-medium rounded-md transition-all ${minDqScore === threshold.value
                                ? 'bg-blue-100 text-blue-700 border border-blue-200 dark:bg-blue-900/40 dark:text-blue-400 dark:border-blue-800 shadow-sm'
                                : 'bg-zinc-100 text-zinc-600 border border-transparent hover:bg-zinc-200 dark:bg-zinc-800 dark:text-zinc-400 dark:hover:bg-zinc-700'
                                }`}
                        >
                            {threshold.label}
                        </button>
                    ))}
                </div>
            </FilterSection >

        </div >
    );
}

function FilterSection({ title, icon, children }: { title: string, icon: React.ReactNode, children: React.ReactNode }) {
    return (
        <div className="space-y-3">
            <h3 className="text-sm font-semibold uppercase tracking-wider text-zinc-500 dark:text-zinc-400 flex items-center gap-2">
                {icon} {title}
            </h3>
            <div className="space-y-2">
                {children}
            </div>
        </div>
    );
}

function CheckboxItem({ label, checked, onChange }: { label: string, checked: boolean, onChange: () => void }) {
    return (
        <label className="flex items-center gap-3 p-1.5 hover:bg-zinc-50 dark:hover:bg-zinc-900 rounded-md cursor-pointer transition-colors group">
            <div className={`w-4 h-4 flex items-center justify-center rounded border transition-colors ${checked
                    ? 'bg-blue-600 border-blue-600 dark:bg-blue-500 dark:border-blue-500 shadow-sm'
                    : 'border-zinc-300 dark:border-zinc-600 bg-white dark:bg-zinc-950 group-hover:border-blue-400'
                }`}>
                {checked && (
                    <svg viewBox="0 0 14 14" fill="none" className="w-3 h-3 text-white">
                        <path d="M3 8L6 11L11 3.5" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" />
                    </svg>
                )}
            </div>
            <span className="text-sm text-zinc-700 dark:text-zinc-300 font-medium truncate">{label}</span>
        </label >
    );
}
