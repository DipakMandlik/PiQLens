import React from 'react';
import { LucideIcon } from 'lucide-react';

interface MetricRowProps {
    icon: LucideIcon;
    label: string;
    value: string | number;
    critical?: boolean;
}

export function MetricRow({ icon: Icon, label, value, critical = false }: MetricRowProps) {
    return (
        <div className="flex items-center justify-between py-1.5">
            <div className="flex items-center gap-2">
                <Icon className={`w-3.5 h-3.5 ${critical ? 'text-red-500' : 'text-slate-400'}`} />
                <span className="text-xs text-slate-600">{label}</span>
            </div>
            <span className={`text-sm font-semibold ${critical ? 'text-red-600' : 'text-slate-900'}`}>
                {typeof value === 'number' ? value.toLocaleString() : value}
            </span>
        </div>
    );
}
