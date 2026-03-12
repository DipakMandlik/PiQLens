'use client';

import { Activity, ArrowRight, Zap } from 'lucide-react';

interface Event {
    eventType: string;
    title: string;
    severity: string;
    detail: string;
    timestamp: string;
}

interface ColumnAnomaliesDriftProps {
    events: Event[];
}

export function ColumnAnomaliesDrift({ events }: ColumnAnomaliesDriftProps) {
    if (events.length === 0) {
        return null;
    }

    return (
        <div className="mb-8">
            <h4 className="text-sm font-bold text-slate-900 uppercase tracking-wide mb-4">Anomalies & Drift Events</h4>

            <div className="relative pl-6 border-l-2 border-slate-200 ml-3 space-y-6">
                {events.map((event, idx) => (
                    <div key={idx} className="relative">
                        {/* Timeline Dot */}
                        <div className={`absolute -left-[31px] top-1 w-4 h-4 rounded-full border-2 border-white shadow-sm ${event.severity === 'HIGH' || event.severity === 'CRITICAL' ? 'bg-red-500' :
                            event.severity === 'MEDIUM' ? 'bg-amber-500' : 'bg-blue-500'
                            }`}></div>

                        <div className="bg-white border border-slate-200 rounded-lg p-3 shadow-sm hover:shadow-md transition-shadow">
                            <div className="flex items-center justify-between mb-1">
                                <div className="flex items-center gap-2">
                                    {event.eventType === 'DRIFT' ? (
                                        <ArrowRight className="w-3 h-3 text-purple-500" />
                                    ) : (
                                        <Zap className="w-3 h-3 text-amber-500" />
                                    )}
                                    <span className="text-xs font-bold text-slate-700 uppercase">{event.eventType}</span>
                                    <span className={`text-[10px] px-1.5 py-0.5 rounded uppercase font-bold ${event.severity === 'HIGH' || event.severity === 'CRITICAL' ? 'bg-red-50 text-red-600' :
                                        event.severity === 'MEDIUM' ? 'bg-amber-50 text-amber-600' : 'bg-blue-50 text-blue-600'
                                        }`}>{event.severity}</span>
                                </div>
                                <span className="text-xs text-slate-400">
                                    {new Date(event.timestamp).toLocaleDateString()}
                                </span>
                            </div>
                            <h5 className="text-sm font-semibold text-slate-800 mb-1">{event.title}</h5>
                            <p className="text-xs text-slate-600">{event.detail}</p>
                        </div>
                    </div>
                ))}
            </div>
        </div>
    );
}
