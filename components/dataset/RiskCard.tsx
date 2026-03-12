'use client';

import { Card, CardContent } from '@/components/ui/card';
import { AlertTriangle } from 'lucide-react';

interface RiskCardProps {
    openAnomalies: number;
    slaBreaches: number;
    riskLevel: 'Low' | 'Medium' | 'High';
}

export function RiskCard({ openAnomalies, slaBreaches, riskLevel }: RiskCardProps) {
    const getRiskColor = (level: string) => {
        switch (level) {
            case 'High':
                return 'bg-red-100 text-red-700';
            case 'Medium':
                return 'bg-yellow-100 text-yellow-700';
            case 'Low':
                return 'bg-green-100 text-green-700';
            default:
                return 'bg-gray-100 text-gray-700';
        }
    };

    return (
        <Card>
            <CardContent className="p-6">
                <div className="flex items-center justify-between mb-4">
                    <h3 className="text-sm font-medium text-gray-500">Risk</h3>
                    <AlertTriangle className="h-4 w-4 text-yellow-500" />
                </div>
                <div className="space-y-3">
                    <div>
                        <div className="text-sm text-gray-600 mb-1">Open Anomalies</div>
                        <div className="text-2xl font-bold text-gray-900">{openAnomalies}</div>
                    </div>
                    <div>
                        <div className="text-sm text-gray-600 mb-1">SLA Breaches</div>
                        <div className="text-2xl font-bold text-gray-900">{slaBreaches}</div>
                    </div>
                    <div className="pt-2">
                        <span className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ${getRiskColor(riskLevel)}`}>
                            ● {riskLevel} Risk
                        </span>
                    </div>
                </div>
            </CardContent>
        </Card>
    );
}
