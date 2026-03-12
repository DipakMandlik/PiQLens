'use client';

import { Card, CardContent } from '@/components/ui/card';
import { TrendingUp, TrendingDown } from 'lucide-react';

interface QualityHealthCardProps {
    score: number;
    status: 'Excellent' | 'Good' | 'Fair' | 'Poor';
    trend?: number;
}

export function QualityHealthCard({ score, status, trend }: QualityHealthCardProps) {
    const getStatusColor = (status: string) => {
        switch (status) {
            case 'Excellent':
                return 'text-green-700';
            case 'Good':
                return 'text-green-600';
            case 'Fair':
                return 'text-yellow-600';
            case 'Poor':
                return 'text-red-600';
            default:
                return 'text-gray-600';
        }
    };

    return (
        <Card>
            <CardContent className="p-6">
                <h3 className="text-sm font-medium text-gray-500 mb-4">Quality Health</h3>
                <div className="flex items-baseline gap-2 mb-2">
                    <span className="text-xs text-gray-500">Quality Score</span>
                    {trend !== undefined && (
                        <span className={`flex items-center text-xs ${trend >= 0 ? 'text-green-600' : 'text-red-600'}`}>
                            {trend >= 0 ? <TrendingUp className="h-3 w-3" /> : <TrendingDown className="h-3 w-3" />}
                            {Math.abs(trend)}%
                        </span>
                    )}
                </div>
                <div className="flex items-end gap-3">
                    <span className="text-4xl font-bold text-gray-900">{score}%</span>
                    <span className={`text-sm font-medium ${getStatusColor(status)} mb-1`}>
                        {status}
                    </span>
                </div>
            </CardContent>
        </Card>
    );
}
