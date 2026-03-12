'use client';

import React from 'react';
import { XCircle } from 'lucide-react';
import { Card, CardContent } from '@/components/ui/card';

interface FailedChecksCardProps {
  totalFailedChecks: number;
  isLoading?: boolean;
  change?: number;
}

export const FailedChecksCard: React.FC<FailedChecksCardProps> = ({
  totalFailedChecks,
  isLoading = false,
  change
}) => {
  return (
    <Card className="border-l-4 border-l-red-500 bg-white shadow-sm">
      <CardContent className="p-6 relative">
        {/* Red X icon in top right */}
        <div className="absolute top-4 right-4">
          <div className="w-10 h-10 rounded-full bg-red-500 flex items-center justify-center">
            <XCircle className="w-6 h-6 text-white" />
          </div>
        </div>

        {/* Label */}
        <div className="mb-4">
          <h3 className="text-xs font-medium text-gray-500 uppercase tracking-wide">
            Failed Checks
          </h3>
        </div>

        {/* Value */}
        <div className="flex items-end">
          {isLoading ? (
            <div className="text-4xl font-bold text-gray-900">-</div>
          ) : (
            <div className="text-4xl font-bold text-gray-900 leading-none">
              {totalFailedChecks.toLocaleString()}
            </div>
          )}
        </div>

        {/* Change indicator */}
        {change !== undefined && !isLoading && (
          <div className="flex items-center gap-1 text-xs font-medium mt-1">
            {change > 0 ? (
              <span className="text-red-600">▲ +{change.toFixed(1)}%</span>
            ) : change < 0 ? (
              <span className="text-green-600">▼ {change.toFixed(1)}%</span>
            ) : (
              <span className="text-gray-400">No Change</span>
            )}
            <span className="text-gray-400 text-[10px] ml-0.5">vs Yesterday</span>
          </div>
        )}
      </CardContent>
    </Card>
  );
};