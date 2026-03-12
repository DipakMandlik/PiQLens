'use client';

import React from 'react';
import { AlertTriangle } from 'lucide-react';
import { Card, CardContent } from '@/components/ui/card';

interface CriticalFailedRecordsCardProps {
  criticalFailedRecords: number;
  isLoading?: boolean;
}

export const CriticalFailedRecordsCard: React.FC<CriticalFailedRecordsCardProps> = ({
  criticalFailedRecords,
  isLoading = false
}) => {
  return (
    <Card className="border-l-4 border-l-orange-500 bg-white shadow-sm">
      <CardContent className="p-6 relative">
        {/* Orange alert triangle icon in top right */}
        <div className="absolute top-4 right-4">
          <div className="w-10 h-10 rounded-full bg-orange-500 flex items-center justify-center">
            <AlertTriangle className="w-6 h-6 text-white" />
          </div>
        </div>

        {/* Label */}
        <div className="mb-4">
          <h3 className="text-xs font-medium text-gray-500 uppercase tracking-wide">
            Critical Failed Records
          </h3>
        </div>

        {/* Value */}
        <div className="flex items-end">
          {isLoading ? (
            <div className="text-4xl font-bold text-gray-900">-</div>
          ) : (
            <div className="text-4xl font-bold text-gray-900 leading-none">
              {criticalFailedRecords.toLocaleString()}
            </div>
          )}
        </div>
      </CardContent>
    </Card>
  );
};