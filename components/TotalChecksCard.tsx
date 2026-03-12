'use client';

import React from 'react';
import { CheckCircle2 } from 'lucide-react';
import { Card, CardContent } from '@/components/ui/card';

interface TotalChecksCardProps {
  totalChecks: number;
  isLoading?: boolean;
}

export const TotalChecksCard: React.FC<TotalChecksCardProps> = ({ 
  totalChecks, 
  isLoading = false 
}) => {
  return (
    <Card className="border-l-4 border-l-purple-500 bg-white shadow-sm">
      <CardContent className="p-6 relative">
        {/* Purple checkmark icon in top right */}
        <div className="absolute top-4 right-4">
          <div className="w-10 h-10 rounded-full bg-purple-500 flex items-center justify-center">
            <CheckCircle2 className="w-6 h-6 text-white" />
          </div>
        </div>

        {/* Label */}
        <div className="mb-4">
          <h3 className="text-xs font-medium text-gray-500 uppercase tracking-wide">
            Total Checks
          </h3>
        </div>

        {/* Value */}
        <div className="flex items-end">
          {isLoading ? (
            <div className="text-4xl font-bold text-gray-900">-</div>
          ) : (
            <div className="text-4xl font-bold text-gray-900 leading-none">
              {totalChecks.toLocaleString()}
            </div>
          )}
        </div>
      </CardContent>
    </Card>
  );
};

