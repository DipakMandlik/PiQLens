'use client';

import React from 'react';
import { Shield } from 'lucide-react';
import { Card, CardContent } from '@/components/ui/card';

interface DataTrustLevelCardProps {
  trustLevel: string;
  isLoading?: boolean;
}

export const DataTrustLevelCard: React.FC<DataTrustLevelCardProps> = ({
  trustLevel,
  isLoading = false
}) => {
  return (
    <Card className="border-l-4 border-l-yellow-500 bg-white shadow-sm">
      <CardContent className="p-6 relative">
        {/* Yellow shield icon in top right */}
        <div className="absolute top-4 right-4">
          <div className="w-10 h-10 rounded-full bg-yellow-500 flex items-center justify-center">
            <Shield className="w-6 h-6 text-white" />
          </div>
        </div>

        {/* Label */}
        <div className="mb-4">
          <h3 className="text-xs font-medium text-gray-500 uppercase tracking-wide">
            Data Trust Level
          </h3>
        </div>

        {/* Value */}
        <div className="flex items-end">
          {isLoading ? (
            <div className="text-2xl font-bold text-gray-900">-</div>
          ) : (
            <div className="text-2xl font-bold text-gray-900 leading-none">
              {trustLevel}
            </div>
          )}
        </div>
      </CardContent>
    </Card>
  );
};