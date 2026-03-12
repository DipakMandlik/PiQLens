'use client';

import React from 'react';
import { CheckCircle2 } from 'lucide-react';
import { Card, CardContent } from '@/components/ui/card';

interface SlaComplianceCardProps {
  slaCompliancePct: number;
  isLoading?: boolean;
}

export const SlaComplianceCard: React.FC<SlaComplianceCardProps> = ({
  slaCompliancePct,
  isLoading = false
}) => {
  return (
    <Card className="border-l-4 border-l-green-500 bg-white shadow-sm">
      <CardContent className="p-6 relative">
        {/* Green checkmark icon in top right */}
        <div className="absolute top-4 right-4">
          <div className="w-10 h-10 rounded-full bg-green-500 flex items-center justify-center">
            <CheckCircle2 className="w-6 h-6 text-white" />
          </div>
        </div>

        {/* Label */}
        <div className="mb-4">
          <h3 className="text-xs font-medium text-gray-500 uppercase tracking-wide">
            SLA Compliance %
          </h3>
        </div>

        {/* Value */}
        <div className="flex items-end">
          {isLoading ? (
            <div className="text-4xl font-bold text-gray-900">-</div>
          ) : (
            <div className="text-4xl font-bold text-gray-900 leading-none">
              {slaCompliancePct.toFixed(1)}%
            </div>
          )}
        </div>

        {/* SLA Status */}
        {!isLoading && (
          <div className="flex items-center gap-1 text-xs font-medium mt-2">
            {slaCompliancePct > 90 ? (
              <span className="text-green-600">SLA Met &gt; 90%</span>
            ) : (
              <span className="text-red-600">SLA Not Met &lt; 90%</span>
            )}
          </div>
        )}
      </CardContent>
    </Card>
  );
};