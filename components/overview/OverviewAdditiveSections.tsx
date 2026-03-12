'use client';

import { useEffect, useState } from 'react';
import { Card } from '@/components/ui/card';
import {
  DatasetMetricsPayload,
} from '@/lib/overview/types';

interface OverviewAdditiveSectionsProps {
  datasetId: string | null;
  selectedRunId: string | null;
  selectedDate: string;
  selectedMode: any;
  onDrillToFailedRecords: () => void;
}

export function OverviewAdditiveSections({
  datasetId,
  selectedRunId,
  selectedDate,
  selectedMode,
  onDrillToFailedRecords,
}: OverviewAdditiveSectionsProps) {
  const [datasetMetrics, setDatasetMetrics] = useState<DatasetMetricsPayload | null>(null);
  const [runMetrics, setRunMetrics] = useState<any | null>(null);
  const [runDelta, setRunDelta] = useState<any | null>(null);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    let cancelled = false;

    async function fetchDatasetMetrics() {
      if (!datasetId) return;
      const response = await fetch(`/api/metrics/dataset/${encodeURIComponent(datasetId)}`);
      const result = await response.json();
      if (!cancelled && result.success) {
        setDatasetMetrics(result.data as DatasetMetricsPayload);
      }
    }

    fetchDatasetMetrics().catch(() => {
      if (!cancelled) {
        setDatasetMetrics(null);
      }
    });

    return () => {
      cancelled = true;
    };
  }, [datasetId]);



  return (
    <div className="space-y-6"></div>
  );
}


