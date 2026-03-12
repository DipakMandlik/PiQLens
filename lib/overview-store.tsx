'use client';

import React, { createContext, useContext, useMemo, useState } from 'react';
import { DateMetricsPayload, DatasetMetricsPayload, DimensionScoreRow, FailureSummary, OverviewScope, RunDeltaPayload, RunMetricsPayload } from '@/lib/overview/types';

const STORAGE_KEY = 'piq_overview_state_v2';

export interface OverviewState {
  datasetId: string;
  selectedDate: string;
  selectedRunId: string | null;
  runType: string;
  scope: OverviewScope;
  metrics: RunMetricsPayload | DateMetricsPayload | DatasetMetricsPayload | null;
  delta: RunDeltaPayload | null;
  dimensionBreakdown: DimensionScoreRow[];
  failureSummary: FailureSummary | null;
  formulaVersion: string;
  aggregationVersion: string;
  computedAt: string;
}

interface OverviewStoreContextType {
  getState: (key: string) => OverviewState;
  patchState: (key: string, patch: Partial<OverviewState>) => void;
  resetState: (key: string) => void;
}

const defaultOverviewState: OverviewState = {
  datasetId: '',
  selectedDate: '',
  selectedRunId: null,
  runType: 'FULL',
  scope: 'RUN',
  metrics: null,
  delta: null,
  dimensionBreakdown: [],
  failureSummary: null,
  formulaVersion: 'dq_formula_v2.0.0',
  aggregationVersion: 'dq_aggregation_v2.0.0',
  computedAt: '',
};

const OverviewStoreContext = createContext<OverviewStoreContextType | undefined>(undefined);

function loadInitialState(): Record<string, OverviewState> {
  if (typeof window === 'undefined') return {};

  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    if (!raw) return {};

    const parsed = JSON.parse(raw) as Record<string, OverviewState>;
    return parsed && typeof parsed === 'object' ? parsed : {};
  } catch {
    return {};
  }
}

export function OverviewStoreProvider({ children }: { children: React.ReactNode }) {
  const [states, setStates] = useState<Record<string, OverviewState>>(loadInitialState);

  const value = useMemo<OverviewStoreContextType>(() => ({
    getState: (key: string) => states[key] || defaultOverviewState,
    patchState: (key: string, patch: Partial<OverviewState>) => {
      setStates((prev) => {
        const nextState: OverviewState = {
          ...(prev[key] || defaultOverviewState),
          ...patch,
        };

        const next = {
          ...prev,
          [key]: nextState,
        };

        try {
          localStorage.setItem(STORAGE_KEY, JSON.stringify(next));
        } catch {
          // Ignore local storage write errors
        }

        return next;
      });
    },
    resetState: (key: string) => {
      setStates((prev) => {
        const next = { ...prev };
        delete next[key];

        try {
          localStorage.setItem(STORAGE_KEY, JSON.stringify(next));
        } catch {
          // Ignore local storage write errors
        }

        return next;
      });
    },
  }), [states]);

  return (
    <OverviewStoreContext.Provider value={value}>
      {children}
    </OverviewStoreContext.Provider>
  );
}

export function useOverviewState(key: string) {
  const context = useContext(OverviewStoreContext);
  if (!context) {
    throw new Error('useOverviewState must be used within OverviewStoreProvider');
  }

  const state = context.getState(key);

  return {
    state,
    patchState: (patch: Partial<OverviewState>) => context.patchState(key, patch),
    resetState: () => context.resetState(key),
  };
}
