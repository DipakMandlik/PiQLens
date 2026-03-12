'use client';

import React, { createContext, useContext, useState, ReactNode } from 'react';

interface FilterState {
  timeRange: string;
  selectedDatasets: string[];
}

interface FilterContextType {
  filters: FilterState;
  setTimeRange: (range: string) => void;
  setSelectedDatasets: (datasets: string[]) => void;
  resetFilters: () => void;
}

const FilterContext = createContext<FilterContextType | undefined>(undefined);

export function FilterProvider({ children }: { children: ReactNode }) {
  const [filters, setFilters] = useState<FilterState>({
    timeRange: 'Last 30 Days',
    selectedDatasets: [],
  });

  const setTimeRange = (range: string) => {
    setFilters((prev) => ({ ...prev, timeRange: range }));
  };

  const setSelectedDatasets = (datasets: string[]) => {
    setFilters((prev) => ({ ...prev, selectedDatasets: datasets }));
  };

  const resetFilters = () => {
    setFilters({
      timeRange: 'Last 30 Days',
      selectedDatasets: [],
    });
  };

  return (
    <FilterContext.Provider
      value={{
        filters,
        setTimeRange,
        setSelectedDatasets,
        resetFilters,
      }}
    >
      {children}
    </FilterContext.Provider>
  );
}

export function useFilters() {
  const context = useContext(FilterContext);
  if (context === undefined) {
    throw new Error('useFilters must be used within a FilterProvider');
  }
  return context;
}

