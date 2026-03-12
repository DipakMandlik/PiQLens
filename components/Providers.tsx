'use client';

import { AppStoreProvider } from '@/lib/store';
import { ToastProvider } from './ui/toast';
import { FilterProvider } from '@/lib/filter-store';

export function Providers({ children }: { children: React.ReactNode }) {
  return (
    <AppStoreProvider>
      <ToastProvider>
        <FilterProvider>
          {children}
        </FilterProvider>
      </ToastProvider>
    </AppStoreProvider>
  );
}

