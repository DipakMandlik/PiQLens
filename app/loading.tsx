/**
 * Dashboard (Home) — Route-level loading skeleton
 * Shows instantly while API calls fetch data
 */

import { SkeletonHeader, SkeletonCircularCard, SkeletonAttentionCard, Skeleton } from '@/components/ui/skeleton';
import { Sidebar } from '@/components/layout/Sidebar';
import { TopNav } from '@/components/layout/TopNav';

export default function DashboardLoading() {
  return (
    <div className="flex flex-col h-screen overflow-hidden bg-gray-50">
      <TopNav />

      <div className="flex flex-1 overflow-hidden">
        <Sidebar />

        <main className="flex-1 overflow-y-auto">
          <div className="px-8 py-6">
            {/* Header skeleton */}
            <div className="mb-8">
              <SkeletonHeader />
            </div>

            {/* KPI cards skeleton — 3 circular metric cards */}
            <div className="grid grid-cols-3 gap-6 mb-6">
              <SkeletonCircularCard />
              <SkeletonCircularCard />
              <SkeletonCircularCard />
            </div>

            {/* Attention card skeleton */}
            <div className="mb-8">
              <SkeletonAttentionCard />
            </div>

            {/* Timeline skeleton */}
            <div className="bg-white border border-slate-200 rounded-lg shadow-sm p-6">
              <Skeleton className="h-5 w-40 mb-4" />
              <div className="space-y-3">
                {Array.from({ length: 4 }).map((_, i) => (
                  <div key={i} className="flex items-center gap-4">
                    <Skeleton className="h-3 w-3 rounded-full shrink-0" />
                    <Skeleton className="h-12 flex-1 rounded" />
                  </div>
                ))}
              </div>
            </div>
          </div>
        </main>
      </div>
    </div>
  );
}
