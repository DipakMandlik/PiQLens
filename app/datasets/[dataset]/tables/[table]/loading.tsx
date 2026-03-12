/**
 * Table Detail Page — Route-level loading skeleton
 * Heaviest page in the app (Snowflake queries, metrics, charts)
 */

import { Skeleton, SkeletonCard, SkeletonTable } from '@/components/ui/skeleton';
import { Sidebar } from '@/components/layout/Sidebar';
import { TopNav } from '@/components/layout/TopNav';

export default function TableDetailLoading() {
  return (
    <div className="flex flex-col h-screen overflow-hidden bg-gray-50">
      <TopNav />
      <div className="flex flex-1 overflow-hidden">
        <Sidebar />
        <main className="flex-1 overflow-y-auto">
          <div className="px-8 py-6">
            {/* Back link */}
            <Skeleton className="h-4 w-32 mb-4" />

            {/* Breadcrumb */}
            <div className="flex items-center gap-2 mb-2">
              <Skeleton className="h-4 w-20" />
              <Skeleton className="h-4 w-4" />
              <Skeleton className="h-4 w-24" />
              <Skeleton className="h-4 w-4" />
              <Skeleton className="h-4 w-32" />
            </div>

            {/* Table header (name, date picker, actions) */}
            <div className="flex items-center justify-between mb-6">
              <div className="space-y-2">
                <div className="flex items-center gap-3">
                  <Skeleton className="h-8 w-8 rounded" />
                  <Skeleton className="h-7 w-48" />
                  <Skeleton className="h-5 w-14 rounded-full" />
                </div>
                <Skeleton className="h-4 w-64" />
              </div>
              <div className="flex items-center gap-3">
                <Skeleton className="h-10 w-40 rounded-md" />
                <Skeleton className="h-10 w-20 rounded-md" />
                <Skeleton className="h-10 w-20 rounded-md" />
                <Skeleton className="h-10 w-24 rounded-md" />
                <Skeleton className="h-10 w-24 rounded-md" />
              </div>
            </div>

            {/* KPI metric cards */}
            <div className="grid grid-cols-4 gap-4 mb-6">
              <SkeletonCard />
              <SkeletonCard />
              <SkeletonCard />
              <SkeletonCard />
            </div>

            {/* Tabs */}
            <div className="flex items-center gap-1 border-b border-slate-200 mb-6">
              {['Overview', 'Checks', 'Failures', 'Activity', 'Data Preview'].map((tab) => (
                <Skeleton key={tab} className="h-9 w-24 rounded-t-md" />
              ))}
            </div>

            {/* Check results table */}
            <SkeletonTable rows={6} cols={6} />
          </div>
        </main>
      </div>
    </div>
  );
}
