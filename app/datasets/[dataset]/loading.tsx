/**
 * Datasets — Route-level loading skeleton
 */

import { Skeleton, SkeletonCard, SkeletonTable } from '@/components/ui/skeleton';
import { Sidebar } from '@/components/layout/Sidebar';
import { TopNav } from '@/components/layout/TopNav';

export default function DatasetsLoading() {
  return (
    <div className="flex flex-col h-screen overflow-hidden bg-gray-50">
      <TopNav />
      <div className="flex flex-1 overflow-hidden">
        <Sidebar />
        <main className="flex-1 overflow-y-auto">
          <div className="px-8 py-6">
            {/* Breadcrumb */}
            <div className="flex items-center gap-2 mb-4">
              <Skeleton className="h-4 w-20" />
              <Skeleton className="h-4 w-4" />
              <Skeleton className="h-4 w-32" />
            </div>

            {/* Title + metadata */}
            <div className="mb-6">
              <Skeleton className="h-7 w-56 mb-2" />
              <Skeleton className="h-4 w-80" />
            </div>

            {/* Summary cards */}
            <div className="grid grid-cols-4 gap-4 mb-6">
              <SkeletonCard />
              <SkeletonCard />
              <SkeletonCard />
              <SkeletonCard />
            </div>

            {/* Tables list */}
            <SkeletonTable rows={6} cols={5} />
          </div>
        </main>
      </div>
    </div>
  );
}
