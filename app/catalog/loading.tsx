/**
 * Catalog — Route-level loading skeleton
 */

import { Skeleton, SkeletonTable } from '@/components/ui/skeleton';
import { Sidebar } from '@/components/layout/Sidebar';
import { TopNav } from '@/components/layout/TopNav';

export default function CatalogLoading() {
  return (
    <div className="flex flex-col h-screen overflow-hidden bg-gray-50">
      <TopNav />
      <div className="flex flex-1 overflow-hidden">
        <Sidebar />
        <main className="flex-1 overflow-y-auto">
          <div className="px-8 py-6">
            {/* Page header */}
            <div className="flex items-center justify-between mb-6">
              <Skeleton className="h-7 w-44" />
              <div className="flex items-center gap-3">
                <Skeleton className="h-10 w-64 rounded-md" />
                <Skeleton className="h-10 w-32 rounded-md" />
              </div>
            </div>

            {/* Filter bar */}
            <div className="flex items-center gap-3 mb-6">
              <Skeleton className="h-9 w-36 rounded-md" />
              <Skeleton className="h-9 w-36 rounded-md" />
              <Skeleton className="h-9 w-36 rounded-md" />
            </div>

            {/* Data table */}
            <SkeletonTable rows={8} cols={5} />
          </div>
        </main>
      </div>
    </div>
  );
}
