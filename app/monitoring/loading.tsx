/**
 * Monitoring — Route-level loading skeleton
 */

import { Skeleton, SkeletonCard, SkeletonTable } from '@/components/ui/skeleton';
import { Sidebar } from '@/components/layout/Sidebar';
import { TopNav } from '@/components/layout/TopNav';

export default function MonitoringLoading() {
  return (
    <div className="flex flex-col h-screen overflow-hidden bg-gray-50">
      <TopNav />
      <div className="flex flex-1 overflow-hidden">
        <Sidebar />
        <main className="flex-1 overflow-y-auto">
          <div className="px-8 py-6">
            <Skeleton className="h-7 w-44 mb-6" />
            <div className="grid grid-cols-4 gap-4 mb-6">
              <SkeletonCard />
              <SkeletonCard />
              <SkeletonCard />
              <SkeletonCard />
            </div>
            <SkeletonTable rows={8} cols={6} />
          </div>
        </main>
      </div>
    </div>
  );
}
