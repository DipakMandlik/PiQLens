/**
 * Reports — Route-level loading skeleton
 */

import { Skeleton, SkeletonTable } from '@/components/ui/skeleton';
import { Sidebar } from '@/components/layout/Sidebar';
import { TopNav } from '@/components/layout/TopNav';

export default function ReportsLoading() {
  return (
    <div className="flex flex-col h-screen overflow-hidden bg-gray-50">
      <TopNav />
      <div className="flex flex-1 overflow-hidden">
        <Sidebar />
        <main className="flex-1 overflow-y-auto">
          <div className="px-8 py-6">
            <div className="flex items-center justify-between mb-6">
              <Skeleton className="h-7 w-36" />
              <Skeleton className="h-10 w-36 rounded-md" />
            </div>
            <SkeletonTable rows={6} cols={5} />
          </div>
        </main>
      </div>
    </div>
  );
}
