/**
 * Intelligence — Route-level loading skeleton
 */

import { Skeleton, SkeletonCard, SkeletonText } from '@/components/ui/skeleton';
import { Sidebar } from '@/components/layout/Sidebar';
import { TopNav } from '@/components/layout/TopNav';

export default function IntelligenceLoading() {
  return (
    <div className="flex flex-col h-screen overflow-hidden bg-gray-50">
      <TopNav />
      <div className="flex flex-1 overflow-hidden">
        <Sidebar />
        <main className="flex-1 overflow-y-auto">
          <div className="px-8 py-6">
            <Skeleton className="h-7 w-52 mb-6" />
            <div className="grid grid-cols-2 gap-6 mb-6">
              <SkeletonCard />
              <SkeletonCard />
            </div>
            <div className="bg-white border border-slate-200 rounded-lg shadow-sm p-6">
              <Skeleton className="h-5 w-40 mb-4" />
              <SkeletonText lines={5} />
            </div>
          </div>
        </main>
      </div>
    </div>
  );
}
