/**
 * DelayedLoader — Shows content only after a threshold delay
 *
 * Prevents flickering for fast responses.
 * If data loads in < 300ms → user sees nothing (smooth UX)
 * If data loads in > 300ms → skeleton appears (no blank screen)
 *
 * Usage:
 *   <DelayedLoader isLoading={isLoading} delay={300}>
 *     <SkeletonCard />
 *   </DelayedLoader>
 *
 *   // Or wrap the actual content:
 *   {isLoading ? (
 *     <DelayedLoader isLoading={true}>
 *       <SkeletonCard />
 *     </DelayedLoader>
 *   ) : (
 *     <ActualContent />
 *   )}
 */

'use client';

import { useState, useEffect, type ReactNode } from 'react';

interface DelayedLoaderProps {
  /** Whether the parent is currently loading */
  isLoading: boolean;
  /** Minimum ms before showing the loader (default: 300ms) */
  delay?: number;
  /** Skeleton/placeholder to show while loading */
  children: ReactNode;
}

export function DelayedLoader({
  isLoading,
  delay = 300,
  children,
}: DelayedLoaderProps) {
  const [showLoader, setShowLoader] = useState(false);

  useEffect(() => {
    if (!isLoading) {
      setShowLoader(false);
      return;
    }

    const timer = setTimeout(() => {
      setShowLoader(true);
    }, delay);

    return () => clearTimeout(timer);
  }, [isLoading, delay]);

  if (!isLoading || !showLoader) return null;

  return <>{children}</>;
}
