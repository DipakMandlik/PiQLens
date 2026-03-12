/**
 * Skeleton — Reusable loading primitives
 *
 * Usage:
 *   <Skeleton className="h-4 w-32" />           // single line
 *   <SkeletonCard />                             // metric card placeholder
 *   <SkeletonTable rows={5} cols={4} />          // data table placeholder
 *   <SkeletonText lines={3} />                   // paragraph placeholder
 */

import React from 'react';

// ─── Base Skeleton Pulse ────────────────────────────────────────────

interface SkeletonProps {
  className?: string;
}

export function Skeleton({ className = '' }: SkeletonProps) {
  return (
    <div
      className={`animate-pulse rounded bg-slate-200 ${className}`}
      aria-hidden="true"
    />
  );
}

// ─── Metric Card Skeleton ───────────────────────────────────────────

export function SkeletonCard() {
  return (
    <div className="bg-white border border-slate-200 rounded-lg p-6 shadow-sm">
      <div className="flex items-center justify-between mb-4">
        <Skeleton className="h-4 w-28" />
        <Skeleton className="h-8 w-8 rounded-full" />
      </div>
      <Skeleton className="h-10 w-20 mb-3" />
      <div className="flex items-center gap-2">
        <Skeleton className="h-3 w-16" />
        <Skeleton className="h-3 w-12" />
      </div>
    </div>
  );
}

// ─── Circular Metric Card Skeleton (matches CircularMetricCard) ─────

export function SkeletonCircularCard() {
  return (
    <div className="bg-white border border-slate-200 rounded-lg p-6 shadow-sm">
      <div className="flex items-center gap-3 mb-4">
        <Skeleton className="h-5 w-5 rounded" />
        <Skeleton className="h-4 w-36" />
      </div>
      <div className="flex items-center justify-center mb-4">
        <Skeleton className="h-24 w-24 rounded-full" />
      </div>
      <div className="space-y-2">
        <div className="flex justify-between">
          <Skeleton className="h-3 w-20" />
          <Skeleton className="h-3 w-10" />
        </div>
        <div className="flex justify-between">
          <Skeleton className="h-3 w-16" />
          <Skeleton className="h-3 w-12" />
        </div>
      </div>
    </div>
  );
}

// ─── Data Table Skeleton ────────────────────────────────────────────

interface SkeletonTableProps {
  rows?: number;
  cols?: number;
}

export function SkeletonTable({ rows = 5, cols = 4 }: SkeletonTableProps) {
  return (
    <div className="bg-white border border-slate-200 rounded-lg overflow-hidden shadow-sm">
      {/* Header */}
      <div className="border-b border-slate-100 px-6 py-3 flex gap-4">
        {Array.from({ length: cols }).map((_, i) => (
          <Skeleton key={`h-${i}`} className="h-4 flex-1" />
        ))}
      </div>
      {/* Rows */}
      {Array.from({ length: rows }).map((_, r) => (
        <div
          key={`r-${r}`}
          className="border-b border-slate-50 px-6 py-3 flex gap-4"
        >
          {Array.from({ length: cols }).map((_, c) => (
            <Skeleton
              key={`r-${r}-c-${c}`}
              className={`h-3.5 flex-1 ${c === 0 ? 'max-w-[200px]' : ''}`}
            />
          ))}
        </div>
      ))}
    </div>
  );
}

// ─── Text Block Skeleton ────────────────────────────────────────────

interface SkeletonTextProps {
  lines?: number;
}

export function SkeletonText({ lines = 3 }: SkeletonTextProps) {
  return (
    <div className="space-y-2">
      {Array.from({ length: lines }).map((_, i) => (
        <Skeleton
          key={`t-${i}`}
          className={`h-3.5 ${i === lines - 1 ? 'w-3/4' : 'w-full'}`}
        />
      ))}
    </div>
  );
}

// ─── Header Skeleton ────────────────────────────────────────────────

export function SkeletonHeader() {
  return (
    <div className="bg-white border border-slate-200 rounded-lg shadow-sm px-8 py-6">
      <div className="flex items-center justify-between">
        <div className="space-y-3 flex-1">
          <Skeleton className="h-6 w-64" />
          <div className="flex items-center gap-4">
            <Skeleton className="h-4 w-40" />
            <Skeleton className="h-5 w-20 rounded-full" />
            <Skeleton className="h-4 w-32" />
          </div>
        </div>
        <div className="flex items-center gap-3">
          <Skeleton className="h-10 w-[240px] rounded-md" />
          <Skeleton className="h-10 w-28 rounded-md" />
          <Skeleton className="h-10 w-32 rounded-md" />
        </div>
      </div>
    </div>
  );
}

// ─── Attention Card Skeleton ────────────────────────────────────────

export function SkeletonAttentionCard() {
  return (
    <div className="bg-white border border-slate-200 rounded-lg shadow-sm p-6">
      <div className="flex items-center justify-between mb-4">
        <Skeleton className="h-5 w-52" />
        <Skeleton className="h-4 w-16" />
      </div>
      <div className="space-y-2">
        {Array.from({ length: 3 }).map((_, i) => (
          <div key={i} className="flex items-center justify-between p-4 border border-slate-100 rounded-md">
            <div className="flex items-center gap-3">
              <Skeleton className="h-5 w-5 rounded" />
              <Skeleton className="h-4 w-60" />
            </div>
            <Skeleton className="h-4 w-4" />
          </div>
        ))}
      </div>
    </div>
  );
}
