'use client';

import React from 'react';
import { useRouter } from 'next/navigation';
import { ArrowLeft } from 'lucide-react';


export interface ComingSoonProps {
  /** Feature name */
  title: string;
  /** Short description of what this feature will do */
  description: string;
  /** Expected capabilities (bullet points) */
  capabilities?: string[];
  /** Optional icon or emoji */
  icon?: string;
}

/**
 * Coming Soon component for incomplete pages
 * Provides a professional placeholder with feature information
 */
export function ComingSoon({
  title,
  description,
  capabilities = [],
  icon = '🚀',
}: ComingSoonProps) {
  const router = useRouter();

  return (
    <div className="min-h-screen flex items-center justify-center bg-gradient-to-br from-slate-50 to-slate-100 dark:from-slate-900 dark:to-slate-800 p-4">
      <div className="max-w-2xl w-full">
        {/* Back Button */}
        <div className="mb-4">
          <button
            onClick={() => router.push('/')}
            className="flex items-center gap-2 px-4 py-2 text-sm font-medium text-slate-600 dark:text-slate-300 hover:text-slate-900 dark:hover:text-white bg-white dark:bg-slate-800 border border-slate-200 dark:border-slate-700 rounded-lg hover:bg-slate-50 dark:hover:bg-slate-700 transition-colors"
          >
            <ArrowLeft className="h-4 w-4" />
            Back to Home
          </button>
        </div>

        {/* Animated container */}
        <div className="bg-white dark:bg-slate-800 rounded-2xl shadow-xl p-8 md:p-12 border border-slate-200 dark:border-slate-700 animate-fade-in">
          {/* Icon with pulse animation */}
          <div className="flex justify-center mb-6">
            <div className="relative">
              <div className="absolute inset-0 bg-blue-500 rounded-full opacity-20 animate-ping"></div>
              <div className="relative text-6xl md:text-7xl animate-bounce-slow">
                {icon}
              </div>
            </div>
          </div>

          {/* Title */}
          <h1 className="text-3xl md:text-4xl font-bold text-center mb-4 bg-gradient-to-r from-blue-600 to-purple-600 bg-clip-text text-transparent">
            {title}
          </h1>

          {/* Coming Soon badge */}
          <div className="flex justify-center mb-6">
            <span className="inline-flex items-center px-4 py-2 rounded-full text-sm font-semibold bg-blue-100 text-blue-800 dark:bg-blue-900 dark:text-blue-200 animate-pulse-slow">
              <svg
                className="w-4 h-4 mr-2"
                fill="none"
                stroke="currentColor"
                viewBox="0 0 24 24"
              >
                <path
                  strokeLinecap="round"
                  strokeLinejoin="round"
                  strokeWidth={2}
                  d="M12 8v4l3 3m6-3a9 9 0 11-18 0 9 9 0 0118 0z"
                />
              </svg>
              Coming Soon
            </span>
          </div>

          {/* Description */}
          <p className="text-center text-lg text-slate-600 dark:text-slate-300 mb-8">
            {description}
          </p>

          {/* Capabilities */}
          {capabilities.length > 0 && (
            <div className="space-y-3">
              <h2 className="text-xl font-semibold text-slate-800 dark:text-slate-200 mb-4">
                Planned Features:
              </h2>
              <ul className="space-y-2">
                {capabilities.map((capability, index) => (
                  <li
                    key={index}
                    className="flex items-start text-slate-700 dark:text-slate-300 animate-slide-in"
                    style={{ animationDelay: `${index * 100}ms` }}
                  >
                    <svg
                      className="w-5 h-5 mr-3 mt-0.5 text-green-500 flex-shrink-0"
                      fill="none"
                      stroke="currentColor"
                      viewBox="0 0 24 24"
                    >
                      <path
                        strokeLinecap="round"
                        strokeLinejoin="round"
                        strokeWidth={2}
                        d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z"
                      />
                    </svg>
                    <span>{capability}</span>
                  </li>
                ))}
              </ul>
            </div>
          )}

          {/* Progress indicator */}
          <div className="mt-8 pt-6 border-t border-slate-200 dark:border-slate-700">
            <div className="flex items-center justify-center text-sm text-slate-500 dark:text-slate-400">
              <svg
                className="w-4 h-4 mr-2 animate-spin-slow"
                fill="none"
                stroke="currentColor"
                viewBox="0 0 24 24"
              >
                <path
                  strokeLinecap="round"
                  strokeLinejoin="round"
                  strokeWidth={2}
                  d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15"
                />
              </svg>
              This feature is currently under development
            </div>
          </div>
        </div>
      </div>

      {/* CSS Animations */}
      <style jsx>{`
        @keyframes fade-in {
          from {
            opacity: 0;
            transform: translateY(20px);
          }
          to {
            opacity: 1;
            transform: translateY(0);
          }
        }

        @keyframes slide-in {
          from {
            opacity: 0;
            transform: translateX(-20px);
          }
          to {
            opacity: 1;
            transform: translateX(0);
          }
        }

        @keyframes bounce-slow {
          0%, 100% {
            transform: translateY(0);
          }
          50% {
            transform: translateY(-10px);
          }
        }

        @keyframes pulse-slow {
          0%, 100% {
            opacity: 1;
          }
          50% {
            opacity: 0.7;
          }
        }

        @keyframes spin-slow {
          from {
            transform: rotate(0deg);
          }
          to {
            transform: rotate(360deg);
          }
        }

        .animate-fade-in {
          animation: fade-in 0.6s ease-out;
        }

        .animate-slide-in {
          animation: slide-in 0.5s ease-out backwards;
        }

        .animate-bounce-slow {
          animation: bounce-slow 2s ease-in-out infinite;
        }

        .animate-pulse-slow {
          animation: pulse-slow 2s ease-in-out infinite;
        }

        .animate-spin-slow {
          animation: spin-slow 3s linear infinite;
        }
      `}</style>
    </div>
  );
}
