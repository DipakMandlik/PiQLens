'use client';

import { useRouter } from 'next/navigation';
import { ArrowLeft } from 'lucide-react';

export interface BackButtonProps {
    /** Optional custom label for the button */
    label?: string;
    /** Optional custom route to navigate to */
    href?: string;
    /** Optional className for styling */
    className?: string;
}

/**
 * Reusable back button component
 * Uses Next.js router for navigation
 */
export function BackButton({
    label = 'Back',
    href,
    className = ''
}: BackButtonProps) {
    const router = useRouter();

    const handleClick = () => {
        if (href) {
            router.push(href);
        } else {
            router.back();
        }
    };

    return (
        <button
            onClick={handleClick}
            className={`
        inline-flex items-center gap-2 px-4 py-2 
        text-sm font-medium text-gray-700 dark:text-gray-200
        bg-white dark:bg-gray-800 
        border border-gray-300 dark:border-gray-600
        rounded-lg
        hover:bg-gray-50 dark:hover:bg-gray-700
        focus:outline-none focus:ring-2 focus:ring-blue-500 focus:ring-offset-2
        transition-colors duration-200
        ${className}
      `}
            aria-label={label}
        >
            <ArrowLeft className="w-4 h-4" />
            <span>{label}</span>
        </button>
    );
}
