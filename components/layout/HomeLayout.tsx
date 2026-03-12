'use client';

import { ReactNode } from 'react';

interface HomeLayoutProps {
    children: ReactNode;
}

/**
 * Home Layout - Full width, no sidebar
 * For the main landing/home page only
 */
export function HomeLayout({ children }: HomeLayoutProps) {
    return (
        <div className="min-h-screen bg-white">
            {children}
        </div>
    );
}
