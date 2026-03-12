'use client';

import { Sidebar } from './Sidebar';
import { TopNav } from './TopNav';

interface AppLayoutProps {
    children: React.ReactNode;
}

export function AppLayout({ children }: AppLayoutProps) {
    return (
        <div className="flex flex-col h-screen overflow-hidden bg-gray-50">
            {/* Top Navigation - Full Width */}
            <TopNav />

            {/* Main Layout Area */}
            <div className="flex flex-1 overflow-hidden">
                {/* Sidebar */}
                <Sidebar />

                {/* Page Content */}
                <main className="flex-1 overflow-y-auto w-full">
                    <div className="container mx-auto px-6 py-8">
                        {children}
                    </div>
                </main>
            </div>
        </div>
    );
}
