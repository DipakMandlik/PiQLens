'use client';

import { useState } from 'react';
import Image from 'next/image';
import Link from 'next/link';
import { usePathname } from 'next/navigation';
import { Search, ChevronDown, Database, Home, BarChart3, Brain, Shield, Settings, FileText, BookOpen, CheckCircle2 } from 'lucide-react';
import { Button } from '../ui/button';
import { ConnectionDialog } from '../ConnectionDialog';
import { SearchDialog } from './SearchDialog';
import { NotificationPopover } from './NotificationPopover';
import { useAppStore } from '@/lib/store';
import {
    DropdownMenu,
    DropdownMenuContent,
    DropdownMenuItem,
    DropdownMenuLabel,
    DropdownMenuSeparator,
    DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu';

export function TopNav() {
    const [isDialogOpen, setIsDialogOpen] = useState(false);
    const [isSearchOpen, setIsSearchOpen] = useState(false);
    const pathname = usePathname();

    const { isConnected } = useAppStore();

    const mainNavItems = [
        { href: '/', label: 'Home', icon: Home },
        { href: '/data', label: 'Data', icon: BarChart3 },
        { href: '/evaluation', label: 'Evaluation', icon: BookOpen },
        { href: '/settings', label: 'Settings', icon: Settings },
    ];

    const profileNavItems = [
        { href: '/governance', label: 'Governance', icon: Shield },
        { href: '/intelligence', label: 'Intelligence', icon: Brain },
        { href: '/reporting', label: 'Reporting', icon: FileText },
    ];

    const isActive = (href: string) => {
        if (href === '/') return pathname === '/';
        return pathname.startsWith(href);
    };

    return (
        <>
            <header className="h-16 bg-white border-b border-gray-200/80 flex items-center pr-6 flex-shrink-0 sticky top-0 z-50 shadow-sm shadow-slate-200/20">
                <div className="flex items-center justify-between w-full relative">
                    {/* Left: Logo + Brand */}
                    <div className="flex items-center justify-center w-64 flex-shrink-0">
                        <Link href="/" className="flex items-center outline-none transition-opacity hover:opacity-90">
                            <Image
                                src="/assets/branding/piqlens-logo.png"
                                alt="PiQLens Logo"
                                width={240}
                                height={80}
                                className="h-[44px] w-auto object-contain"
                                priority
                            />
                        </Link>
                    </div>

                    {/* Center: Main Navigation */}
                    <nav className="hidden lg:flex items-center gap-2 absolute left-1/2 transform -translate-x-1/2">
                        {mainNavItems.map((item) => {
                            const Icon = item.icon;
                            const active = isActive(item.href);
                            return (
                                <Link
                                    key={item.href}
                                    href={item.href}
                                    className={`
                                        flex items-center gap-2.5 px-4 py-2.5 rounded-lg text-[14px] font-medium transition-all duration-200
                                        ${active
                                            ? 'bg-blue-50/80 text-blue-700 shadow-sm ring-1 ring-blue-600/10'
                                            : 'text-gray-600 hover:bg-gray-100/80 hover:text-gray-900 focus:outline-none focus-visible:ring-2 focus-visible:ring-blue-500/50'
                                        }
                                    `}
                                >
                                    <Icon className={`h-4 w-4 ${active ? 'text-blue-600' : 'text-gray-400'}`} />
                                    {item.label}
                                </Link>
                            );
                        })}
                    </nav>

                    {/* Right: Actions */}
                    <div className="flex items-center gap-4 flex-1 justify-end">
                        {/* Search */}
                        <button
                            onClick={() => setIsSearchOpen(true)}
                            className="p-2.5 hover:bg-gray-100/80 rounded-full transition-all duration-200 text-gray-400 hover:text-gray-700 outline-none focus-visible:ring-2 focus-visible:ring-blue-500/50"
                            aria-label="Search"
                        >
                            <Search className="h-5 w-5" />
                        </button>

                        {/* Notifications */}
                        <NotificationPopover />

                        <div className="w-px h-6 bg-gray-200/80 mx-1"></div>

                        {/* User Menu */}
                        <DropdownMenu>
                            <DropdownMenuTrigger asChild>
                                <button className="flex items-center gap-2 p-1.5 hover:bg-gray-100/80 rounded-xl transition-all duration-200 outline-none cursor-pointer focus-visible:ring-2 focus-visible:ring-blue-500/50 group">
                                    <div className="h-8 w-8 bg-gradient-to-tr from-blue-600 to-indigo-600 rounded-lg flex items-center justify-center text-white text-sm font-semibold shadow-sm group-hover:shadow transition-shadow">
                                        U
                                    </div>
                                    <ChevronDown className="h-4 w-4 text-gray-400 group-hover:text-gray-700 transition-colors" />
                                </button>
                            </DropdownMenuTrigger>
                            <DropdownMenuContent align="end" className="w-56 rounded-xl border-gray-100 shadow-lg">
                                <DropdownMenuLabel className="font-semibold text-gray-900">My Account</DropdownMenuLabel>
                                <DropdownMenuSeparator className="bg-gray-100" />
                                {profileNavItems.map((item) => {
                                    const Icon = item.icon;
                                    return (
                                        <DropdownMenuItem key={item.href} asChild className="rounded-lg focus:bg-gray-50 focus:text-blue-700 cursor-pointer my-0.5">
                                            <Link href={item.href} className="flex items-center w-full text-gray-600 px-2 py-1.5">
                                                <Icon className="mr-2.5 h-4 w-4 text-gray-400" />
                                                <span className="font-medium text-[13px]">{item.label}</span>
                                            </Link>
                                        </DropdownMenuItem>
                                    );
                                })}
                                <DropdownMenuSeparator className="bg-gray-100" />
                                <DropdownMenuItem className="text-red-600 cursor-pointer rounded-lg focus:bg-red-50 focus:text-red-700 font-medium text-[13px] px-2 py-1.5 my-0.5">
                                    Log out
                                </DropdownMenuItem>
                            </DropdownMenuContent>
                        </DropdownMenu>

                        {/* Connect Button */}
                        <Button
                            onClick={() => !isConnected && setIsDialogOpen(true)}
                            disabled={isConnected}
                            className={`flex items-center gap-2.5 px-5 py-2.5 rounded-lg shadow-sm font-medium transition-all duration-300 ml-1 ${isConnected ? 'bg-emerald-50 text-emerald-700 border border-emerald-200/60 cursor-default shadow-none' : 'bg-blue-600 hover:bg-blue-700 text-white hover:shadow-md hover:-translate-y-0.5 active:translate-y-0 active:shadow-sm'}`}
                        >
                            {isConnected ? <CheckCircle2 className="h-4 w-4 text-emerald-500" /> : <Database className="h-4 w-4" />}
                            {isConnected ? 'Connected' : 'Connect'}
                        </Button>
                    </div>
                </div>
            </header>

            <SearchDialog
                open={isSearchOpen}
                onOpenChange={setIsSearchOpen}
            />

            <ConnectionDialog
                isOpen={isDialogOpen}
                onClose={() => setIsDialogOpen(false)}
            />
        </>
    );
}


