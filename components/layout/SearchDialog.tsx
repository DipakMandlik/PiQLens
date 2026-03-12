'use client';

import { useState, useEffect } from 'react';
import { Search, FileText, Database, ArrowRight, X } from 'lucide-react';
import { Dialog, DialogContent, DialogTitle, DialogDescription } from '@/components/ui/dialog';
import { Input } from '@/components/ui/input';
import { Button } from '@/components/ui/button';
import Link from 'next/link';

interface SearchDialogProps {
    open: boolean;
    onOpenChange: (open: boolean) => void;
}

export function SearchDialog({ open, onOpenChange }: SearchDialogProps) {
    const [query, setQuery] = useState('');
    const [suggestions, setSuggestions] = useState<any[]>([]);

    useEffect(() => {
        if (open) {
            setQuery('');
        }
    }, [open]);

    useEffect(() => {
        const fetchSearchData = async () => {
            try {
                const response = await fetch('/api/snowflake/database-hierarchy');
                const result = await response.json();

                if (result.success && result.data) {
                    const flatTables: any[] = [];
                    // Flatten data structure to grab just tables
                    result.data.forEach((db: any) => {
                        db.schemas?.forEach((schema: any) => {
                            schema.tables?.forEach((table: any) => {
                                flatTables.push({
                                    id: table.id,
                                    title: table.name,
                                    type: 'dataset',
                                    desc: `Dataset in ${db.name} > ${schema.name}`,
                                    icon: Database,
                                    href: table.href || `/datasets/${table.id}`,
                                });
                            });
                        });
                    });

                    const defaultSuggestions = [
                        { id: 'rep-1', title: 'Data Quality Summary Report', type: 'report', desc: 'Monthly summary of DQ metrics across core tables', icon: FileText, href: '#' },
                    ];

                    setSuggestions([...flatTables, ...defaultSuggestions]);
                }
            } catch (error) {
                console.error('Error fetching search data:', error);
            }
        };

        fetchSearchData();
    }, []);

    const filteredSuggestions = suggestions.filter(item =>
        !query ||
        item.title.toLowerCase().includes(query.toLowerCase()) ||
        item.desc.toLowerCase().includes(query.toLowerCase())
    );

    return (
        <Dialog open={open} onOpenChange={onOpenChange}>
            <DialogContent className="sm:max-w-2xl p-0 gap-0 border-none shadow-2xl bg-white overflow-hidden [&>button]:hidden">
                {/* Visually hidden elements for accessibility compliance */}
                <DialogTitle className="sr-only">Search</DialogTitle>
                <DialogDescription className="sr-only">Search across datasets, reports, and intelligence.</DialogDescription>

                <div className="flex items-center border-b border-gray-100 px-4 py-4">
                    <Search className="h-5 w-5 text-gray-400 mr-3 shrink-0" />
                    <input
                        autoFocus
                        className="flex-1 bg-transparent text-base text-gray-900 placeholder:text-gray-400 outline-none h-8 w-full"
                        placeholder="Search datasets, reports, metrics..."
                        value={query}
                        onChange={(e) => setQuery(e.target.value)}
                    />
                    {query && (
                        <button
                            onClick={() => setQuery('')}
                            className="p-1 hover:bg-gray-100 rounded-full text-gray-400 hover:text-gray-600 ml-2"
                        >
                            <X className="h-4 w-4" />
                        </button>
                    )}
                    <div className="hidden sm:flex items-center gap-1.5 ml-4">
                        <kbd className="pointer-events-none inline-flex h-5 select-none items-center gap-1 rounded border border-gray-200 bg-gray-50 px-1.5 font-mono text-[10px] font-medium text-gray-500">
                            ESC
                        </kbd>
                        <span className="text-xs text-gray-400">to close</span>
                    </div>
                </div>

                <div className="max-h-[60vh] overflow-y-auto p-2">
                    {!query && (
                        <div className="px-4 py-3 text-xs font-semibold text-gray-400 uppercase tracking-wider">
                            Recent Searches
                        </div>
                    )}

                    {query && filteredSuggestions.length === 0 && (
                        <div className="py-14 text-center">
                            <Search className="h-10 w-10 text-gray-200 mx-auto mb-3" />
                            <p className="text-sm text-gray-500 font-medium">No results found for &quot;{query}&quot;</p>
                            <p className="text-xs text-gray-400 mt-1">Try another search term or browse the catalog.</p>
                        </div>
                    )}

                    {filteredSuggestions.length > 0 && (
                        <ul className="space-y-1">
                            {filteredSuggestions.map((item) => {
                                const Icon = item.icon;
                                return (
                                    <li key={item.id}>
                                        <Link
                                            href={item.href || (item.type === 'dataset' ? `/datasets/${item.id}` : '#')}
                                            onClick={() => onOpenChange(false)}
                                            className="group flex items-center gap-3 w-full p-3 rounded-xl hover:bg-blue-50/60 transition-colors"
                                        >
                                            <div className={`
                        flex items-center justify-center h-10 w-10 rounded-lg shrink-0
                        ${item.type === 'dataset' ? 'bg-blue-100/50 text-blue-600' : 'bg-purple-100/50 text-purple-600'}
                      `}>
                                                <Icon className="h-5 w-5" />
                                            </div>
                                            <div className="flex-1 text-left">
                                                <h4 className="text-sm font-semibold text-gray-900 group-hover:text-blue-700 transition-colors">
                                                    {item.title}
                                                </h4>
                                                <p className="text-xs text-gray-500 line-clamp-1 mt-0.5">
                                                    {item.desc}
                                                </p>
                                            </div>
                                            <ArrowRight className="h-4 w-4 text-gray-300 opacity-0 group-hover:opacity-100 group-hover:text-blue-500 group-hover:-translate-x-1 transition-all" />
                                        </Link>
                                    </li>
                                );
                            })}
                        </ul>
                    )}
                </div>

                <div className="bg-gray-50 border-t border-gray-100 p-3 pt-3 px-4 flex justify-between items-center text-xs text-gray-500">
                    <div>
                        Search powered by <span className="font-semibold text-gray-700">PiQLens</span> AI
                    </div>
                    <Button variant="ghost" size="sm" className="h-auto p-0 px-2 text-blue-600 hover:text-blue-700 hover:bg-transparent" onClick={() => onOpenChange(false)}>
                        Close
                    </Button>
                </div>
            </DialogContent>
        </Dialog>
    );
}
