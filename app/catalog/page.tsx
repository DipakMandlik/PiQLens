import React from 'react';
import { Metadata } from 'next';
import CatalogClient from './catalog-client';

export const metadata: Metadata = {
    title: 'Data Catalog | πQLens',
    description: 'Enterprise Searchable Data Catalog with integrated Data Quality, Lineage, and Governance.',
};

export default function CatalogPage() {
    return (
        <div className="flex flex-col h-[calc(100vh-4rem)] bg-zinc-50 dark:bg-zinc-950 overflow-hidden">
            <div className="flex-1 overflow-hidden">
                <CatalogClient />
            </div>
        </div>
    );
}
