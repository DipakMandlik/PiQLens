import React from 'react';
import { Metadata } from 'next';
import CatalogDetailClient from './catalog-detail-client';

export const metadata: Metadata = {
    title: 'Dataset Details | πQLens Catalog',
    description: 'View full schema, quality, and governance metadata for this dataset.',
};

export default async function TableDetailPage(
    props: { params: Promise<{ database: string; schema: string; table: string }> | { database: string; schema: string; table: string } }
) {
    const params = await props.params;

    return (
        <div className="flex flex-col h-[calc(100vh-4rem)] bg-zinc-50 dark:bg-zinc-950 overflow-hidden">
            <div className="flex-1 overflow-hidden">
                <CatalogDetailClient
                    database={params.database}
                    schema={params.schema}
                    table={params.table}
                />
            </div>
        </div>
    );
}
