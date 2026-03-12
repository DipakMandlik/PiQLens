/**
 * Navigation Configuration
 * Defines the hierarchical menu structure for the sidebar
 */

import {
    LayoutDashboard,
    Database,
    BarChart3,
    AlertTriangle,
    FileText,
    Folder,
    ChevronRight,
    type LucideIcon,
} from 'lucide-react';

export interface NavigationItem {
    id: string;
    label: string;
    icon: LucideIcon;
    href?: string;
    badge?: 'success' | 'warning' | 'info';
    children?: NavigationItem[];
}

export interface NavigationSection {
    title: string;
    items: NavigationItem[];
}

// Real Snowflake databases with schema → table hierarchy
export const sampleDatabases = [
    {
        id: 'banking_dw',
        name: 'BANKING_DW',
        schemas: [
            {
                id: 'bronze',
                name: 'BRONZE',
                tables: [
                    { id: 'stg_customer', name: 'STG_CUSTOMER', href: '/datasets/BANKING_DW.BRONZE/tables/STG_CUSTOMER' },
                    { id: 'stg_account', name: 'STG_ACCOUNT', href: '/datasets/BANKING_DW.BRONZE/tables/STG_ACCOUNT' },
                    { id: 'stg_transaction', name: 'STG_TRANSACTION', href: '/datasets/BANKING_DW.BRONZE/tables/STG_TRANSACTION' },
                    { id: 'stg_daily_balance', name: 'STG_DAILY_BALANCE', href: '/datasets/BANKING_DW.BRONZE/tables/STG_DAILY_BALANCE' },
                    { id: 'stg_fx_rate', name: 'STG_FX_RATE', href: '/datasets/BANKING_DW.BRONZE/tables/STG_FX_RATE' },
                ],
            },
        ],
    },
];

// Real DQ datasets from DATA_QUALITY_DB
export const sampleDatasets = [
    { id: 'dq_metrics', name: 'DQ_METRICS', href: '/datasets/DATA_QUALITY_DB.DQ_METRICS/tables/DQ_METRICS' },
    { id: 'dq_ai_insights', name: 'DQ_AI_INSIGHTS', href: '/datasets/DATA_QUALITY_DB.PUBLIC/tables/DQ_AI_INSIGHTS' },
];

export const navigationConfig: Record<string, NavigationSection> = {
    data: {
        title: 'DATA',
        items: [
            {
                id: 'all-datasets',
                label: 'All Datasets',
                icon: Database,
                href: '/datasets',
            },
            ...sampleDatabases.map((db) => ({
                id: db.id,
                label: db.name,
                icon: Database,
                children: db.schemas.map((schema) => ({
                    id: `${db.id}_${schema.id}`,
                    label: schema.name,
                    icon: Folder,
                    children: schema.tables.map((table) => ({
                        id: table.id,
                        label: table.name,
                        icon: FileText,
                        href: table.href,
                    })),
                })),
            })),
        ],
    },
    datasets: {
        title: 'DATASETS',
        items: sampleDatasets.map((dataset) => ({
            id: dataset.id,
            label: dataset.name,
            icon: BarChart3,
            href: dataset.href,
        })),
    },
};
