'use client';

import { useEffect, useState } from 'react';
import { Database, FileText, Folder } from 'lucide-react';
import { MenuItem } from '../navigation/MenuItem';
import { useAppStore } from '@/lib/store';

interface HierarchyTable {
  id: string;
  name: string;
  href: string;
}

interface HierarchySchema {
  id: string;
  name: string;
  tables: HierarchyTable[];
}

interface HierarchyDatabase {
  id: string;
  name: string;
  schemas: HierarchySchema[];
}

const HIDDEN_DATABASES = new Set<string>();

export function Sidebar() {
  const { isConnected } = useAppStore();
  const [databases, setDatabases] = useState<HierarchyDatabase[]>([]);
  const [isLoading, setIsLoading] = useState(true);

  useEffect(() => {
    if (!isConnected) {
      setDatabases([]);
      setIsLoading(false);
      return;
    }

    const fetchDatabases = async () => {
      setIsLoading(true);
      try {
        const response = await fetch('/api/snowflake/database-hierarchy');
        const result = await response.json();

        if (result.success && result.data) {
          setDatabases(result.data as HierarchyDatabase[]);
        }
      } catch (error) {
        console.error('Error fetching databases:', error);
      } finally {
        setIsLoading(false);
      }
    };

    fetchDatabases();
  }, [isConnected]);

  const dataItems = [
    {
      id: 'all-datasets',
      label: 'All Datasets',
      icon: Database,
      href: '/datasets',
    },
    ...databases
      .filter((db) => !HIDDEN_DATABASES.has(db.name.toUpperCase()))
      .map((db) => ({
        id: db.id,
        label: db.name,
        icon: Database,
        snowflakeIcon: true,
        children: db.schemas.map((schema) => ({
          id: `${db.id}_${schema.id}`,
          label: schema.name,
          icon: Folder,
          snowflakeIcon: true,
          children: schema.tables.map((table) => ({
            id: table.id,
            label: table.name,
            icon: FileText,
            snowflakeIcon: true,
            href: table.href,
          })),
        })),
      })),
  ];

  return (
    <aside className="w-64 bg-slate-50 border-r border-slate-200 flex-shrink-0 overflow-y-auto">
      <div className="p-4">
        <h3 className="px-3 mb-2 text-xs font-semibold text-slate-500 uppercase tracking-wider">Datasets</h3>

        <div className="rounded-lg border border-slate-200 bg-white p-2">
          {isLoading ? (
            <div className="px-3 py-2 text-sm text-slate-500">Loading...</div>
          ) : (
            <nav className="space-y-0.5">
              {dataItems.map((item) => (
                <MenuItem key={item.id} item={item} />
              ))}
            </nav>
          )}
        </div>
      </div>
    </aside>
  );
}
