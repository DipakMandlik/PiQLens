'use client';

import { useState, useEffect } from 'react';
import { ColumnListPanel, ColumnItem } from './ColumnListPanel';
import { ColumnDetailPanel } from './ColumnDetailPanel';

interface FieldsTabProps {
    database: string;
    schema: string;
    table: string;
}

export function FieldsTab({ database, schema, table }: FieldsTabProps) {
    const [columns, setColumns] = useState<ColumnItem[]>([]);
    const [selectedColumn, setSelectedColumn] = useState<string | null>(null);
    const [isLoading, setIsLoading] = useState(false);

    useEffect(() => {
        const fetchColumns = async () => {
            setIsLoading(true);
            try {
                const response = await fetch(
                    `/api/v1/assets/${encodeURIComponent(table)}/columns?database=${encodeURIComponent(database)}&schema=${encodeURIComponent(schema)}`
                );
                const json = await response.json();
                if (json.success) {
                    setColumns(json.data);
                    // Auto-select first column if none selected
                    if (json.data.length > 0 && !selectedColumn) {
                        // Optional: setSelectedColumn(json.data[0].columnName);
                    }
                }
            } catch (error) {
                console.error('Failed to fetch columns:', error);
            } finally {
                setIsLoading(false);
            }
        };

        fetchColumns();
    }, [database, schema, table]);

    return (
        <div className="flex h-[calc(100vh-200px)] border rounded-lg overflow-hidden bg-white shadow-sm">
            <ColumnListPanel
                columns={columns}
                selectedColumn={selectedColumn}
                onSelectColumn={setSelectedColumn}
                isLoading={isLoading}
            />
            <ColumnDetailPanel
                database={database}
                schema={schema}
                table={table}
                column={selectedColumn}
            />
        </div>
    );
}
