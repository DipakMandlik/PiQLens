/**
 * Core types for the Data Catalog feature
 */

export interface ColumnMeta {
    name: string;
    dataType: string;
    isNullable: boolean;
    ordinalPosition: number;
    comment?: string;
    defaultValue?: string;
}

export interface ConstraintMeta {
    constraintName: string;
    constraintType: 'PRIMARY KEY' | 'FOREIGN KEY' | 'UNIQUE' | string;
    columnName: string;
    // If it's a foreign key, we could optionally track what it references
    referencedTable?: string;
    referencedColumn?: string;
}

export interface DataCatalogTable {
    // Identification
    database: string;
    schema: string;
    table: string;
    businessDomain: string;

    // Structural
    columns: ColumnMeta[];
    constraints: ConstraintMeta[];

    // Storage / Operational
    rowCount: number;
    sizeBytes: number;
    createdAt: string;
    lastModified: string;
    owner: string;

    // Lineage
    upstream: string[]; // List of fully-qualified table/view names
    downstream: string[];

    // Usage
    queryCount30d: number;
    lastAccessed: string;
    usageClassification: 'High' | 'Medium' | 'Low' | 'Unknown';

    // Governance & Tags
    classification: string;
    tags: Record<string, string>; // e.g. { PII: 'true', SENSITIVITY: 'HIGH' }

    // Data Quality Overlay (from dq_daily_summary)
    dqScore: number | null;
    failureRate: number | null;
    trustLevel: string | null;
    qualityGrade: string | null;
    slaMet: boolean | null;
}

// Minimal type for listing tables in the catalog search view (lighter payload)
export interface CatalogListEntry {
    id: string; // db.schema.table
    database: string;
    schema: string;
    table: string;
    businessDomain: string;
    rowCount: number;
    sizeBytes: number;
    lastModified: string;
    usageClassification: string;
    trustLevel: string | null;
    dqScore: number | null;
    qualityGrade: string | null;
    tags: Record<string, string>;
}
