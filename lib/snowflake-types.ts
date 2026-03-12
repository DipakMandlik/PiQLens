export interface SnowflakeConfig {
    account?: string;
    accountUrl?: string;
    username: string;
    password?: string;
    token?: string; // JWT/OAuth token
    warehouse?: string; // Optional - can be set later
    database?: string;  // Optional - can be set later
    schema?: string;    // Optional - can be set later
    role?: string;
}

export interface QueryResult {
    columns: string[];
    rows: any[][];
    rowCount: number;
}
