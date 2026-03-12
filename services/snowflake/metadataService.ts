/**
 * Snowflake Metadata Service
 *
 * Wraps the Snowflake metadata query behind the CacheManager
 * to avoid redundant CURRENT_VERSION() / CURRENT_ACCOUNT() calls.
 *
 * TTL: 10 minutes — metadata changes extremely rarely.
 * Does NOT modify connection or auth logic.
 */

import { cacheManager } from '@/lib/runtime/cacheManager';
import type { QueryResult } from '@/lib/snowflake';

const METADATA_KEY = 'snowflake:metadata';
const METADATA_TTL = 600; // 10 minutes

const METADATA_QUERY = `
  SELECT 
    CURRENT_VERSION() as version,
    CURRENT_ACCOUNT() as account,
    CURRENT_USER() as user,
    CURRENT_ROLE() as role
`;

/**
 * Fetch Snowflake metadata with caching.
 * Accepts a live connection and the executeQuery function.
 * Returns the same QueryResult shape the connect route expects.
 */
export async function getMetadata(
    connection: any,
    executeQuery: (conn: any, sql: string) => Promise<QueryResult>,
): Promise<QueryResult> {
    return cacheManager.getOrSet<QueryResult>(
        METADATA_KEY,
        METADATA_TTL,
        () => executeQuery(connection, METADATA_QUERY),
    );
}

/**
 * Force-invalidate metadata cache (e.g. on disconnect or role change).
 */
export function invalidateMetadata(): void {
    cacheManager.invalidate(METADATA_KEY);
}
