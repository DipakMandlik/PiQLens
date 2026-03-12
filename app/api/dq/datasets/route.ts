import { NextRequest, NextResponse } from 'next/server';
import { getServerConfig } from '@/lib/server-config';
import { getOrSetCache, CachePrefix, CacheTTL, generateCacheKey } from '@/lib/cache-service';

export const runtime = 'nodejs';

/**
 * GET /api/dq/datasets?database=BANKING_DW&schema=BRONZE
 * Fetches available datasets (tables) from specified schema
 * with metadata (rowCount, created, lastAltered)
 * 
 * Query Parameters:
 * - database: Database name (default: BANKING_DW)
 * - schema: Schema name (default: BRONZE)
 * 
 * Cached for 1 hour to improve performance
 */
export async function GET(request: NextRequest) {
  try {
    const config = getServerConfig();
    if (!config) {
      return NextResponse.json(
        {
          success: false,
          error: 'Not connected to Snowflake. Please connect first.',
        },
        { status: 401 }
      );
    }

    // Get database and schema from query parameters
    const { searchParams } = new URL(request.url);
    const database = searchParams.get('database') || config.database || 'BANKING_DW';
    const schema = searchParams.get('schema') || config.schema || 'BRONZE';

    // Generate cache key with dynamic database and schema
    const cacheKey = generateCacheKey(CachePrefix.DATASET, database, schema);

    // Try to get from cache first
    const cachedData = await getOrSetCache(
      cacheKey,
      async () => {
        // Cache miss - fetch from Snowflake
        const { snowflakePool, executeQueryObjects } = await import('@/lib/snowflake');
        const connection = await snowflakePool.getConnection(config);

        // Fetch all tables from the specified schema using fully qualified identifiers
        // Use string interpolation for DB as binding before INFORMATION_SCHEMA can cause syntax errors
        const query = `
          SELECT 
            TABLE_NAME, 
            ROW_COUNT,
            BYTES, 
            CREATED, 
            LAST_ALTERED 
          FROM ${database}.INFORMATION_SCHEMA.TABLES
          WHERE TABLE_SCHEMA = ?
          ORDER BY TABLE_NAME
        `;

        const rows = await executeQueryObjects(connection, query, [schema]);

        const datasets = rows.map((row: any) => ({
          name: row.TABLE_NAME,
          database: database,
          schema: schema,
          rowCount: row.ROW_COUNT || 0,
          bytes: row.BYTES || 0,
          created: row.CREATED,
          lastAltered: row.LAST_ALTERED,
        }));

        return {
          success: true,
          data: datasets,
          rowCount: datasets.length,
        };
      },
      CacheTTL.DATASET // 1 hour cache
    );

    // Add cache header
    const response = NextResponse.json(cachedData);
    response.headers.set('X-Cache-Key', cacheKey);

    return response;
  } catch (error: any) {
    console.error('Error fetching datasets:', error);
    return NextResponse.json(
      {
        success: false,
        error: error.message || 'Failed to fetch datasets',
      },
      { status: 500 }
    );
  }
}
