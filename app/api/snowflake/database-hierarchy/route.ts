import { NextRequest, NextResponse } from 'next/server';
import { snowflakePool, executeQuery, ensureConnectionContext } from '@/lib/snowflake';
import { getServerConfig } from '@/lib/server-config';
import { createErrorResponse } from '@/lib/errors';
import { logger } from '@/lib/logger';
import { cache, CacheTTL, generateCacheKey } from '@/lib/cache';

/**
 * GET /api/snowflake/database-hierarchy
 * Fetches complete database → schema → table hierarchy from Snowflake
 * Returns structured data for sidebar navigation
 */
export async function GET(request: NextRequest) {
    const startTime = Date.now();
    const endpoint = '/api/snowflake/database-hierarchy';

    try {
        logger.logApiRequest(endpoint, 'GET');

        // Check cache first (cache for 5 minutes)
        const cacheKey = generateCacheKey(endpoint);
        const cachedData = cache.get(cacheKey);
        if (cachedData) {
            logger.logApiResponse(endpoint, true, Date.now() - startTime);
            return NextResponse.json({
                success: true,
                data: cachedData,
                metadata: { cached: true, timestamp: new Date().toISOString() },
            });
        }

        const config = getServerConfig();
        if (!config) {
            return NextResponse.json(
                {
                    success: false,
                    error: {
                        code: 'AUTH_FAILED',
                        message: 'Not connected',
                        userMessage: 'Please connect to Snowflake first.',
                    },
                },
                { status: 401 }
            );
        }

        const connection = await snowflakePool.getConnection(config);
        await ensureConnectionContext(connection, config);

        // Get all databases first
        const showDbQuery = 'SHOW DATABASES';
        const dbResult = await executeQuery(connection, showDbQuery);

        // Build hierarchical structure
        const databases: Record<string, any> = {};
        const excludedDatabases = [
            'SNOWFLAKE',
            'SNOWFLAKE_SAMPLE_DATA',
            'SNOWFLAKE_LEARNING_DB',
            'DATA_QUALITY_DB'
        ];

        // Process each database
        for (const dbRow of dbResult.rows) {
            const dbName = dbRow[1]; // Database name is in column 1

            // Skip system databases
            if (excludedDatabases.includes(dbName)) {
                continue;
            }

            // Initialize database
            databases[dbName] = {
                id: dbName.toLowerCase().replace(/_/g, '-'),
                name: dbName,
                schemas: {},
            };

            try {
                // Get schemas for this database
                const showSchemasQuery = `SHOW SCHEMAS IN DATABASE ${dbName}`;
                const schemaResult = await executeQuery(connection, showSchemasQuery);

                for (const schemaRow of schemaResult.rows) {
                    const schemaName = schemaRow[1]; // Schema name is in column 1

                    // Skip INFORMATION_SCHEMA and system DQ schemas
                    if (schemaName === 'INFORMATION_SCHEMA' || schemaName === 'DATA_QUALITY') {
                        continue;
                    }

                    // Initialize schema
                    databases[dbName].schemas[schemaName] = {
                        id: schemaName.toLowerCase().replace(/_/g, '-'),
                        name: schemaName,
                        tables: [],
                    };

                    try {
                        // Get tables for this schema
                        const showTablesQuery = `SHOW TABLES IN ${dbName}.${schemaName}`;
                        const tableResult = await executeQuery(connection, showTablesQuery);

                        for (const tableRow of tableResult.rows) {
                            const tableName = tableRow[1]; // Table name is in column 1

                            // Add table
                            databases[dbName].schemas[schemaName].tables.push({
                                id: tableName.toLowerCase().replace(/_/g, '-'),
                                name: tableName,
                                href: `/datasets/${dbName}.${schemaName}/tables/${tableName}`,
                            });
                        }
                    } catch (tableError) {
                        logger.warn(`Error fetching tables for ${dbName}.${schemaName}`, { error: tableError });
                    }
                }
            } catch (schemaError) {
                logger.warn(`Error fetching schemas for ${dbName}`, { error: schemaError });
            }
        }

        // Convert to array format and filter out empty databases
        const databasesArray = Object.values(databases)
            .map((db: any) => ({
                id: db.id,
                name: db.name,
                schemas: Object.values(db.schemas).filter((schema: any) => schema.tables.length > 0),
            }))
            .filter((db: any) => db.schemas.length > 0);

        // Cache for 5 minutes
        cache.set(cacheKey, databasesArray, CacheTTL.REFERENCE_DATA);

        logger.logApiResponse(endpoint, true, Date.now() - startTime);

        return NextResponse.json({
            success: true,
            data: databasesArray,
            metadata: {
                cached: false,
                timestamp: new Date().toISOString(),
                databaseCount: databasesArray.length,
            },
        });
    } catch (error: any) {
        logger.error('Error fetching database hierarchy', error, { endpoint });
        return NextResponse.json(createErrorResponse(error), { status: 500 });
    }
}
