import { NextRequest, NextResponse } from 'next/server';
import { snowflakePool, executeQueryObjects, ensureConnectionContext } from '@/lib/snowflake';
import { getServerConfig } from '@/lib/server-config';
import { logger } from '@/lib/logger';
import { createErrorResponse } from '@/lib/errors';
import { retryQuery } from '@/lib/retry';

const endpoint = '/api/dq/yesterday-summary';

export async function GET(request: NextRequest) {
    const startTime = Date.now();
    const { searchParams } = new URL(request.url);
    const dateParam = searchParams.get('date');

    // Default to CURRENT_DATE if no date provided, otherwise use provided date
    // targetDate is the "Today" of the comparison, so we want (targetDate - 1)
    let targetDateSql = 'CURRENT_DATE()';
    if (dateParam && /^\d{4}-\d{2}-\d{2}$/.test(dateParam)) {
        targetDateSql = `'${dateParam}'::DATE`;
    }

    try {
        logger.info('Fetching yesterday summary', { endpoint, dateParam });

        const config = getServerConfig();
        if (!config) {
            return NextResponse.json({ success: false, error: 'Not connected' }, { status: 401 });
        }

        const connection = await snowflakePool.getConnection(config);
        await ensureConnectionContext(connection, config);

        const result = await retryQuery(async () => {
            const query = `
                SELECT
                    DQ_SCORE as YESTERDAY_OVERALL_SCORE,
                    COMPLETENESS_SCORE as YESTERDAY_COMPLETENESS,
                    UNIQUENESS_SCORE as YESTERDAY_UNIQUENESS,
                    VALIDITY_SCORE as YESTERDAY_VALIDITY,
                    CONSISTENCY_SCORE as YESTERDAY_CONSISTENCY,
                    FRESHNESS_SCORE as YESTERDAY_FRESHNESS,
                    VOLUME_SCORE as YESTERDAY_VOLUME,
                    TOTAL_CHECKS as YESTERDAY_CHECKS,
                    PASSED_CHECKS as YESTERDAY_PASSED,
                    FAILED_CHECKS as YESTERDAY_FAILED,
                    LAST_RUN_TS as YESTERDAY_TIMESTAMP
                FROM DATA_QUALITY_DB.DQ_METRICS.DQ_DAILY_SUMMARY
                WHERE SUMMARY_DATE = ${targetDateSql} - 1
                ORDER BY LAST_RUN_TS DESC
                LIMIT 1
            `;

            return await executeQueryObjects(connection, query);
        }, 'yesterday-summary');

        const duration = Date.now() - startTime;

        // No data for yesterday
        if (!result || result.length === 0) {
            logger.info('No yesterday data found', { endpoint, duration });
            return NextResponse.json({
                success: true,
                data: null,
                metadata: {
                    duration,
                    timestamp: new Date().toISOString()
                }
            });
        }

        const row = result[0];

        // Helper function to normalize scores
        const normalizeScore = (val: any): number => {
            if (val === null || val === undefined) return 0;
            const num = Number(val);
            return (num <= 1 && num > -1 && num !== 0) ? num * 100 : num;
        };

        const completeness = normalizeScore(row.YESTERDAY_COMPLETENESS);
        const uniqueness = normalizeScore(row.YESTERDAY_UNIQUENESS);
        const coverage = (completeness + uniqueness) / 2;

        const data = {
            yesterdayOverallScore: normalizeScore(row.YESTERDAY_OVERALL_SCORE),
            yesterdayCoverageScore: coverage,
            yesterdayValidityScore: normalizeScore(row.YESTERDAY_VALIDITY),
            yesterdayChecks: row.YESTERDAY_CHECKS || 0,
            yesterdayPassed: row.YESTERDAY_PASSED || 0,
            yesterdayFailed: row.YESTERDAY_FAILED || 0,
            yesterdayTimestamp: row.YESTERDAY_TIMESTAMP || null,
            yesterdayDimensions: {
                completeness: completeness,
                validity: normalizeScore(row.YESTERDAY_VALIDITY),
                uniqueness: uniqueness,
                consistency: normalizeScore(row.YESTERDAY_CONSISTENCY),
                freshness: normalizeScore(row.YESTERDAY_FRESHNESS),
                volume: normalizeScore(row.YESTERDAY_VOLUME)
            }
        };

        logger.info('Yesterday summary fetched successfully', {
            endpoint,
            duration,
            score: data.yesterdayOverallScore
        });

        return NextResponse.json({
            success: true,
            data,
            metadata: {
                duration,
                timestamp: new Date().toISOString()
            }
        });

    } catch (error: any) {
        const duration = Date.now() - startTime;
        logger.error('Error fetching yesterday summary', error, { endpoint });
        return NextResponse.json(createErrorResponse(error), { status: 500 });
    }
}
