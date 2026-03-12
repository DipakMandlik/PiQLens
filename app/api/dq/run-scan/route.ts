import { NextRequest, NextResponse } from 'next/server';
import { snowflakePool, executeQuery, ensureConnectionContext } from '@/lib/snowflake';
import { getServerConfig } from '@/lib/server-config';
import { invalidateCache } from '@/lib/valkey';
import { rateLimit } from '@/lib/rate-limit';
import { JobLock } from '@/lib/job-lock';

/**
 * POST /api/dq/run-scan
 * 
 * Execute DQ scan with mode selection (Incremental or Full).
 * 
 * Request body:
 * {
 *   scanType: 'incremental' | 'full',
 *   datasetId?: string,  // Optional: filter by dataset
 *   ruleType?: string    // Optional: filter by rule type
 * }
 * 
 * Returns the RUN_ID for polling status.
 */
export async function POST(request: NextRequest) {
    let jobLock: JobLock | null = null;

    try {
        // --- 1. Rate Limiting ---
        const ip = request.headers.get('x-forwarded-for') || 'anonymous';
        // Limit to 5 scan triggers per minute per IP
        const rateLimitResult = await rateLimit(ip, 'run-scan', 5, 60);

        if (!rateLimitResult.allowed) {
            return NextResponse.json(
                { success: false, error: 'Too many requests. Please wait before triggering another scan.' },
                { status: 429, headers: { 'Retry-After': Math.ceil((rateLimitResult.resetTime - Date.now()) / 1000).toString() } }
            );
        }

        const body = await request.json();
        const { scanType = 'full', datasetId = null, ruleType = null, triggered_by } = body;

        // --- 2. Job Locking ---
        const lockTarget = datasetId || 'FULL_SYSTEM';
        jobLock = new JobLock('run-scan', lockTarget, 600); // 10 minute lock

        const lockAcquired = await jobLock.acquire();
        if (!lockAcquired) {
            return NextResponse.json(
                { success: false, error: `A scan is already running for ${lockTarget}. Please wait for it to finish.` },
                { status: 409 }
            );
        }

        const config = getServerConfig();

        if (!config) {
            if (jobLock) await jobLock.release();
            return NextResponse.json(
                {
                    success: false,
                    error: 'Not connected to Snowflake. Please connect first.',
                },
                { status: 401 }
            );
        }

        const connection = await snowflakePool.getConnection(config);
        await ensureConnectionContext(connection, config);

        // Determine which stored procedure to call
        const procedureName = scanType === 'incremental'
            ? 'SP_EXECUTE_DQ_CHECKS_INCREMENTAL'
            : 'SP_EXECUTE_DQ_CHECKS';

        // Build parameter list (all SPs accept dataset_id, rule_type, run_mode)
        const params = [
            datasetId ? `'${datasetId}'` : 'NULL',
            ruleType ? `'${ruleType}'` : 'NULL',
            `'FULL'` // run_mode (FULL vs CRITICAL_ONLY)
        ].join(', ');

        const query = `CALL DATA_QUALITY_DB.DQ_ENGINE.${procedureName}(${params})`;

        console.log(`Executing ${scanType} scan:`, query);

        const result = await executeQuery(connection, query);

        // Parse the JSON result returned by the stored procedure
        let resultData;
        try {
            resultData = JSON.parse(result.rows[0][0]);
        } catch (_e) {
            resultData = { run_id: 'unknown', status: 'completed' };
        }
        // Backfill RUN_TYPE and EXECUTION_MODE on the SP-created row
        if (resultData.run_id && resultData.run_id !== 'unknown') {
            try {
                const runType = scanType === 'incremental' ? 'INCREMENTAL_SCAN' : 'FULL_SCAN';
                const executionMode = triggered_by && ['SCHEDULED', 'SCHEDULER', 'S', 'SYSTEM', 'SCHEDULED_TASK'].includes(String(triggered_by).toUpperCase()) ? 'SCHEDULED' : 'MANUAL';
                const updateSQL = `
                    UPDATE DATA_QUALITY_DB.DQ_METRICS.DQ_RUN_CONTROL
                    SET RUN_TYPE = ?, EXECUTION_MODE = ?
                    WHERE RUN_ID = ? AND (RUN_TYPE IS NULL OR EXECUTION_MODE IS NULL)
                `;
                await executeQuery(connection, updateSQL, [runType, executionMode, resultData.run_id]);

                // --- CACHE INVALIDATION HOOK ---
                // Trigger massive namespace clear so dashboards fetch fresh aggregated results
                Promise.allSettled([
                    invalidateCache('piqlens:*:dq:*'),
                    invalidateCache('piqlens:*:dashboard:*'),
                    invalidateCache('piqlens:*:catalog:dataset-lineage:*')
                ]).catch(err => console.error('Failed to invalidate caches post-run:', err));

            } catch (updateErr: any) {
                console.warn('Failed to backfill RUN_TYPE/EXECUTION_MODE:', updateErr.message);
            }
        }

        return NextResponse.json({
            success: true,
            data: {
                runId: resultData.run_id,
                scanType,
                status: resultData.status,
                message: `${scanType === 'incremental' ? 'Incremental' : 'Full'} scan initiated successfully`
            }
        });
    } catch (error: any) {
        console.error('Error running DQ scan:', error);

        return NextResponse.json(
            {
                success: false,
                error: error.message || 'Failed to run DQ scan',
            },
            { status: 500 }
        );
    } finally {
        if (jobLock) {
            await jobLock.release().catch(e => console.error('Failed to release job lock:', e));
        }
    }
}

