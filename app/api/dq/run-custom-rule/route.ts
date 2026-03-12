import { NextRequest, NextResponse } from "next/server";
import { getServerConfig } from "@/lib/server-config";
import { snowflakePool } from "@/lib/snowflake";
import { invalidateCache } from "@/lib/valkey";
import { rateLimit } from "@/lib/rate-limit";
import { JobLock } from "@/lib/job-lock";

// POST /api/dq/run-custom-rule
// Body: { dataset_id: string, rule_name: string, column_name?: string | null, threshold?: number | null, run_mode?: string }
export async function POST(request: NextRequest) {
  let jobLock: JobLock | null = null;

  try {
    // --- 1. Rate Limiting ---
    const ip = request.headers.get('x-forwarded-for') || 'anonymous';
    // Max 20 custom rules per minute per IP
    const rateLimitResult = await rateLimit(ip, 'run-custom-rule', 20, 60);

    if (!rateLimitResult.allowed) {
      return NextResponse.json(
        { success: false, error: 'Too many custom rule executions requested. Please wait.' },
        { status: 429, headers: { 'Retry-After': Math.ceil((rateLimitResult.resetTime - Date.now()) / 1000).toString() } }
      );
    }

    const body = await request.json();
    const searchParams = request.nextUrl.searchParams;
    const dqDatabase = (searchParams.get("dqDatabase") || "DATA_QUALITY_DB").toUpperCase();
    const dqEngineSchema = (searchParams.get("dqEngineSchema") || "DQ_ENGINE").toUpperCase();
    const { dataset_id, rule_name, column_name = null, threshold = null, run_mode = 'ADHOC' } = body || {};

    if (!dataset_id || !rule_name) {
      return NextResponse.json(
        { success: false, error: "Missing required payload: dataset_id, rule_name" },
        { status: 400 }
      );
    }

    // --- 2. Job Locking ---
    const lockTarget = `${dataset_id}_${rule_name}_${column_name || 'NOCOL'}`;
    jobLock = new JobLock('run-custom-rule', lockTarget, 120); // 2 minute max
    const lockAcquired = await jobLock.acquire();

    if (!lockAcquired) {
      return NextResponse.json(
        { success: false, error: `This rule is already currently running for ${dataset_id}.` },
        { status: 409 }
      );
    }

    // Try server-stored config first, then fall back to environment variables
    let conn: any;
    try {
      const config = getServerConfig();
      conn = await snowflakePool.getConnection(config || undefined);
    } catch (e: any) {
      return NextResponse.json(
        { success: false, error: `Unable to establish Snowflake connection: ${e?.message || e}` },
        { status: 401 }
      );
    }

    // Call stored procedure with fully-qualified path: <DQ_DB>.<DQ_ENGINE_SCHEMA>.sp_run_custom_rule(...)
    const sql = `CALL ${dqDatabase}.${dqEngineSchema}.sp_run_custom_rule(?, ?, ?, ?, ?)`;
    const binds = [dataset_id, rule_name, column_name, threshold, run_mode];

    const result = await new Promise<any>((resolve, reject) => {
      conn.execute({
        sqlText: sql,
        binds,
        complete: (err: any, _stmt: any, rows: any) => {
          if (err) reject(err);
          else resolve(rows);
        },
      });
    });

    console.log("SP result:", JSON.stringify(result, null, 2));

    // --- CACHE INVALIDATION HOOK ---
    // Trigger massive namespace clear so dashboards fetch fresh aggregated results
    Promise.allSettled([
      invalidateCache('piqlens:*:dq:*'),
      invalidateCache('piqlens:*:dashboard:*'),
      invalidateCache('piqlens:*:catalog:dataset-lineage:*')
    ]).catch(err => console.error('Failed to invalidate caches post-custom-rule:', err));


    return NextResponse.json({ success: true, data: result });
  } catch (error: any) {
    console.error("POST /api/dq/run-custom-rule error:", error);
    return NextResponse.json(
      { success: false, error: error.message || "Failed to run custom rule" },
      { status: 500 }
    );
  } finally {
    if (jobLock) {
      await jobLock.release().catch(e => console.error('Failed to release custom rule lock:', e));
    }
  }
}
