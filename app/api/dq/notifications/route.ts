import { NextRequest, NextResponse } from "next/server";
import { snowflakePool, executeQuery, ensureConnectionContext } from "@/lib/snowflake";
import { getServerConfig } from "@/lib/server-config";

export const dynamic = "force-dynamic";
export const revalidate = 0;

export async function GET(request: NextRequest) {
  try {
    const config = getServerConfig();
    if (!config) {
      return NextResponse.json({ success: false, error: "Not connected to Snowflake" }, { status: 401 });
    }

    const connection = await snowflakePool.getConnection(config);
    await ensureConnectionContext(connection, config);

    // Fetch the 15 most recent real runs with their statuses to build notifications
    // Filter out RUN_TYPE = 'PROFILING' if the user doesn't want them or keep them but ensure table name exists
    const query = `
      SELECT
        rc.RUN_ID,
        rc.RUN_STATUS,
        rc.RUN_TYPE,
        rc.START_TS,
        rc.END_TS,
        COALESCE(rc.TOTAL_CHECKS, 0) AS TOTAL_CHECKS,
        COALESCE(rc.FAILED_CHECKS, 0) AS FAILED_CHECKS,
        MIN(cr.TABLE_NAME) AS TABLE_NAME,
        MIN(cr.DATABASE_NAME) AS DB_NAME,
        MIN(cr.SCHEMA_NAME) AS SCH_NAME
      FROM DATA_QUALITY_DB.DQ_METRICS.DQ_RUN_CONTROL rc
      JOIN DATA_QUALITY_DB.DQ_METRICS.DQ_CHECK_RESULTS cr
        ON rc.RUN_ID = cr.RUN_ID
      WHERE rc.RUN_STATUS IN ('COMPLETED', 'FAILED', 'COMPLETED_WITH_FAILURES', 'WARNING', 'HEALTHY', 'CRITICAL')
        AND rc.START_TS IS NOT NULL
        AND cr.TABLE_NAME IS NOT NULL
      GROUP BY
        rc.RUN_ID,
        rc.RUN_STATUS,
        rc.RUN_TYPE,
        rc.START_TS,
        rc.END_TS,
        rc.TOTAL_CHECKS,
        rc.FAILED_CHECKS
      ORDER BY rc.START_TS DESC
      LIMIT 15
    `;

    const result = await executeQuery(connection, query);

    const notifications = result.rows.map((row: any[]) => {
      const runId = row[0] || '';
      const runStatus = (row[1] || '').toUpperCase();
      const runType = (row[2] || '').toUpperCase();
      const startTs = row[3];
      const endTs = row[4] || startTs;
      const totalChecks = Number(row[5]) || 0;
      const failedChecks = Number(row[6]) || 0;
      const tableName = row[7] || 'Unknown Table';

      let type: 'alert' | 'info' | 'success' = 'info';
      let title = 'Data Quality Scan';
      let desc = '';

      if (runType === 'PROFILING') {
        type = 'info';
        title = 'Dataset Profiling Complete';
        desc = `Automatic data profiling was successfully completed for table ${tableName}.`;
      } else if (runStatus === 'FAILED' || runStatus === 'COMPLETED_WITH_FAILURES' || runStatus === 'CRITICAL') {
        type = 'alert';
        title = 'Data Quality Alert';
        desc = `${tableName} scan finished with ${failedChecks} failed checks out of ${totalChecks} total checks executed.`;
      } else if (runStatus === 'COMPLETED' || runStatus === 'HEALTHY' || failedChecks === 0) {
        type = 'success';
        title = 'Data Quality Passed';
        desc = `${tableName} passed all ${totalChecks} active data quality checks successfully.`;
      } else if (runStatus === 'WARNING') {
        type = 'info';
        title = 'Data Quality Warning';
        desc = `${tableName} scan completed with warnings that may require attention.`;
      } else {
        type = 'info';
        title = `Scan: ${runStatus}`;
        desc = `Data quality scan for ${tableName} ended with status: ${runStatus}.`;
      }

      // Format time safely, handling Snowflake UTC timestamps
      const endTsDate = endTs ? new Date(endTs) : new Date(startTs);
      
      // Calculate diff in minutes
      const now = new Date();
      const diffMs = now.getTime() - endTsDate.getTime();
      const diffMins = Math.max(0, Math.floor(diffMs / 60000));
      
      const diffHours = Math.floor(diffMins / 60);
      const diffDays = Math.floor(diffHours / 24);

      let timeStr = 'Just now';
      if (diffDays > 0) {
        // If more than 7 days, show the actual date
        if (diffDays > 7 && !isNaN(endTsDate.getTime())) {
          timeStr = endTsDate.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' });
        } else {
          timeStr = `${diffDays} day${diffDays > 1 ? 's' : ''} ago`;
        }
      } 
      else if (diffHours > 0) timeStr = `${diffHours} hr${diffHours > 1 ? 's' : ''} ago`;
      else if (diffMins > 0) timeStr = `${diffMins} min${diffMins > 1 ? 's' : ''} ago`;

      return {
        id: runId,
        title,
        desc,
        time: timeStr,
        type,
        read: false,
      };
    });

    return NextResponse.json({ success: true, data: notifications }, {
        headers: { 'Cache-Control': 'no-store, no-cache, must-revalidate' }
    });
  } catch (error: any) {
    console.error("Error fetching notifications:", error);
    return NextResponse.json(
      { success: false, error: error.message || "Failed to fetch notifications" },
      { status: 500, headers: { 'Cache-Control': 'no-store' } }
    );
  }
}
