import { NextRequest, NextResponse } from "next/server";
import { getServerConfig } from "@/lib/server-config";
import { snowflakePool } from "@/lib/snowflake";

// POST /api/dq/run-profiling
// Executes table profiling and stores results in DQ_COLUMN_PROFILE
export async function POST(request: NextRequest) {
  try {
    const body = await request.json();
    const { dataset_id, database, schema, table, profile_level = "BASIC", triggered_by } = body;
    const dqDatabase = (body.dqDatabase || "DATA_QUALITY_DB").toUpperCase();
    const dqMetricsSchema = (body.dqMetricsSchema || "DQ_METRICS").toUpperCase();

    if (!database || !schema || !table) {
      return NextResponse.json(
        { success: false, error: "Missing required parameters: database, schema, table" },
        { status: 400 }
      );
    }

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

    const startTime = Date.now();

    // Generate unique run_id
    const runId = `DQ_PROFILE_${Date.now()}_${Math.random().toString(36).substring(2, 8).toUpperCase()}`;

    // Resolve dataset_id if not provided - DO NOT use fallback to ensure consistency
    let resolvedDatasetId = dataset_id;
    if (!resolvedDatasetId) {
      try {
        const datasetSQL = `
          SELECT DATASET_ID 
          FROM ${dqDatabase}.DQ_CONFIG.DATASET_CONFIG
          WHERE UPPER(SOURCE_DATABASE) = UPPER(?)
            AND UPPER(SOURCE_SCHEMA) = UPPER(?)
            AND UPPER(SOURCE_TABLE) = UPPER(?)
          LIMIT 1
        `;
        const datasetResult = await new Promise<any[]>((resolve, reject) => {
          conn.execute({
            sqlText: datasetSQL,
            binds: [database, schema, table],
            complete: (err: any, _stmt: any, rows: any) => {
              if (err) reject(err);
              else resolve(rows || []);
            },
          });
        });
        if (datasetResult.length > 0 && datasetResult[0].DATASET_ID) {
          resolvedDatasetId = datasetResult[0].DATASET_ID;
        } else {
          // No fallback - dataset must be registered for consistency
          console.warn(`Dataset not registered for ${database}.${schema}.${table}`);
          return NextResponse.json(
            { success: false, error: `Dataset not registered for ${database}.${schema}.${table}` },
            { status: 404 }
          );
        }
      } catch (lookupErr: any) {
        console.warn("Dataset lookup failed:", lookupErr.message);
        return NextResponse.json(
          { success: false, error: `Dataset lookup failed: ${lookupErr.message}` },
          { status: 500 }
        );
      }
    }

    if (!resolvedDatasetId) {
      return NextResponse.json(
        { success: false, error: "Missing dataset_id" },
        { status: 400 }
      );
    }


    // Insert run control record - use triggered_by param if provided, else CURRENT_USER()
    const triggeredByValue = triggered_by ? `'${triggered_by}'` : 'CURRENT_USER()';
    const executionMode = triggered_by && ['SCHEDULED', 'SCHEDULER', 'S', 'SYSTEM', 'SCHEDULED_TASK'].includes(String(triggered_by).toUpperCase()) ? 'SCHEDULED' : 'MANUAL';
    const insertRunSQL = `
      INSERT INTO ${dqDatabase}.${dqMetricsSchema}.DQ_RUN_CONTROL (
        RUN_ID, RUN_TYPE, EXECUTION_MODE, TRIGGERED_BY, START_TS, RUN_STATUS, TOTAL_CHECKS, CREATED_TS
      ) VALUES (?, 'PROFILING', '${executionMode}', ${triggeredByValue}, CURRENT_TIMESTAMP(), 'RUNNING', 0, CURRENT_TIMESTAMP())
    `;
    await new Promise<void>((resolve, reject) => {
      conn.execute({
        sqlText: insertRunSQL,
        binds: [runId],
        complete: (err: any) => {
          if (err) reject(err);
          else resolve();
        },
      });
    });

    // Fetch columns from INFORMATION_SCHEMA
    const columnsSQL = `
      SELECT 
        COLUMN_NAME, 
        DATA_TYPE, 
        IS_NULLABLE, 
        ORDINAL_POSITION
      FROM ${database}.INFORMATION_SCHEMA.COLUMNS
      WHERE TABLE_CATALOG = ?
        AND TABLE_SCHEMA = ?
        AND TABLE_NAME = ?
      ORDER BY ORDINAL_POSITION
    `;
    const columnsResult = await new Promise<any[]>((resolve, reject) => {
      conn.execute({
        sqlText: columnsSQL,
        binds: [database.toUpperCase(), schema.toUpperCase(), table.toUpperCase()],
        complete: (err: any, _stmt: any, rows: any) => {
          if (err) reject(err);
          else resolve(rows || []);
        },
      });
    });

    if (columnsResult.length === 0) {
      // Update run control to FAILED
      await updateRunControl(conn, dqDatabase, dqMetricsSchema, runId, "FAILED", 0, 0, 0, 0, startTime);
      return NextResponse.json(
        { success: false, error: `No columns found for table ${database}.${schema}.${table}` },
        { status: 404 }
      );
    }

    const profileResults: any[] = [];
    let processedColumns = 0;

    // Batch columns by type to minimize queries
    const numericCols: string[] = [];
    const stringCols: string[] = [];
    const dateCols: string[] = [];
    const colTypeMap = new Map<string, string>();

    for (const col of columnsResult) {
      const type = col.DATA_TYPE.toUpperCase();
      colTypeMap.set(col.COLUMN_NAME, type);

      if (type.includes("NUMBER") || type.includes("INT") || type.includes("FLOAT") ||
        type.includes("DECIMAL") || type.includes("DOUBLE") || type.includes("REAL")) {
        numericCols.push(col.COLUMN_NAME);
      } else if (type.includes("DATE") || type.includes("TIME") || type.includes("TIMESTAMP")) {
        dateCols.push(col.COLUMN_NAME);
      } else {
        // Default to string treatment for others
        stringCols.push(col.COLUMN_NAME);
      }
    }



    // Helper to run batch query
    const runBatchProfile = async (cols: string[], category: 'NUMERIC' | 'STRING' | 'DATE') => {
      if (cols.length === 0) return;

      const selects: string[] = [];

      // Build one massive SELECT statement
      cols.forEach(col => {
        // Escape column name for SQL identifier: "My "" Column"
        const safeCol = `"${col.replace(/"/g, '""')}"`;

        selects.push(`COUNT(${safeCol}) AS "cnt_${col}"`);
        selects.push(`COUNT(DISTINCT ${safeCol}) AS "dst_${col}"`);

        if (category === 'NUMERIC') {
          selects.push(`MIN(${safeCol})::VARCHAR AS "min_${col}"`);
          selects.push(`MAX(${safeCol})::VARCHAR AS "max_${col}"`);
          selects.push(`AVG(${safeCol}) AS "avg_${col}"`);
          selects.push(`STDDEV(${safeCol}) AS "std_${col}"`);
        } else if (category === 'STRING') {
          selects.push(`MIN(${safeCol})::VARCHAR AS "min_${col}"`);
          selects.push(`MAX(${safeCol})::VARCHAR AS "max_${col}"`);
          selects.push(`MIN(LENGTH(${safeCol})) AS "min_len_${col}"`);
          selects.push(`MAX(LENGTH(${safeCol})) AS "max_len_${col}"`);
          selects.push(`AVG(LENGTH(${safeCol})) AS "avg_len_${col}"`);
        } else if (category === 'DATE') {
          selects.push(`MIN(${safeCol})::VARCHAR AS "min_${col}"`);
          selects.push(`MAX(${safeCol})::VARCHAR AS "max_${col}"`);
          selects.push(`SUM(CASE WHEN ${safeCol} > CURRENT_TIMESTAMP() THEN 1 ELSE 0 END) AS "fut_${col}"`);
        }
      });

      // Add total count once
      selects.push('COUNT(*) AS "total_rows"');

      const sql = `SELECT ${selects.join(", ")} FROM ${database}.${schema}."${table}"`;
      console.log(`[PROFILE_DEBUG] Executing batch ${category} for ${cols.length} columns.`);

      try {
        const rows = await new Promise<any[]>((resolve, reject) => {
          conn.execute({
            sqlText: sql,
            complete: (err: any, _stmt: any, rows: any) => {
              if (err) reject(err); else resolve(rows || []);
            }
          });
        });

        const row = rows && rows.length > 0 ? rows[0] : null;
        if (!row) {
          throw new Error(`Batch query returned no rows for ${category}`);
        }

        const totalRows = row["total_rows"];

        // Process results back into objects
        for (const col of cols) {
          const stats: any = {
            MIN_VALUE: null, MAX_VALUE: null, AVG_VALUE: null, STDDEV_VALUE: null,
            MIN_LENGTH: null, MAX_LENGTH: null, AVG_LENGTH: null,
            FUTURE_DATE_COUNT: null
          };
          stats.TOTAL_RECORDS = totalRows ?? 0;
          stats.NULL_COUNT = totalRows - (row[`cnt_${col}`] || 0);
          stats.DISTINCT_COUNT = row[`dst_${col}`] ?? 0;

          if (category === 'NUMERIC') {
            stats.MIN_VALUE = row[`min_${col}`] ?? null;
            stats.MAX_VALUE = row[`max_${col}`] ?? null;
            stats.AVG_VALUE = row[`avg_${col}`] ?? null;
            stats.STDDEV_VALUE = row[`std_${col}`] ?? null;
          } else if (category === 'STRING') {
            stats.MIN_VALUE = row[`min_${col}`] ?? null;
            stats.MAX_VALUE = row[`max_${col}`] ?? null;
            stats.MIN_LENGTH = row[`min_len_${col}`] ?? null;
            stats.MAX_LENGTH = row[`max_len_${col}`] ?? null;
            stats.AVG_LENGTH = row[`avg_len_${col}`] ?? null;
          } else if (category === 'DATE') {
            stats.MIN_VALUE = row[`min_${col}`] ?? null;
            stats.MAX_VALUE = row[`max_${col}`] ?? null;
            stats.FUTURE_DATE_COUNT = row[`fut_${col}`] ?? null;
          }

          const dataType = colTypeMap.get(col) || 'UNKNOWN';

          const insertProfileSQL = `
              INSERT INTO ${dqDatabase}.${dqMetricsSchema}.DQ_COLUMN_PROFILE (
                RUN_ID, DATASET_ID, DATABASE_NAME, SCHEMA_NAME, TABLE_NAME,
                COLUMN_NAME, DATA_TYPE,
                TOTAL_RECORDS, NULL_COUNT, DISTINCT_COUNT,
                MIN_VALUE, MAX_VALUE, AVG_VALUE, STDDEV_VALUE,
                MIN_LENGTH, MAX_LENGTH, AVG_LENGTH,
                FUTURE_DATE_COUNT, PROFILE_TS
              ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP())
            `;

          await new Promise<void>((resolve, reject) => {
            conn.execute({
              sqlText: insertProfileSQL,
              binds: [
                runId, resolvedDatasetId, database.toUpperCase(), schema.toUpperCase(), table.toUpperCase(),
                col, dataType,
                stats.TOTAL_RECORDS, stats.NULL_COUNT, stats.DISTINCT_COUNT,
                stats.MIN_VALUE, stats.MAX_VALUE, stats.AVG_VALUE, stats.STDDEV_VALUE,
                stats.MIN_LENGTH, stats.MAX_LENGTH, stats.AVG_LENGTH,
                stats.FUTURE_DATE_COUNT
              ],
              complete: (err: any) => { if (err) reject(err); else resolve(); }
            });
          });

          profileResults.push({
            column_name: col,
            data_type: dataType,
            total_records: stats.TOTAL_RECORDS,
            null_count: stats.NULL_COUNT,
            distinct_count: stats.DISTINCT_COUNT,
            min_value: stats.MIN_VALUE,
            max_value: stats.MAX_VALUE,
            avg_value: stats.AVG_VALUE,
            stddev_value: stats.STDDEV_VALUE,
            min_length: stats.MIN_LENGTH,
            max_length: stats.MAX_LENGTH,
            avg_length: stats.AVG_LENGTH,
            future_date_count: stats.FUTURE_DATE_COUNT,
            success: true
          });
          processedColumns++;
        }

      } catch (err: any) {
        console.error(`[PROFILE_ERROR] Error processing batch ${category}:`, err);
        // Fallback or error reporting for these cols
        cols.forEach(col => {
          profileResults.push({ column_name: col, error: err.message || "Batch processing failed" });
        });
      }
    };

    // Execute batches
    // Note: Snowflake has query size limits, usually around 1MB text. 
    // Unless table has 1000s of columns, one batch per type is fine.
    // Safe bet: batch in chunks of 50 columns if needed, but keeping it simple for now.

    await runBatchProfile(numericCols, 'NUMERIC');
    await runBatchProfile(stringCols, 'STRING');
    await runBatchProfile(dateCols, 'DATE');

    // Insert synthetic checks into DQ_CHECK_RESULTS based on profiling flags
    // This makes profiling visible in Run Results modal and Activity tab
    let passedChecks = 0;
    let failedChecks = 0;
    let warningChecks = 0;

    for (const result of profileResults) {
      if (result.error) continue; // Skip columns with errors

      const totalRecords = result.total_records || 0;
      const nullCount = result.null_count || 0;
      const distinctCount = result.distinct_count || 0;
      const nullPercent = totalRecords > 0 ? (nullCount / totalRecords) * 100 : 0;
      const distinctPercent = totalRecords > 0 ? (distinctCount / totalRecords) * 100 : 0;
      const futureDateCount = result.future_date_count || 0;

      // Define synthetic profiling checks
      const syntheticChecks: Array<{
        ruleName: string;
        ruleType: string;
        status: "PASSED" | "FAILED" | "WARNING";
        passRate: number;
        threshold: number;
        totalRecords: number;
        invalidRecords: number;
        failureReason: string | null;
      }> = [];

      // Check 1: High Nulls (>20% nulls = FAILED, >5% = WARNING)
      if (nullPercent > 20) {
        syntheticChecks.push({
          ruleName: "PROFILE_HIGH_NULLS",
          ruleType: "COMPLETENESS",
          status: "FAILED",
          passRate: 100 - nullPercent,
          threshold: 80,
          totalRecords,
          invalidRecords: nullCount,
          failureReason: `${nullPercent.toFixed(1)}% null values detected`,
        });
        failedChecks++;
      } else if (nullPercent > 5) {
        syntheticChecks.push({
          ruleName: "PROFILE_NULL_CHECK",
          ruleType: "COMPLETENESS",
          status: "WARNING",
          passRate: 100 - nullPercent,
          threshold: 95,
          totalRecords,
          invalidRecords: nullCount,
          failureReason: `${nullPercent.toFixed(1)}% null values detected`,
        });
        warningChecks++;
      } else {
        syntheticChecks.push({
          ruleName: "PROFILE_NULL_CHECK",
          ruleType: "COMPLETENESS",
          status: "PASSED",
          passRate: 100 - nullPercent,
          threshold: 95,
          totalRecords,
          invalidRecords: nullCount,
          failureReason: null,
        });
        passedChecks++;
      }

      // Check 2: Low Cardinality (<10% distinct and >1 distinct = WARNING)
      if (distinctPercent < 10 && distinctCount > 1 && totalRecords > 10) {
        syntheticChecks.push({
          ruleName: "PROFILE_LOW_CARDINALITY",
          ruleType: "UNIQUENESS",
          status: "WARNING",
          passRate: distinctPercent,
          threshold: 10,
          totalRecords,
          invalidRecords: totalRecords - distinctCount,
          failureReason: `Only ${distinctCount} distinct values (${distinctPercent.toFixed(1)}%)`,
        });
        warningChecks++;
      }

      // Check 3: Future Dates (any future dates = FAILED)
      if (futureDateCount > 0) {
        syntheticChecks.push({
          ruleName: "PROFILE_FUTURE_DATES",
          ruleType: "VALIDITY",
          status: "FAILED",
          passRate: ((totalRecords - futureDateCount) / totalRecords) * 100,
          threshold: 100,
          totalRecords,
          invalidRecords: futureDateCount,
          failureReason: `${futureDateCount} records with future dates`,
        });
        failedChecks++;
      }

      // Insert synthetic checks into DQ_CHECK_RESULTS
      for (const check of syntheticChecks) {
        try {
          const insertCheckSQL = `
            INSERT INTO ${dqDatabase}.${dqMetricsSchema}.DQ_CHECK_RESULTS (
              RUN_ID, DATASET_ID, DATABASE_NAME, SCHEMA_NAME, TABLE_NAME,
              COLUMN_NAME, RULE_NAME, RULE_TYPE, CHECK_STATUS,
              PASS_RATE, THRESHOLD, TOTAL_RECORDS, INVALID_RECORDS,
              FAILURE_REASON, CREATED_TS
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP())
          `;
          await new Promise<void>((resolve, reject) => {
            conn.execute({
              sqlText: insertCheckSQL,
              binds: [
                runId,
                resolvedDatasetId,
                database.toUpperCase(),
                schema.toUpperCase(),
                table.toUpperCase(),
                result.column_name,
                check.ruleName,
                check.ruleType,
                check.status,
                check.passRate,
                check.threshold,
                check.totalRecords,
                check.invalidRecords,
                check.failureReason,
              ],
              complete: (err: any) => {
                if (err) reject(err);
                else resolve();
              },
            });
          });
        } catch (checkErr: any) {
          console.warn(`Error inserting synthetic check for ${result.column_name}:`, checkErr.message);
        }
      }
    }

    // Determine final run status based on checks
    const finalStatus = failedChecks > 0 ? "COMPLETED_WITH_FAILURES" : "COMPLETED";
    const totalChecks = passedChecks + failedChecks + warningChecks;

    // Update run control with final status and check counts
    await updateRunControl(
      conn, dqDatabase, dqMetricsSchema, runId, finalStatus,
      totalChecks, passedChecks, failedChecks, warningChecks, startTime
    );

    const durationSeconds = (Date.now() - startTime) / 1000;

    return NextResponse.json({
      success: true,
      data: {
        run_id: runId,
        dataset_id: resolvedDatasetId,
        database,
        schema,
        table,
        profile_level,
        columns_profiled: processedColumns,
        total_columns: columnsResult.length,
        total_checks: totalChecks,
        passed_checks: passedChecks,
        failed_checks: failedChecks,
        warning_checks: warningChecks,
        duration_seconds: durationSeconds,
        results: profileResults,
      },
    });
  } catch (error: any) {
    console.error("POST /api/dq/run-profiling error:", error);
    return NextResponse.json(
      { success: false, error: error.message || "Failed to run profiling" },
      { status: 500 }
    );
  }
}

async function updateRunControl(
  conn: any,
  dqDatabase: string,
  dqMetricsSchema: string,
  runId: string,
  status: string,
  totalChecks: number,
  passedChecks: number,
  failedChecks: number,
  warningChecks: number,
  startTime: number
) {
  const durationSeconds = (Date.now() - startTime) / 1000;
  const updateSQL = `
    UPDATE ${dqDatabase}.${dqMetricsSchema}.DQ_RUN_CONTROL
    SET RUN_STATUS = ?,
        END_TS = CURRENT_TIMESTAMP(),
        DURATION_SECONDS = ?,
        TOTAL_CHECKS = ?,
        PASSED_CHECKS = ?,
        FAILED_CHECKS = ?,
        WARNING_CHECKS = ?
    WHERE RUN_ID = ?
  `;
  await new Promise<void>((resolve, reject) => {
    conn.execute({
      sqlText: updateSQL,
      binds: [status, durationSeconds, totalChecks, passedChecks, failedChecks, warningChecks, runId],
      complete: (err: any) => {
        if (err) reject(err);
        else resolve();
      },
    });
  });
}

