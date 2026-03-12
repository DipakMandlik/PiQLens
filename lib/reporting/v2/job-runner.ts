import { mkdir, stat, unlink, writeFile } from 'fs/promises';
import fs from 'fs';
import path from 'path';
import { v4 as uuidv4 } from 'uuid';
import { executeQueryObjects, ensureConnectionContext, snowflakePool } from '@/lib/snowflake';
import type { DQDailySummary } from '@/lib/types';
import { getServerConfig } from '@/lib/server-config';
import { logger } from '@/lib/logger';
import { assembleReportPayload } from './assembler';
import { renderReportCsv } from './formatters/csv';
import { renderReportXlsx } from './formatters/xlsx';
import { renderDQDailyExecutiveReport } from './formatters/dq-daily-executive';
import { aggregateReportData, REPORTING_CONSTANTS } from './query-service';
import {
  GenerateReportV2Request,
  GenerateReportV2Response,
  ReportFormat,
  ReportJobRecord,
  ReportStatus,
  StatusResponse,
} from './types';

const STORAGE_PATH = process.env.REPORTS_STORAGE_PATH || path.join(process.cwd(), 'reports', 'v2');
const MAX_RETRIES = Number(process.env.REPORT_JOB_MAX_RETRIES || 2);
const STALE_RUNNING_MINUTES = Number(process.env.REPORT_JOB_STALE_MINUTES || 30);
const FILE_RETENTION_DAYS = Number(process.env.REPORT_FILE_RETENTION_DAYS || 30);

function parseMetadata(metadata: unknown): Record<string, unknown> {
  if (!metadata) return {};
  if (typeof metadata === 'object') return metadata as Record<string, unknown>;

  try {
    return JSON.parse(String(metadata));
  } catch {
    return {};
  }
}

function normalizeRequest(input: GenerateReportV2Request): GenerateReportV2Request {
  const format = String(input.format || '').toLowerCase();
  const variant = String(input.variant || '').toLowerCase();
  const mode = String(input.mode || '').toLowerCase();

  if (format !== 'csv' && format !== 'xlsx') {
    throw new Error('format must be csv or xlsx');
  }
  if (variant !== 'summary' && variant !== 'detailed') {
    throw new Error('variant must be summary or detailed');
  }
  if (mode !== 'date_aggregate' && mode !== 'run') {
    throw new Error('mode must be date_aggregate or run');
  }

  if (mode === 'date_aggregate' && !input.date) {
    throw new Error('date is required for date_aggregate mode');
  }
  if (mode === 'run' && !input.runId) {
    throw new Error('runId is required for run mode');
  }

  return {
    format: format as ReportFormat,
    variant: variant as 'summary' | 'detailed',
    mode: mode as 'date_aggregate' | 'run',
    date: input.date,
    runId: input.runId,
    scope: input.scope || 'platform',
    dataset: input.dataset,
    generatedBy: input.generatedBy || 'system',
  };
}

function parseDatasetTriplet(dataset?: string): { databaseName: string; schemaName: string; tableName: string } | null {
  if (!dataset || !dataset.trim()) return null;
  const parts = dataset.split('.').map((p) => p.trim()).filter(Boolean);
  if (parts.length !== 3) return null;

  return {
    databaseName: parts[0].toUpperCase(),
    schemaName: parts[1].toUpperCase(),
    tableName: parts[2].toUpperCase(),
  };
}

async function fetchDailySummaryRowsForExecutiveReport(request: GenerateReportV2Request): Promise<DQDailySummary[]> {
  if (request.mode !== 'date_aggregate' || !request.date) {
    return [];
  }

  const filter = parseDatasetTriplet(request.dataset);
  const filterSql = filter
    ? `
      AND UPPER(d.DATABASE_NAME) = ?
      AND UPPER(d.SCHEMA_NAME) = ?
      AND UPPER(d.TABLE_NAME) = ?
    `
    : '';

  const binds: unknown[] = [
    request.date,
    ...(filter ? [filter.databaseName, filter.schemaName, filter.tableName] : []),
  ];

  const connection = await getConnection();
  const rows = await executeQueryObjects(
    connection,
    `
    SELECT
      d.SUMMARY_ID,
      TO_CHAR(d.SUMMARY_DATE, 'YYYY-MM-DD') AS SUMMARY_DATE,
      d.DATASET_ID,
      d.DATABASE_NAME,
      d.SCHEMA_NAME,
      d.TABLE_NAME,
      d.BUSINESS_DOMAIN,
      d.TOTAL_CHECKS,
      d.PASSED_CHECKS,
      d.FAILED_CHECKS,
      d.WARNING_CHECKS,
      d.SKIPPED_CHECKS,
      d.DQ_SCORE,
      d.PREV_DAY_SCORE,
      d.SCORE_TREND,
      d.COMPLETENESS_SCORE,
      d.UNIQUENESS_SCORE,
      d.VALIDITY_SCORE,
      d.CONSISTENCY_SCORE,
      d.FRESHNESS_SCORE,
      d.VOLUME_SCORE,
      d.TRUST_LEVEL,
      d.QUALITY_GRADE,
      d.IS_SLA_MET,
      d.TOTAL_RECORDS,
      d.FAILED_RECORDS_COUNT,
      d.FAILURE_RATE,
      d.TOTAL_EXECUTION_TIME_SEC,
      d.TOTAL_CREDITS_CONSUMED,
      d.LAST_RUN_ID,
      d.LAST_RUN_TS,
      d.LAST_RUN_STATUS,
      d.CREATED_TS,
      d.UPDATED_TS
    FROM DATA_QUALITY_DB.DQ_METRICS.DQ_DAILY_SUMMARY d
    WHERE d.SUMMARY_DATE = ?
      ${filterSql}
    ORDER BY d.DATABASE_NAME, d.SCHEMA_NAME, d.TABLE_NAME
    `,
    binds
  );

  return rows as unknown as DQDailySummary[];
}

async function getConnection() {
  const config = getServerConfig();
  if (!config) {
    throw new Error('Database configuration not available');
  }

  const connection = await snowflakePool.getConnection(config);
  await ensureConnectionContext(connection, config);
  return connection;
}

function mapReportRow(row: Record<string, unknown>): ReportJobRecord {
  return {
    reportId: String(row.REPORT_ID || ''),
    status: String(row.STATUS || 'PENDING') as ReportStatus,
    format: row.FORMAT ? String(row.FORMAT) : null,
    filePath: row.FILE_PATH ? String(row.FILE_PATH) : null,
    reportDate: row.REPORT_DATE ? String(row.REPORT_DATE) : null,
    generatedAt: row.GENERATED_AT ? String(row.GENERATED_AT) : null,
    generatedBy: row.GENERATED_BY ? String(row.GENERATED_BY) : null,
    scope: row.SCOPE ? String(row.SCOPE) : null,
    metadata: parseMetadata(row.METADATA),
  };
}

async function fetchReportJob(reportId: string): Promise<ReportJobRecord | null> {
  const connection = await getConnection();
  const rows = await executeQueryObjects(
    connection,
    `
    SELECT
      REPORT_ID,
      STATUS,
      FORMAT,
      FILE_PATH,
      REPORT_DATE,
      GENERATED_AT,
      GENERATED_BY,
      SCOPE,
      METADATA
    FROM DATA_QUALITY_DB.DQ_CONFIG.DQ_REPORTS
    WHERE REPORT_ID = ?
    LIMIT 1
    `,
    [reportId]
  );

  if (!rows.length) {
    return null;
  }

  return mapReportRow(rows[0]);
}

function getScopeValue(request: GenerateReportV2Request): string {
  if (request.dataset && request.dataset.trim()) return request.dataset.trim();
  if (request.scope === 'dataset') return request.dataset?.trim() || 'DATASET';
  return 'PLATFORM';
}

export async function enqueueReportJob(input: GenerateReportV2Request): Promise<GenerateReportV2Response> {
  const request = normalizeRequest(input);
  const reportId = uuidv4();
  const reportDate = request.date || new Date().toISOString().slice(0, 10);

  const connection = await getConnection();
  const metadata = {
    request,
    retryCount: 0,
    maxRetries: MAX_RETRIES,
    createdAt: new Date().toISOString(),
  };

  await executeQueryObjects(
    connection,
    `
    INSERT INTO DATA_QUALITY_DB.DQ_CONFIG.DQ_REPORTS
      (REPORT_ID, REPORT_TYPE, SCOPE, REPORT_DATE, GENERATED_AT, GENERATED_BY, FORMAT, FILE_PATH, STATUS, METADATA, DOWNLOAD_COUNT)
    SELECT
      ?, 'PLATFORM', ?, ?, CURRENT_TIMESTAMP(), ?, ?, '', 'PENDING', PARSE_JSON(?), 0
    `,
    [
      reportId,
      getScopeValue(request),
      reportDate,
      request.generatedBy || 'system',
      request.format.toUpperCase(),
      JSON.stringify(metadata),
    ]
  );

  setTimeout(() => {
    void processReportJob(reportId).catch((error) => {
      logger.error('Failed to process queued report job', error, { reportId });
    });
  }, 0);

  return {
    reportId,
    status: 'PENDING',
  };
}

function isStaleRunningJob(job: ReportJobRecord): boolean {
  if (job.status !== 'RUNNING') return false;

  if (!job.generatedAt) return true;
  const generatedAt = new Date(job.generatedAt).getTime();
  if (Number.isNaN(generatedAt)) return true;

  const staleAt = generatedAt + STALE_RUNNING_MINUTES * 60 * 1000;
  return Date.now() > staleAt;
}

async function updateJobState(reportId: string, status: ReportStatus, metadata: Record<string, unknown>, extra?: { filePath?: string; format?: string }) {
  const connection = await getConnection();
  await executeQueryObjects(
    connection,
    `
    UPDATE DATA_QUALITY_DB.DQ_CONFIG.DQ_REPORTS
    SET
      STATUS = ?,
      FILE_PATH = COALESCE(?, FILE_PATH),
      FORMAT = COALESCE(?, FORMAT),
      METADATA = PARSE_JSON(?),
      GENERATED_AT = CURRENT_TIMESTAMP()
    WHERE REPORT_ID = ?
    `,
    [status, extra?.filePath || null, extra?.format || null, JSON.stringify(metadata), reportId]
  );
}

function sanitizeFilePart(value: string): string {
  return value.replace(/[^a-zA-Z0-9_-]/g, '_').slice(0, 80);
}

async function writeReportFile(reportId: string, format: ReportFormat, content: string | Buffer): Promise<{ filePath: string; size: number }> {
  if (!fs.existsSync(STORAGE_PATH)) {
    await mkdir(STORAGE_PATH, { recursive: true });
  }

  const filePath = path.join(STORAGE_PATH, `${sanitizeFilePart(reportId)}.${format}`);
  await writeFile(filePath, content);
  const fileStat = await stat(filePath);

  return {
    filePath,
    size: Number(fileStat.size || 0),
  };
}

export async function processReportJob(reportId: string): Promise<void> {
  const start = Date.now();
  const job = await fetchReportJob(reportId);
  if (!job) {
    throw new Error(`Report job not found: ${reportId}`);
  }

  if (job.status === 'COMPLETED') {
    return;
  }

  if (job.status === 'RUNNING' && !isStaleRunningJob(job)) {
    return;
  }

  const baseMetadata = parseMetadata(job.metadata);
  const retryCount = Number(baseMetadata.retryCount || 0);

  if (retryCount > MAX_RETRIES) {
    await updateJobState(reportId, 'FAILED', {
      ...baseMetadata,
      error: 'Max retries exceeded',
      errorMessage: 'Max retries exceeded',
      failedAt: new Date().toISOString(),
    });
    return;
  }

  const requestInput = (baseMetadata.request || {}) as GenerateReportV2Request;
  const request = normalizeRequest(requestInput);

  const runningMetadata = {
    ...baseMetadata,
    retryCount,
    startedAt: new Date().toISOString(),
    statusMessage: 'Generating report',
  };

  await updateJobState(reportId, 'RUNNING', runningMetadata);

  try {
    const aggregated = await aggregateReportData(request);
    const payload = assembleReportPayload(request, aggregated, reportId);

    let fileContent: string | Buffer;
    let preferredFileName: string | null = null;

    if (request.format === 'csv') {
      fileContent = renderReportCsv(payload);
    } else {
      const shouldUseExecutiveLayout = request.mode === 'date_aggregate' && Boolean(request.date);

      if (shouldUseExecutiveLayout) {
        const summaryRows = await fetchDailySummaryRowsForExecutiveReport(request);

        if (summaryRows.length > 0 && request.date) {
          const executive = await renderDQDailyExecutiveReport({
            summaryDate: request.date,
            rows: summaryRows,
          });

          fileContent = Buffer.from(executive.buffer);
          preferredFileName = executive.fileName;
        } else {
          fileContent = await renderReportXlsx(payload);
        }
      } else {
        fileContent = await renderReportXlsx(payload);
      }
    }

    const written = await writeReportFile(reportId, request.format, fileContent);

    const completedMetadata = {
      ...runningMetadata,
      completedAt: new Date().toISOString(),
      durationMs: Date.now() - start,
      totalDatasets: payload.summary.totalDatasets,
      totalChecks: payload.summary.totalChecks,
      passedChecks: payload.summary.passedChecks,
      failedChecks: payload.summary.failedChecks,
      successRate: payload.summary.successRate,
      runIds: payload.metadata.runIds,
      runCount: payload.metadata.runCount,
      failureRowsReturned: payload.metadata.failureRowsReturned,
      failureRowsTotal: payload.metadata.failureRowsTotal,
      failureRowsTruncated: payload.metadata.failureRowsTruncated,
      rowCountExported:
        payload.datasets.length +
        (payload.failures?.length || 0) +
        1,
      fileSizeBytes: written.size,
      failureRowLimit: REPORTING_CONSTANTS.FAILURE_ROW_LIMIT,
      preferredFileName,
      reportStyle: preferredFileName ? 'DQ_DAILY_EXECUTIVE' : 'STANDARD_V2',
    };

    await updateJobState(reportId, 'COMPLETED', completedMetadata, {
      filePath: written.filePath,
      format: request.format.toUpperCase(),
    });

    logger.info('Report generation completed', {
      reportId,
      mode: request.mode,
      variant: request.variant,
      format: request.format,
      durationMs: Date.now() - start,
    });
  } catch (error: unknown) {
    const failedMetadata = {
      ...runningMetadata,
      retryCount: retryCount + 1,
      error: error instanceof Error ? error.message : 'Unknown report generation error',
      errorMessage: error instanceof Error ? error.message : 'Unknown report generation error',
      failedAt: new Date().toISOString(),
    };

    await updateJobState(reportId, 'FAILED', failedMetadata);
    logger.error('Report generation failed', error, { reportId, retryCount });
  }
}

export async function processPendingJobs(limit = 2): Promise<void> {
  const connection = await getConnection();
  const rows = await executeQueryObjects(
    connection,
    `
    SELECT REPORT_ID
    FROM DATA_QUALITY_DB.DQ_CONFIG.DQ_REPORTS
    WHERE STATUS = 'PENDING'
       OR (STATUS = 'RUNNING' AND GENERATED_AT < DATEADD(minute, -${STALE_RUNNING_MINUTES}, CURRENT_TIMESTAMP()))
    ORDER BY GENERATED_AT ASC
    LIMIT ?
    `,
    [limit]
  );

  for (const row of rows) {
    await processReportJob(String(row.REPORT_ID));
  }

  await cleanupExpiredReportFiles();
}

export async function getReportStatus(reportId: string): Promise<StatusResponse | null> {
  const job = await fetchReportJob(reportId);
  if (!job) return null;

  if (job.status === 'PENDING' || job.status === 'RUNNING') {
    setTimeout(() => {
      void processReportJob(reportId).catch(() => undefined);
    }, 0);
  }

  const status: StatusResponse = {
    reportId,
    status: job.status,
  };

  if (job.status === 'COMPLETED') {
    status.downloadUrl = `/api/reports/v2/download/${reportId}`;
  }

  if (job.status === 'FAILED') {
    const failedMessage = parseMetadata(job.metadata)?.errorMessage;
    status.error = typeof failedMessage === 'string' && failedMessage ? failedMessage : 'Report generation failed';
  }

  return status;
}

export async function listReportHistory(limit = 100): Promise<ReportJobRecord[]> {
  const connection = await getConnection();
  const rows = await executeQueryObjects(
    connection,
    `
    SELECT
      REPORT_ID,
      STATUS,
      FORMAT,
      FILE_PATH,
      REPORT_DATE,
      GENERATED_AT,
      GENERATED_BY,
      SCOPE,
      METADATA
    FROM DATA_QUALITY_DB.DQ_CONFIG.DQ_REPORTS
    ORDER BY GENERATED_AT DESC
    LIMIT ?
    `,
    [limit]
  );

  return rows.map(mapReportRow);
}

export async function getLatestCompletedReport(): Promise<ReportJobRecord | null> {
  const connection = await getConnection();
  const rows = await executeQueryObjects(
    connection,
    `
    SELECT
      REPORT_ID,
      STATUS,
      FORMAT,
      FILE_PATH,
      REPORT_DATE,
      GENERATED_AT,
      GENERATED_BY,
      SCOPE,
      METADATA
    FROM DATA_QUALITY_DB.DQ_CONFIG.DQ_REPORTS
    WHERE STATUS = 'COMPLETED'
    ORDER BY GENERATED_AT DESC
    LIMIT 1
    `
  );

  if (!rows.length) return null;
  return mapReportRow(rows[0]);
}

export async function incrementDownloadCount(reportId: string): Promise<void> {
  const connection = await getConnection();
  await executeQueryObjects(
    connection,
    `
    UPDATE DATA_QUALITY_DB.DQ_CONFIG.DQ_REPORTS
    SET DOWNLOAD_COUNT = COALESCE(DOWNLOAD_COUNT, 0) + 1
    WHERE REPORT_ID = ?
    `,
    [reportId]
  );
}

export function getStoragePath(): string {
  return STORAGE_PATH;
}

export async function getReportJobRecord(reportId: string): Promise<ReportJobRecord | null> {
  return fetchReportJob(reportId);
}




export async function cleanupExpiredReportFiles(): Promise<void> {
  const connection = await getConnection();
  const rows = await executeQueryObjects(
    connection,
    `
    SELECT REPORT_ID, FILE_PATH
    FROM DATA_QUALITY_DB.DQ_CONFIG.DQ_REPORTS
    WHERE STATUS = 'COMPLETED'
      AND FILE_PATH IS NOT NULL
      AND GENERATED_AT < DATEADD(day, -${FILE_RETENTION_DAYS}, CURRENT_TIMESTAMP())
    `
  );

  for (const row of rows as Array<Record<string, unknown>>) {
    const filePath = row.FILE_PATH ? String(row.FILE_PATH) : '';
    if (!filePath) continue;

    if (fs.existsSync(filePath)) {
      await unlink(filePath);
    }
  }
}





