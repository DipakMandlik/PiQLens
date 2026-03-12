import type { DQDailySummary } from '@/lib/types';

export type ReportFormat = 'csv' | 'xlsx';
export type ReportVariant = 'summary' | 'detailed';
export type ReportMode = 'date_aggregate' | 'run';
export type ReportScope = 'platform' | 'dataset';

export type ReportStatus = 'PENDING' | 'RUNNING' | 'COMPLETED' | 'FAILED';

export interface GenerateReportV2Request {
  format: ReportFormat;
  variant: ReportVariant;
  mode: ReportMode;
  date?: string;
  runId?: string;
  scope?: ReportScope;
  dataset?: string;
  generatedBy?: string;
}

export interface DQDailyExecutiveRequest {
  summaryDate: string;
  rows: DQDailySummary[];
}

export interface DQDailyExecutiveRenderResult {
  buffer: Buffer;
  fileName: string;
}

export interface GenerateReportV2Response {
  reportId: string;
  status: ReportStatus;
}

export interface StatusResponse {
  reportId: string;
  status: ReportStatus;
  downloadUrl?: string;
  error?: string;
}

export interface ReportHeader {
  reportTitle: string;
  executionMode: 'DATE_AGGREGATE' | 'RUN';
  runReference: string;
  executionDate: string;
  generatedTimestamp: string;
}

export interface SummaryMetrics {
  totalDatasets: number;
  totalChecks: number;
  passedChecks: number;
  failedChecks: number;
  successRate: number;
}

export interface DailySummaryInsights {
  datasetCount: number;
  dqScore: number;
  completenessScore: number;
  uniquenessScore: number;
  validityScore: number;
  consistencyScore: number;
  freshnessScore: number;
  volumeScore: number;
  trustLevel: string;
  qualityGrade: string;
  isSlaMet: boolean;
  totalRecords: number;
  failedRecordsCount: number;
  failureRate: number;
  prevDayScore: number;
  scoreDelta: number;
  scoreTrend: string;
}

export interface DatasetBreakdownRow {
  databaseName: string;
  schemaName: string;
  tableName: string;
  totalChecks: number;
  passedChecks: number;
  failedChecks: number;
  successRate: number;
  lastCheckTimestamp: string;
}

export interface FailureDetailRow {
  runId: string;
  databaseName: string;
  schemaName: string;
  tableName: string;
  columnName: string;
  ruleName: string;
  ruleType: string;
  checkStatus: string;
  invalidRecords: number;
  totalRecords: number;
  passRate: number;
  threshold: number;
  failureReason: string;
  checkTimestamp: string;
}

export interface ReportPayload {
  reportId: string;
  header: ReportHeader;
  summary: SummaryMetrics;
  dailyInsights?: DailySummaryInsights;
  datasets: DatasetBreakdownRow[];
  failures?: FailureDetailRow[];
  metadata: {
    format: ReportFormat;
    variant: ReportVariant;
    mode: ReportMode;
    scope: ReportScope;
    dataset?: string;
    runIds: string[];
    runCount: number;
    failureRowsReturned: number;
    failureRowsTotal: number;
    failureRowsTruncated: boolean;
    generatedBy: string;
  };
}

export interface AggregatedReportData {
  executionDate: string;
  runIds: string[];
  summary: SummaryMetrics;
  dailyInsights?: DailySummaryInsights;
  datasets: DatasetBreakdownRow[];
  failures: FailureDetailRow[];
  failureRowsTotal: number;
}

export interface ReportJobRecord {
  reportId: string;
  status: ReportStatus;
  format: string | null;
  filePath: string | null;
  reportDate: string | null;
  generatedAt: string | null;
  generatedBy: string | null;
  scope: string | null;
  metadata: Record<string, unknown>;
}


