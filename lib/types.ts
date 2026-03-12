// Type definitions for Data Quality tables

export interface DQCheckResult {
  CHECK_ID: number;
  RUN_ID: string;
  CHECK_TIMESTAMP: string;
  DATASET_ID: string | null;
  DATABASE_NAME: string | null;
  SCHEMA_NAME: string | null;
  TABLE_NAME: string | null;
  COLUMN_NAME: string | null;
  RULE_ID: number | null;
  RULE_NAME: string | null;
  RULE_TYPE: string | null;
  RULE_LEVEL: string | null;
  TOTAL_RECORDS: number | null;
  VALID_RECORDS: number | null;
  INVALID_RECORDS: number | null;
  NULL_RECORDS: number | null;
  DUPLICATE_RECORDS: number | null;
  PASS_RATE: number | null;
  THRESHOLD: number | null;
  CHECK_STATUS: string | null;
  EXECUTION_TIME_MS: number | null;
  EXECUTION_CREDITS: number | null;
  FAILURE_REASON: string | null;
  SAMPLE_INVALID_VALUES: any;
  CREATED_TS: string;
}

export interface DQDailySummary {
  SUMMARY_ID: number;
  SUMMARY_DATE: string;
  DATASET_ID: string | null;
  DATABASE_NAME: string | null;
  SCHEMA_NAME: string | null;
  TABLE_NAME: string | null;
  BUSINESS_DOMAIN: string | null;
  TOTAL_CHECKS: number | null;
  PASSED_CHECKS: number | null;
  FAILED_CHECKS: number | null;
  WARNING_CHECKS: number | null;
  SKIPPED_CHECKS: number | null;
  DQ_SCORE: number | null;
  PREV_DAY_SCORE: number | null;
  SCORE_TREND: string | null;
  COMPLETENESS_SCORE: number | null;
  UNIQUENESS_SCORE: number | null;
  VALIDITY_SCORE: number | null;
  CONSISTENCY_SCORE: number | null;
  FRESHNESS_SCORE: number | null;
  VOLUME_SCORE: number | null;
  TRUST_LEVEL: string | null;
  QUALITY_GRADE: string | null;
  IS_SLA_MET: boolean | null;
  TOTAL_RECORDS: number | null;
  FAILED_RECORDS_COUNT: number | null;
  FAILURE_RATE: number | null;
  TOTAL_EXECUTION_TIME_SEC: number | null;
  TOTAL_CREDITS_CONSUMED: number | null;
  LAST_RUN_ID: string | null;
  LAST_RUN_TS: string | null;
  LAST_RUN_STATUS: string | null;
  CREATED_TS: string;
  UPDATED_TS: string | null;
}

export interface DQFailedRecord {
  FAILURE_ID: number;
  CHECK_ID: number;
  RUN_ID: string;
  DATASET_ID: string | null;
  TABLE_NAME: string | null;
  COLUMN_NAME: string | null;
  RULE_NAME: string | null;
  RULE_TYPE: string | null;
  FAILURE_TYPE: string | null;
  FAILED_RECORD_PK: string | null;
  FAILED_COLUMN_VALUE: string | null;
  EXPECTED_PATTERN: string | null;
  ACTUAL_VALUE_TYPE: string | null;
  RELATED_COLUMNS: any;
  ROW_CONTEXT: any;
  FAILURE_CATEGORY: string | null;
  IS_CRITICAL: boolean | null;
  CAN_AUTO_REMEDIATE: boolean | null;
  REMEDIATION_SUGGESTION: string | null;
  DEBUG_SQL: string | null;
  DETECTED_TS: string;
  CREATED_TS: string;
}

export interface DQRunControl {
  RUN_ID: string;
  TRIGGERED_BY: string | null;
  START_TS: string | null;
  END_TS: string | null;
  DURATION_SECONDS: number | null;
  RUN_STATUS: string | null;
  TOTAL_DATASETS: number | null;
  TOTAL_CHECKS: number | null;
  PASSED_CHECKS: number | null;
  FAILED_CHECKS: number | null;
  WARNING_CHECKS: number | null;
  SKIPPED_CHECKS: number | null;
  ERROR_MESSAGE: string | null;
  CREATED_TS: string;
}

// KPI Data Types
export interface KPIData {
  overallDQScore: number;
  totalChecks: number;
  passedChecks: number;
  failedChecks: number;
  warningChecks: number;
  totalRecords: number;
  failedRecords: number;
  lastRunStatus: string;
  lastRunTime: string;
  qualityGrade: string;
  slaCompliance: number;
  scoreTrend: string;
}

