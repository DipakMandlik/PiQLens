export type OverviewScope = "RUN" | "DATE" | "DATASET";

export type OverviewRunType =
  | "FULL"
  | "INCREMENTAL"
  | "CUSTOM_SCAN"
  | "PROFILING"
  | "UNKNOWN";

export interface FormulaMetadata {
  formula_version: string;
  aggregation_version: string;
  check_score_formula: string;
  dimension_score_formula: string;
  dataset_score_formula: string;
  weights: {
    dimension_weights: Record<string, number>;
    check_weights: Record<string, number>;
  };
}

export interface MetricEnvelope<T> {
  computed_at: string;
  formula_version: string;
  aggregation_version: string;
  scope: OverviewScope;
  formulas: FormulaMetadata;
  data: T;
}

export interface DimensionScoreRow {
  dimension: string;
  score: number;
  failed_checks: number;
  total_checks: number;
  records_failed: number;
  impact_level: "LOW" | "MEDIUM" | "HIGH" | "CRITICAL";
  trend_vs_previous: number | null;
  weight: number;
}

export interface FailureSummary {
  top_failed_checks: Array<{
    rule_name: string;
    failures: number;
    failed_records: number;
    failure_rate: number;
  }>;
  most_impacted_columns: Array<{
    column_name: string;
    failures: number;
    failed_records: number;
  }>;
  top_failure_patterns: Array<{
    pattern: string;
    occurrences: number;
  }>;
  anomalies_triggered: Array<{
    code: string;
    severity: "LOW" | "MEDIUM" | "HIGH" | "CRITICAL";
    description: string;
    value: number;
  }>;
}

export interface RunMetricsPayload {
  run_id: string;
  dataset_id: string;
  run_type: OverviewRunType;
  execution_timestamp: string | null;
  execution_duration: number;
  status: string;
  records_scanned: number;
  records_passed: number;
  records_failed: number;
  failure_rate: number;
  checks_executed: number;
  quality_score: number;
  dimension_scores: Record<string, number>;
  check_results: {
    passed: number;
    failed: number;
    warning: number;
    error: number;
    skipped: number;
  };
  anomaly_flags: string[];
  sparkline_7d: Array<{ date: string; score: number }>;
  stability_index: number;
  volatility_score: number;
  anomaly_frequency: number;
}

export interface DateMetricsPayload {
  dataset_id: string;
  date: string;
  run_count: number;
  latest_run_id: string | null;
  total_records_scanned_date: number;
  total_failed_date: number;
  avg_score_date: number;
  failure_rate_date: number;
  checks_executed_date: number;
  sparkline_7d: Array<{ date: string; score: number }>;
  stability_index: number;
  volatility_score: number;
  anomaly_frequency: number;
  dimension_breakdown: DimensionScoreRow[];
  failure_summary: FailureSummary;
}

export interface DatasetMetricsPayload {
  dataset_id: string;
  total_runs: number;
  lifetime_records_scanned: number;
  lifetime_records_failed: number;
  lifetime_failure_rate: number;
  lifetime_checks_executed: number;
  stability_index: number;
  volatility_score: number;
  anomaly_count_total: number;
  anomaly_frequency: number;
  weighted_lifetime_score: number;
  last_run_id: string | null;
  last_run_timestamp: string | null;
  sparkline_30d: Array<{ date: string; score: number }>;
  dimension_breakdown: DimensionScoreRow[];
  failure_summary: FailureSummary;
}

export interface RunDeltaPayload {
  run_id: string;
  previous_run_id: string | null;
  delta_score: number | null;
  delta_fail_rate: number | null;
  delta_records: number | null;
  current_score: number;
  previous_score: number | null;
  current_fail_rate: number;
  previous_fail_rate: number | null;
  current_records: number;
  previous_records: number | null;
}

