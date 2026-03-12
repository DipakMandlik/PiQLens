import { FormulaMetadata } from "@/lib/overview/types";

export const DQ_FORMULA_VERSION = "dq_formula_v2.0.0";
export const AGGREGATION_VERSION = "dq_aggregation_v2.0.0";

const DEFAULT_DIMENSION_WEIGHTS: Record<string, number> = {
  COMPLETENESS: 1,
  UNIQUENESS: 1,
  VALIDITY: 1,
  CONSISTENCY: 1,
  FRESHNESS: 1,
  VOLUME: 1,
  INTEGRITY: 1,
  TIMELINESS: 1,
};

const DEFAULT_CHECK_WEIGHTS: Record<string, number> = {
  COMPLETENESS: 1,
  UNIQUENESS: 1,
  VALIDITY: 1,
  CONSISTENCY: 1,
  FRESHNESS: 1,
  VOLUME: 1,
  INTEGRITY: 1,
  TIMELINESS: 1,
};

interface NormalizedCheckRow {
  ruleType: string;
  totalRecords: number;
  invalidRecords: number;
  validRecords: number;
  checkStatus: string;
}

function safeParseWeightMap(value: string | undefined, fallback: Record<string, number>) {
  if (!value) return fallback;
  try {
    const parsed = JSON.parse(value) as Record<string, number>;
    if (!parsed || typeof parsed !== "object") return fallback;

    const normalized: Record<string, number> = { ...fallback };
    for (const key of Object.keys(parsed)) {
      const numeric = Number(parsed[key]);
      if (Number.isFinite(numeric) && numeric > 0) {
        normalized[key.toUpperCase()] = numeric;
      }
    }
    return normalized;
  } catch {
    return fallback;
  }
}

const configuredDimensionWeights = safeParseWeightMap(
  process.env.DQ_DIMENSION_WEIGHTS,
  DEFAULT_DIMENSION_WEIGHTS
);

const configuredCheckWeights = safeParseWeightMap(
  process.env.DQ_CHECK_WEIGHTS,
  DEFAULT_CHECK_WEIGHTS
);

export function getDimensionWeight(dimension: string): number {
  return configuredDimensionWeights[dimension.toUpperCase()] ?? 1;
}

export function getCheckWeight(ruleType: string): number {
  return configuredCheckWeights[ruleType.toUpperCase()] ?? 1;
}

export function getFormulaMetadata(): FormulaMetadata {
  return {
    formula_version: DQ_FORMULA_VERSION,
    aggregation_version: AGGREGATION_VERSION,
    check_score_formula:
      "check_score = 100 if pass; check_score = (1 - failure_rate) * 100 if partial; check_score = 0 if fully failed",
    dimension_score_formula:
      "dimension_score = SUM(check_score * check_weight) / SUM(check_weight)",
    dataset_score_formula:
      "dataset_score = SUM(dimension_score * dimension_weight) / SUM(dimension_weight)",
    weights: {
      dimension_weights: configuredDimensionWeights,
      check_weights: configuredCheckWeights,
    },
  };
}

export function roundTo(value: number, digits = 2): number {
  const factor = 10 ** digits;
  return Math.round(value * factor) / factor;
}

function normalizeStatus(status: string): string {
  return (status || "").toUpperCase().trim();
}

export function computeCheckScore(row: NormalizedCheckRow): number {
  const status = normalizeStatus(row.checkStatus);
  const total = Math.max(0, Number(row.totalRecords || 0));
  const invalid = Math.max(0, Number(row.invalidRecords || 0));
  const valid = Math.max(0, Number(row.validRecords || 0));

  if (status === "PASSED" || status === "PASS") return 100;
  if (status === "SKIPPED") return 100;

  if (total <= 0) {
    if (status === "FAILED" || status === "FAIL" || status === "ERROR") {
      return 0;
    }
    return valid > 0 ? 100 : 0;
  }

  const failureRate = Math.max(0, Math.min(1, invalid / total));
  if (failureRate >= 1 || ((status === "FAILED" || status === "FAIL" || status === "ERROR") && valid === 0)) {
    return 0;
  }

  if (failureRate <= 0) return 100;
  return roundTo((1 - failureRate) * 100, 4);
}

export function computeDimensionScores(checkRows: NormalizedCheckRow[]): Record<string, number> {
  const grouped = new Map<
    string,
    { weightedScore: number; totalWeight: number }
  >();

  for (const row of checkRows) {
    const dimension = (row.ruleType || "UNKNOWN").toUpperCase();
    const weight = getCheckWeight(dimension);
    const checkScore = computeCheckScore(row);

    const existing = grouped.get(dimension) || { weightedScore: 0, totalWeight: 0 };
    existing.weightedScore += checkScore * weight;
    existing.totalWeight += weight;
    grouped.set(dimension, existing);
  }

  const dimensionScores: Record<string, number> = {};
  for (const [dimension, scoreState] of grouped.entries()) {
    const score = scoreState.totalWeight > 0
      ? scoreState.weightedScore / scoreState.totalWeight
      : 0;
    dimensionScores[dimension] = roundTo(score);
  }

  return dimensionScores;
}

export function computeDatasetScore(dimensionScores: Record<string, number>): number {
  let weightedSum = 0;
  let totalWeight = 0;

  for (const [dimension, score] of Object.entries(dimensionScores)) {
    const weight = getDimensionWeight(dimension);
    weightedSum += score * weight;
    totalWeight += weight;
  }

  if (totalWeight <= 0) return 0;
  return roundTo(weightedSum / totalWeight);
}

export function scoreToImpactLevel(score: number): "LOW" | "MEDIUM" | "HIGH" | "CRITICAL" {
  if (score >= 98) return "LOW";
  if (score >= 90) return "MEDIUM";
  if (score >= 75) return "HIGH";
  return "CRITICAL";
}

export type { NormalizedCheckRow };
