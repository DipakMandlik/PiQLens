import ExcelJS from 'exceljs';
import type { DQDailySummary } from '@/lib/types';
import type { DQDailyExecutiveRenderResult, DQDailyExecutiveRequest } from '../types';

const SHEET_NAME = 'DQ_Daily_Executive_Report';

const COLORS = {
  navy: 'FF0F172A',
  lightGrey: 'FFF3F4F6',
  border: 'FFD1D5DB',
  green: 'FF16A34A',
  amber: 'FFF59E0B',
  red: 'FFDC2626',
  white: 'FFFFFFFF',
  textDark: 'FF111827',
};

interface NormalizedRow {
  SUMMARY_DATE: string;
  DATABASE_NAME: string;
  SCHEMA_NAME: string;
  TABLE_NAME: string;
  BUSINESS_DOMAIN: string;
  PASSED_CHECKS: number;
  FAILED_CHECKS: number;
  WARNING_CHECKS: number;
  SKIPPED_CHECKS: number;
  DQ_SCORE: number;
  PREV_DAY_SCORE: number;
  COMPLETENESS_SCORE: number;
  UNIQUENESS_SCORE: number;
  VALIDITY_SCORE: number;
  CONSISTENCY_SCORE: number;
  FRESHNESS_SCORE: number;
  VOLUME_SCORE: number;
  TRUST_LEVEL: string;
  QUALITY_GRADE: string;
  IS_SLA_MET: boolean;
  TOTAL_RECORDS: number;
  FAILED_RECORDS_COUNT: number;
  FAILURE_RATE_PCT: number;
  TOTAL_EXECUTION_TIME_SEC: number;
  TOTAL_CREDITS_CONSUMED: number;
  LAST_RUN_STATUS: string;
}

interface DomainSummary {
  domain: string;
  avgDQ: number;
  avgFailureRatePct: number;
  slaCompliancePct: number;
}

interface Aggregates {
  overallDQ: number;
  prevDayAvg: number;
  scoreChange: number;
  totalTables: number;
  totalRecords: number;
  totalFailedRecords: number;
  overallFailureRatePct: number;
  slaCompliancePct: number;
  totalExecTimeSec: number;
  totalCredits: number;
  dimensionAverages: Array<{ label: string; value: number; bestTable: string; bestScore: number; lowestTable: string; lowestScore: number }>;
  operational: Array<{ label: string; total: number; pct: number; topTable: string; topValue: number }>;
  trend: Array<{ label: string; count: number; avgDelta: number; sampleTables: string }>;
  domainSummary: DomainSummary[];
  trustDistribution: Array<{ label: string; count: number }>;
  gradeDistribution: Array<{ label: string; count: number }>;
  runStatusDistribution: Array<{ label: string; count: number }>;
}

function clamp(value: number, min: number, max: number): number {
  return Math.max(min, Math.min(max, value));
}

function roundToTwo(value: number): number {
  return Number((value || 0).toFixed(2));
}

function asNumber(value: unknown): number {
  const n = Number(value ?? 0);
  if (!Number.isFinite(n)) return 0;
  return n;
}

function nonNegative(value: unknown): number {
  return Math.max(0, asNumber(value));
}

function normalizeScore(value: unknown): number {
  const raw = asNumber(value);
  const normalized = raw !== 0 && raw > -1 && raw < 1 ? raw * 100 : raw;
  return roundToTwo(clamp(normalized, 0, 100));
}

function normalizeRatePct(value: unknown, failedRecords: number, totalRecords: number): number {
  const input = asNumber(value);
  let pct = input;

  if (!Number.isFinite(input) || input <= 0) {
    pct = totalRecords > 0 ? (failedRecords / totalRecords) * 100 : 0;
  } else if (input > 0 && input <= 1) {
    pct = input * 100;
  }

  return roundToTwo(clamp(pct, 0, 100));
}

function toDateOnly(value: string): string {
  if (!value) return '';
  return String(value).slice(0, 10);
}

function sanitizeText(value: unknown, fallback = ''): string {
  if (value === null || value === undefined) return fallback;
  const text = String(value).trim();
  return text || fallback;
}

function sum(values: number[]): number {
  return values.reduce((acc, value) => acc + value, 0);
}

function avg(values: number[]): number {
  if (!values.length) return 0;
  return sum(values) / values.length;
}

function toPercentDecimal(percentValue: number): number {
  return roundToTwo(percentValue) / 100;
}

function safeRatePct(numerator: number, denominator: number): number {
  if (denominator <= 0) return 0;
  return roundToTwo((numerator / denominator) * 100);
}

function ensureSummaryDate(summaryDate: string): string {
  const trimmed = summaryDate.trim();
  if (!/^\d{4}-\d{2}-\d{2}$/.test(trimmed)) {
    throw new Error('summaryDate must be in YYYY-MM-DD format.');
  }
  return trimmed;
}

function normalizeRows(summaryDate: string, rows: DQDailySummary[]): NormalizedRow[] {
  const normalizedRows: NormalizedRow[] = rows.map((row, index) => {
    const rowDate = toDateOnly(String(row.SUMMARY_DATE || ''));
    if (rowDate !== summaryDate) {
      throw new Error(`Row at index ${index} has SUMMARY_DATE=${rowDate}, expected ${summaryDate}.`);
    }

    const totalRecords = nonNegative(row.TOTAL_RECORDS);
    const failedRecords = nonNegative(row.FAILED_RECORDS_COUNT);

    const qualityGrade = sanitizeText(row.QUALITY_GRADE, 'F').toUpperCase();

    return {
      SUMMARY_DATE: rowDate,
      DATABASE_NAME: sanitizeText(row.DATABASE_NAME, 'UNKNOWN_DB'),
      SCHEMA_NAME: sanitizeText(row.SCHEMA_NAME, 'UNKNOWN_SCHEMA'),
      TABLE_NAME: sanitizeText(row.TABLE_NAME, 'UNKNOWN_TABLE'),
      BUSINESS_DOMAIN: sanitizeText(row.BUSINESS_DOMAIN, 'UNMAPPED'),
      PASSED_CHECKS: nonNegative(row.PASSED_CHECKS),
      FAILED_CHECKS: nonNegative(row.FAILED_CHECKS),
      WARNING_CHECKS: nonNegative(row.WARNING_CHECKS),
      SKIPPED_CHECKS: nonNegative(row.SKIPPED_CHECKS),
      DQ_SCORE: normalizeScore(row.DQ_SCORE),
      PREV_DAY_SCORE: normalizeScore(row.PREV_DAY_SCORE),
      COMPLETENESS_SCORE: normalizeScore(row.COMPLETENESS_SCORE),
      UNIQUENESS_SCORE: normalizeScore(row.UNIQUENESS_SCORE),
      VALIDITY_SCORE: normalizeScore(row.VALIDITY_SCORE),
      CONSISTENCY_SCORE: normalizeScore(row.CONSISTENCY_SCORE),
      FRESHNESS_SCORE: normalizeScore(row.FRESHNESS_SCORE),
      VOLUME_SCORE: normalizeScore(row.VOLUME_SCORE),
      TRUST_LEVEL: sanitizeText(row.TRUST_LEVEL, 'UNKNOWN').toUpperCase(),
      QUALITY_GRADE: qualityGrade,
      IS_SLA_MET: Boolean(row.IS_SLA_MET),
      TOTAL_RECORDS: totalRecords,
      FAILED_RECORDS_COUNT: failedRecords,
      FAILURE_RATE_PCT: normalizeRatePct(row.FAILURE_RATE, failedRecords, totalRecords),
      TOTAL_EXECUTION_TIME_SEC: nonNegative(row.TOTAL_EXECUTION_TIME_SEC),
      TOTAL_CREDITS_CONSUMED: nonNegative(row.TOTAL_CREDITS_CONSUMED),
      LAST_RUN_STATUS: sanitizeText(row.LAST_RUN_STATUS, 'UNKNOWN').toUpperCase(),
    };
  });

  return normalizedRows;
}

function countDistribution(values: string[]): Array<{ label: string; count: number }> {
  const map = new Map<string, number>();
  for (const value of values) {
    map.set(value, (map.get(value) || 0) + 1);
  }
  return [...map.entries()]
    .map(([label, count]) => ({ label, count }))
    .sort((a, b) => b.count - a.count || a.label.localeCompare(b.label));
}
function computeAggregates(rows: NormalizedRow[]): Aggregates {
  const totalTables = rows.length;
  const totalRecords = sum(rows.map((row) => row.TOTAL_RECORDS));
  const totalFailedRecords = sum(rows.map((row) => row.FAILED_RECORDS_COUNT));
  const overallFailureRatePct = safeRatePct(totalFailedRecords, totalRecords);

  const slaMetCount = rows.filter((row) => row.IS_SLA_MET).length;
  const slaCompliancePct = safeRatePct(slaMetCount, totalTables);

  const overallDQ = roundToTwo(avg(rows.map((row) => row.DQ_SCORE)));
  const prevDayAvg = roundToTwo(avg(rows.map((row) => row.PREV_DAY_SCORE)));
  const scoreChange = roundToTwo(overallDQ - prevDayAvg);

  const totalExecTimeSec = roundToTwo(sum(rows.map((row) => row.TOTAL_EXECUTION_TIME_SEC)));
  const totalCredits = roundToTwo(sum(rows.map((row) => row.TOTAL_CREDITS_CONSUMED)));

  const tableLabel = (row: NormalizedRow) => `${row.DATABASE_NAME}.${row.SCHEMA_NAME}.${row.TABLE_NAME}`;

  const dimensionConfigs: Array<{ label: string; getScore: (row: NormalizedRow) => number }> = [
    { label: 'Completeness', getScore: (row) => row.COMPLETENESS_SCORE },
    { label: 'Uniqueness', getScore: (row) => row.UNIQUENESS_SCORE },
    { label: 'Validity', getScore: (row) => row.VALIDITY_SCORE },
    { label: 'Consistency', getScore: (row) => row.CONSISTENCY_SCORE },
    { label: 'Freshness', getScore: (row) => row.FRESHNESS_SCORE },
    { label: 'Volume', getScore: (row) => row.VOLUME_SCORE },
  ];

  const dimensionAverages = dimensionConfigs.map((dimension) => {
    const sorted = [...rows].sort((a, b) => dimension.getScore(b) - dimension.getScore(a));
    const best = sorted[0];
    const lowest = sorted[sorted.length - 1];

    return {
      label: dimension.label,
      value: roundToTwo(avg(rows.map((row) => dimension.getScore(row)))),
      bestTable: best ? tableLabel(best) : '-',
      bestScore: best ? roundToTwo(dimension.getScore(best)) : 0,
      lowestTable: lowest ? tableLabel(lowest) : '-',
      lowestScore: lowest ? roundToTwo(dimension.getScore(lowest)) : 0,
    };
  });

  const passedChecks = sum(rows.map((row) => row.PASSED_CHECKS));
  const failedChecks = sum(rows.map((row) => row.FAILED_CHECKS));
  const warningChecks = sum(rows.map((row) => row.WARNING_CHECKS));
  const skippedChecks = sum(rows.map((row) => row.SKIPPED_CHECKS));
  const allChecks = passedChecks + failedChecks + warningChecks + skippedChecks;

  const operationalMetrics: Array<{ label: string; total: number; getValue: (row: NormalizedRow) => number }> = [
    { label: 'Passed Checks', total: passedChecks, getValue: (row) => row.PASSED_CHECKS },
    { label: 'Failed Checks', total: failedChecks, getValue: (row) => row.FAILED_CHECKS },
    { label: 'Warning Checks', total: warningChecks, getValue: (row) => row.WARNING_CHECKS },
    { label: 'Skipped Checks', total: skippedChecks, getValue: (row) => row.SKIPPED_CHECKS },
  ];

  const operational = operationalMetrics.map((metric) => {
    const topRow = [...rows].sort((a, b) => metric.getValue(b) - metric.getValue(a))[0];
    return {
      label: metric.label,
      total: metric.total,
      pct: safeRatePct(metric.total, allChecks),
      topTable: topRow ? tableLabel(topRow) : '-',
      topValue: topRow ? metric.getValue(topRow) : 0,
    };
  });

  const improvedRows = rows.filter((row) => row.DQ_SCORE > row.PREV_DAY_SCORE);
  const degradedRows = rows.filter((row) => row.DQ_SCORE < row.PREV_DAY_SCORE);
  const stableRows = rows.filter((row) => row.DQ_SCORE === row.PREV_DAY_SCORE);

  const trendBuckets: Array<{ label: string; list: NormalizedRow[] }> = [
    { label: 'Improved Tables', list: improvedRows },
    { label: 'Degraded Tables', list: degradedRows },
    { label: 'Stable Tables', list: stableRows },
  ];

  const trend = trendBuckets.map((bucket) => {
    const count = bucket.list.length;
    const avgDelta = count > 0 ? roundToTwo(avg(bucket.list.map((row) => row.DQ_SCORE - row.PREV_DAY_SCORE))) : 0;
    const sampleTables = bucket.list.slice(0, 3).map((row) => tableLabel(row)).join(', ');

    return {
      label: bucket.label,
      count,
      avgDelta,
      sampleTables: sampleTables || '-',
    };
  });

  const domainMap = new Map<string, NormalizedRow[]>();
  for (const row of rows) {
    if (!domainMap.has(row.BUSINESS_DOMAIN)) {
      domainMap.set(row.BUSINESS_DOMAIN, []);
    }
    domainMap.get(row.BUSINESS_DOMAIN)?.push(row);
  }

  const domainSummary = [...domainMap.entries()]
    .map(([domain, list]) => {
      const domainSla = safeRatePct(list.filter((row) => row.IS_SLA_MET).length, list.length);
      return {
        domain,
        avgDQ: roundToTwo(avg(list.map((row) => row.DQ_SCORE))),
        avgFailureRatePct: roundToTwo(avg(list.map((row) => row.FAILURE_RATE_PCT))),
        slaCompliancePct: domainSla,
      };
    })
    .sort((a, b) => b.avgDQ - a.avgDQ || a.domain.localeCompare(b.domain));

  return {
    overallDQ,
    prevDayAvg,
    scoreChange,
    totalTables,
    totalRecords,
    totalFailedRecords,
    overallFailureRatePct,
    slaCompliancePct,
    totalExecTimeSec,
    totalCredits,
    dimensionAverages,
    operational,
    trend,
    domainSummary,
    trustDistribution: countDistribution(rows.map((row) => row.TRUST_LEVEL)),
    gradeDistribution: countDistribution(rows.map((row) => row.QUALITY_GRADE)),
    runStatusDistribution: countDistribution(rows.map((row) => row.LAST_RUN_STATUS)),
  };
}

function assertValidationGates(rows: NormalizedRow[], aggregates: Aggregates): void {
  if (!rows.length) {
    throw new Error('Validation failed: rows.length must be greater than 0.');
  }

  if (aggregates.totalTables <= 0) {
    throw new Error('Validation failed: totalTables must be greater than 0.');
  }

  if (aggregates.totalRecords <= 0) {
    throw new Error('Validation failed: totalRecords must be greater than 0.');
  }

  const rates = [
    aggregates.overallFailureRatePct,
    aggregates.slaCompliancePct,
    ...rows.map((row) => row.FAILURE_RATE_PCT),
    ...aggregates.domainSummary.map((row) => row.avgFailureRatePct),
    ...aggregates.domainSummary.map((row) => row.slaCompliancePct),
  ];

  for (const rate of rates) {
    if (rate < 0 || rate > 100) {
      throw new Error(`Validation failed: percentage out of range [0,100]. Value=${rate}`);
    }
  }
}

function setAllBorders(sheet: ExcelJS.Worksheet, range: string): void {
  const [start, end] = range.split(':');
  const startCol = start.match(/[A-Z]+/)?.[0] || 'A';
  const startRow = Number(start.match(/\d+/)?.[0] || 1);
  const endCol = end.match(/[A-Z]+/)?.[0] || startCol;
  const endRow = Number(end.match(/\d+/)?.[0] || startRow);

  const startColNum = startCol.split('').reduce((n, c) => n * 26 + c.charCodeAt(0) - 64, 0);
  const endColNum = endCol.split('').reduce((n, c) => n * 26 + c.charCodeAt(0) - 64, 0);

  for (let row = startRow; row <= endRow; row += 1) {
    for (let col = startColNum; col <= endColNum; col += 1) {
      sheet.getCell(row, col).border = {
        top: { style: 'thin', color: { argb: COLORS.border } },
        left: { style: 'thin', color: { argb: COLORS.border } },
        bottom: { style: 'thin', color: { argb: COLORS.border } },
        right: { style: 'thin', color: { argb: COLORS.border } },
      };
    }
  }
}

function styleSectionHeader(sheet: ExcelJS.Worksheet, range: string, text: string): void {
  sheet.mergeCells(range);
  const cell = sheet.getCell(range.split(':')[0]);
  cell.value = text;
  cell.font = { bold: true, color: { argb: COLORS.white }, size: 12 };
  cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: COLORS.navy } };
  cell.alignment = { horizontal: 'left', vertical: 'middle' };
  setAllBorders(sheet, range);
}

function styleHeaderCell(cell: ExcelJS.Cell, text: string): void {
  cell.value = text;
  cell.font = { bold: true, color: { argb: COLORS.textDark } };
  cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: COLORS.lightGrey } };
  cell.alignment = { horizontal: 'left', vertical: 'middle' };
}

function applyRangeFill(sheet: ExcelJS.Worksheet, range: string, color: string): void {
  const [start, end] = range.split(':');
  const startCol = start.match(/[A-Z]+/)?.[0] || 'A';
  const startRow = Number(start.match(/\d+/)?.[0] || 1);
  const endCol = end.match(/[A-Z]+/)?.[0] || startCol;
  const endRow = Number(end.match(/\d+/)?.[0] || startRow);

  const startColNum = startCol.split('').reduce((n, ch) => n * 26 + ch.charCodeAt(0) - 64, 0);
  const endColNum = endCol.split('').reduce((n, ch) => n * 26 + ch.charCodeAt(0) - 64, 0);

  for (let row = startRow; row <= endRow; row += 1) {
    for (let col = startColNum; col <= endColNum; col += 1) {
      sheet.getCell(row, col).fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: color } };
    }
  }
}

function getScoreColor(score: number): string {
  if (score >= 90) return COLORS.green;
  if (score >= 75) return COLORS.amber;
  return COLORS.red;
}

function getMetricColor(label: string): string {
  const value = label.toUpperCase();
  if (value.includes('PASSED')) return COLORS.green;
  if (value.includes('FAILED')) return COLORS.red;
  if (value.includes('WARNING')) return COLORS.amber;
  return COLORS.lightGrey;
}

function getTrendColor(label: string): string {
  const value = label.toUpperCase();
  if (value.includes('IMPROVED')) return COLORS.green;
  if (value.includes('DEGRADED')) return COLORS.red;
  return COLORS.amber;
}

function getTrustColor(label: string): string {
  const value = label.toUpperCase();
  if (value.includes('HIGH')) return COLORS.green;
  if (value.includes('MEDIUM')) return COLORS.amber;
  if (value.includes('LOW')) return COLORS.red;
  return COLORS.lightGrey;
}

function getGradeColor(label: string): string {
  const grade = label.trim().toUpperCase().slice(0, 1);
  if (grade === 'A' || grade === 'B') return COLORS.green;
  if (grade === 'C') return COLORS.amber;
  return COLORS.red;
}

function getRunStatusColor(label: string): string {
  const value = label.toUpperCase();
  if (value.includes('FAIL')) return COLORS.red;
  if (value.includes('COMPLETE') || value.includes('SUCCESS')) return COLORS.green;
  if (value.includes('RUN') || value.includes('WARN')) return COLORS.amber;
  return COLORS.lightGrey;
}

function applyBadgeStyle(cell: ExcelJS.Cell, color: string, horizontal: 'left' | 'center' = 'center'): void {
  const fontColor = color === COLORS.lightGrey || color === COLORS.white || color === COLORS.amber ? COLORS.textDark : COLORS.white;
  const currentFont = cell.font ? { ...cell.font } : {};
  cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: color } };
  cell.font = { ...currentFont, bold: true, color: { argb: fontColor } };
  cell.alignment = { horizontal, vertical: 'middle' };
}

function drawKpiBlock(
  sheet: ExcelJS.Worksheet,
  titleRange: string,
  valueRange: string,
  title: string,
  value: number | string,
  valueOptions?: { numFmt?: string; bold?: boolean }
): string {
  sheet.mergeCells(titleRange);
  const titleCell = sheet.getCell(titleRange.split(':')[0]);
  titleCell.value = title;
  titleCell.font = { bold: true, color: { argb: COLORS.textDark }, size: 10 };
  titleCell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: COLORS.lightGrey } };
  titleCell.alignment = { horizontal: 'left', vertical: 'middle', wrapText: true };

  sheet.mergeCells(valueRange);
  const valueCellAddress = valueRange.split(':')[0];
  const valueCell = sheet.getCell(valueCellAddress);
  valueCell.value = value;
  valueCell.font = { bold: valueOptions?.bold !== false, color: { argb: COLORS.textDark }, size: 14 };
  valueCell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: COLORS.white } };
  valueCell.alignment = { horizontal: 'center', vertical: 'middle' };
  if (valueOptions?.numFmt) {
    valueCell.numFmt = valueOptions.numFmt;
  }

  setAllBorders(sheet, titleRange);
  setAllBorders(sheet, valueRange);

  return valueCellAddress;
}

function addConditionalFormatting(sheet: ExcelJS.Worksheet, config: unknown): void {
  (sheet as any).addConditionalFormatting(config);
}

function applyKpiConditionalFormatting(sheet: ExcelJS.Worksheet, refs: {
  dqCell: string;
  failureRateCell: string;
  slaCell: string;
  scoreChangeCell: string;
}): void {
  addConditionalFormatting(sheet, {
    ref: refs.dqCell,
    rules: [
      {
        type: 'cellIs',
        operator: 'greaterThan',
        formulae: ['89.9999'],
        style: {
          fill: { type: 'pattern', pattern: 'solid', bgColor: { argb: COLORS.green }, fgColor: { argb: COLORS.green } },
          font: { color: { argb: COLORS.white }, bold: true },
        },
      },
      {
        type: 'cellIs',
        operator: 'between',
        formulae: ['75', '89.9999'],
        style: {
          fill: { type: 'pattern', pattern: 'solid', bgColor: { argb: COLORS.amber }, fgColor: { argb: COLORS.amber } },
          font: { color: { argb: COLORS.white }, bold: true },
        },
      },
      {
        type: 'cellIs',
        operator: 'lessThan',
        formulae: ['75'],
        style: {
          fill: { type: 'pattern', pattern: 'solid', bgColor: { argb: COLORS.red }, fgColor: { argb: COLORS.red } },
          font: { color: { argb: COLORS.white }, bold: true },
        },
      },
    ],
  });

  addConditionalFormatting(sheet, {
    ref: refs.slaCell,
    rules: [
      {
        type: 'cellIs',
        operator: 'lessThan',
        formulae: ['0.95'],
        style: {
          fill: { type: 'pattern', pattern: 'solid', bgColor: { argb: COLORS.red }, fgColor: { argb: COLORS.red } },
          font: { color: { argb: COLORS.white }, bold: true },
        },
      },
    ],
  });

  addConditionalFormatting(sheet, {
    ref: refs.failureRateCell,
    rules: [
      {
        type: 'cellIs',
        operator: 'greaterThan',
        formulae: ['0.05'],
        style: {
          fill: { type: 'pattern', pattern: 'solid', bgColor: { argb: COLORS.red }, fgColor: { argb: COLORS.red } },
          font: { color: { argb: COLORS.white }, bold: true },
        },
      },
    ],
  });

  addConditionalFormatting(sheet, {
    ref: refs.scoreChangeCell,
    rules: [
      {
        type: 'cellIs',
        operator: 'greaterThan',
        formulae: ['0'],
        style: { font: { color: { argb: COLORS.green }, bold: true } },
      },
      {
        type: 'cellIs',
        operator: 'lessThan',
        formulae: ['0'],
        style: { font: { color: { argb: COLORS.red }, bold: true } },
      },
    ],
  });
}
function renderWorkbookLayout(
  sheet: ExcelJS.Worksheet,
  rows: NormalizedRow[],
  aggregates: Aggregates,
  summaryDate: string
): number {
  sheet.pageSetup.orientation = 'landscape';
  sheet.pageSetup.fitToPage = true;
  sheet.pageSetup.fitToHeight = 1;
  sheet.pageSetup.fitToWidth = 1;
  sheet.views = [{ state: 'frozen', ySplit: 4, showGridLines: false }];

  const widths = [16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16, 16];
  widths.forEach((width, index) => {
    sheet.getColumn(index + 1).width = width;
  });

  sheet.mergeCells('A1:N1');
  const titleCell = sheet.getCell('A1');
  titleCell.value = 'πQLens - Daily Data Quality Executive Report';
  titleCell.font = { size: 20, bold: true, color: { argb: COLORS.white } };
  titleCell.alignment = { horizontal: 'center', vertical: 'middle' };
  titleCell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: COLORS.navy } };
  setAllBorders(sheet, 'A1:N1');

  sheet.mergeCells('A2:N2');
  const subtitleCell = sheet.getCell('A2');
  subtitleCell.value = `Based on dq_daily_summary | SUMMARY_DATE: ${summaryDate}`;
  subtitleCell.font = { size: 12, color: { argb: COLORS.white } };
  subtitleCell.alignment = { horizontal: 'center', vertical: 'middle' };
  subtitleCell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: COLORS.navy } };
  setAllBorders(sheet, 'A2:N2');

  styleSectionHeader(sheet, 'A4:N4', 'SECTION A - Executive KPI Summary');

  const overallDQCell = drawKpiBlock(sheet, 'A5:C5', 'A6:C6', 'Overall DQ Score', aggregates.overallDQ, { numFmt: '0.00' });
  drawKpiBlock(sheet, 'D5:F5', 'D6:F6', 'Previous Day Average Score', aggregates.prevDayAvg, { numFmt: '0.00' });
  const scoreChangeCell = drawKpiBlock(sheet, 'G5:I5', 'G6:I6', 'Score Change', aggregates.scoreChange, {
    numFmt: '+0.00;-0.00;0.00',
  });
  drawKpiBlock(sheet, 'J5:K5', 'J6:K6', 'Total Tables', aggregates.totalTables, { numFmt: '#,##0' });
  drawKpiBlock(sheet, 'L5:N5', 'L6:N6', 'Total Records', aggregates.totalRecords, { numFmt: '#,##0' });

  drawKpiBlock(sheet, 'A7:C7', 'A8:C8', 'Total Failed Records', aggregates.totalFailedRecords, { numFmt: '#,##0' });
  const failureRateCell = drawKpiBlock(sheet, 'D7:F7', 'D8:F8', 'Overall Failure Rate', toPercentDecimal(aggregates.overallFailureRatePct), {
    numFmt: '0.00%',
  });
  const slaCell = drawKpiBlock(sheet, 'G7:I7', 'G8:I8', 'SLA Compliance %', toPercentDecimal(aggregates.slaCompliancePct), {
    numFmt: '0.00%',
  });
  drawKpiBlock(sheet, 'J7:K7', 'J8:K8', 'Total Execution Time (s)', aggregates.totalExecTimeSec, { numFmt: '#,##0.00' });
  drawKpiBlock(sheet, 'L7:N7', 'L8:N8', 'Total Credits Consumed', aggregates.totalCredits, { numFmt: '#,##0.00' });

  applyKpiConditionalFormatting(sheet, {
    dqCell: overallDQCell,
    failureRateCell,
    slaCell,
    scoreChangeCell,
  });

  styleSectionHeader(sheet, 'A10:F10', 'SECTION B - DQ Dimension Breakdown');
  styleHeaderCell(sheet.getCell('A11'), 'Dimension');
  styleHeaderCell(sheet.getCell('B11'), 'Avg Score');
  styleHeaderCell(sheet.getCell('C11'), 'Best Table');
  styleHeaderCell(sheet.getCell('D11'), 'Best Score');
  styleHeaderCell(sheet.getCell('E11'), 'Lowest Table');
  styleHeaderCell(sheet.getCell('F11'), 'Lowest Score');
  setAllBorders(sheet, 'A11:F11');

  aggregates.dimensionAverages.forEach((item, index) => {
    const row = 12 + index;
    const bandColor = index % 2 === 0 ? COLORS.white : COLORS.lightGrey;

    applyRangeFill(sheet, `A${row}:F${row}`, bandColor);

    sheet.getCell(`A${row}`).value = item.label;
    sheet.getCell(`B${row}`).value = item.value;
    sheet.getCell(`C${row}`).value = item.bestTable;
    sheet.getCell(`D${row}`).value = item.bestScore;
    sheet.getCell(`E${row}`).value = item.lowestTable;
    sheet.getCell(`F${row}`).value = item.lowestScore;

    sheet.getCell(`B${row}`).numFmt = '0.00';
    sheet.getCell(`D${row}`).numFmt = '0.00';
    sheet.getCell(`F${row}`).numFmt = '0.00';

    applyBadgeStyle(sheet.getCell(`B${row}`), getScoreColor(item.value));
    applyBadgeStyle(sheet.getCell(`D${row}`), getScoreColor(item.bestScore));
    applyBadgeStyle(sheet.getCell(`F${row}`), getScoreColor(item.lowestScore));

    setAllBorders(sheet, `A${row}:F${row}`);
  });

  styleSectionHeader(sheet, 'G10:L10', 'SECTION C - Operational Check Distribution');
  styleHeaderCell(sheet.getCell('G11'), 'Metric');
  styleHeaderCell(sheet.getCell('H11'), 'Total');
  styleHeaderCell(sheet.getCell('I11'), 'Percent');
  styleHeaderCell(sheet.getCell('J11'), 'Top Contributing Table');
  styleHeaderCell(sheet.getCell('K11'), 'Top Value');
  styleHeaderCell(sheet.getCell('L11'), 'Remarks');
  setAllBorders(sheet, 'G11:L11');

  aggregates.operational.forEach((item, index) => {
    const row = 12 + index;
    const bandColor = index % 2 === 0 ? COLORS.white : COLORS.lightGrey;
    const metricColor = getMetricColor(item.label);

    applyRangeFill(sheet, `G${row}:L${row}`, bandColor);

    sheet.getCell(`G${row}`).value = item.label;
    sheet.getCell(`H${row}`).value = item.total;
    sheet.getCell(`I${row}`).value = toPercentDecimal(item.pct);
    sheet.getCell(`J${row}`).value = item.topTable;
    sheet.getCell(`K${row}`).value = item.topValue;
    sheet.getCell(`L${row}`).value = item.pct >= 80 ? 'Dominant share' : item.pct >= 20 ? 'Moderate share' : 'Low share';

    sheet.getCell(`H${row}`).numFmt = '#,##0';
    sheet.getCell(`I${row}`).numFmt = '0.00%';
    sheet.getCell(`K${row}`).numFmt = '#,##0';

    applyBadgeStyle(sheet.getCell(`G${row}`), metricColor, 'left');
    applyBadgeStyle(sheet.getCell(`I${row}`), metricColor);

    const remarkCell = sheet.getCell(`L${row}`);
    remarkCell.font = { bold: true, color: { argb: metricColor === COLORS.lightGrey ? COLORS.textDark : metricColor } };

    setAllBorders(sheet, `G${row}:L${row}`);
  });

  styleSectionHeader(sheet, 'A19:F19', 'SECTION F - Trend vs Previous Day');
  styleHeaderCell(sheet.getCell('A20'), 'Trend');
  styleHeaderCell(sheet.getCell('B20'), 'Count');
  styleHeaderCell(sheet.getCell('C20'), 'Avg Delta');
  styleHeaderCell(sheet.getCell('D20'), 'Sample Tables');
  sheet.mergeCells('D20:F20');
  setAllBorders(sheet, 'A20:F20');

  aggregates.trend.forEach((item, index) => {
    const row = 21 + index;
    const bandColor = index % 2 === 0 ? COLORS.white : COLORS.lightGrey;
    const trendColor = getTrendColor(item.label);
    const deltaColor = item.avgDelta > 0 ? COLORS.green : item.avgDelta < 0 ? COLORS.red : COLORS.amber;

    applyRangeFill(sheet, `A${row}:F${row}`, bandColor);

    sheet.getCell(`A${row}`).value = item.label;
    sheet.getCell(`B${row}`).value = item.count;
    sheet.getCell(`C${row}`).value = item.avgDelta;
    sheet.getCell(`B${row}`).numFmt = '#,##0';
    sheet.getCell(`C${row}`).numFmt = '+0.00;-0.00;0.00';
    sheet.mergeCells(`D${row}:F${row}`);
    sheet.getCell(`D${row}`).value = item.sampleTables;

    applyBadgeStyle(sheet.getCell(`A${row}`), trendColor, 'left');
    applyBadgeStyle(sheet.getCell(`C${row}`), deltaColor);

    setAllBorders(sheet, `A${row}:F${row}`);
  });

  styleSectionHeader(sheet, 'G19:N19', 'SECTION G - Governance Summary');

  styleHeaderCell(sheet.getCell('G20'), 'Trust Level');
  styleHeaderCell(sheet.getCell('H20'), 'Count');
  setAllBorders(sheet, 'G20:H20');

  aggregates.trustDistribution.forEach((item, index) => {
    const row = 21 + index;
    const bandColor = index % 2 === 0 ? COLORS.white : COLORS.lightGrey;

    applyRangeFill(sheet, `G${row}:H${row}`, bandColor);

    sheet.getCell(`G${row}`).value = item.label;
    sheet.getCell(`H${row}`).value = item.count;

    applyBadgeStyle(sheet.getCell(`G${row}`), getTrustColor(item.label), 'left');
    setAllBorders(sheet, `G${row}:H${row}`);
  });

  styleHeaderCell(sheet.getCell('I20'), 'Quality Grade');
  styleHeaderCell(sheet.getCell('J20'), 'Count');
  setAllBorders(sheet, 'I20:J20');

  aggregates.gradeDistribution.forEach((item, index) => {
    const row = 21 + index;
    const bandColor = index % 2 === 0 ? COLORS.white : COLORS.lightGrey;

    applyRangeFill(sheet, `I${row}:J${row}`, bandColor);

    sheet.getCell(`I${row}`).value = item.label;
    sheet.getCell(`J${row}`).value = item.count;

    applyBadgeStyle(sheet.getCell(`I${row}`), getGradeColor(item.label));
    setAllBorders(sheet, `I${row}:J${row}`);
  });

  styleHeaderCell(sheet.getCell('K20'), 'Last Run Status');
  styleHeaderCell(sheet.getCell('L20'), 'Count');
  setAllBorders(sheet, 'K20:L20');

  aggregates.runStatusDistribution.forEach((item, index) => {
    const row = 21 + index;
    const bandColor = index % 2 === 0 ? COLORS.white : COLORS.lightGrey;

    applyRangeFill(sheet, `K${row}:L${row}`, bandColor);

    sheet.getCell(`K${row}`).value = item.label;
    sheet.getCell(`L${row}`).value = item.count;

    applyBadgeStyle(sheet.getCell(`K${row}`), getRunStatusColor(item.label), 'left');
    setAllBorders(sheet, `K${row}:L${row}`);
  });

  sheet.mergeCells('M20:N20');
  styleHeaderCell(sheet.getCell('M20'), 'Datasets Processed');
  setAllBorders(sheet, 'M20:N20');
  sheet.mergeCells('M21:N21');
  sheet.getCell('M21').value = rows.length;
  sheet.getCell('M21').font = { bold: true, color: { argb: COLORS.textDark } };
  sheet.getCell('M21').fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: COLORS.lightGrey } };
  sheet.getCell('M21').alignment = { horizontal: 'center', vertical: 'middle' };
  setAllBorders(sheet, 'M21:N21');

  sheet.mergeCells('M22:N22');
  styleHeaderCell(sheet.getCell('M22'), 'Generated On');
  setAllBorders(sheet, 'M22:N22');
  sheet.mergeCells('M23:N23');
  sheet.getCell('M23').value = new Date().toISOString();
  sheet.getCell('M23').fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: COLORS.white } };
  sheet.getCell('M23').alignment = { horizontal: 'center', vertical: 'middle' };
  setAllBorders(sheet, 'M23:N23');

  const governanceDataEndRow = Math.max(
    20 + Math.max(1, aggregates.trustDistribution.length),
    20 + Math.max(1, aggregates.gradeDistribution.length),
    20 + Math.max(1, aggregates.runStatusDistribution.length),
    23
  );
  const sectionDHeaderRow = Math.max(25, governanceDataEndRow + 2);
  styleSectionHeader(sheet, `A${sectionDHeaderRow}:K${sectionDHeaderRow}`, 'SECTION D - Table-Level Risk Grid');
  const tableStartRow = sectionDHeaderRow + 1;
  const riskRows = rows.map((row) => [
    row.DATABASE_NAME,
    row.SCHEMA_NAME,
    row.TABLE_NAME,
    row.BUSINESS_DOMAIN,
    row.DQ_SCORE,
    toPercentDecimal(row.FAILURE_RATE_PCT),
    row.FAILED_CHECKS,
    row.TRUST_LEVEL,
    row.QUALITY_GRADE,
    row.IS_SLA_MET,
    row.LAST_RUN_STATUS,
  ]);

  sheet.addTable({
    name: 'RiskGrid',
    ref: `A${tableStartRow}`,
    headerRow: true,
    style: { theme: 'TableStyleMedium2', showRowStripes: true },
    columns: [
      { name: 'DATABASE_NAME' },
      { name: 'SCHEMA_NAME' },
      { name: 'TABLE_NAME' },
      { name: 'BUSINESS_DOMAIN' },
      { name: 'DQ_SCORE' },
      { name: 'FAILURE_RATE' },
      { name: 'FAILED_CHECKS' },
      { name: 'TRUST_LEVEL' },
      { name: 'QUALITY_GRADE' },
      { name: 'IS_SLA_MET' },
      { name: 'LAST_RUN_STATUS' },
    ],
    rows: riskRows,
  });

  const tableDataStart = tableStartRow + 1;
  const tableDataEnd = tableStartRow + rows.length;

  for (let row = tableDataStart; row <= tableDataEnd; row += 1) {
    sheet.getCell(`E${row}`).numFmt = '0.00';
    sheet.getCell(`F${row}`).numFmt = '0.00%';
    sheet.getCell(`G${row}`).numFmt = '#,##0';
  }

  if (rows.length > 0) {
    addConditionalFormatting(sheet, {
      ref: `E${tableDataStart}:E${tableDataEnd}`,
      rules: [
        {
          type: 'colorScale',
          cfvo: [
            { type: 'num', value: 0 },
            { type: 'num', value: 75 },
            { type: 'num', value: 100 },
          ],
          color: [{ argb: COLORS.red }, { argb: COLORS.amber }, { argb: COLORS.green }],
        },
      ],
    });

    addConditionalFormatting(sheet, {
      ref: `F${tableDataStart}:F${tableDataEnd}`,
      rules: [
        {
          type: 'cellIs',
          operator: 'greaterThan',
          formulae: ['0.05'],
          style: {
            fill: { type: 'pattern', pattern: 'solid', fgColor: { argb: COLORS.red } },
            font: { color: { argb: COLORS.white }, bold: true },
          },
        },
      ],
    });

    addConditionalFormatting(sheet, {
      ref: `J${tableDataStart}:J${tableDataEnd}`,
      rules: [
        {
          type: 'expression',
          formulae: [`=J${tableDataStart}=FALSE`],
          style: {
            fill: { type: 'pattern', pattern: 'solid', fgColor: { argb: COLORS.red } },
            font: { color: { argb: COLORS.white }, bold: true },
          },
        },
      ],
    });

    addConditionalFormatting(sheet, {
      ref: `K${tableDataStart}:K${tableDataEnd}`,
      rules: [
        {
          type: 'containsText',
          operator: 'containsText',
          text: 'FAILED',
          style: {
            fill: { type: 'pattern', pattern: 'solid', fgColor: { argb: COLORS.red } },
            font: { color: { argb: COLORS.white }, bold: true },
          },
        },
      ],
    });

    const gradeRange = `I${tableDataStart}:I${tableDataEnd}`;
    const gradeRules: Array<{ grade: string; color: string }> = [
      { grade: 'A', color: COLORS.green },
      { grade: 'B', color: 'FF65A30D' },
      { grade: 'C', color: COLORS.amber },
      { grade: 'D', color: 'FFEA580C' },
      { grade: 'E', color: COLORS.red },
      { grade: 'F', color: COLORS.red },
    ];

    addConditionalFormatting(sheet, {
      ref: gradeRange,
      rules: gradeRules.map((rule) => ({
        type: 'expression',
        formulae: [`=LEFT(I${tableDataStart},1)="${rule.grade}"`],
        style: {
          fill: { type: 'pattern', pattern: 'solid', fgColor: { argb: rule.color } },
          font: { color: { argb: COLORS.white }, bold: true },
        },
      })),
    });
  }

  const sectionERow = tableDataEnd + 3;
  styleSectionHeader(sheet, `A${sectionERow}:D${sectionERow}`, 'SECTION E - Business Domain Heatmap');

  styleHeaderCell(sheet.getCell(`A${sectionERow + 1}`), 'BUSINESS_DOMAIN');
  styleHeaderCell(sheet.getCell(`B${sectionERow + 1}`), 'AVG_DQ_SCORE');
  styleHeaderCell(sheet.getCell(`C${sectionERow + 1}`), 'AVG_FAILURE_RATE');
  styleHeaderCell(sheet.getCell(`D${sectionERow + 1}`), 'SLA_COMPLIANCE_%');
  setAllBorders(sheet, `A${sectionERow + 1}:D${sectionERow + 1}`);

  aggregates.domainSummary.forEach((item, index) => {
    const row = sectionERow + 2 + index;
    const bandColor = index % 2 === 0 ? COLORS.white : COLORS.lightGrey;

    applyRangeFill(sheet, `A${row}:D${row}`, bandColor);

    sheet.getCell(`A${row}`).value = item.domain;
    sheet.getCell(`B${row}`).value = item.avgDQ;
    sheet.getCell(`C${row}`).value = toPercentDecimal(item.avgFailureRatePct);
    sheet.getCell(`D${row}`).value = toPercentDecimal(item.slaCompliancePct);

    sheet.getCell(`A${row}`).font = { bold: true, color: { argb: COLORS.textDark } };
    sheet.getCell(`B${row}`).numFmt = '0.00';
    sheet.getCell(`C${row}`).numFmt = '0.00%';
    sheet.getCell(`D${row}`).numFmt = '0.00%';

    setAllBorders(sheet, `A${row}:D${row}`);
  });

  if (aggregates.domainSummary.length > 0) {
    const domainDataStart = sectionERow + 2;
    const domainDataEnd = sectionERow + 1 + aggregates.domainSummary.length;
    addConditionalFormatting(sheet, {
      ref: `B${domainDataStart}:B${domainDataEnd}`,
      rules: [
        {
          type: 'colorScale',
          cfvo: [
            { type: 'min' },
            { type: 'percentile', value: 50 },
            { type: 'max' },
          ],
          color: [{ argb: COLORS.red }, { argb: COLORS.amber }, { argb: COLORS.green }],
        },
      ],
    });

    addConditionalFormatting(sheet, {
      ref: `C${domainDataStart}:C${domainDataEnd}`,
      rules: [
        {
          type: 'colorScale',
          cfvo: [
            { type: 'min' },
            { type: 'percentile', value: 50 },
            { type: 'max' },
          ],
          color: [{ argb: COLORS.green }, { argb: COLORS.amber }, { argb: COLORS.red }],
        },
      ],
    });

    addConditionalFormatting(sheet, {
      ref: `D${domainDataStart}:D${domainDataEnd}`,
      rules: [
        {
          type: 'colorScale',
          cfvo: [
            { type: 'min' },
            { type: 'percentile', value: 50 },
            { type: 'max' },
          ],
          color: [{ argb: COLORS.red }, { argb: COLORS.amber }, { argb: COLORS.green }],
        },
      ],
    });
  }

  const footerRow = sectionERow + aggregates.domainSummary.length + 4;
  sheet.mergeCells(`A${footerRow}:N${footerRow}`);
  sheet.getCell(`A${footerRow}`).value = 'πQLens Enterprise Governance Report';
  sheet.getCell(`A${footerRow}`).alignment = { horizontal: 'center', vertical: 'middle' };
  sheet.getCell(`A${footerRow}`).font = { bold: true, color: { argb: COLORS.navy }, size: 12 };

  sheet.mergeCells(`A${footerRow + 1}:N${footerRow + 1}`);
  sheet.getCell(`A${footerRow + 1}`).value = `Generated on: ${new Date().toISOString()}`;
  sheet.getCell(`A${footerRow + 1}`).alignment = { horizontal: 'center', vertical: 'middle' };
  sheet.getCell(`A${footerRow + 1}`).font = { color: { argb: COLORS.textDark }, size: 10 };

  return footerRow + 1;
}

async function assertWorkbookReadable(buffer: Buffer): Promise<void> {
  const workbook = new ExcelJS.Workbook();
  await (workbook.xlsx as any).load(Buffer.from(buffer));
}

function buildFileName(summaryDate: string): string {
  return `PI_QLens_DQ_Report_${summaryDate.replace(/-/g, '_')}.xlsx`;
}
export async function renderDQDailyExecutiveReport(
  input: DQDailyExecutiveRequest
): Promise<DQDailyExecutiveRenderResult> {
  const summaryDate = ensureSummaryDate(input.summaryDate || '');

  if (!Array.isArray(input.rows) || input.rows.length === 0) {
    throw new Error('rows must be a non-empty array of dq_daily_summary records.');
  }

  const rows = normalizeRows(summaryDate, input.rows);
  const aggregates = computeAggregates(rows);
  assertValidationGates(rows, aggregates);

  const workbook = new ExcelJS.Workbook();
  const sheet = workbook.addWorksheet(SHEET_NAME);

  const usedTopRow = renderWorkbookLayout(sheet, rows, aggregates, summaryDate);

  for (let r = 1; r <= usedTopRow; r += 1) {
    sheet.getRow(r).height = 22;
  }
  sheet.getRow(1).height = 34;
  sheet.getRow(2).height = 24;

  const outputBuffer = Buffer.from(await workbook.xlsx.writeBuffer());

  await assertWorkbookReadable(outputBuffer);

  return {
    buffer: outputBuffer,
    fileName: buildFileName(summaryDate),
  };
}

