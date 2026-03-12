import ExcelJS from 'exceljs';
import { DailySummaryInsights, ReportPayload } from '../types';

const COLORS = {
  titleBg: 'FF1E3A8A',
  titleText: 'FFFFFFFF',
  sectionBg: 'FFE2E8F0',
  sectionText: 'FF0F172A',
  tableHeaderBg: 'FFE5E7EB',
  tableHeaderText: 'FF1F2937',
  border: 'FFD1D5DB',
  goodBg: 'FFE8F8EF',
  goodText: 'FF166534',
  warnBg: 'FFFEF3C7',
  warnText: 'FF92400E',
  badBg: 'FFFEE2E2',
  badText: 'FF991B1B',
  neutralBg: 'FFF1F5F9',
  neutralText: 'FF334155',
};

const SUMMARY_LAYOUT = {
  startCol: 2,
  endCol: 7,
  labelStart: 2,
  labelEnd: 4,
  valueStart: 5,
  valueEnd: 7,
};

type InsightTone = 'good' | 'warn' | 'bad' | 'neutral';

function formatDisplayDate(dateValue: string): string {
  if (!dateValue) return '';
  const m = dateValue.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (!m) return dateValue;
  return `${m[3]}-${m[2]}-${m[1]}`;
}

function colLetter(col: number): string {
  let value = col;
  let output = '';
  while (value > 0) {
    const mod = (value - 1) % 26;
    output = String.fromCharCode(65 + mod) + output;
    value = Math.floor((value - mod) / 26);
  }
  return output;
}

function cellRef(col: number, row: number): string {
  return `${colLetter(col)}${row}`;
}

function rangeRef(startCol: number, row: number, endCol: number): string {
  return `${cellRef(startCol, row)}:${cellRef(endCol, row)}`;
}

function applyBorder(cell: ExcelJS.Cell) {
  cell.border = {
    top: { style: 'thin', color: { argb: COLORS.border } },
    left: { style: 'thin', color: { argb: COLORS.border } },
    bottom: { style: 'thin', color: { argb: COLORS.border } },
    right: { style: 'thin', color: { argb: COLORS.border } },
  };
}

function applyRowBorders(sheet: ExcelJS.Worksheet, row: number, startCol: number, endCol: number) {
  for (let col = startCol; col <= endCol; col += 1) {
    applyBorder(sheet.getCell(row, col));
  }
}

function applySectionHeaderStyle(row: ExcelJS.Row) {
  row.font = { bold: true, color: { argb: COLORS.sectionText }, size: 12 };
  row.fill = {
    type: 'pattern',
    pattern: 'solid',
    fgColor: { argb: COLORS.sectionBg },
  };
  row.alignment = { vertical: 'middle', horizontal: 'center' };
}

function applyTableHeaderStyle(row: ExcelJS.Row) {
  row.font = { bold: true, color: { argb: COLORS.tableHeaderText } };
  row.fill = {
    type: 'pattern',
    pattern: 'solid',
    fgColor: { argb: COLORS.tableHeaderBg },
  };
  row.alignment = { vertical: 'middle', horizontal: 'center' };
}

function autoFitColumns(worksheet: ExcelJS.Worksheet, min = 12, max = 48) {
  worksheet.columns.forEach((column) => {
    let longest = min;
    column.eachCell?.({ includeEmpty: true }, (cell) => {
      const raw = cell.value;
      const text = typeof raw === 'string' ? raw : String(raw ?? '');
      longest = Math.max(longest, text.length);
    });
    column.width = Math.max(min, Math.min(max, longest + 2));
  });
}

function qualityBand(successRate: number): string {
  if (successRate >= 95) return 'Excellent';
  if (successRate >= 85) return 'Good';
  if (successRate >= 70) return 'Fair';
  return 'Critical Attention';
}

function toneForSuccessRate(successRate: number): InsightTone {
  if (successRate >= 95) return 'good';
  if (successRate >= 85) return 'warn';
  return 'bad';
}

function toneColors(tone: InsightTone): { bg: string; text: string } {
  if (tone === 'good') return { bg: COLORS.goodBg, text: COLORS.goodText };
  if (tone === 'warn') return { bg: COLORS.warnBg, text: COLORS.warnText };
  if (tone === 'bad') return { bg: COLORS.badBg, text: COLORS.badText };
  return { bg: COLORS.neutralBg, text: COLORS.neutralText };
}

function setSectionHeader(sheet: ExcelJS.Worksheet, row: number, title: string) {
  const range = rangeRef(SUMMARY_LAYOUT.startCol, row, SUMMARY_LAYOUT.endCol);
  sheet.mergeCells(range);
  sheet.getCell(cellRef(SUMMARY_LAYOUT.startCol, row)).value = title;
  applySectionHeaderStyle(sheet.getRow(row));
  applyRowBorders(sheet, row, SUMMARY_LAYOUT.startCol, SUMMARY_LAYOUT.endCol);
}

function setKeyValueRow(
  sheet: ExcelJS.Worksheet,
  row: number,
  label: string,
  value: string | number,
  options?: {
    valueNumFmt?: string;
    valueAlign?: 'left' | 'right' | 'center';
    labelAlign?: 'left' | 'right' | 'center';
    tone?: InsightTone;
  }
) {
  sheet.mergeCells(rangeRef(SUMMARY_LAYOUT.labelStart, row, SUMMARY_LAYOUT.labelEnd));
  sheet.mergeCells(rangeRef(SUMMARY_LAYOUT.valueStart, row, SUMMARY_LAYOUT.valueEnd));

  const labelCell = sheet.getCell(cellRef(SUMMARY_LAYOUT.labelStart, row));
  const valueCell = sheet.getCell(cellRef(SUMMARY_LAYOUT.valueStart, row));

  labelCell.value = label;
  labelCell.font = { bold: true, color: { argb: 'FF334155' } };
  labelCell.alignment = { horizontal: options?.labelAlign || 'center', vertical: 'middle' };

  valueCell.value = value;
  valueCell.alignment = { horizontal: options?.valueAlign || 'center', vertical: 'middle' };

  if (options?.valueNumFmt) {
    valueCell.numFmt = options.valueNumFmt;
  }

  if (options?.tone) {
    const tone = toneColors(options.tone);
    valueCell.fill = {
      type: 'pattern',
      pattern: 'solid',
      fgColor: { argb: tone.bg },
    };
    valueCell.font = { bold: true, color: { argb: tone.text } };
  }

  applyRowBorders(sheet, row, SUMMARY_LAYOUT.startCol, SUMMARY_LAYOUT.endCol);
}

function buildFallbackDailyInsights(payload: ReportPayload): DailySummaryInsights {
  const totalRecords = payload.failures?.reduce((sum, row) => sum + row.totalRecords, 0) || 0;
  const failedRecordsCount = payload.failures?.reduce((sum, row) => sum + row.invalidRecords, 0) || 0;
  const failureRate = totalRecords > 0 ? (failedRecordsCount / totalRecords) * 100 : 0;

  return {
    datasetCount: payload.summary.totalDatasets,
    dqScore: payload.summary.successRate,
    completenessScore: payload.summary.successRate,
    uniquenessScore: payload.summary.successRate,
    validityScore: payload.summary.successRate,
    consistencyScore: payload.summary.successRate,
    freshnessScore: payload.summary.successRate,
    volumeScore: payload.summary.successRate,
    trustLevel: payload.summary.failedChecks > 0 ? 'MEDIUM' : 'HIGH',
    qualityGrade: qualityBand(payload.summary.successRate),
    isSlaMet: payload.summary.successRate >= 90,
    totalRecords,
    failedRecordsCount,
    failureRate,
    prevDayScore: 0,
    scoreDelta: 0,
    scoreTrend: 'STABLE',
  };
}

function addSummarySheet(workbook: ExcelJS.Workbook, payload: ReportPayload) {
  const sheet = workbook.addWorksheet('Summary');
  const reportDateDisplay = formatDisplayDate(payload.header.executionDate);
  const daily = payload.dailyInsights || buildFallbackDailyInsights(payload);
  const failedDatasets = payload.datasets.filter((d) => d.failedChecks > 0).length;
  const worstDataset = [...payload.datasets].sort((a, b) => a.successRate - b.successRate)[0];
  const detailedRows = payload.failures?.length || 0;

  sheet.getColumn(1).width = 6;
  sheet.getColumn(2).width = 24;
  sheet.getColumn(3).width = 16;
  sheet.getColumn(4).width = 16;
  sheet.getColumn(5).width = 18;
  sheet.getColumn(6).width = 18;
  sheet.getColumn(7).width = 18;
  sheet.getColumn(8).width = 6;

  sheet.mergeCells(`${cellRef(SUMMARY_LAYOUT.startCol, 1)}:${cellRef(SUMMARY_LAYOUT.endCol, 1)}`);
  const titleCell = sheet.getCell(cellRef(SUMMARY_LAYOUT.startCol, 1));
  titleCell.value = `Pi_Qlens Report for ${reportDateDisplay}`;
  titleCell.font = { bold: true, size: 15, color: { argb: COLORS.titleText } };
  titleCell.fill = {
    type: 'pattern',
    pattern: 'solid',
    fgColor: { argb: COLORS.titleBg },
  };
  titleCell.alignment = { vertical: 'middle', horizontal: 'center' };
  applyRowBorders(sheet, 1, SUMMARY_LAYOUT.startCol, SUMMARY_LAYOUT.endCol);

  sheet.mergeCells(`${cellRef(SUMMARY_LAYOUT.startCol, 2)}:${cellRef(SUMMARY_LAYOUT.endCol, 2)}`);
  const subtitleCell = sheet.getCell(cellRef(SUMMARY_LAYOUT.startCol, 2));
  subtitleCell.value = 'Enterprise Data Quality Summary';
  subtitleCell.font = { italic: true, color: { argb: 'FF475569' } };
  subtitleCell.alignment = { vertical: 'middle', horizontal: 'center' };

  let row = 4;

  setSectionHeader(sheet, row, 'Report Header');
  row += 1;
  setKeyValueRow(sheet, row, 'Report Title', payload.header.reportTitle);
  row += 1;
  setKeyValueRow(sheet, row, 'Execution Mode', payload.header.executionMode);
  row += 1;
  setKeyValueRow(sheet, row, 'Run ID', payload.header.runReference);
  row += 1;
  setKeyValueRow(sheet, row, 'Execution Date', reportDateDisplay);
  row += 1;
  setKeyValueRow(sheet, row, 'Generated Timestamp', payload.header.generatedTimestamp);
  row += 2;

  setSectionHeader(sheet, row, 'Summary Metrics');
  row += 1;
  setKeyValueRow(sheet, row, 'Total Datasets', payload.summary.totalDatasets, { valueNumFmt: '#,##0' });
  row += 1;
  setKeyValueRow(sheet, row, 'Total Checks', payload.summary.totalChecks, { valueNumFmt: '#,##0' });
  row += 1;
  setKeyValueRow(sheet, row, 'Passed Checks', payload.summary.passedChecks, { valueNumFmt: '#,##0', tone: 'good' });
  row += 1;
  setKeyValueRow(sheet, row, 'Failed Checks', payload.summary.failedChecks, {
    valueNumFmt: '#,##0',
    tone: payload.summary.failedChecks > 0 ? 'bad' : 'good',
  });
  row += 1;
  setKeyValueRow(sheet, row, 'Success Rate (%)', payload.summary.successRate / 100, {
    valueNumFmt: '0.00%',
    tone: toneForSuccessRate(payload.summary.successRate),
  });
  row += 2;

  setSectionHeader(sheet, row, 'Daily Summary Insights (DQ_DAILY_SUMMARY)');
  row += 1;
  setKeyValueRow(sheet, row, 'DQ Score (%)', daily.dqScore / 100, {
    valueNumFmt: '0.00%',
    tone: toneForSuccessRate(daily.dqScore),
  });
  row += 1;
  setKeyValueRow(sheet, row, 'Prev Day Score (%)', daily.prevDayScore / 100, { valueNumFmt: '0.00%' });
  row += 1;
  setKeyValueRow(sheet, row, 'Score Delta (%)', daily.scoreDelta / 100, {
    valueNumFmt: '0.00%',
    tone: daily.scoreDelta > 0 ? 'good' : daily.scoreDelta < 0 ? 'bad' : 'neutral',
  });
  row += 1;
  setKeyValueRow(sheet, row, 'Score Trend', daily.scoreTrend, {
    tone: daily.scoreTrend.toUpperCase().includes('IMPROV') ? 'good' : daily.scoreTrend.toUpperCase().includes('DEGRAD') ? 'bad' : 'neutral',
  });
  row += 1;
  setKeyValueRow(sheet, row, 'Trust Level', daily.trustLevel, {
    tone: daily.trustLevel.toUpperCase() === 'HIGH' ? 'good' : daily.trustLevel.toUpperCase() === 'LOW' ? 'bad' : 'warn',
  });
  row += 1;
  setKeyValueRow(sheet, row, 'Quality Grade', daily.qualityGrade, { tone: 'neutral' });
  row += 1;
  setKeyValueRow(sheet, row, 'SLA Met', daily.isSlaMet ? 'Yes' : 'No', { tone: daily.isSlaMet ? 'good' : 'bad' });
  row += 1;
  setKeyValueRow(sheet, row, 'Failure Rate (%)', daily.failureRate / 100, {
    valueNumFmt: '0.00%',
    tone: daily.failureRate === 0 ? 'good' : daily.failureRate <= 5 ? 'warn' : 'bad',
  });
  row += 1;
  setKeyValueRow(sheet, row, 'Total Records', daily.totalRecords, { valueNumFmt: '#,##0' });
  row += 1;
  setKeyValueRow(sheet, row, 'Failed Records', daily.failedRecordsCount, {
    valueNumFmt: '#,##0',
    tone: daily.failedRecordsCount > 0 ? 'bad' : 'good',
  });
  row += 1;
  setKeyValueRow(sheet, row, 'Completeness Score (%)', daily.completenessScore / 100, { valueNumFmt: '0.00%' });
  row += 1;
  setKeyValueRow(sheet, row, 'Uniqueness Score (%)', daily.uniquenessScore / 100, { valueNumFmt: '0.00%' });
  row += 1;
  setKeyValueRow(sheet, row, 'Validity Score (%)', daily.validityScore / 100, { valueNumFmt: '0.00%' });
  row += 1;
  setKeyValueRow(sheet, row, 'Consistency Score (%)', daily.consistencyScore / 100, { valueNumFmt: '0.00%' });
  row += 1;
  setKeyValueRow(sheet, row, 'Freshness Score (%)', daily.freshnessScore / 100, { valueNumFmt: '0.00%' });
  row += 1;
  setKeyValueRow(sheet, row, 'Volume Score (%)', daily.volumeScore / 100, { valueNumFmt: '0.00%' });
  row += 2;

  setSectionHeader(sheet, row, 'Executive Insights');
  row += 1;
  setKeyValueRow(sheet, row, 'Quality Band', qualityBand(payload.summary.successRate), {
    tone: toneForSuccessRate(payload.summary.successRate),
  });
  row += 1;
  setKeyValueRow(sheet, row, 'Datasets With Failures', failedDatasets, {
    valueNumFmt: '#,##0',
    tone: failedDatasets > 0 ? 'warn' : 'good',
  });
  row += 1;
  setKeyValueRow(
    sheet,
    row,
    'Highest Risk Dataset',
    worstDataset ? `${worstDataset.databaseName}.${worstDataset.schemaName}.${worstDataset.tableName}` : '-',
    { tone: worstDataset && worstDataset.failedChecks > 0 ? 'warn' : 'neutral' }
  );
  row += 1;
  setKeyValueRow(sheet, row, 'Detailed Failure Rows', detailedRows, {
    valueNumFmt: '#,##0',
    tone: detailedRows > 0 ? 'warn' : 'neutral',
  });

  sheet.pageSetup.horizontalCentered = true;
  sheet.views = [{ state: 'frozen', ySplit: 4 }];
}

function addDatasetsSheet(workbook: ExcelJS.Workbook, payload: ReportPayload) {
  const sheet = workbook.addWorksheet('Datasets');

  sheet.mergeCells('A1:H1');
  sheet.getCell('A1').value = 'Dataset Breakdown';
  sheet.getCell('A1').font = { bold: true, size: 13, color: { argb: COLORS.titleText } };
  sheet.getCell('A1').fill = {
    type: 'pattern',
    pattern: 'solid',
    fgColor: { argb: COLORS.titleBg },
  };
  sheet.getCell('A1').alignment = { horizontal: 'center', vertical: 'middle' };

  sheet.addRow([]);
  sheet.addRow([
    'Database',
    'Schema',
    'Table',
    'Total Checks',
    'Passed Checks',
    'Failed Checks',
    'Success Rate',
    'Last Check Timestamp',
  ]);

  applyTableHeaderStyle(sheet.getRow(3));

  for (const item of payload.datasets) {
    const row = sheet.addRow([
      item.databaseName,
      item.schemaName,
      item.tableName,
      item.totalChecks,
      item.passedChecks,
      item.failedChecks,
      item.successRate / 100,
      item.lastCheckTimestamp,
    ]);

    if (item.successRate < 85) {
      row.getCell(7).fill = {
        type: 'pattern',
        pattern: 'solid',
        fgColor: { argb: COLORS.warnBg },
      };
      row.getCell(7).font = { bold: true, color: { argb: COLORS.warnText } };
    }
  }

  const lastRow = Math.max(sheet.rowCount, 3);
  for (let r = 3; r <= lastRow; r += 1) {
    for (let c = 1; c <= 8; c += 1) {
      applyBorder(sheet.getCell(r, c));
    }
  }

  sheet.getColumn(4).numFmt = '#,##0';
  sheet.getColumn(5).numFmt = '#,##0';
  sheet.getColumn(6).numFmt = '#,##0';
  sheet.getColumn(7).numFmt = '0.00%';
  sheet.pageSetup.horizontalCentered = true;
  sheet.views = [{ state: 'frozen', ySplit: 3 }];
  autoFitColumns(sheet);
}

function addFailuresSheet(workbook: ExcelJS.Workbook, payload: ReportPayload) {
  if (!payload.failures || payload.failures.length === 0) return;

  const sheet = workbook.addWorksheet('Failures');

  sheet.mergeCells('A1:N1');
  sheet.getCell('A1').value = 'Failed Checks Detailed';
  sheet.getCell('A1').font = { bold: true, size: 13, color: { argb: COLORS.titleText } };
  sheet.getCell('A1').fill = {
    type: 'pattern',
    pattern: 'solid',
    fgColor: { argb: COLORS.titleBg },
  };
  sheet.getCell('A1').alignment = { horizontal: 'center', vertical: 'middle' };

  sheet.addRow([]);
  sheet.addRow([
    'Run ID',
    'Database',
    'Schema',
    'Table',
    'Column',
    'Rule Name',
    'Rule Type',
    'Status',
    'Invalid Records',
    'Total Records',
    'Pass Rate',
    'Threshold',
    'Failure Reason',
    'Check Timestamp',
  ]);

  applyTableHeaderStyle(sheet.getRow(3));

  for (const item of payload.failures) {
    const row = sheet.addRow([
      item.runId,
      item.databaseName,
      item.schemaName,
      item.tableName,
      item.columnName,
      item.ruleName,
      item.ruleType,
      item.checkStatus,
      item.invalidRecords,
      item.totalRecords,
      item.passRate / 100,
      item.threshold / 100,
      item.failureReason,
      item.checkTimestamp,
    ]);

    row.getCell(8).fill = {
      type: 'pattern',
      pattern: 'solid',
      fgColor: { argb: COLORS.badBg },
    };
    row.getCell(8).font = { bold: true, color: { argb: COLORS.badText } };
  }

  const lastRow = Math.max(sheet.rowCount, 3);
  for (let r = 3; r <= lastRow; r += 1) {
    for (let c = 1; c <= 14; c += 1) {
      applyBorder(sheet.getCell(r, c));
    }
  }

  sheet.getColumn(9).numFmt = '#,##0';
  sheet.getColumn(10).numFmt = '#,##0';
  sheet.getColumn(11).numFmt = '0.00%';
  sheet.getColumn(12).numFmt = '0.00%';
  sheet.pageSetup.horizontalCentered = true;
  sheet.views = [{ state: 'frozen', ySplit: 3 }];
  autoFitColumns(sheet, 12, 54);
}

export async function renderReportXlsx(payload: ReportPayload): Promise<Buffer> {
  const workbook = new ExcelJS.Workbook();
  workbook.creator = 'Pi-Qualytics';
  workbook.created = new Date();

  addSummarySheet(workbook, payload);
  addDatasetsSheet(workbook, payload);
  addFailuresSheet(workbook, payload);

  return Buffer.from(await workbook.xlsx.writeBuffer());
}
