import test from 'node:test';
import assert from 'node:assert/strict';
import ExcelJS from 'exceljs';
import { renderDQDailyExecutiveReport } from '../formatters/dq-daily-executive';
import type { DQDailySummary } from '@/lib/types';

const SUMMARY_DATE = '2026-02-23';

function makeRow(overrides: Partial<DQDailySummary> = {}): DQDailySummary {
  return {
    SUMMARY_ID: 1,
    SUMMARY_DATE,
    DATASET_ID: 'ds_1',
    DATABASE_NAME: 'DB1',
    SCHEMA_NAME: 'SC1',
    TABLE_NAME: 'TB1',
    BUSINESS_DOMAIN: 'Customer',
    TOTAL_CHECKS: 100,
    PASSED_CHECKS: 90,
    FAILED_CHECKS: 5,
    WARNING_CHECKS: 3,
    SKIPPED_CHECKS: 2,
    DQ_SCORE: 90,
    PREV_DAY_SCORE: 80,
    SCORE_TREND: 'IMPROVING',
    COMPLETENESS_SCORE: 92,
    UNIQUENESS_SCORE: 88,
    VALIDITY_SCORE: 91,
    CONSISTENCY_SCORE: 90,
    FRESHNESS_SCORE: 89,
    VOLUME_SCORE: 93,
    TRUST_LEVEL: 'HIGH',
    QUALITY_GRADE: 'A',
    IS_SLA_MET: true,
    TOTAL_RECORDS: 1000,
    FAILED_RECORDS_COUNT: 50,
    FAILURE_RATE: 5,
    TOTAL_EXECUTION_TIME_SEC: 120,
    TOTAL_CREDITS_CONSUMED: 1.25,
    LAST_RUN_ID: 'RUN_1',
    LAST_RUN_TS: '2026-02-23T10:00:00.000Z',
    LAST_RUN_STATUS: 'COMPLETED',
    CREATED_TS: '2026-02-23T10:00:00.000Z',
    UPDATED_TS: '2026-02-23T10:05:00.000Z',
    ...overrides,
  };
}

test('renderDQDailyExecutiveReport returns non-empty buffer and correct filename', async () => {
  const inputRows = [makeRow(), makeRow({ SUMMARY_ID: 2, TABLE_NAME: 'TB2', DQ_SCORE: 70, PREV_DAY_SCORE: 60 })];
  const result = await renderDQDailyExecutiveReport({ summaryDate: SUMMARY_DATE, rows: inputRows });

  assert.ok(result.buffer.length > 0);
  assert.equal(result.fileName, 'PI_QLens_DQ_Daily_Report_2026_02_23.xlsx');
});

test('report workbook includes target sheet and computed KPI values', async () => {
  const inputRows = [
    makeRow({ SUMMARY_ID: 1, DQ_SCORE: 90, PREV_DAY_SCORE: 80, TOTAL_RECORDS: 1000, FAILED_RECORDS_COUNT: 50 }),
    makeRow({ SUMMARY_ID: 2, TABLE_NAME: 'TB2', DQ_SCORE: 70, PREV_DAY_SCORE: 60, TOTAL_RECORDS: 500, FAILED_RECORDS_COUNT: 50 }),
  ];

  const result = await renderDQDailyExecutiveReport({ summaryDate: SUMMARY_DATE, rows: inputRows });

  const wb = new ExcelJS.Workbook();
  await wb.xlsx.load(Buffer.from(result.buffer) as any);
  const sheet = wb.getWorksheet('DQ_Daily_Executive_Report');

  assert.ok(sheet);
  assert.equal(Number(sheet!.getCell('A6').value), 80);
  assert.equal(Number(sheet!.getCell('D6').value), 70);
  assert.equal(Number(sheet!.getCell('J6').value), 2);
  assert.equal(Number(sheet!.getCell('B21').value), 2);
  assert.equal(Number(sheet!.getCell('B22').value), 0);
  assert.equal(Number(sheet!.getCell('B23').value), 0);
});

test('derives row failure rate when FAILURE_RATE is null', async () => {
  const inputRows = [
    makeRow({ SUMMARY_ID: 1, FAILURE_RATE: null, FAILED_RECORDS_COUNT: 10, TOTAL_RECORDS: 100, TABLE_NAME: 'TB_FAIL' }),
  ];

  const result = await renderDQDailyExecutiveReport({ summaryDate: SUMMARY_DATE, rows: inputRows });

  const wb = new ExcelJS.Workbook();
  await wb.xlsx.load(Buffer.from(result.buffer) as any);
  const sheet = wb.getWorksheet('DQ_Daily_Executive_Report');

  const failureRateCell = sheet!.getCell('F27').value as number;
  assert.equal(Number(failureRateCell), 0.1);
});

test('throws validation error when rows are empty', async () => {
  await assert.rejects(
    async () => renderDQDailyExecutiveReport({ summaryDate: SUMMARY_DATE, rows: [] }),
    /non-empty array/i
  );
});

test('throws validation error when total records are zero', async () => {
  const inputRows = [makeRow({ SUMMARY_ID: 1, TOTAL_RECORDS: 0, FAILED_RECORDS_COUNT: 0, FAILURE_RATE: 0 })];

  await assert.rejects(
    async () => renderDQDailyExecutiveReport({ summaryDate: SUMMARY_DATE, rows: inputRows }),
    /totalRecords must be greater than 0/i
  );
});

test('detail sections are populated with data rows', async () => {
  const result = await renderDQDailyExecutiveReport({ summaryDate: SUMMARY_DATE, rows: [makeRow()] });

  const wb = new ExcelJS.Workbook();
  await wb.xlsx.load(Buffer.from(result.buffer) as any);
  const sheet = wb.getWorksheet('DQ_Daily_Executive_Report');

  assert.equal(String(sheet!.getCell('A12').value), 'Completeness');
  assert.equal(String(sheet!.getCell('G12').value), 'Passed Checks');
  assert.equal(String(sheet!.getCell('A21').value), 'Improved Tables');
  assert.notEqual(String(sheet!.getCell('C12').value), '');
  assert.notEqual(String(sheet!.getCell('J12').value), '');
  assert.notEqual(String(sheet!.getCell('D21').value), '');
});


