import test from 'node:test';
import assert from 'node:assert/strict';
import { renderReportCsv } from '../formatters/csv';
import { ReportPayload } from '../types';

const payload: ReportPayload = {
  reportId: 'r1',
  header: {
    reportTitle: 'Data Quality Execution Report',
    executionMode: 'RUN',
    runReference: 'RUN_123',
    executionDate: '2026-02-20',
    generatedTimestamp: '2026-02-20 10:10:10 IST',
  },
  summary: {
    totalDatasets: 1,
    totalChecks: 10,
    passedChecks: 9,
    failedChecks: 1,
    successRate: 90,
  },
  datasets: [
    {
      databaseName: 'DB',
      schemaName: 'SC',
      tableName: 'TB',
      totalChecks: 10,
      passedChecks: 9,
      failedChecks: 1,
      successRate: 90,
      lastCheckTimestamp: '2026-02-20 10:00:00 IST',
    },
  ],
  metadata: {
    format: 'csv',
    variant: 'summary',
    mode: 'run',
    scope: 'platform',
    runIds: ['RUN_123'],
    runCount: 1,
    failureRowsReturned: 0,
    failureRowsTotal: 0,
    failureRowsTruncated: false,
    generatedBy: 'test',
  },
};

test('renderReportCsv emits required section order', () => {
  const csv = renderReportCsv(payload);
  const idxHeader = csv.indexOf('Report Header');
  const idxSummary = csv.indexOf('Summary Metrics');
  const idxDataset = csv.indexOf('Dataset Breakdown Table');

  assert.ok(idxHeader >= 0);
  assert.ok(idxSummary > idxHeader);
  assert.ok(idxDataset > idxSummary);
});
