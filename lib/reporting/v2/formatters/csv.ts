import { ReportPayload } from '../types';
import { sanitizeText } from '../format-utils';

function escapeCsv(value: unknown): string {
  const text = sanitizeText(value);
  if (text.includes(',') || text.includes('"') || text.includes('\n') || text.includes('\r')) {
    return `"${text.replace(/"/g, '""')}"`;
  }
  return text;
}

function row(values: unknown[]): string {
  return values.map(escapeCsv).join(',');
}

export function renderReportCsv(payload: ReportPayload): string {
  const lines: string[] = [];

  lines.push('Report Header');
  lines.push(row(['Report Title', payload.header.reportTitle]));
  lines.push(row(['Execution Mode', payload.header.executionMode]));
  lines.push(row(['Run ID', payload.header.runReference]));
  lines.push(row(['Execution Date', payload.header.executionDate]));
  lines.push(row(['Generated Timestamp', payload.header.generatedTimestamp]));

  lines.push('');
  lines.push('Summary Metrics');
  lines.push(row(['Total Datasets', payload.summary.totalDatasets]));
  lines.push(row(['Total Checks', payload.summary.totalChecks]));
  lines.push(row(['Passed Checks', payload.summary.passedChecks]));
  lines.push(row(['Failed Checks', payload.summary.failedChecks]));
  lines.push(row(['Success Rate (%)', payload.summary.successRate.toFixed(2)]));

  if (payload.dailyInsights) {
    lines.push('');
    lines.push('Daily Summary Insights');
    lines.push(row(['Dataset Count', payload.dailyInsights.datasetCount]));
    lines.push(row(['DQ Score (%)', payload.dailyInsights.dqScore.toFixed(2)]));
    lines.push(row(['Completeness (%)', payload.dailyInsights.completenessScore.toFixed(2)]));
    lines.push(row(['Uniqueness (%)', payload.dailyInsights.uniquenessScore.toFixed(2)]));
    lines.push(row(['Validity (%)', payload.dailyInsights.validityScore.toFixed(2)]));
    lines.push(row(['Consistency (%)', payload.dailyInsights.consistencyScore.toFixed(2)]));
    lines.push(row(['Freshness (%)', payload.dailyInsights.freshnessScore.toFixed(2)]));
    lines.push(row(['Volume (%)', payload.dailyInsights.volumeScore.toFixed(2)]));
    lines.push(row(['Trust Level', payload.dailyInsights.trustLevel]));
    lines.push(row(['Quality Grade', payload.dailyInsights.qualityGrade]));
    lines.push(row(['SLA Met', payload.dailyInsights.isSlaMet ? 'Yes' : 'No']));
    lines.push(row(['Total Records', payload.dailyInsights.totalRecords]));
    lines.push(row(['Failed Records', payload.dailyInsights.failedRecordsCount]));
    lines.push(row(['Failure Rate (%)', payload.dailyInsights.failureRate.toFixed(2)]));
    lines.push(row(['Previous Day Score (%)', payload.dailyInsights.prevDayScore.toFixed(2)]));
    lines.push(row(['Score Delta (%)', payload.dailyInsights.scoreDelta.toFixed(2)]));
    lines.push(row(['Score Trend', payload.dailyInsights.scoreTrend]));
  }

  lines.push('');
  lines.push('Dataset Breakdown Table');
  lines.push(
    row([
      'Database',
      'Schema',
      'Table',
      'Total Checks',
      'Passed Checks',
      'Failed Checks',
      'Success Rate (%)',
      'Last Check Timestamp',
    ])
  );

  for (const dataset of payload.datasets) {
    lines.push(
      row([
        dataset.databaseName,
        dataset.schemaName,
        dataset.tableName,
        dataset.totalChecks,
        dataset.passedChecks,
        dataset.failedChecks,
        dataset.successRate.toFixed(2),
        dataset.lastCheckTimestamp,
      ])
    );
  }

  if (payload.failures && payload.failures.length > 0) {
    lines.push('');
    lines.push('Failed Checks Detailed Table');
    lines.push(
      row([
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
        'Pass Rate (%)',
        'Threshold (%)',
        'Failure Reason',
        'Check Timestamp',
      ])
    );

    for (const failure of payload.failures) {
      lines.push(
        row([
          failure.runId,
          failure.databaseName,
          failure.schemaName,
          failure.tableName,
          failure.columnName,
          failure.ruleName,
          failure.ruleType,
          failure.checkStatus,
          failure.invalidRecords,
          failure.totalRecords,
          failure.passRate.toFixed(2),
          failure.threshold.toFixed(2),
          failure.failureReason,
          failure.checkTimestamp,
        ])
      );
    }
  }

  return `${lines.join('\n')}\n`;
}
