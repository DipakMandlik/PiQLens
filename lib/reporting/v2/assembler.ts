import { v4 as uuidv4 } from 'uuid';
import { formatTimestampIST } from './format-utils';
import { AggregatedReportData, GenerateReportV2Request, ReportPayload, ReportScope } from './types';

function resolveScope(request: GenerateReportV2Request): ReportScope {
  if (request.scope === 'dataset') return 'dataset';
  if (request.dataset && request.dataset.trim() && request.dataset.trim().toUpperCase() !== 'PLATFORM') {
    return 'dataset';
  }
  return 'platform';
}

function buildRunReference(request: GenerateReportV2Request, data: AggregatedReportData): string {
  if (request.mode === 'run') {
    return data.runIds[0] || request.runId || '';
  }

  return `AGGREGATE_${data.executionDate} (${data.runIds.length} runs)`;
}

function formatDisplayDate(dateValue: string): string {
  const match = dateValue.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (!match) return dateValue;
  return `${match[3]}-${match[2]}-${match[1]}`;
}

export function assembleReportPayload(
  request: GenerateReportV2Request,
  data: AggregatedReportData,
  reportId?: string
): ReportPayload {
  const generatedBy = request.generatedBy || 'system';
  const format = request.format;
  const variant = request.variant;
  const reportDateDisplay = formatDisplayDate(data.executionDate);

  const payload: ReportPayload = {
    reportId: reportId || uuidv4(),
    header: {
      reportTitle: request.scope === 'dataset' || request.dataset
        ? `Pi_Qlens Dataset Report for ${reportDateDisplay}`
        : `Pi_Qlens Report for ${reportDateDisplay}`,
      executionMode: request.mode === 'run' ? 'RUN' : 'DATE_AGGREGATE',
      runReference: buildRunReference(request, data),
      executionDate: data.executionDate,
      generatedTimestamp: formatTimestampIST(new Date()),
    },
    summary: data.summary,
    dailyInsights: data.dailyInsights,
    datasets: data.datasets,
    metadata: {
      format,
      variant,
      mode: request.mode,
      scope: resolveScope(request),
      dataset: request.dataset,
      runIds: data.runIds,
      runCount: data.runIds.length,
      failureRowsReturned: variant === 'detailed' ? data.failures.length : 0,
      failureRowsTotal: variant === 'detailed' ? data.failureRowsTotal : 0,
      failureRowsTruncated: variant === 'detailed' ? data.failureRowsTotal > data.failures.length : false,
      generatedBy,
    },
  };

  if (variant === 'detailed') {
    payload.failures = data.failures;
  }

  return payload;
}

