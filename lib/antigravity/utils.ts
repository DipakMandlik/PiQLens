import { ExecutiveAnswer } from '@/components/ExecutiveAnswerBlock';

/**
 * Creates an ExecutiveAnswer from legacy insight format
 * Moved here to support both server and client usage
 */
export function createExecutiveAnswerFromLegacy(
    data: any[],
    intent: { intentType: string; metrics: string[] },
    sql: string,
    chartConfig?: any
): ExecutiveAnswer {
    // Generate business-friendly explanations based on data
    const rowCount = data.length;
    const metric = intent.metrics[0] || 'quality';

    let whatHappened = 'Analysis complete.';
    let whyItHappened = 'See evidence below for details.';
    let whatIsImpacted = 'Review the affected metrics to understand scope.';
    let whatShouldBeDone = 'Take appropriate action based on findings.';
    let severity: 'INFO' | 'WARNING' | 'CRITICAL' = 'INFO';

    // Generate context-aware explanations
    if (rowCount === 0) {
        whatHappened = `No ${metric} issues found in the selected time period.`;
        whyItHappened = 'The data appears to be operating within expected parameters.';
        whatIsImpacted = 'No immediate business impact identified.';
        whatShouldBeDone = 'Continue monitoring. No action required.';
        severity = 'INFO';
    } else if (intent.intentType === 'ROOT_CAUSE') {
        whatHappened = `Identified ${rowCount} potential root cause indicators for the ${metric} issue.`;
        whyItHappened = 'The underlying factors contributing to this issue are detailed in the evidence.';
        whatIsImpacted = 'Dependent processes and downstream consumers may be affected.';
        whatShouldBeDone = 'Investigate the identified factors and implement corrective measures.';
        severity = 'WARNING';
    } else if (intent.intentType === 'TREND') {
        whatHappened = `Analyzed ${metric} trend with ${rowCount} data points.`;
        whyItHappened = 'The trend reflects changes in data quality over the selected time window.';
        whatIsImpacted = 'Trend direction may affect planning and operational decisions.';
        whatShouldBeDone = 'Monitor if trend is declining. Address underlying causes if negative.';
        severity = 'INFO';
    } else if (intent.intentType === 'DISTRIBUTION') {
        whatHappened = `Found ${rowCount} items in the ${metric} distribution.`;
        whyItHappened = 'Distribution shows how issues are spread across different dimensions.';
        whatIsImpacted = 'Areas with higher concentration may require priority attention.';
        whatShouldBeDone = 'Focus remediation efforts on highest-impact areas first.';
        severity = 'INFO';
    }

    // Build metrics from data
    const metrics: { label: string; value: string }[] = [];
    if (data.length > 0) {
        const firstRow = data[0];
        Object.entries(firstRow).slice(0, 5).forEach(([key, value]) => {
            metrics.push({
                label: key.replace(/_/g, ' '),
                value: String(value)
            });
        });
    }

    return {
        whatHappened,
        whyItHappened,
        whatIsImpacted,
        whatShouldBeDone,
        evidence: {
            metrics,
            sql,
            chartData: chartConfig ? data : undefined,
            chartConfig
        },
        severity
    };
}
