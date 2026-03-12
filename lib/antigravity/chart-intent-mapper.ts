/**
 * Antigravity Chart Intent Mapper
 * Maps metric semantics to chart types deterministically.
 * 
 * Rule: No AI guessing visuals — rules first.
 */

export type ChartType = 'LINE' | 'BAR' | 'AREA' | 'DAG' | 'PIE' | 'NONE';

export interface ChartConfig {
    type: ChartType;
    xAxis: string;
    yAxis: string;
    title: string;
    color: string;
    threshold?: number;
}

// Deterministic mapping: Metric/Question → Chart Type
const CHART_MAPPINGS: Record<string, ChartConfig> = {
    // Freshness metrics
    freshness_trend: {
        type: 'LINE',
        xAxis: 'time_bucket',
        yAxis: 'freshness_hours',
        title: 'Freshness Over Time',
        color: '#10b981',
        threshold: 24,
    },
    freshness_anomaly_start: {
        type: 'LINE',
        xAxis: 'METRIC_TIME',
        yAxis: 'freshness_hours',
        title: 'Freshness Breach Detection',
        color: '#ef4444',
        threshold: 24,
    },

    // Quality metrics
    failed_checks_breakdown: {
        type: 'BAR',
        xAxis: 'RULE_NAME',
        yAxis: 'failure_count',
        title: 'Failed Checks by Rule',
        color: '#f59e0b',
    },
    null_rate_by_column: {
        type: 'BAR',
        xAxis: 'COLUMN_NAME',
        yAxis: 'NULL_PERCENTAGE',
        title: 'Null Rate by Column',
        color: '#6366f1',
    },
    quality_score_trend: {
        type: 'LINE',
        xAxis: 'SUMMARY_DATE',
        yAxis: 'DQ_SCORE',
        title: 'Quality Score Trend',
        color: '#22c55e',
        threshold: 80,
    },

    // Anomaly metrics
    metric_deviation: {
        type: 'LINE',
        xAxis: 'METRIC_TIME',
        yAxis: 'METRIC_VALUE',
        title: 'Metric Deviation',
        color: '#8b5cf6',
    },
    row_count_trend: {
        type: 'LINE',
        xAxis: 'time_point',
        yAxis: 'row_count',
        title: 'Row Count Over Time',
        color: '#06b6d4',
    },

    // Impact visualization
    downstream_impact: {
        type: 'DAG',
        xAxis: 'DOWNSTREAM_ASSET_ID',
        yAxis: 'RELATIONSHIP_TYPE',
        title: 'Downstream Impact',
        color: '#ec4899',
    },
};

/**
 * Gets chart configuration for a question key.
 */
export function getChartConfig(questionKey: string): ChartConfig {
    return CHART_MAPPINGS[questionKey] || {
        type: 'BAR',
        xAxis: 'x',
        yAxis: 'y',
        title: 'Data Visualization',
        color: '#64748b',
    };
}

/**
 * Transforms raw query data into chart-ready format.
 */
export function transformDataForChart(
    data: any[],
    config: ChartConfig
): { chartData: any[]; xKey: string; yKey: string } {
    // Normalize column names (Snowflake returns uppercase)
    const xKey = config.xAxis.toUpperCase();
    const yKey = config.yAxis.toUpperCase();

    const chartData = data.map(row => {
        const normalizedRow: Record<string, any> = {};
        Object.keys(row).forEach(key => {
            normalizedRow[key.toUpperCase()] = row[key];
        });

        // Format dates for display
        if (normalizedRow[xKey] && typeof normalizedRow[xKey] === 'string' && normalizedRow[xKey].includes('T')) {
            const date = new Date(normalizedRow[xKey]);
            normalizedRow[xKey + '_FORMATTED'] = date.toLocaleDateString() + ' ' + date.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
        }

        return normalizedRow;
    });

    return { chartData, xKey, yKey };
}

/**
 * Gets appropriate color palette based on chart type.
 */
export function getChartColors(chartType: ChartType): string[] {
    switch (chartType) {
        case 'BAR':
            return ['#6366f1', '#8b5cf6', '#a855f7', '#c084fc', '#d8b4fe'];
        case 'LINE':
            return ['#10b981', '#22c55e', '#34d399', '#4ade80'];
        case 'AREA':
            return ['#0891b2', '#06b6d4', '#22d3ee', '#67e8f9'];
        case 'DAG':
            return ['#ec4899', '#f472b6', '#f9a8d4'];
        default:
            return ['#64748b', '#94a3b8', '#cbd5e1'];
    }
}
