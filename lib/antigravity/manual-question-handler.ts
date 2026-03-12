/**
 * Antigravity Manual Question Handler
 * DEPRECATED: Replaced by Strict Gemini Execution Engine.
 * 
 * This file stays to satisfy existing imports until full migration.
 */

import { ParsedIntent, IntentType, MetricType, TimeWindow } from './intent-interpreter';
import { ChartType } from './chart-intent-mapper';
import { executeWithGemini } from './gemini-execution-engine';
import { introspectSchema } from './schema-reader';

export interface QuestionResponse {
  success: boolean;
  intent: ParsedIntent;
  sql: string;
  data: any[];
  chart: {
    type: ChartType;
    title: string;
    xAxis: string;
    yAxis: string;
    color: string;
  };
  explanation: string;
  error?: string;
  interpretation?: any; // New field for Executive Answer
}

/**
 * Handles a manual question - Redirects to Strict Gemini Engine
 */
export async function handleManualQuestion(
  intent: ParsedIntent,
  assetId: string
): Promise<QuestionResponse> {
  // Determine context from assetId
  const [db, schema, table] = assetId.split('.');

  // We need schema registry for strict mode. 
  // ideally this is passed in, but for this legacy adapter we fetch it.
  // This is inefficient but functional for the adapter.
  const registry = await introspectSchema(db, [schema]);

  const result = await executeWithGemini(
    intent.originalQuestion || "Analyze this asset", // We need the question! 
    assetId,
    registry
  );

  if (result.status !== 'SUCCESS') {
    return {
      success: false,
      intent,
      sql: result.sql || '',
      data: [],
      chart: { type: 'NONE', title: '', xAxis: '', yAxis: '', color: '#94a3b8' },
      explanation: result.error?.message || 'Execution failed',
      error: result.error?.message
    };
  }

  // Adapt to legacy response format
  return {
    success: true,
    intent,
    sql: result.sql || '',
    data: result.data || [],
    chart: {
      type: 'LINE', // Default, logic moved to UI/Engine
      title: 'Analysis Result',
      xAxis: result.plan?.group_by?.[0] || 'DATE',
      yAxis: result.plan?.metrics?.[0] || 'VALUE',
      color: '#6366f1'
    },
    explanation: result.interpretation?.whatHappened || 'Analysis complete',
    interpretation: result.interpretation
  };
}
