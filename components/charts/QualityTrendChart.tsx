'use client';

import { useMemo } from 'react';
import {
  AreaChart,
  Area,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
} from 'recharts';

export type QualityHistoryPoint = {
  scan_date: string;
  dq_score: number;
};

interface Props {
  data: QualityHistoryPoint[];
}

export function QualityTrendChart({ data }: Props) {
  // Data arrives ASC from backend (oldest → newest). No reversal needed.
  const chartData = useMemo(() => {
    if (!data || data.length === 0) return [];

    return data.map(day => {
      const [y, m, d] = day.scan_date.split('-');
      const dateObj = new Date(Number(y), Number(m) - 1, Number(d));
      const formattedDate = dateObj.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });

      return {
        formattedDate,
        dq_score: day.dq_score,
      };
    });
  }, [data]);

  if (chartData.length === 0) {
    return (
      <div className="flex flex-col items-center justify-center h-full text-slate-400 py-10">
        <svg className="w-10 h-10 mb-2 text-slate-300" fill="none" viewBox="0 0 24 24" stroke="currentColor">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z" />
        </svg>
        <p className="text-sm font-medium">No execution history found</p>
        <p className="text-xs mt-1">Run a scan to start tracking quality scores</p>
      </div>
    );
  }

  const CustomTooltip = ({ active, payload, label }: any) => {
    if (active && payload && payload.length) {
      const score = payload[0].value;
      return (
        <div className="bg-white/95 backdrop-blur-sm p-3 border border-slate-200 shadow-xl rounded-xl min-w-[140px]">
          <p className="font-semibold text-slate-800 text-[13px] mb-1.5 pb-1.5 border-b border-slate-100">{label}</p>
          <div className="flex justify-between items-center">
            <span className="text-xs text-slate-500 font-medium">DQ Score</span>
            <span className="text-sm font-bold text-indigo-600">{score.toFixed(2)}%</span>
          </div>
        </div>
      );
    }
    return null;
  };

  return (
    <div className="w-full h-[260px] pt-4 pr-2">
      <ResponsiveContainer width="100%" height="100%">
        <AreaChart
          data={chartData}
          margin={{ top: 10, right: 10, left: -20, bottom: 5 }}
        >
          <defs>
            <linearGradient id="dqScoreGradient" x1="0" y1="0" x2="0" y2="1">
              <stop offset="5%" stopColor="#4F46E5" stopOpacity={0.25} />
              <stop offset="95%" stopColor="#4F46E5" stopOpacity={0.02} />
            </linearGradient>
          </defs>
          <CartesianGrid strokeDasharray="4 4" vertical={false} stroke="#E2E8F0" />
          <XAxis
            dataKey="formattedDate"
            axisLine={false}
            tickLine={false}
            tick={{ fontSize: 11, fill: '#64748B', fontWeight: 500 }}
            dy={10}
          />
          <YAxis
            domain={[0, 100]}
            axisLine={false}
            tickLine={false}
            tick={{ fontSize: 11, fill: '#64748B', fontWeight: 500 }}
            tickFormatter={(val) => `${val}%`}
            dx={-5}
          />
          <Tooltip
            content={<CustomTooltip />}
            cursor={{ stroke: '#CBD5E1', strokeWidth: 1, strokeDasharray: '4 4' }}
            isAnimationActive={false}
          />
          <Area
            type="monotone"
            dataKey="dq_score"
            stroke="#4F46E5"
            strokeWidth={2.5}
            fill="url(#dqScoreGradient)"
            dot={{ r: 4, fill: '#ffffff', stroke: '#4F46E5', strokeWidth: 2 }}
            activeDot={{ r: 6, fill: '#4F46E5', stroke: '#ffffff', strokeWidth: 2.5 }}
            animationDuration={1200}
          />
        </AreaChart>
      </ResponsiveContainer>
    </div>
  );
}
