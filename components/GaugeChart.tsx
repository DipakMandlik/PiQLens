'use client';

import React from 'react';

interface GaugeChartProps {
  value: number;
  min?: number;
  max?: number;
  label: string;
  change?: number;
}

export const GaugeChart: React.FC<GaugeChartProps> = ({ 
  value, 
  min = 0, 
  max = 100, 
  label,
  change 
}) => {
  const radius = 80; // Radius of the arc
  const stroke = 20; // Thickness of the arc
  const cx = 100;
  const cy = 95; // Center Y positioned near the bottom of the viewBox

  // Helper to calculate coordinates on the circle
  const getCoords = (percent: number) => {
    // 0% corresponds to 180 degrees (Left)
    // 100% corresponds to 0 degrees (Right)
    const angle = 180 - (percent * 180);
    const rad = (angle * Math.PI) / 180;
    
    // SVG coordinate system: Y increases downwards
    return {
      x: cx + (radius * Math.cos(rad)),
      y: cy - (radius * Math.sin(rad))
    };
  };

  // Draw segment
  const drawArc = (startP: number, endP: number, color: string) => {
    if (startP >= endP) return null;
    const start = getCoords(startP);
    const end = getCoords(endP);
    
    // Large arc flag is always 0 for segments < 180 degrees
    // Sweep flag is 1 for clockwise direction
    const d = [
      `M ${start.x} ${start.y}`,
      `A ${radius} ${radius} 0 0 1 ${end.x} ${end.y}`
    ].join(' ');

    return (
      <path 
        key={`arc-${startP}-${endP}`}
        d={d} 
        fill="none" 
        stroke={color} 
        strokeWidth={stroke}
        strokeLinecap="butt"
      />
    );
  };

  // Normalized value for needle position (0 to 1)
  const norm = Math.min(Math.max((value - min) / (max - min), 0), 1);
  
  return (
    <div className="flex flex-col items-center justify-center w-full h-full p-2">
      <h3 className="text-sm font-medium text-gray-500 uppercase tracking-wide mb-2">{label}</h3>
      
      {/* Chart Graphic */}
      <div className="relative w-full aspect-[2/1] max-w-[220px]">
        <svg viewBox="0 0 200 110" className="w-full h-full overflow-visible">
          {/* Segments */}
          {/* Red Zone: 0 to 0.9 */}
          {drawArc(0, 0.5, '#ef4444')}
          {/* Yellow Zone: 0.9 to 0.95 */}
          {drawArc(0.5, 0.9, '#eab308')}
          {/* Green Zone: 0.95 to 1.0 */}
          {drawArc(0.9, 1, '#22c55e')}
          
          {/* Needle: Line from center pointing out */}
          {/* Starts at Center (cx, cy) */}
          {/* Ends at tip (radius + small overlap) */}
          <line 
            x1={cx} y1={cy} 
            x2={cx - radius - 2} y2={cy} // Initial position pointing Left (0%)
            stroke="#1e293b" 
            strokeWidth="4"
            strokeLinecap="round"
            transform={`rotate(${norm * 180} ${cx} ${cy})`}
            className="transition-transform duration-1000 ease-out"
          />
          
          {/* Center Pivot Circle */}
          <circle cx={cx} cy={cy} r="6" fill="#1e293b" />
        </svg>
      </div>

      {/* Value Text - Placed OUTSIDE the SVG area below the gauge */}
      <div className="flex flex-col items-center mt-1">
         <span className="text-3xl font-bold text-gray-900 leading-none tracking-tight">{value}%</span>
         {change !== undefined && (
           <div className="flex items-center gap-1 text-xs font-medium mt-1">
              {change > 0 ? (
                  <span className="text-green-600">▲ +{change.toFixed(1)}%</span>
              ) : change < 0 ? (
                  <span className="text-red-600">▼ {change.toFixed(1)}%</span>
              ) : (
                  <span className="text-gray-400">No Change</span>
              )}
              <span className="text-gray-400 text-[10px] ml-0.5">vs Yesterday</span>
           </div>
         )}
      </div>
    </div>
  );
};

