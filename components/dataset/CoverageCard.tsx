'use client';

import { Card, CardContent } from '@/components/ui/card';
import { PieChart, Pie, Cell, ResponsiveContainer } from 'recharts';

interface CoverageCardProps {
    totalChecks: number;
    passed: number;
    failed: number;
}

export function CoverageCard({ totalChecks, passed, failed }: CoverageCardProps) {
    const passedPercentage = ((passed / totalChecks) * 100).toFixed(1);

    const data = [
        { name: 'Passed', value: passed },
        { name: 'Failed', value: failed },
    ];

    const COLORS = ['#10b981', '#ef4444']; // green, red

    return (
        <Card>
            <CardContent className="p-6">
                <h3 className="text-sm font-medium text-gray-500 mb-4">Coverage</h3>
                <div className="flex items-center justify-between">
                    <div className="flex-1">
                        <div className="mb-3">
                            <div className="text-sm text-gray-600 mb-1">Total Checks</div>
                            <div className="text-2xl font-bold text-gray-900">{totalChecks}</div>
                        </div>
                        <div className="space-y-2">
                            <div className="flex items-center justify-between text-sm">
                                <div className="flex items-center gap-2">
                                    <div className="h-3 w-3 rounded-full bg-green-500"></div>
                                    <span className="text-gray-600">{passed} Passed</span>
                                </div>
                            </div>
                            <div className="flex items-center justify-between text-sm">
                                <div className="flex items-center gap-2">
                                    <div className="h-3 w-3 rounded-full bg-red-500"></div>
                                    <span className="text-gray-600">{failed} Failed</span>
                                </div>
                            </div>
                            <div className="text-lg font-bold text-gray-900 mt-2">
                                {passedPercentage}%
                            </div>
                        </div>
                    </div>
                    <div className="w-32 h-32">
                        <ResponsiveContainer width="100%" height="100%">
                            <PieChart>
                                <Pie
                                    data={data}
                                    cx="50%"
                                    cy="50%"
                                    innerRadius={30}
                                    outerRadius={50}
                                    dataKey="value"
                                    startAngle={90}
                                    endAngle={-270}
                                >
                                    {data.map((entry, index) => (
                                        <Cell key={`cell-${index}`} fill={COLORS[index]} />
                                    ))}
                                </Pie>
                            </PieChart>
                        </ResponsiveContainer>
                    </div>
                </div>
            </CardContent>
        </Card>
    );
}
