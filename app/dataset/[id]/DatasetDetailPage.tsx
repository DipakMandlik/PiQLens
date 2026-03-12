'use client';

import { useState } from 'react';
import { Calendar, Play, BarChart3, Eye, AlertTriangle, FileText } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { Tabs, TabsList, TabsTrigger, TabsContent } from '@/components/ui/tabs';
import { QualityHealthCard } from '@/components/dataset/QualityHealthCard';
import { CoverageCard } from '@/components/dataset/CoverageCard';
import { RiskCard } from '@/components/dataset/RiskCard';

export default function DatasetDetailPage() {
    const [activeTab, setActiveTab] = useState('overview');

    return (
        <div className="space-y-6">
            {/* Header */}
            <div className="flex items-center justify-between">
                <div className="flex items-center gap-4">
                    <div className="flex items-center gap-3">
                        <div className="p-2 bg-blue-50 rounded-lg">
                            <BarChart3 className="h-6 w-6 text-blue-600" />
                        </div>
                        <div>
                            <div className="flex items-center gap-2">
                                <h1 className="text-2xl font-bold text-gray-900">STG_ACCOUNT</h1>
                                <span className="px-2 py-0.5 bg-gray-100 text-gray-700 text-xs font-medium rounded">
                                    STG
                                </span>
                                <span className="px-2 py-0.5 bg-green-100 text-green-700 text-xs font-medium rounded">
                                    Active
                                </span>
                            </div>
                        </div>
                    </div>
                </div>
                <div className="flex items-center gap-3">
                    <Button variant="outline" className="flex items-center gap-2">
                        <Calendar className="h-4 w-4" />
                        Schedule
                    </Button>
                    <Button className="flex items-center gap-2">
                        <Play className="h-4 w-4" />
                        Run Scan
                    </Button>
                </div>
            </div>

            {/* Tabs */}
            <Tabs value={activeTab} onValueChange={setActiveTab}>
                <TabsList className="border-b border-gray-200 bg-transparent p-0 h-auto">
                    <TabsTrigger value="overview" className="flex items-center gap-2 data-[state=active]:border-b-2 data-[state=active]:border-blue-600 rounded-none">
                        <BarChart3 className="h-4 w-4" />
                        Overview
                    </TabsTrigger>
                    <TabsTrigger value="quality" className="flex items-center gap-2 data-[state=active]:border-b-2 data-[state=active]:border-blue-600 rounded-none">
                        <Eye className="h-4 w-4" />
                        Quality
                    </TabsTrigger>
                    <TabsTrigger value="observability" className="flex items-center gap-2 data-[state=active]:border-b-2 data-[state=active]:border-blue-600 rounded-none">
                        <BarChart3 className="h-4 w-4" />
                        Observability
                    </TabsTrigger>
                    <TabsTrigger value="anomalies" className="flex items-center gap-2 data-[state=active]:border-b-2 data-[state=active]:border-blue-600 rounded-none">
                        <AlertTriangle className="h-4 w-4" />
                        Anomalies
                    </TabsTrigger>
                    <TabsTrigger value="fields" className="flex items-center gap-2 data-[state=active]:border-b-2 data-[state=active]:border-blue-600 rounded-none">
                        <FileText className="h-4 w-4" />
                        Fields
                    </TabsTrigger>
                </TabsList>

                <TabsContent value="overview" className="mt-6">
                    <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
                        <QualityHealthCard score={82} status="Good" trend={2} />
                        <CoverageCard totalChecks={88} passed={72} failed={16} />
                        <RiskCard openAnomalies={5} slaBreaches={1} riskLevel="High" />
                    </div>
                </TabsContent>

                <TabsContent value="quality" className="mt-6">
                    <div className="text-center py-12 text-gray-500">
                        Quality metrics coming soon...
                    </div>
                </TabsContent>

                <TabsContent value="observability" className="mt-6">
                    <div className="text-center py-12 text-gray-500">
                        Observability metrics coming soon...
                    </div>
                </TabsContent>

                <TabsContent value="anomalies" className="mt-6">
                    <div className="text-center py-12 text-gray-500">
                        Anomalies list coming soon...
                    </div>
                </TabsContent>

                <TabsContent value="fields" className="mt-6">
                    <div className="text-center py-12 text-gray-500">
                        Fields list coming soon...
                    </div>
                </TabsContent>
            </Tabs>
        </div>
    );
}
