'use client';

import { TopNav } from '@/components/layout/TopNav';
import { BackButton } from '@/components/back-button';
import { Brain, Sparkles, Zap, TrendingUp, Target } from 'lucide-react';

export default function IntelligencePage() {
    return (
        <div className="flex flex-col h-screen overflow-hidden bg-gradient-to-br from-purple-50 via-white to-blue-50">
            {/* Top Navigation */}
            <TopNav />

            {/* Main Content - Full Width */}
            <main className="flex-1 overflow-y-auto">
                <div className="max-w-6xl mx-auto px-6 py-10">
                    <div className="mb-6">
                        <BackButton href="/" label="Back to Home" />
                    </div>

                    {/* Header */}
                    <div className="text-center mb-12">
                        <div className="inline-flex items-center justify-center p-4 bg-gradient-to-br from-purple-100 to-blue-100 rounded-2xl mb-4">
                            <Brain className="h-12 w-12 text-purple-600" />
                        </div>
                        <h1 className="text-4xl font-bold text-gray-900 mb-3">Intelligence & Insights</h1>
                        <p className="text-lg text-gray-600 max-w-2xl mx-auto">
                            AI-driven data quality recommendations and anomaly detection.
                        </p>
                    </div>

                    {/* Feature Cards */}
                    <div className="grid grid-cols-1 md:grid-cols-2 gap-6 mb-8">
                        {/* AI Copilot */}
                        <div className="bg-white p-8 rounded-2xl border-2 border-purple-100 shadow-sm hover:shadow-md transition-all hover:border-purple-200">
                            <div className="flex flex-col items-center text-center">
                                <div className="p-4 bg-gradient-to-br from-purple-100 to-purple-50 rounded-xl mb-4">
                                    <Brain className="h-10 w-10 text-purple-600" />
                                </div>
                                <h3 className="text-xl font-bold text-gray-900 mb-2">AI Copilot</h3>
                                <p className="text-gray-600 mb-4 leading-relaxed">
                                    Natural language interface to query data quality metrics, get root cause analysis, and receive intelligent recommendations.
                                </p>
                                <div className="flex flex-wrap gap-2 justify-center mb-4">
                                    <span className="px-3 py-1 bg-purple-50 text-purple-700 rounded-full text-xs font-medium">
                                        Natural Language
                                    </span>
                                    <span className="px-3 py-1 bg-purple-50 text-purple-700 rounded-full text-xs font-medium">
                                        Root Cause Analysis
                                    </span>
                                    <span className="px-3 py-1 bg-purple-50 text-purple-700 rounded-full text-xs font-medium">
                                        Auto-Fix Suggestions
                                    </span>
                                </div>
                                <span className="px-4 py-2 bg-purple-100 text-purple-700 rounded-full text-sm font-semibold">
                                    Coming Soon
                                </span>
                            </div>
                        </div>

                        {/* Predictive Quality */}
                        <div className="bg-white p-8 rounded-2xl border-2 border-blue-100 shadow-sm hover:shadow-md transition-all hover:border-blue-200">
                            <div className="flex flex-col items-center text-center">
                                <div className="p-4 bg-gradient-to-br from-blue-100 to-blue-50 rounded-xl mb-4">
                                    <Sparkles className="h-10 w-10 text-blue-600" />
                                </div>
                                <h3 className="text-xl font-bold text-gray-900 mb-2">Predictive Quality</h3>
                                <p className="text-gray-600 mb-4 leading-relaxed">
                                    Forecast potential quality incidents before they impact downstream consumers using ML-powered trend analysis.
                                </p>
                                <div className="flex flex-wrap gap-2 justify-center mb-4">
                                    <span className="px-3 py-1 bg-blue-50 text-blue-700 rounded-full text-xs font-medium">
                                        Anomaly Detection
                                    </span>
                                    <span className="px-3 py-1 bg-blue-50 text-blue-700 rounded-full text-xs font-medium">
                                        Trend Forecasting
                                    </span>
                                    <span className="px-3 py-1 bg-blue-50 text-blue-700 rounded-full text-xs font-medium">
                                        Early Warnings
                                    </span>
                                </div>
                                <span className="px-4 py-2 bg-blue-100 text-blue-700 rounded-full text-sm font-semibold">
                                    Coming Soon
                                </span>
                            </div>
                        </div>

                        {/* Smart Alerts */}
                        <div className="bg-white p-8 rounded-2xl border-2 border-amber-100 shadow-sm hover:shadow-md transition-all hover:border-amber-200">
                            <div className="flex flex-col items-center text-center">
                                <div className="p-4 bg-gradient-to-br from-amber-100 to-amber-50 rounded-xl mb-4">
                                    <Zap className="h-10 w-10 text-amber-600" />
                                </div>
                                <h3 className="text-xl font-bold text-gray-900 mb-2">Smart Alerts</h3>
                                <p className="text-gray-600 mb-4 leading-relaxed">
                                    Context-aware alerting that learns from your response patterns to reduce noise and highlight critical issues.
                                </p>
                                <div className="flex flex-wrap gap-2 justify-center mb-4">
                                    <span className="px-3 py-1 bg-amber-50 text-amber-700 rounded-full text-xs font-medium">
                                        Noise Reduction
                                    </span>
                                    <span className="px-3 py-1 bg-amber-50 text-amber-700 rounded-full text-xs font-medium">
                                        Priority Scoring
                                    </span>
                                    <span className="px-3 py-1 bg-amber-50 text-amber-700 rounded-full text-xs font-medium">
                                        Auto-Routing
                                    </span>
                                </div>
                                <span className="px-4 py-2 bg-amber-100 text-amber-700 rounded-full text-sm font-semibold">
                                    Coming Soon
                                </span>
                            </div>
                        </div>

                        {/* Pattern Recognition */}
                        <div className="bg-white p-8 rounded-2xl border-2 border-green-100 shadow-sm hover:shadow-md transition-all hover:border-green-200">
                            <div className="flex flex-col items-center text-center">
                                <div className="p-4 bg-gradient-to-br from-green-100 to-green-50 rounded-xl mb-4">
                                    <TrendingUp className="h-10 w-10 text-green-600" />
                                </div>
                                <h3 className="text-xl font-bold text-gray-900 mb-2">Pattern Recognition</h3>
                                <p className="text-gray-600 mb-4 leading-relaxed">
                                    Automatically discover data quality patterns, correlations, and recurring issues across your data ecosystem.
                                </p>
                                <div className="flex flex-wrap gap-2 justify-center mb-4">
                                    <span className="px-3 py-1 bg-green-50 text-green-700 rounded-full text-xs font-medium">
                                        Auto-Discovery
                                    </span>
                                    <span className="px-3 py-1 bg-green-50 text-green-700 rounded-full text-xs font-medium">
                                        Correlation Analysis
                                    </span>
                                    <span className="px-3 py-1 bg-green-50 text-green-700 rounded-full text-xs font-medium">
                                        Impact Mapping
                                    </span>
                                </div>
                                <span className="px-4 py-2 bg-green-100 text-green-700 rounded-full text-sm font-semibold">
                                    Coming Soon
                                </span>
                            </div>
                        </div>
                    </div>

                    {/* Bottom Info Card */}
                    <div className="bg-white rounded-2xl border border-gray-200 shadow-sm p-8">
                        <div className="flex items-start gap-4">
                            <div className="p-3 bg-gradient-to-br from-purple-100 to-blue-100 rounded-xl">
                                <Target className="h-6 w-6 text-purple-600" />
                            </div>
                            <div className="flex-1">
                                <h3 className="text-lg font-bold text-gray-900 mb-2">AI-Native Data Quality Platform</h3>
                                <p className="text-gray-600 leading-relaxed mb-4">
                                    We're building advanced AI capabilities to transform how you monitor, understand, and improve data quality.
                                    These features will leverage machine learning to provide proactive insights, automated remediation, and
                                    intelligent decision support.
                                </p>
                                <div className="flex flex-wrap gap-3">
                                    <div className="flex items-center gap-2 text-sm text-gray-600">
                                        <div className="w-2 h-2 bg-purple-500 rounded-full"></div>
                                        <span>ML-Powered Insights</span>
                                    </div>
                                    <div className="flex items-center gap-2 text-sm text-gray-600">
                                        <div className="w-2 h-2 bg-blue-500 rounded-full"></div>
                                        <span>Automated Root Cause Analysis</span>
                                    </div>
                                    <div className="flex items-center gap-2 text-sm text-gray-600">
                                        <div className="w-2 h-2 bg-green-500 rounded-full"></div>
                                        <span>Intelligent Recommendations</span>
                                    </div>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
            </main>
        </div>
    );
}
