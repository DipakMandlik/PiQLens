'use client';

import React, { useState } from 'react';
import { TopNav } from '@/components/layout/TopNav';
import {
    ChevronDown,
    Info,
    CheckCircle,
    Shield,
    Activity,
    AlertTriangle,
    Database,
    FileText,
    HelpCircle,
    Clock
} from 'lucide-react';
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card';
import { BackButton } from '@/components/back-button';

// --- Local Components for Evaluation Page ---

const SectionHeader = ({ title, icon: Icon }: { title: string; icon: any }) => (
    <div className="flex items-center gap-2 mb-4 mt-8 border-b pb-2">
        <div className="p-2 bg-blue-50 rounded-lg text-blue-600">
            <Icon className="h-5 w-5" />
        </div>
        <h2 className="text-2xl font-bold text-gray-900">{title}</h2>
    </div>
);

const AccordionItem = ({ title, children, isOpen, onClick }: { title: string, children: React.ReactNode, isOpen: boolean, onClick: () => void }) => (
    <div className="border border-gray-200 rounded-lg mb-2 overflow-hidden">
        <button
            onClick={onClick}
            className={`w-full flex items-center justify-between p-4 text-left font-medium transition-colors ${isOpen ? 'bg-blue-50 text-blue-700' : 'bg-white text-gray-700 hover:bg-gray-50'}`}
        >
            <span>{title}</span>
            <ChevronDown className={`h-5 w-5 transition-transform duration-200 ${isOpen ? 'rotate-180 text-blue-600' : 'text-gray-400'}`} />
        </button>
        <div className={`transition-all duration-200 ease-in-out ${isOpen ? 'max-h-[1000px] opacity-100' : 'max-h-0 opacity-0 overflow-hidden'}`}>
            <div className="p-4 bg-white border-t border-gray-100 text-gray-600 leading-relaxed">
                {children}
            </div>
        </div>
    </div>
);

const KPICard = ({
    name,
    description,
    whyItMatters,
    calculation,
    dataSource,
    interpretation,
    icon: Icon
}: {
    name: string;
    description: string;
    whyItMatters: string;
    calculation: React.ReactNode;
    dataSource: string;
    interpretation: React.ReactNode;
    icon: any;
}) => (
    <Card className="mb-6 border-l-4 border-l-blue-500 shadow-sm hover:shadow-md transition-shadow">
        <CardHeader className="pb-2">
            <div className="flex items-center gap-3">
                <div className="p-2 bg-gray-100 rounded-md">
                    <Icon className="h-6 w-6 text-gray-700" />
                </div>
                <CardTitle className="text-xl text-gray-900">{name}</CardTitle>
            </div>
        </CardHeader>
        <CardContent className="grid grid-cols-1 md:grid-cols-2 gap-6 pt-4">
            <div className="space-y-4">
                <div>
                    <h4 className="text-sm font-semibold text-gray-900 uppercase tracking-wide mb-1">What It Represents</h4>
                    <p className="text-gray-600 text-sm leading-relaxed">{description}</p>
                </div>
                <div>
                    <h4 className="text-sm font-semibold text-gray-900 uppercase tracking-wide mb-1">Why It Matters</h4>
                    <p className="text-gray-600 text-sm leading-relaxed">{whyItMatters}</p>
                </div>
                <div>
                    <h4 className="text-sm font-semibold text-gray-900 uppercase tracking-wide mb-1">Data Source</h4>
                    <div className="flex items-center gap-2 text-sm text-gray-600 bg-gray-50 p-2 rounded border border-gray-100">
                        <Database className="h-3 w-3" />
                        {dataSource}
                    </div>
                </div>
            </div>
            <div className="space-y-4">
                <div>
                    <h4 className="text-sm font-semibold text-gray-900 uppercase tracking-wide mb-1">How It Is Calculated</h4>
                    <div className="bg-gray-50 p-3 rounded-md border border-gray-100 text-sm font-mono text-gray-700">
                        {calculation}
                    </div>
                </div>
                <div>
                    <h4 className="text-sm font-semibold text-gray-900 uppercase tracking-wide mb-1">Interpretation Guide</h4>
                    <div className="text-sm text-gray-600 leading-relaxed">
                        {interpretation}
                    </div>
                </div>
            </div>
        </CardContent>
    </Card>
);

export default function EvaluationPage() {
    const [openSections, setOpenSections] = useState<Record<string, boolean>>({
        methodology: true,
        interpretation: false,
        scope: false,
        faq: false
    });

    const toggleSection = (key: string) => {
        setOpenSections(prev => ({ ...prev, [key]: !prev[key] }));
    };

    return (
        <div className="flex flex-col h-screen overflow-hidden bg-gray-50">
            <TopNav />
            <main className="flex-1 overflow-y-auto">
                <div className="max-w-5xl mx-auto px-6 py-10">
                    <div className="mb-6">
                        <BackButton href="/" label="Back to Home" />
                    </div>

                    <div className="mb-10 text-center md:text-left">
                        <h1 className="text-4xl font-bold text-gray-900 mb-3">Evaluation & Methodology</h1>
                        <p className="text-lg text-gray-500 max-w-3xl">
                            The single source of truth for how Pi-Qualytics defines, calculates, and evaluates data quality metrics.
                        </p>
                    </div>

                    {/* 1. OVERVIEW */}
                    <SectionHeader title="Overview" icon={Info} />
                    <Card className="mb-8 border-none shadow-sm bg-gradient-to-br from-white to-blue-50/30">
                        <CardContent className="p-6">
                            <p className="text-gray-700 leading-relaxed mb-4">
                                In Pi-Qualytics, <strong>Data Quality (DQ)</strong> is not just a static number. It is a dynamic measure of how trustworthy your data is for business operations today.
                            </p>
                            <p className="text-gray-700 leading-relaxed mb-4">
                                Our evaluation framework focuses on <strong>"Today's Load"</strong>. Unlike historical reports that look back at trends, the critical metrics you see on the Dashboard and in Daily Reports represent the health of the data loaded <em>in the current calendar day</em>. This ensures that if yesterday's data was bad but today's is fixed, your score reflects the current reality.
                            </p>
                            <div className="flex gap-4 mt-6">
                                <div className="flex-1 bg-white p-4 rounded-lg border border-gray-100 shadow-sm">
                                    <h3 className="font-semibold text-gray-900 mb-2 flex items-center gap-2">
                                        <CheckCircle className="h-4 w-4 text-green-500" /> Trust
                                    </h3>
                                    <p className="text-sm text-gray-500">Scores are deterministic and based on hard rules, not AI guesses.</p>
                                </div>
                                <div className="flex-1 bg-white p-4 rounded-lg border border-gray-100 shadow-sm">
                                    <h3 className="font-semibold text-gray-900 mb-2 flex items-center gap-2">
                                        <Clock className="h-4 w-4 text-blue-500" /> Timeliness
                                    </h3>
                                    <p className="text-sm text-gray-500">Metrics prioritize the latest scan to give real-time feedback.</p>
                                </div>
                            </div>
                        </CardContent>
                    </Card>

                    {/* 2. METRICS & KPIs */}
                    <SectionHeader title="Metrics & KPIs" icon={Activity} />

                    <KPICard
                        name="Overall Quality Score"
                        icon={Shield}
                        description="A consolidated score representing the overall health of today's scanned data."
                        whyItMatters="It provides a single, executive-level number to answer 'Is the data good?' without needing to analyze thousands of individual rows."
                        dataSource="Snowflake DQ_CHECK_RESULTS table (Aggregated Daily)"
                        calculation={
                            <>
                                <p>Weighted Average of:</p>
                                <ul className="list-disc ml-5 mt-1 space-y-1">
                                    <li>Completeness Checks</li>
                                    <li>Validity Checks</li>
                                    <li>Uniqueness Checks</li>
                                    <li>Freshness Checks</li>
                                </ul>
                                <p className="mt-2 text-xs opacity-75">Score = (Passed Checks / Total Checks) * 100</p>
                            </>
                        }
                        interpretation={
                            <ul className="space-y-2">
                                <li className="flex items-center gap-2">
                                    <span className="w-2 h-2 rounded-full bg-green-500"></span>
                                    <strong>90–100%: Healthy.</strong> Data is reliable for use.
                                </li>
                                <li className="flex items-center gap-2">
                                    <span className="w-2 h-2 rounded-full bg-yellow-500"></span>
                                    <strong>70–89%: Needs Attention.</strong> Minor issues exist.
                                </li>
                                <li className="flex items-center gap-2">
                                    <span className="w-2 h-2 rounded-full bg-red-500"></span>
                                    <strong>&lt; 70%: Action Required.</strong> Significant quality failures.
                                </li>
                            </ul>
                        }
                    />

                    <KPICard
                        name="Coverage Score"
                        icon={Database}
                        description="A measure of how much of your data assets are being actively monitored by DQ rules."
                        whyItMatters="A perfect quality score on only 1% of your data is misleading. High coverage ensures no blind spots."
                        dataSource="Snowflake DQ_METRICS schema"
                        calculation={
                            <>
                                <p>Simple Ratio:</p>
                                <p className="mt-1">Coverage = (Tables with &gt;0 Checks / Total Active Tables) * 100</p>
                            </>
                        }
                        interpretation={
                            <p>
                                A low coverage score indicates that you have tables in your warehouse that have <strong>no active quality rules</strong>.
                                These tables are essentially "untrusted" because their quality is unknown.
                            </p>
                        }
                    />

                    <KPICard
                        name="Validity Score"
                        icon={CheckCircle}
                        description="The percentage of records that adhere to defined business formats and reference constraints."
                        whyItMatters="Ensures data is syntactically correct (e.g., email formats, positive prices) and referentially integral."
                        dataSource="DQ_CHECK_RESULTS where RULE_TYPE='VALIDITY'"
                        calculation={
                            <>
                                <p>Validity Rate =</p>
                                <p>(Valid Records / Total Records Checked) * 100</p>
                                <p className="mt-2 text-xs italic">Only applies to rules of type 'VALIDITY' or 'CONSISTENCY'.</p>
                            </>
                        }
                        interpretation={
                            <p>
                                If Validity is low but Completeness is high, it means you have data present, but it is <strong>garbage or incorrect</strong>
                                (e.g., negative ages, malformed dates).
                            </p>
                        }
                    />

                    <KPICard
                        name="Critical Anomalies"
                        icon={AlertTriangle}
                        description="The count of records violating 'Critical' severity rules that require immediate remediation."
                        whyItMatters="These are show-stoppers. These records are often excluded from downstream reporting to prevent business errors."
                        dataSource="DQ_FAILED_RECORDS where IS_CRITICAL=TRUE"
                        calculation={
                            <>
                                <p>Sum of all rows in DQ_FAILED_RECORDS where:</p>
                                <ul className="list-disc ml-5 mt-1">
                                    <li>IS_CRITICAL = TRUE</li>
                                    <li>DETECTED_DATE = CURRENT_DATE</li>
                                </ul>
                            </>
                        }
                        interpretation={
                            <p>
                                Any value &gt; 0 is an incident. These should be investigated immediately via the "Data" tab or "Issues" list.
                            </p>
                        }
                    />


                    {/* 3. CALCULATIONS & METHODOLOGY */}
                    <SectionHeader title="Calculation Methodology" icon={FileText} />
                    <AccordionItem
                        title="Weighting Logic"
                        isOpen={openSections.methodology}
                        onClick={() => toggleSection('methodology')}
                    >
                        <p className="mb-3">
                            Pi-Qualytics uses a <strong>deterministic, unweighted model</strong> by default for the Main Dashboard. This means every check is treated equally: a failure in a simple "Not Null" check counts the same as a failure in a complex "Business Logic" check for the purpose of the top-level score.
                        </p>
                        <p className="mb-3">
                            <strong>Why?</strong> This prevents "score inflation" where thousands of easy checks hide a few critical failures.
                        </p>
                        <p>
                            However, <strong>Critical Flags</strong> override this logic in the "Attention Required" section. A single Critical failure will trigger a high-severity alert regardless of the overall score.
                        </p>
                    </AccordionItem>

                    <AccordionItem
                        title="Date Scoping"
                        isOpen={openSections.methodology}
                        onClick={() => toggleSection('methodology')}
                    >
                        <p>
                            <strong>Daily Mode (Default):</strong> Metrics aggregate <em>all runs</em> that started between <code>00:00:00</code> and <code>23:59:59</code> of the selected day (server time).
                        </p>
                        <p className="mt-2">
                            This means if you run a scan at 9 AM and another at 4 PM, the "Daily Data Quality Overview" will show the combined results (Total Checks = Run 1 + Run 2).
                        </p>
                    </AccordionItem>


                    {/* 4. INTERPRETATION GUIDE */}
                    <SectionHeader title="Interpretation Guide" icon={HelpCircle} />
                    <AccordionItem
                        title="Why score can drop even if records increased?"
                        isOpen={openSections.interpretation}
                        onClick={() => toggleSection('interpretation')}
                    >
                        <p>
                            If you load more data, you often introduce more variance. If the new batch of data has a lower quality rate than the previous batch, your overall average will drop.
                            Volume does not equal Quality.
                        </p>
                    </AccordionItem>
                    <AccordionItem
                        title="Why 'No Scan Today' means No Score?"
                        isOpen={openSections.interpretation}
                        onClick={() => toggleSection('interpretation')}
                    >
                        <p>
                            We do not carry over scores from yesterday. Data Quality is a point-in-time assessment. If we haven't checked today's data, we cannot certify its quality.
                            Thus, the system defaults to an "Unknown" or "No Data" state rather than showing potentially stale "Good" scores.
                        </p>
                    </AccordionItem>


                    {/* 5. DATA SCOPE */}
                    <SectionHeader title="Data Scope & Limitations" icon={Database} />
                    <AccordionItem
                        title="Scope of Evaluation"
                        isOpen={openSections.scope}
                        onClick={() => toggleSection('scope')}
                    >
                        <ul className="list-disc pl-5 space-y-2">
                            <li><strong>Production Only:</strong> Metrics reflect only tables in the configured Production/Reporting environment. Sandbox tables are excluded.</li>
                            <li><strong>Active Rules Only:</strong> Rules disabled in the Governance console are completely ignored from calculation.</li>
                            <li><strong>No Retroactive Updates:</strong> If you fix a rule definition, it applies to <em>future scans only</em>. Past scores are immutable audit logs.</li>
                        </ul>
                    </AccordionItem>

                </div>
            </main>
        </div>
    );
}
