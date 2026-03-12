import React from "react";
import { BackButton } from "@/components/back-button";
import { ReportHistoryTab } from "@/components/reporting/ReportHistoryTab";

export default function ReportsDashboard() {
    return (
        <div className="min-h-screen bg-gray-50 p-8">
            <div className="mx-auto max-w-7xl">
                {/* Header */}
                <div className="mb-8">
                    <div className="mb-4">
                        <BackButton href="/reporting" label="Back to Reporting" />
                    </div>
                    <h1 className="mb-2 text-3xl font-bold text-gray-900">Intelligence Reports</h1>
                    <p className="text-gray-600">
                        View, track, and download authoritative daily intelligence reports. Generate new platform reports from the Reporting or Home pages.
                    </p>
                </div>

                {/* Content Area */}
                <div className="rounded-xl border border-slate-200 bg-white shadow-sm p-6">
                    <ReportHistoryTab />
                </div>
            </div>
        </div>
    );
}

