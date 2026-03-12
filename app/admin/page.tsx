'use client';

import { AppLayout } from '@/components/layout/AppLayout';
import { BackButton } from '@/components/back-button';
import { Users, Settings, Bell, Database } from 'lucide-react';

export default function AdminPage() {
    return (
        <AppLayout>
            <div className="p-8">
                <div className="mb-6">
                    <BackButton href="/" label="Back to Home" />
                </div>
                <div className="mb-8">
                    <h1 className="text-3xl font-bold text-gray-900">Administration</h1>
                    <p className="text-gray-500 mt-2">System settings, user management, and configuration.</p>
                </div>

                <div className="bg-white rounded-xl border border-gray-200 shadow-sm divide-y divide-gray-100">
                    <div className="p-6 flex items-center justify-between hover:bg-gray-50 transition-colors cursor-pointer">
                        <div className="flex items-center gap-4">
                            <div className="p-3 bg-gray-100 rounded-lg">
                                <Users className="h-6 w-6 text-gray-600" />
                            </div>
                            <div>
                                <h3 className="text-lg font-medium text-gray-900">User Management</h3>
                                <p className="text-sm text-gray-500">Add users, manage roles and permissions</p>
                            </div>
                        </div>
                        <button className="px-4 py-2 border border-gray-300 rounded-lg text-sm font-medium hover:bg-white text-gray-700">Manage</button>
                    </div>

                    <div className="p-6 flex items-center justify-between hover:bg-gray-50 transition-colors cursor-pointer">
                        <div className="flex items-center gap-4">
                            <div className="p-3 bg-gray-100 rounded-lg">
                                <Database className="h-6 w-6 text-gray-600" />
                            </div>
                            <div>
                                <h3 className="text-lg font-medium text-gray-900">Connection Settings</h3>
                                <p className="text-sm text-gray-500">Configure Snowflake and other data source connections</p>
                            </div>
                        </div>
                        <button className="px-4 py-2 border border-gray-300 rounded-lg text-sm font-medium hover:bg-white text-gray-700">Configure</button>
                    </div>

                    <div className="p-6 flex items-center justify-between hover:bg-gray-50 transition-colors cursor-pointer">
                        <div className="flex items-center gap-4">
                            <div className="p-3 bg-gray-100 rounded-lg">
                                <Bell className="h-6 w-6 text-gray-600" />
                            </div>
                            <div>
                                <h3 className="text-lg font-medium text-gray-900">Notification Channels</h3>
                                <p className="text-sm text-gray-500">Setup Slack, Email, and Webhook alerts</p>
                            </div>
                        </div>
                        <button className="px-4 py-2 border border-gray-300 rounded-lg text-sm font-medium hover:bg-white text-gray-700">Setup</button>
                    </div>

                    <div className="p-6 flex items-center justify-between hover:bg-gray-50 transition-colors cursor-pointer">
                        <div className="flex items-center gap-4">
                            <div className="p-3 bg-gray-100 rounded-lg">
                                <Settings className="h-6 w-6 text-gray-600" />
                            </div>
                            <div>
                                <h3 className="text-lg font-medium text-gray-900">System Preferences</h3>
                                <p className="text-sm text-gray-500">General application settings and defaults</p>
                            </div>
                        </div>
                        <button className="px-4 py-2 border border-gray-300 rounded-lg text-sm font-medium hover:bg-white text-gray-700">Edit</button>
                    </div>
                </div>
            </div>
        </AppLayout>
    );
}
