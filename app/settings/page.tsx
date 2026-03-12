'use client';

import { ComingSoon } from '@/components/coming-soon';

export default function SettingsPage() {
    return (
        <ComingSoon
            title="Settings"
            description="Customize your Pi-Qualytics experience with comprehensive settings and preferences."
            capabilities={[
                'User profile management',
                'Notification preferences',
                'Data quality thresholds',
                'Email alerts configuration',
                'Team collaboration settings',
                'API key management',
                'Integration settings',
                'Export preferences',
            ]}
            icon="⚙️"
        />
    );
}
