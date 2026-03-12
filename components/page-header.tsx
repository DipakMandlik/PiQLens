'use client';

import { BackButton } from '@/components/back-button';
import { ReactNode } from 'react';

export interface PageHeaderProps {
    /** Page title */
    title: string;
    /** Optional page description */
    description?: string;
    /** Show back button */
    showBackButton?: boolean;
    /** Custom back button label */
    backButtonLabel?: string;
    /** Custom back button href */
    backButtonHref?: string;
    /** Optional icon to display next to title */
    icon?: ReactNode;
    /** Optional actions (buttons, etc.) to display on the right */
    actions?: ReactNode;
}

/**
 * Reusable page header component with optional back button
 */
export function PageHeader({
    title,
    description,
    showBackButton = false,
    backButtonLabel,
    backButtonHref,
    icon,
    actions,
}: PageHeaderProps) {
    return (
        <div className="mb-8">
            {showBackButton && (
                <div className="mb-4">
                    <BackButton label={backButtonLabel} href={backButtonHref} />
                </div>
            )}

            <div className="flex items-start justify-between">
                <div>
                    <h1 className="text-3xl font-bold text-gray-900 dark:text-white flex items-center gap-3">
                        {icon && <span className="text-blue-600">{icon}</span>}
                        {title}
                    </h1>
                    {description && (
                        <p className="text-gray-500 dark:text-gray-400 mt-2 max-w-3xl">
                            {description}
                        </p>
                    )}
                </div>

                {actions && (
                    <div className="flex items-center gap-2">
                        {actions}
                    </div>
                )}
            </div>
        </div>
    );
}
