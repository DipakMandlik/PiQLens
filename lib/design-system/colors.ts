/**
 * Design System - Color Palette
 * Enterprise-grade color system for Pi-Qualytics
 */

export const colors = {
    // Primary - Blue (Main brand color)
    primary: {
        50: '#eff6ff',
        100: '#dbeafe',
        200: '#bfdbfe',
        300: '#93c5fd',
        400: '#60a5fa',
        500: '#3b82f6',  // Main blue
        600: '#2563eb',
        700: '#1d4ed8',
        800: '#1e40af',
        900: '#1e3a8a',
    },

    // Gray - Neutral colors
    gray: {
        50: '#f9fafb',
        100: '#f3f4f6',
        200: '#e5e7eb',
        300: '#d1d5db',
        400: '#9ca3af',
        500: '#6b7280',
        600: '#4b5563',
        700: '#374151',
        800: '#1f2937',
        900: '#111827',
    },

    // Success - Green (Prod environment)
    success: {
        50: '#f0fdf4',
        100: '#dcfce7',
        500: '#10b981',
        600: '#059669',
        700: '#047857',
    },

    // Warning - Orange (Staging environment)
    warning: {
        50: '#fffbeb',
        100: '#fef3c7',
        500: '#f59e0b',
        600: '#d97706',
        700: '#b45309',
    },

    // Info - Cyan (Dev environment)
    info: {
        50: '#ecfeff',
        100: '#cffafe',
        500: '#06b6d4',
        600: '#0891b2',
        700: '#0e7490',
    },

    // Error - Red
    error: {
        50: '#fef2f2',
        100: '#fee2e2',
        500: '#ef4444',
        600: '#dc2626',
        700: '#b91c1c',
    },

    // Semantic colors
    background: {
        primary: '#ffffff',
        secondary: '#f9fafb',
        tertiary: '#f3f4f6',
    },

    border: {
        light: '#e5e7eb',
        medium: '#d1d5db',
        dark: '#9ca3af',
    },

    text: {
        primary: '#111827',
        secondary: '#4b5563',
        tertiary: '#6b7280',
        disabled: '#9ca3af',
        inverse: '#ffffff',
    },
} as const;

export type ColorPalette = typeof colors;
