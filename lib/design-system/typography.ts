/**
 * Design System - Typography
 * Font families, sizes, weights, and line heights
 */

export const typography = {
    // Font families
    fontFamily: {
        sans: ['Inter', '-apple-system', 'BlinkMacSystemFont', 'Segoe UI', 'Roboto', 'sans-serif'],
        mono: ['JetBrains Mono', 'Menlo', 'Monaco', 'Courier New', 'monospace'],
    },

    // Font sizes
    fontSize: {
        xs: '0.75rem',      // 12px
        sm: '0.875rem',     // 14px
        base: '1rem',       // 16px
        lg: '1.125rem',     // 18px
        xl: '1.25rem',      // 20px
        '2xl': '1.5rem',    // 24px
        '3xl': '1.875rem',  // 30px
        '4xl': '2.25rem',   // 36px
    },

    // Font weights
    fontWeight: {
        normal: '400',
        medium: '500',
        semibold: '600',
        bold: '700',
    },

    // Line heights
    lineHeight: {
        tight: '1.25',
        normal: '1.5',
        relaxed: '1.75',
    },

    // Letter spacing
    letterSpacing: {
        tight: '-0.025em',
        normal: '0',
        wide: '0.025em',
    },
} as const;

export type Typography = typeof typography;
