/**
 * Design System - Main Export
 * Centralized export for all design tokens
 */

export { colors, type ColorPalette } from './colors';
export { typography, type Typography } from './typography';
export { spacing, borderRadius, shadows, type Spacing, type BorderRadius, type Shadows } from './spacing';

// Re-export everything as a single design system object
import { colors } from './colors';
import { typography } from './typography';
import { spacing, borderRadius, shadows } from './spacing';

export const designSystem = {
    colors,
    typography,
    spacing,
    borderRadius,
    shadows,
} as const;

export type DesignSystem = typeof designSystem;
