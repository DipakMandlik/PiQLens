export type DataViewMode = 'TABLE' | 'TODAY';

export const DATA_VIEW_MODES: DataViewMode[] = ['TABLE', 'TODAY'];

export function isDataViewMode(value: string | null | undefined): value is DataViewMode {
  if (!value) return false;
  return DATA_VIEW_MODES.includes(value.toUpperCase() as DataViewMode);
}

export function legacyScopeToMode(scope?: string | null): DataViewMode {
  const normalized = (scope || '').trim().toUpperCase();
  if (normalized === 'INCREMENTAL') return 'TODAY';
  if (normalized === 'FULL') return 'TABLE';
  return 'TABLE';
}

export function normalizeMode(input?: string | null): DataViewMode {
  const normalized = (input || '').trim().toUpperCase();
  if (isDataViewMode(normalized)) return normalized;
  return legacyScopeToMode(normalized);
}

export function modeLabel(mode: DataViewMode): string {
  switch (mode) {
    case 'TABLE':
      return 'Table';
    case 'TODAY':
      return 'Today';
    default:
      return 'Table';
  }
}
