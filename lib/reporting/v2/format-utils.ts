const IST_TIMEZONE = 'Asia/Kolkata';

function toDate(value: string | Date | null | undefined): Date | null {
  if (!value) return null;
  const d = value instanceof Date ? value : new Date(value);
  if (Number.isNaN(d.getTime())) return null;
  return d;
}

export function formatTimestampIST(value: string | Date | null | undefined): string {
  const date = toDate(value);
  if (!date) return '';

  return `${new Intl.DateTimeFormat('en-CA', {
    timeZone: IST_TIMEZONE,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  }).format(date)} ${new Intl.DateTimeFormat('en-GB', {
    timeZone: IST_TIMEZONE,
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: false,
  }).format(date)} IST`;
}

export function formatDateIST(value: string | Date | null | undefined): string {
  const date = toDate(value);
  if (!date) return '';
  return new Intl.DateTimeFormat('en-CA', {
    timeZone: IST_TIMEZONE,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  }).format(date);
}

export function ensureNonNegativeNumber(value: unknown): number {
  const n = Number(value ?? 0);
  if (!Number.isFinite(n)) return 0;
  return n < 0 ? 0 : n;
}

export function roundToTwo(value: number): number {
  return Number((value || 0).toFixed(2));
}

export function computeSuccessRate(passedChecks: number, totalChecks: number): number {
  if (!totalChecks || totalChecks <= 0) {
    return 0;
  }
  return roundToTwo((passedChecks / totalChecks) * 100);
}

export function sanitizeText(value: unknown): string {
  if (value === null || value === undefined) return '';
  return String(value);
}
