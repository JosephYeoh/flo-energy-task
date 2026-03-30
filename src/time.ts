const VALID_INTERVAL_LENGTHS = new Set([5, 15, 30]);
const OFFSETS_CACHE = new Map<number, number[]>();

export function isValidIntervalLength(value: number): boolean {
  return VALID_INTERVAL_LENGTHS.has(value);
}

export function getIntervalCount(intervalLength: number): number {
  return Math.floor(1440 / intervalLength);
}

export function getOffsetsMs(intervalLength: number): number[] {
  const cached = OFFSETS_CACHE.get(intervalLength);
  if (cached) return cached;
  const count = getIntervalCount(intervalLength);
  const offsets: number[] = new Array(count);
  for (let i = 0; i < count; i += 1) {
    offsets[i] = (i + 1) * intervalLength * 60 * 1000;
  }
  OFFSETS_CACHE.set(intervalLength, offsets);
  return offsets;
}

function parseDateYYYYMMDD(value: string): { year: number; month: number; day: number } {
  if (!/^\d{8}$/.test(value)) {
    throw new Error(`Invalid IntervalDate: ${value}`);
  }
  const year = Number(value.slice(0, 4));
  const month = Number(value.slice(4, 6));
  const day = Number(value.slice(6, 8));
  if (!Number.isFinite(year) || !Number.isFinite(month) || !Number.isFinite(day)) {
    throw new Error(`Invalid IntervalDate: ${value}`);
  }
  if (month < 1 || month > 12 || day < 1 || day > 31) {
    throw new Error(`Invalid IntervalDate: ${value}`);
  }
  return { year, month, day };
}

export function dateToUtcMs(dateStr: string): number {
  const { year, month, day } = parseDateYYYYMMDD(dateStr);
  return Date.UTC(year, month - 1, day, 0, 0, 0, 0);
}

export function formatTimestampUtc(ms: number): string {
  const d = new Date(ms);
  const yyyy = d.getUTCFullYear();
  const mm = String(d.getUTCMonth() + 1).padStart(2, "0");
  const dd = String(d.getUTCDate()).padStart(2, "0");
  const hh = String(d.getUTCHours()).padStart(2, "0");
  const mi = String(d.getUTCMinutes()).padStart(2, "0");
  const ss = String(d.getUTCSeconds()).padStart(2, "0");
  return `${yyyy}-${mm}-${dd} ${hh}:${mi}:${ss}`;
}
