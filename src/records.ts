import { Record100, Record200, Record300, Record900 } from "./types";
import { getIntervalCount, isValidIntervalLength } from "./time";

export function normalizeFields(raw: unknown[]): string[] {
  return raw.map((value) => (value === undefined || value === null ? "" : String(value)));
}

export function parseRecordIndicator(fields: string[]): string {
  return (fields[0] ?? "").trim();
}

export function parse100(fields: string[]): Record100 {
  return { type: "100", raw: fields };
}

export function parse900(fields: string[]): Record900 {
  return { type: "900", raw: fields };
}

export function parse200(fields: string[]): Record200 {
  if (fields.length < 9) {
    throw new Error("Invalid 200 record: expected at least 9 fields");
  }
  const nmi = (fields[1] ?? "").trim();
  if (!nmi) {
    throw new Error("Invalid 200 record: missing NMI");
  }
  const intervalLengthRaw = (fields[8] ?? "").trim();
  const intervalLength = Number(intervalLengthRaw);
  if (!Number.isFinite(intervalLength) || !isValidIntervalLength(intervalLength)) {
    throw new Error(`Invalid 200 record: invalid IntervalLength ${intervalLengthRaw}`);
  }
  return {
    type: "200",
    raw: fields,
    nmi,
    intervalLength,
    intervalLengthRaw,
  };
}

export function parse300(fields: string[], intervalLength: number): Record300 {
  if (fields.length < 2) {
    throw new Error("Invalid 300 record: expected at least 2 fields");
  }
  const intervalDate = (fields[1] ?? "").trim();
  const intervalCount = getIntervalCount(intervalLength);
  const expectedMinLength = 2 + intervalCount;
  if (fields.length < expectedMinLength) {
    throw new Error(`Invalid 300 record: expected at least ${expectedMinLength} fields`);
  }
  const intervalValues = fields.slice(2, 2 + intervalCount).map((value) => value.trim());
  return {
    type: "300",
    raw: fields,
    intervalDate,
    intervalValues,
  };
}
