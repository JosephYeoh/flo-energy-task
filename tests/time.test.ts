import { describe, expect, it } from "vitest";
import { dateToUtcMs, formatTimestampUtc, getOffsetsMs, getIntervalCount } from "../src/time";

describe("time utilities", () => {
  it("computes interval counts", () => {
    expect(getIntervalCount(30)).toBe(48);
    expect(getIntervalCount(15)).toBe(96);
    expect(getIntervalCount(5)).toBe(288);
  });

  it("formats period-ending timestamps", () => {
    const base = dateToUtcMs("20250301");
    const offsets = getOffsetsMs(30);
    const ts = formatTimestampUtc(base + offsets[0]);
    expect(ts).toBe("2025-03-01 00:30:00");
  });
});
