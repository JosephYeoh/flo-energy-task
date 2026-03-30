import { describe, expect, it } from "vitest";
import { parse200, parse300 } from "../src/records";

describe("record parsing", () => {
  it("parses 200 interval length", () => {
    const rec = parse200(["200", "NEM1234567", "E1", "1", "E1", "N1", "01009", "kWh", "30", "20250101"]);
    expect(rec.nmi).toBe("NEM1234567");
    expect(rec.intervalLength).toBe(30);
  });

  it("parses 300 values for 30-minute interval", () => {
    const values = new Array(48).fill("1.0");
    const rec = parse300(["300", "20250301", ...values], 30);
    expect(rec.intervalValues.length).toBe(48);
  });
});
