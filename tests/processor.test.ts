import { describe, expect, it } from "vitest";
import fs from "fs";
import path from "path";
import os from "os";
import { processFile } from "../src/processor";

function makeTempDir(): string {
  return fs.mkdtempSync(path.join(os.tmpdir(), "nem12-"));
}

function buildSample300(date: string): string {
  const values = new Array(48).fill("1.0");
  return ["300", date, ...values].join(",");
}

describe("processFile", () => {
  it("logs duplicate 300 records", async () => {
    const dir = makeTempDir();
    const inputPath = path.join(dir, "input.csv");
    const outputPath = path.join(dir, "output.sql");
    const lines = [
      "100,NEM12,202503010000,UNITEDDP,NEMMCO",
      "200,NEM1201009,E1E2,1,E1,N1,01009,kWh,30,20250310",
      buildSample300("20250301"),
      buildSample300("20250301"),
      "900",
    ];
    fs.writeFileSync(inputPath, lines.join("\n"));

    const result = await processFile({
      inputPath,
      outputPath,
      batchSize: 1000,
    });

    expect(result.duplicate300Count).toBe(1);
    expect(fs.existsSync(outputPath)).toBe(true);
    const output = fs.readFileSync(outputPath, "utf8");
    expect(output).toContain("INSERT INTO meter_readings");
  });
});
