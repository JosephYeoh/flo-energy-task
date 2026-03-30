import fs from "fs";
import { parse } from "csv-parse";
import { buildInsert, buildRow } from "./sql";
import { normalizeFields, parse100, parse200, parse300, parse900, parseRecordIndicator } from "./records";
import { dateToUtcMs, formatTimestampUtc, getOffsetsMs } from "./time";
import { Record200 } from "./types";

export interface ProcessOptions {
  inputPath: string;
  outputPath: string;
  batchSize: number;
  logMissingPath?: string;
}

export interface ProcessResult {
  recordsRead: number;
  rowsEmitted: number;
  rowsSkipped: number;
  duplicate300Count: number;
}

async function writeWithBackpressure(stream: fs.WriteStream, data: string): Promise<void> {
  if (stream.write(data)) return;
  await new Promise<void>((resolve) => stream.once("drain", () => resolve()));
}

export async function processFile(options: ProcessOptions): Promise<ProcessResult> {
  const { inputPath, outputPath, batchSize, logMissingPath } = options;
  const tmpPath = `${outputPath}.tmp`;

  await fs.promises.rm(tmpPath, { force: true });

  const input = fs.createReadStream(inputPath, { encoding: "utf8" });
  const output = fs.createWriteStream(tmpPath, { encoding: "utf8" });
  const missingLog = logMissingPath
    ? fs.createWriteStream(logMissingPath, { encoding: "utf8" })
    : null;

  const parser = parse({
    bom: true,
    relax_quotes: true,
    relax_column_count: true,
    trim: false,
  });

  input.pipe(parser);

  let current200: Record200 | null = null;
  const seen300 = new Set<string>();
  const rows: string[] = [];

  let recordsRead = 0;
  let rowsEmitted = 0;
  let rowsSkipped = 0;
  let duplicate300Count = 0;

  const flushRows = async () => {
    if (rows.length === 0) return;
    const sql = buildInsert(rows);
    rowsEmitted += rows.length;
    rows.length = 0;
    await writeWithBackpressure(output, sql);
  };

  const cleanup = async () => {
    input.destroy();
    parser.destroy();
    output.destroy();
    if (missingLog) missingLog.destroy();
    await fs.promises.rm(tmpPath, { force: true });
  };

  try {
    for await (const record of parser) {
      recordsRead += 1;
      const fields = normalizeFields(record);
      const indicator = parseRecordIndicator(fields);

      if (indicator === "100") {
        parse100(fields);
        current200 = null;
        continue;
      }

      if (indicator === "200") {
        current200 = parse200(fields);
        continue;
      }

      if (indicator === "300") {
        if (!current200) {
          throw new Error("300 record encountered before a valid 200 record");
        }
        const record300 = parse300(fields, current200.intervalLength);

        const duplicateKey = `${current200.nmi}|${record300.intervalDate}`;
        if (seen300.has(duplicateKey)) {
          duplicate300Count += 1;
          console.warn(`Duplicate 300 record detected for ${duplicateKey}`);
        } else {
          seen300.add(duplicateKey);
        }

        const baseMs = dateToUtcMs(record300.intervalDate);
        const offsetsMs = getOffsetsMs(current200.intervalLength);

        for (let i = 0; i < record300.intervalValues.length; i += 1) {
          const rawValue = record300.intervalValues[i];
          if (!rawValue) {
            rowsSkipped += 1;
            if (missingLog) {
              missingLog.write(
                `${current200.nmi},${record300.intervalDate},${i + 1}\n`
              );
            }
            continue;
          }
          const value = Number(rawValue);
          if (!Number.isFinite(value)) {
            throw new Error(`Invalid interval value: ${rawValue}`);
          }
          const timestamp = formatTimestampUtc(baseMs + offsetsMs[i]);
          rows.push(buildRow(current200.nmi, timestamp, value.toString()));
          if (rows.length >= batchSize) {
            await flushRows();
          }
        }
        continue;
      }

      if (indicator === "900") {
        parse900(fields);
        current200 = null;
        continue;
      }

      if (indicator === "400" || indicator === "500") {
        continue;
      }

      throw new Error(`Unexpected record indicator: ${indicator || "<empty>"}`);
    }

    await flushRows();
    await new Promise<void>((resolve, reject) => {
      output.end(() => resolve());
      output.on("error", reject);
    });
    if (missingLog) {
      await new Promise<void>((resolve, reject) => {
        missingLog.end(() => resolve());
        missingLog.on("error", reject);
      });
    }
    await fs.promises.rename(tmpPath, outputPath);

    return { recordsRead, rowsEmitted, rowsSkipped, duplicate300Count };
  } catch (error) {
    await cleanup();
    throw error;
  }
}
