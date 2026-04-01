import fs from "fs";
import path from "path";
import { parse } from "csv-parse";
import { buildInsert, buildRow } from "./sql";
import { parse100, parse200, parse300, parse900, Record200 } from "./records";
import { dateToUtcMs, formatTimestampUtc, getOffsetsMs } from "./time";

export interface ProcessOptions {
  inputPath: string;
  outputPath: string;
  skippedPath: string;
  batchSize: number;
}

export interface ProcessResult {
  recordsRead: number;
  rowsEmitted: number;
  rowsSkipped: number;
}

/**
 * Write to a stream and await drain if the internal buffer is full.
 */
async function writeWithBackpressure(stream: fs.WriteStream, data: string): Promise<void> {
  if (stream.write(data)) return;
  await new Promise<void>((resolve) => stream.once("drain", () => resolve()));
}

/**
 * Stream a NEM12 file into batched SQL inserts with a skipped-row audit log.
 */
export async function processFile(options: ProcessOptions): Promise<ProcessResult> {
  const { inputPath, outputPath, batchSize, skippedPath } = options;
  // Write output atomically via temp file; always keep a skipped-log next to it.
  const tmpPath = `${outputPath}.tmp`;

  await fs.promises.mkdir(path.dirname(outputPath), { recursive: true });
  await fs.promises.mkdir(path.dirname(skippedPath), { recursive: true });

  await fs.promises.rm(tmpPath, { force: true });
  await fs.promises.rm(skippedPath, { force: true });

  // Streams for input, output SQL, and skipped-row audit log.
  const input = fs.createReadStream(inputPath, { encoding: "utf8" });
  const output = fs.createWriteStream(tmpPath, { encoding: "utf8" });
  const skippedLog = fs.createWriteStream(skippedPath, { encoding: "utf8" });

  // CSV parser yields one record per NEM12 line.
  const parser = parse({
    bom: true,
    relax_quotes: true,
    relax_column_count: true,
    trim: true,
  });

  input.pipe(parser);

  // State for the current 200 record context.
  let current200: Record200 | null = null;
  const rows: string[] = [];

  let recordsRead = 0;
  let rowsEmitted = 0;
  let rowsSkipped = 0;

  // Flush the in-memory batch to the output stream.
  const flushRows = async () => {
    if (rows.length === 0) return;
    const sql = buildInsert(rows);
    rowsEmitted += rows.length;
    rows.length = 0;
    await writeWithBackpressure(output, sql);
  };

  // Best-effort cleanup for partial runs.
  const cleanup = async () => {
    input.destroy();
    parser.destroy();
    output.destroy();
    skippedLog.destroy();
    await fs.promises.rm(tmpPath, { force: true });
  };

  try {
    for await (const record of parser) {
      recordsRead += 1;
      // Normalize to strings for consistent parsing.
      const fields = (record as unknown[]).map((v) => String(v ?? ""));
      const indicator = fields[0] ?? "";

      if (indicator === "100") {
        parse100(fields);
        current200 = null;
        continue;
      }

      if (indicator === "200") {
        // Update context for subsequent 300 records.
        current200 = parse200(fields);
        continue;
      }

      if (indicator === "300") {
        if (!current200) {
          throw new Error("300 record encountered before a valid 200 record");
        }
        const record300 = parse300(fields, current200.intervalLength);

        // Precompute timestamps for interval positions.
        const baseMs = dateToUtcMs(record300.intervalDate);
        const offsetsMs = getOffsetsMs(current200.intervalLength);

        const logSkipped = (index: number, reason: string, rawValue: string) => {
          skippedLog.write(
            `${current200!.nmi},${record300.intervalDate},${index},${reason},${rawValue}\n`
          );
        };

        for (let i = 0; i < record300.intervalValues.length; i += 1) {
          const rawValue = record300.intervalValues[i];
          if (!rawValue) {
            rowsSkipped += 1;
            // Missing interval value: skip and log.
            logSkipped(i + 1, "missing_value", "");
            continue;
          }
          const value = Number(rawValue);
          if (!Number.isFinite(value)) {
            rowsSkipped += 1;
            // Non-numeric interval value: skip and log.
            logSkipped(i + 1, "invalid_number", rawValue);
            continue;
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

      // Ignore 400/500 until explicitly supported.
      if (indicator === "400" || indicator === "500") {
        continue;
      }

      throw new Error(`Unexpected record indicator: ${indicator || "<empty>"}`);
    }

    // Final flush and close streams.
    await flushRows();
    await new Promise<void>((resolve, reject) => {
      output.end(() => resolve());
      output.on("error", reject);
    });
    await new Promise<void>((resolve, reject) => {
      skippedLog.end(() => resolve());
      skippedLog.on("error", reject);
    });
    await fs.promises.rename(tmpPath, outputPath);

    return { recordsRead, rowsEmitted, rowsSkipped };
  } catch (error) {
    await cleanup();
    throw error;
  }
}
