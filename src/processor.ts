import fs from "fs";
import path from "path";
import { Readable } from "stream";
import { pipeline } from "stream/promises";
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

  async function* generateSqlBatches(): AsyncGenerator<string> {
    // Build and clear the current batch, returning SQL or null if empty.
    const emitBatch = (): string | null => {
      if (rows.length === 0) return null;
      const sql = buildInsert(rows);
      rowsEmitted += rows.length;
      rows.length = 0;
      return sql;
    };

    // Consume parsed records and yield SQL when a batch is ready.
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

        // Log skipped interval values for auditing.
        const logSkipped = (index: number, reason: string, rawValue: string) => {
          skippedLog.write(
            `${current200!.nmi},${record300.intervalDate},${index},${reason},${rawValue}\n`,
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
            const sql = emitBatch();
            if (sql) yield sql;
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

    // Flush any remaining rows at end-of-file.
    const finalSql = emitBatch();
    if (finalSql) yield finalSql;
  }

  try {
    const sqlStream = Readable.from(generateSqlBatches(), { encoding: "utf8" });
    await pipeline(sqlStream, output);
    await new Promise<void>((resolve, reject) => {
      skippedLog.end(() => resolve());
      skippedLog.on("error", reject);
    });
    await fs.promises.rename(tmpPath, outputPath);

    return { recordsRead, rowsEmitted, rowsSkipped };
  } catch (error) {
    // Best-effort cleanup for partial runs.
    input.destroy();
    parser.destroy();
    output.destroy();
    skippedLog.destroy();
    await fs.promises.rm(tmpPath, { force: true });
    throw error;
  }
}
