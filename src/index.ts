#!/usr/bin/env node
import { processFile } from "./processor";

interface CliOptions {
  inputPath?: string;
  outputPath?: string;
  batchSize?: number;
  logMissingPath?: string;
}

function parseArgs(argv: string[]): CliOptions {
  const options: CliOptions = {};
  for (let i = 2; i < argv.length; i += 1) {
    const arg = argv[i];
    if (arg === "--input" && argv[i + 1]) {
      options.inputPath = argv[i + 1];
      i += 1;
      continue;
    }
    if (arg === "--output" && argv[i + 1]) {
      options.outputPath = argv[i + 1];
      i += 1;
      continue;
    }
    if (arg === "--batch-size" && argv[i + 1]) {
      options.batchSize = Number(argv[i + 1]);
      i += 1;
      continue;
    }
    if (arg === "--log-missing" && argv[i + 1]) {
      options.logMissingPath = argv[i + 1];
      i += 1;
      continue;
    }
    if (arg === "--help" || arg === "-h") {
      options.inputPath = "__help__";
      return options;
    }
  }
  return options;
}

function printUsage(): void {
  const msg = `Usage: nem12-to-sql --input <path> --output <path> [--batch-size <n>] [--log-missing <path>]

Environment variables:
  INPUT_PATH, OUTPUT_PATH, BATCH_SIZE, LOG_MISSING_PATH
`;
  process.stderr.write(msg);
}

async function main(): Promise<void> {
  const cli = parseArgs(process.argv);
  if (cli.inputPath === "__help__") {
    printUsage();
    process.exit(0);
  }

  const inputPath = cli.inputPath || process.env.INPUT_PATH;
  const outputPath = cli.outputPath || process.env.OUTPUT_PATH;
  const batchSizeRaw = cli.batchSize ?? Number(process.env.BATCH_SIZE ?? "1000");
  const logMissingPath = cli.logMissingPath || process.env.LOG_MISSING_PATH;

  if (!inputPath || !outputPath) {
    printUsage();
    process.exit(1);
  }

  if (!Number.isFinite(batchSizeRaw) || batchSizeRaw <= 0) {
    console.error("Invalid batch size");
    process.exit(1);
  }

  try {
    const result = await processFile({
      inputPath,
      outputPath,
      batchSize: Math.floor(batchSizeRaw),
      logMissingPath,
    });
    console.error(
      `Processed ${result.recordsRead} records. Emitted ${result.rowsEmitted} rows. Skipped ${result.rowsSkipped} rows. Duplicates ${result.duplicate300Count}.`
    );
  } catch (error) {
    console.error(`Failed to process file: ${(error as Error).message}`);
    process.exit(1);
  }
}

main();
