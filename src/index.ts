#!/usr/bin/env node
import { processFile } from "./processor";

interface CliOptions {
  inputPath?: string;
  outputPath?: string;
  batchSize?: number;
}

/**
 * Parse CLI arguments into a structured options object.
 */
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
    if (arg === "--help" || arg === "-h") {
      options.inputPath = "__help__";
      return options;
    }
  }
  return options;
}

/**
 * Print CLI usage information to stderr.
 */
function printUsage(): void {
  const msg = `Usage: nem12-to-sql --input <path> --output <path> [--batch-size <n>]`;
  process.stderr.write(msg);
}

/**
 * Entry point for the CLI.
 */
async function main(): Promise<void> {
  const cli = parseArgs(process.argv);
  if (cli.inputPath === "__help__") {
    printUsage();
    process.exit(0);
  }

  const { inputPath, outputPath } = cli;
  const batchSizeRaw = cli.batchSize ?? 1000;

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
    });
    console.error(
      `Processed ${result.recordsRead} records. Emitted ${result.rowsEmitted} rows. Skipped ${result.rowsSkipped} rows.`,
    );
  } catch (error) {
    console.error(`Failed to process file: ${(error as Error).message}`);
    process.exit(1);
  }
}

main();
