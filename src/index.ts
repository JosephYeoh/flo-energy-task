#!/usr/bin/env node
import { processDirectoryOnce } from "./directory";

interface CliOptions {
  inputDir?: string;
  outputDir?: string;
  logsDir?: string;
  batchSize?: number;
}

/**
 * Parse CLI arguments into a structured options object.
 */
function parseArgs(argv: string[]): CliOptions {
  const options: CliOptions = {};
  for (let i = 2; i < argv.length; i += 1) {
    const arg = argv[i];
    if (arg === "--input-dir" && argv[i + 1]) {
      options.inputDir = argv[i + 1];
      i += 1;
      continue;
    }
    if (arg === "--output-dir" && argv[i + 1]) {
      options.outputDir = argv[i + 1];
      i += 1;
      continue;
    }
    if (arg === "--logs-dir" && argv[i + 1]) {
      options.logsDir = argv[i + 1];
      i += 1;
      continue;
    }
    if (arg === "--batch-size" && argv[i + 1]) {
      options.batchSize = Number(argv[i + 1]);
      i += 1;
      continue;
    }
    if (arg === "--help" || arg === "-h") {
      options.inputDir = "__help__";
      return options;
    }
  }
  return options;
}

/**
 * Print CLI usage information to stderr.
 */
function printUsage(): void {
  const msg = `Usage:
  nem12-to-sql --input-dir <dir> --output-dir <dir> --logs-dir <dir> [--batch-size <n>]`;
  process.stderr.write(msg);
}

/**
 * Entry point for the CLI.
 */
async function main(): Promise<void> {
  const cli = parseArgs(process.argv);
  if (cli.inputDir === "__help__") {
    printUsage();
    process.exit(0);
  }

  const inputDir = cli.inputDir;
  const outputDir = cli.outputDir;
  const logsDir = cli.logsDir;
  const batchSizeRaw = cli.batchSize ?? 1000;

  if (!Number.isFinite(batchSizeRaw) || batchSizeRaw <= 0) {
    console.error("Invalid batch size");
    process.exit(1);
  }

  try {
    const batchSize = Math.floor(batchSizeRaw);

    if (!inputDir || !outputDir || !logsDir) {
      printUsage();
      process.exit(1);
    }

    const summary = await processDirectoryOnce({ inputDir, outputDir, logsDir, batchSize });
    console.error(
      `Processed files=${summary.filesProcessed} failed=${summary.filesFailed} rowsEmitted=${summary.rowsEmitted} rowsSkipped=${summary.rowsSkipped}`,
    );
  } catch (error) {
    console.error(`Failed to process file: ${(error as Error).message}`);
    process.exit(1);
  }
}

main();
