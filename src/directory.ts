import fs from "fs";
import path from "path";
import { processFile, ProcessResult } from "./processor";

export interface DirectoryOptions {
  inputDir: string;
  outputDir: string;
  logsDir: string;
  batchSize: number;
}

export interface DirectorySummary {
  filesProcessed: number;
  filesFailed: number;
  rowsEmitted: number;
  rowsSkipped: number;
  lastFile?: string;
}

export interface FileOutcome {
  fileName: string;
  result: ProcessResult | null;
}

interface LogDirs {
  errorDir: string;
  skippedDir: string;
}

/**
 * Process all files currently in the input directory and exit.
 */
export async function processDirectoryOnce(options: DirectoryOptions): Promise<DirectorySummary> {
  const { inputDir, outputDir, logsDir, batchSize } = options;
  await resetDir(outputDir);
  await resetDir(logsDir);
  await ensureLogDirs(logsDir);

  const entries = await fs.promises.readdir(inputDir, { withFileTypes: true });
  const files = entries
    .filter((entry) => entry.isFile() && !entry.name.startsWith("."))
    .map((entry) => entry.name)
    .sort();

  const summary: DirectorySummary = {
    filesProcessed: 0,
    filesFailed: 0,
    rowsEmitted: 0,
    rowsSkipped: 0,
  };

  for (const fileName of files) {
    const outcome = await processByFileName(
      {
        fileName,
        inputDir,
        outputDir,
        logsDir,
        batchSize,
      },
    );
    applyOutcomeToSummary(summary, outcome);
  }

  const summaryPath = path.join(logsDir, "summary.json");
  await fs.promises.writeFile(summaryPath, `${JSON.stringify(summary, null, 2)}\n`, "utf8");

  return summary;
}

/**
 * Process a single filename.
 */
async function processByFileName(
  options: {
    fileName: string;
    inputDir: string;
    outputDir: string;
    logsDir: string;
    batchSize: number;
  },
): Promise<FileOutcome> {
  const { fileName, inputDir, outputDir, logsDir, batchSize } = options;
  const inputPath = path.join(inputDir, fileName);

  const baseName = path.parse(fileName).name || fileName;
  const outputPath = path.join(outputDir, `${baseName}.sql`);
  
  const logDirs = getLogDirs(logsDir);
  const skippedPath = path.join(logDirs.skippedDir, `${baseName}.skipped.csv`);
  const errorPath = path.join(logDirs.errorDir, `${baseName}.error.txt`);

  try {
    const result = await processFile({
      inputPath,
      outputPath,
      skippedPath,
      batchSize,
    });
    return { fileName, result };
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    await fs.promises.writeFile(errorPath, `${message}\n`, "utf8");
    console.error(`Failed to process ${fileName}: ${message}`);
    return { fileName, result: null };
  }
}

/**
 * Apply a single-file outcome to the aggregate summary.
 */
export function applyOutcomeToSummary(summary: DirectorySummary, outcome: FileOutcome): void {
  if (outcome.result) {
    summary.filesProcessed += 1;
    summary.rowsEmitted += outcome.result.rowsEmitted;
    summary.rowsSkipped += outcome.result.rowsSkipped;
    summary.lastFile = outcome.fileName;
  } else {
    summary.filesFailed += 1;
    summary.lastFile = outcome.fileName;
  }
}

/**
 * Ensure the _logs folder layout is available.
 */
async function ensureLogDirs(logsDir: string): Promise<void> {
  const { errorDir, skippedDir } = getLogDirs(logsDir);

  await fs.promises.mkdir(errorDir, { recursive: true });
  await fs.promises.mkdir(skippedDir, { recursive: true });

  return;
}

function getLogDirs(logsDir: string): LogDirs {
  return {
    errorDir: path.join(logsDir, "error"),
    skippedDir: path.join(logsDir, "skipped"),
  };
}

async function resetDir(targetDir: string): Promise<void> {
  await fs.promises.rm(targetDir, { recursive: true, force: true });
  await fs.promises.mkdir(targetDir, { recursive: true });
}
