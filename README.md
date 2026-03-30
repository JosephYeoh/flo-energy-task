# NEM12 Meter Readings Generator

Strict, streaming NEM12 parser that expands interval values into `meter_readings` SQL upserts.

## Usage

Local:

```bash
npm install
npm run build
node dist/index.js --input ./input.csv --output ./output.sql --batch-size 1000
```

Options:
- `--input <path>` required
- `--output <path>` required
- `--batch-size <n>` optional (default 1000)
- `--log-missing <path>` optional (logs skipped blank interval values)

Environment variables:
- `INPUT_PATH`, `OUTPUT_PATH`, `BATCH_SIZE`, `LOG_MISSING_PATH`

## Docker

```bash
docker build -t nem12-meter-readings:latest .
docker run --rm \
  -v $(pwd)/data:/data \
  -e INPUT_PATH=/data/in/input.csv \
  -e OUTPUT_PATH=/data/out/output.sql \
  nem12-meter-readings:latest
```

## Docker Compose

```bash
docker compose up --build
```

Place the input file at `./data/in/input.csv`. Output will be written to `./data/out/output.sql`.

## Notes

- Only record types 100/200/300/900 are supported in this version.
- Any unexpected record causes the file to be rejected (no output).
- Duplicate 300 records for the same `(nmi, IntervalDate)` are logged.
