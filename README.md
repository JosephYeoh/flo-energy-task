# NEM12 Meter Readings Generator

Strict, streaming NEM12 parser that expands interval values into `meter_readings` SQL upserts.

## Usage

Directory batch:

```bash
npm install
npm run build
node dist/index.js --input-dir ./samples --output-dir ./data/out --logs-dir ./data/_logs
```

## Docker Compose

```bash
docker compose up --build
```

Place input files at `./samples`. Outputs are written to `./data/out` with `.sql` extension.
When using Docker Compose, logs are written to `./data/_logs`.

Skipped rows are always logged to `<logs-dir>/skipped/<file>.skipped.csv` in CSV format:
`nmi,intervalDate,intervalIndex,reason,rawValue`.

Inputs remain in place. Errors are written to `<logs-dir>/error/<file>.error.txt`.
The final summary is written to `<logs-dir>/summary.json`.

## Notes

- Only record types 100/200/300/900 are supported in this version.
- Any unexpected record causes the file to be rejected (no output).
