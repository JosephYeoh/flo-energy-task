/**
 * Build a single VALUES row for the meter_readings insert.
 */
export function buildRow(nmi: string, timestamp: string, consumption: string): string {
  return `('${nmi}','${timestamp}',${consumption})`;
}

/**
 * Build a multi-row INSERT with ON CONFLICT upsert.
 */
export function buildInsert(rows: string[]): string {
  const joined = rows.join(",\n  ");
  return `INSERT INTO meter_readings ("nmi","timestamp","consumption") VALUES\n  ${joined}\nON CONFLICT ("nmi","timestamp") DO UPDATE SET "consumption" = EXCLUDED."consumption";\n`;
}
