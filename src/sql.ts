export function escapeSqlString(value: string): string {
  return value.replace(/'/g, "''");
}

export function buildRow(nmi: string, timestamp: string, consumption: string): string {
  return `('${escapeSqlString(nmi)}','${timestamp}',${consumption})`;
}

export function buildInsert(rows: string[]): string {
  return `INSERT INTO meter_readings ("nmi","timestamp","consumption") VALUES ${rows.join(",")} ON CONFLICT ("nmi","timestamp") DO UPDATE SET "consumption" = EXCLUDED."consumption";\n`;
}
