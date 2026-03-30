export type RecordType = "100" | "200" | "300" | "900";

export interface BaseRecord {
  type: RecordType;
  raw: string[];
}

export interface Record100 extends BaseRecord {
  type: "100";
}

export interface Record200 extends BaseRecord {
  type: "200";
  nmi: string;
  intervalLength: number;
  intervalLengthRaw: string;
}

export interface Record300 extends BaseRecord {
  type: "300";
  intervalDate: string;
  intervalValues: string[];
}

export interface Record900 extends BaseRecord {
  type: "900";
}

export type NEM12Record = Record100 | Record200 | Record300 | Record900;
