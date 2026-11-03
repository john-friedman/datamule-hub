export type Accession = string | bigint | number;
export type Scalar = string | number | boolean | bigint;
export type Many<T> = T | readonly T[];
export type Range<T> = { start: T; end: T };
export type DateFilter = string | readonly [string, string] | Range<string>;
export type TimeFilter = number | readonly [number, number] | Range<number>;
export type Row = Record<string, unknown>;
export type Columns = Record<string, unknown[]>;
export type ByteStream = ReadableStream<Uint8Array>;
export type WriteFile = (path: string, body: ByteStream) => Promise<void>;

export interface RequestOptions {
  api_key?: string;
  signal?: AbortSignal;
  /** Timeout in seconds. Applied through response-body consumption. */
  timeout?: number;
  /** Override the API origin, for a proxy or self-hosted service. */
  base_url?: string;
  fetch?: typeof globalThis.fetch;
}

export interface OutputOptions {
  /** Stream each output file to storage. Defaults to local files in Node.js. */
  write_file?: WriteFile;
}

export interface FilingFilters extends RequestOptions {
  cik?: Many<string | number>;
  ticker?: Many<string>;
  accession?: Many<Accession>;
  submission_type?: Many<string>;
  filing_date?: DateFilter;
  report_date?: DateFilter;
  detected_time?: TimeFilter;
  contains_xbrl?: boolean;
  document_type?: Many<string>;
  filename?: Many<string>;
  sequence?: Many<number>;
  page?: number;
  page_size?: number;
}

export interface Billing {
  total_charge?: number;
  remaining_balance?: number | null;
  remaining_api_key_credits?: number | null;
  [name: string]: unknown;
}

export interface DownloadLink {
  download_url: string;
  object_key: string;
  size_bytes?: number;
  size_gb?: number;
  expires_at?: string;
  /** S3 last-modified timestamp in Unix seconds. */
  last_modified?: number;
  billing: Billing;
}

export interface FileResult {
  filename: string;
  size_bytes: number;
  size_gb: number;
  cost: number;
  remaining_balance: number | null;
  remaining_api_key_credits: number | null;
  billing: Billing;
}
