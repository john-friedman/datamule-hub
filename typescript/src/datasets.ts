import { api_json, ApiError, billing_result, checked_fetch, log_billing, object, parallel, require_output, save, sleep } from './core.js';
import { DOCUMENT_TYPES_BY_TABLE } from './document_types.js';
import { fetch_body } from './sec_filings_archive.js';
import { filings } from './sec_filings_lookup.js';
import { format_accession, normalize_accessions } from './utils.js';
import { merge_schema, parquet_rows, parquet_source, parquet_stream, remote_file } from './parquet.js';
import type { ParquetOptions, ParquetSource } from './parquet.js';
import type { Billing, DownloadLink, FileResult, OutputOptions, RequestOptions, Row, Accession } from './types.js';

export const METADATA_TABLES = ['filings', 'entities', 'former_companies', 'series', 'classes'].map(name => `filing_metadata_${name}`);
const paths: Record<string, string> = {
  filer_metadata: 'metadata/filer_metadata/data.parquet', filer_names: 'metadata/filer_names/data.parquet',
  simple_xbrl: 'datasets/simple_xbrl/data.parquet', simple_xbrl_table: 'datasets/simple_xbrl/data.parquet',
  submissions_metadata: 'metadata/submissions_metadata/data.parquet', sec_master_submissions: 'metadata/submissions_metadata/data.parquet', sec_master_submissions_table: 'metadata/submissions_metadata/data.parquet',
};
for (const name of ['sec_submission_details', 'sec_accession_cik', 'sec_documents']) {
  paths[name] = paths[`${name}_table`] = `metadata/${name}_table/data.parquet`;
}
for (const name of METADATA_TABLES) paths[name] = `datasets/metadata/${name}/data.parquet`;

export function resolve_path(dataset: string): string {
  const value = String(dataset ?? '').trim().replace(/^\/+|\/+$/g, '');
  if (!value) throw new TypeError('dataset is required.');
  const mapped = paths[value.toLowerCase().replaceAll('-', '_')];
  if (mapped) return mapped;
  if (/^(datasets|metadata|monitor-dumps|sec-filings)\//.test(value)) return value;
  if (/^(xml2tables|xmltables)\//.test(value)) return `datasets/xml2tables/${value.split('/').slice(1).join('/')}/data.parquet`;
  if (value.startsWith('simple_xbrl/')) return `datasets/${value}/data.parquet`;
  return `datasets/xml2tables/${value}/data.parquet`;
}

export async function get_link(dataset: string, options: RequestOptions = {}): Promise<DownloadLink> {
  const path = resolve_path(dataset);
  const payload = await api_json('/v3/get-s3-link', options, { path });
  const data = object(payload.data);
  if (typeof data.download_url !== 'string') throw new Error('Missing dataset download URL.');
  return { ...data, download_url: data.download_url, object_key: String(data.object_key ?? path), billing: object(object(payload.metadata ?? {}).billing ?? {}) as Billing } as DownloadLink;
}

export interface DownloadOptions extends RequestOptions, OutputOptions, ParquetOptions {
  filename?: string;
  /** Compatibility option; fetch controls network chunking. */
  chunk_size?: number;
  from_storage?: 'both' | 'daily' | 'delta';
  accession?: readonly Accession[];
  max_workers?: number;
}
export interface DatasetResult extends FileResult {
  object_key: string | null;
  object_keys?: string[];
  accession?: string[];
  missing_accessions?: string[];
}

function table_name(path: string): string | undefined {
  const match = /^datasets\/(xml2tables|metadata)\/([^/]+)\/data\.parquet$/.exec(path);
  return match && (match[1] === 'xml2tables' || METADATA_TABLES.includes(match[2]!)) ? match[2] : undefined;
}

function source_key(table: string | undefined, date: string, accession: Accession): string {
  const number = format_accession(accession);
  return table ? `sec-filings/${METADATA_TABLES.includes(table) ? 'metadata' : 'xml2tables'}/${date}/${table}/${number}.parquet` : `sec-filings/simple_xbrl/${date}/${number}.parquet`;
}

function eastern_parts(value: Date): { date: string; hour: number } {
  const parts = new Intl.DateTimeFormat('en-US', { timeZone: 'America/New_York', year: 'numeric', month: '2-digit', day: '2-digit', hour: '2-digit', hourCycle: 'h23' }).formatToParts(value);
  const get = (type: string) => parts.find(p => p.type === type)!.value;
  return { date: `${get('year')}-${get('month')}-${get('day')}`, hour: Number(get('hour')) };
}

function last_delta_date(now: Date): Date {
  const current = eastern_parts(now);
  const last = new Date(`${current.date}T00:00:00Z`);
  if (current.hour < 6) last.setUTCDate(last.getUTCDate() - 1);
  return last;
}

function delta_dates(last_modified: number, now: Date): string[] {
  // Each overnight snapshot contains all filing days before its Eastern publication date.
  const first = eastern_parts(new Date(last_modified * 1000)).date;
  const last = last_delta_date(now);
  const dates: string[] = [];
  for (const date = new Date(`${first}T00:00:00Z`); date <= last; date.setUTCDate(date.getUTCDate() + 1)) {
    dates.push(date.toISOString().slice(0, 10));
  }
  return dates;
}

async function candidates(table: string | undefined, dates: string[], accessions: string[] | undefined, options: DownloadOptions): Promise<Set<string>> {
  const filters = table ? METADATA_TABLES.includes(table) ? {} : { document_type: DOCUMENT_TYPES_BY_TABLE[table] } : { contains_xbrl: true };
  if (table && !METADATA_TABLES.includes(table) && !DOCUMENT_TYPES_BY_TABLE[table]) throw new Error(`Unknown XML table: ${table}`);
  const result = new Set<string>();
  for (const date of accessions ? [undefined] : dates) {
    for await (const filing of filings({ ...options, ...filters, accession: accessions, filing_date: date }, !table)) {
      result.add(source_key(table, filing.filingDate, filing.accession));
    }
  }
  return result;
}

async function delta_link(path: string, options: DownloadOptions): Promise<DownloadLink | undefined> {
  for (let attempt = 0; attempt < 2; attempt++) {
    try { return await get_link(path, { ...options, timeout: options.timeout ?? 30 }); }
    catch (error) {
      if (!(error instanceof ApiError) || error.status !== 404) throw error;
      if (!attempt) {
        console.info(`[datamulehub] Dataset object unavailable; retrying in 3s: ${path}`);
        await sleep(3, options.signal);
      }
    }
  }
  console.info(`[datamulehub] Dataset object unavailable; skipping: ${path}`);
}

async function as_source(link: DownloadLink, options: DownloadOptions, key?: string): Promise<ParquetSource> {
  return parquet_source(await remote_file(link.download_url, link.size_bytes, options), key);
}

export async function download(dataset: string, options: DownloadOptions = {}): Promise<DatasetResult> {
  require_output(options);
  const mode = options.from_storage ?? 'both';
  if (!['both', 'daily', 'delta'].includes(mode)) throw new TypeError('from_storage must be both, daily, or delta.');
  const path = resolve_path(dataset), table = table_name(path);
  const has_delta = table !== undefined || path === paths.simple_xbrl;
  const accessions = normalize_accessions(options.accession);
  if (accessions && (!has_delta || mode === 'daily')) throw new TypeError('accession requires an XML, metadata, or simple_xbrl dataset and from_storage=both or delta.');
  if (mode === 'delta' && !has_delta) throw new TypeError('This dataset does not support delta storage.');
  const started = Date.now();
  console.info(`[datamulehub] Dataset download started: dataset=${dataset} from_storage=${mode}`);
  const complete = (result: DatasetResult): DatasetResult => {
    log_billing(`Dataset download complete: filename=${result.filename} size_bytes=${result.size_bytes} elapsed=${((Date.now() - started) / 1000).toFixed(1)}s`, result.billing);
    if (result.missing_accessions?.length) console.info(`[datamulehub] Dataset download missing accessions: count=${result.missing_accessions.length}`);
    return result;
  };
  let filename = options.filename ?? (accessions ? `${table ?? 'simple_xbrl'}.parquet` : path.split('/').at(-1)!);
  const bills: Billing[] = [];
  let snapshot: ParquetSource | undefined;
  const daily = mode !== 'delta' && !accessions ? await get_link(path, options) : undefined;
  if (daily) bills.push(daily.billing);
  let dates: string[] = [];
  if (has_delta && mode !== 'daily' && !accessions) {
    const now = new Date();
    if (daily) {
      if (typeof daily.last_modified !== 'number' || !Number.isFinite(daily.last_modified)) throw new Error('Daily download link is missing a valid last_modified timestamp.');
      dates = delta_dates(daily.last_modified, now);
    } else dates = [last_delta_date(now).toISOString().slice(0, 10)];
  }
  if (daily && !dates.length) {
    const body = await fetch_body(daily.download_url, options, response => {
      if (options.filename !== undefined) return;
      const value = response.headers.get('content-disposition') ?? '';
      const encoded = /filename\*=UTF-8''([^;]+)/i.exec(value)?.[1];
      const plain = /filename\s*=\s*(?:"([^"]+)"|([^;]+))/i.exec(value);
      const name = encoded ? decodeURIComponent(encoded) : plain?.[1] ?? plain?.[2];
      if (name) filename = name.trim().split(/[\\/]/).at(-1) || filename;
    });
    const size = await save(filename, body, options);
    return complete({ ...billing_result(filename, size, bills), object_key: daily.object_key });
  }
  const keys = await candidates(table, dates, accessions, options);
  if (daily) snapshot = await as_source(daily, options);
  console.info(`[datamulehub] Dataset delta lookup complete: candidates=${keys.size}`);
  const sources: ParquetSource[] = snapshot ? [snapshot] : [];
  const object_keys: string[] = [];
  // Only footers and links are retained here. Payloads are decoded sequentially below.
  for await (const result of parallel([...keys].sort(), options.max_workers ?? 4, async key => {
    const link = await delta_link(key, options);
    return link ? { source: await as_source(link, options, key), link, key } : undefined;
  })) {
    if (result) { sources.push(result.source); bills.push(result.link.billing); object_keys.push(result.key); }
  }
  console.info(`[datamulehub] Dataset delta files available: files=${object_keys.length} missing=${keys.size - object_keys.length}`);
  if (daily && object_keys.length === 0) {
    const size = await save(filename, await fetch_body(daily.download_url, options), options);
    return complete({ ...billing_result(filename, size, bills), object_key: daily.object_key });
  }
  console.info(`[datamulehub] Dataset merge started: sources=${sources.length} filename=${filename}`);
  const schema = merge_schema(sources, true, !table && object_keys.length > 0);
  if (!sources.length && !table) {
    const text_columns = ['taxonomy', 'name', 'value', 'period_start_date', 'period_end_date', 'members', 'filingDate', '_sourceKey'];
    schema.splice(0, schema.length,
      { name: 'root', num_children: 2 + text_columns.length },
      ...['accessionNumber', 'context_id'].map(name => ({ name, type: 'INT64' as const, converted_type: 'UINT_64' as const, repetition_type: 'OPTIONAL' as const })),
      ...text_columns.map(name => ({ name, type: 'BYTE_ARRAY' as const, converted_type: 'UTF8' as const, repetition_type: 'OPTIONAL' as const })),
    );
  }
  const rows = (async function* (): AsyncGenerator<Row> {
    for (const source of sources) for await (const row of parquet_rows(source, options, undefined, true)) {
      if (source.key) {
        row._sourceKey = source.key;
        if (!table && row.filingDate === undefined) row.filingDate = source.key.split('/')[2];
      }
      yield row;
    }
  })();
  const size = await save(filename, parquet_stream(rows, schema, options), options);
  const downloaded = new Set(object_keys.map(key => key.split('/').at(-1)!.replace(/\.parquet$/, '')));
  return complete({
    ...billing_result(filename, size, bills), object_key: accessions ? null : path,
    ...(accessions ? { object_keys, accession: accessions, missing_accessions: accessions.filter(a => !downloaded.has(a)) } : {}),
  });
}
