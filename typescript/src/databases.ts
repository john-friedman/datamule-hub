import { api_json, api_response, billing_result, byte_stream, chunks, log_billing, object, positive, require_output, save } from './core.js';
import { parquet_rows, parquet_source, parquet_stream } from './parquet.js';
import type { ParquetOptions } from './parquet.js';
import { spool } from './io.js';
import { tar_entries } from './tar.js';
import { format_accession } from './utils.js';
import type { Billing, ByteStream, OutputOptions, RequestOptions, Row } from './types.js';
import type { SchemaElement } from 'hyparquet';

export interface QueryOptions extends RequestOptions, OutputOptions, ParquetOptions {
  from_storage?: 'both' | 'daily' | 'delta';
  output_dir?: string;
  wait_seconds?: number | null;
  chunk_size?: number;
}
export interface QueryResult {
  output_dir: string;
  files: string[];
  query_id: string | null;
  cost: number;
  remaining_balance: number | null;
  remaining_api_key_credits: number | null;
  billing: Billing;
}

const FIELDS = ['accession', 'ciks', 'submission_type', 'detected_time', 'detection_method'];
function is_fastest(sql: string): boolean { return /\bfrom\s+["`]?fastest_sec_filings_metadata["`]?(?![\w])/i.test(sql); }

function parse_fastest(sql: string): { columns: string[]; params: URLSearchParams } {
  const match = /^\s*select\s+(.+?)\s+from\s+["`]?fastest_sec_filings_metadata["`]?(?:\s+where\s+(.*?))?(?:\s+order\s+by\s+([a-z_]+)(?:\s+(asc|desc))?)?(?:\s+limit\s+(\d+))?\s*;?\s*$/is.exec(sql);
  if (!match) throw new TypeError('Unsupported fastest_sec_filings_metadata SQL. Use SELECT, WHERE joined by AND, ORDER BY detected_time, and LIMIT.');
  const columns = match[1]!.trim() === '*' ? FIELDS : match[1]!.split(',').map(c => c.trim().replace(/^["`]|["`]$/g, '').toLowerCase());
  if (columns.some(c => !FIELDS.includes(c)) || new Set(columns).size !== columns.length) throw new TypeError('Invalid or duplicate SELECT columns.');
  const params = new URLSearchParams();
  const set = (name: string, value: string | number) => { if (params.has(name)) throw new TypeError(`Duplicate ${name} filter.`); params.set(name, String(value)); };
  if (match[5]) set('limit', positive(Number(match[5]), 'LIMIT'));
  if (match[3]) {
    if (match[3].toLowerCase() !== 'detected_time') throw new TypeError('Only ORDER BY detected_time is supported.');
    set('order', match[4]?.toLowerCase() ?? 'asc');
  }
  if (match[2]) for (const condition of match[2].split(/\s+and\s+/i)) {
    const parsed = /^(accession|cik|submission_type|detection_method|detected_time)\s*(=|>=|<=|>|<)\s*(.+?)$/i.exec(condition.trim());
    if (!parsed) throw new TypeError(`Unsupported WHERE condition: ${condition}`);
    const field = parsed[1]!.toLowerCase(), op = parsed[2]!, raw = parsed[3]!.trim();
    let value: string;
    if (/^\d+$/.test(raw)) value = raw;
    else if (/^'(?:[^']|'')*'$/.test(raw)) value = raw.slice(1, -1).replaceAll("''", "'");
    else if (/^"(?:[^"]|"")*"$/.test(raw)) value = raw.slice(1, -1).replaceAll('""', '"');
    else throw new TypeError(`Unsupported SQL literal: ${raw}`);
    if (field === 'detected_time') {
      const n = positive(Number(value), field);
      if (op === '>' || op === '>=') set('detected_after', n - (op === '>=' ? 1 : 0));
      else if (op === '<' || op === '<=') set('detected_before', n + (op === '<=' ? 1 : 0));
      else { set('detected_after', n - 1); set('detected_before', n + 1); }
    } else {
      if (op !== '=') throw new TypeError(`${field} only supports =.`);
      if (!value.trim()) throw new TypeError(`${field} cannot be empty.`);
      set(field, field === 'accession' ? format_accession(value, 'int') : field === 'cik' ? positive(Number(value), field) : value);
    }
  }
  const categorical = ['cik', 'submission_type', 'detection_method'].filter(name => params.has(name));
  const time = params.has('detected_after') || params.has('detected_before');
  if (categorical.length > 1 || categorical.length && (time || params.has('order')) || params.has('accession') && (categorical.length || time || params.has('order'))) throw new TypeError('Unsupported combination of fastest metadata filters.');
  return { columns, params };
}

async function fastest(sql: string, options: RequestOptions): Promise<{ rows: Row[]; billing: Billing; columns: string[] }> {
  const { columns, params } = parse_fastest(sql);
  const payload = await api_json(`/v3/fastest-sec-filings-metadata?${params}`, options);
  if (!Array.isArray(payload.data)) throw new Error('Expected fastest metadata rows.');
  const rows = payload.data.map(value => {
    const row = object(value);
    return Object.fromEntries(columns.map(column => [column, row[column] ?? null]));
  });
  return { rows, columns, billing: object(object(payload.metadata ?? {}).billing ?? {}) as Billing };
}

function fastest_schema(columns: string[]): SchemaElement[] {
  const schema: SchemaElement[] = [{ name: 'root', num_children: columns.length }];
  for (const name of columns) {
    if (name === 'ciks') schema.push(
      { name, repetition_type: 'OPTIONAL', converted_type: 'LIST', num_children: 1 },
      { name: 'list', repetition_type: 'REPEATED', num_children: 1 },
      { name: 'element', repetition_type: 'OPTIONAL', type: 'INT64' },
    );
    else schema.push(name === 'detected_time' ? { name, type: 'INT64', repetition_type: 'OPTIONAL' } : { name, type: 'BYTE_ARRAY', converted_type: 'UTF8', repetition_type: 'OPTIONAL' });
  }
  return schema;
}

async function sniff(body: ByteStream): Promise<{ parquet: boolean; body: ByteStream }> {
  const iterator = chunks(body);
  const initial: Uint8Array[] = [];
  let length = 0;
  while (length < 4) {
    const next = await iterator.next();
    if (next.done) throw new Error('Truncated query result.');
    initial.push(next.value); length += next.value.length;
  }
  const magic = new Uint8Array(4);
  let offset = 0;
  for (const piece of initial) {
    const n = Math.min(piece.length, 4 - offset); magic.set(piece.subarray(0, n), offset); offset += n;
    if (offset === 4) break;
  }
  return {
    parquet: new TextDecoder().decode(magic) === 'PAR1',
    body: byte_stream((async function* () { try { yield* initial; yield* iterator; } finally { await iterator.return(undefined); } })()),
  };
}

export async function query(sql: string, options: QueryOptions = {}): Promise<QueryResult> {
  require_output(options);
  if (typeof sql !== 'string' || !sql.trim()) throw new TypeError('sql is required.');
  const from_storage = options.from_storage ?? 'both';
  if (!['both', 'daily', 'delta'].includes(from_storage)) throw new TypeError('from_storage must be both, daily, or delta.');
  const directory = options.output_dir ?? 'datamule-query-result';
  const started = Date.now();
  console.info(`[datamulehub] Query started: engine=${is_fastest(sql) ? 'fastest metadata' : 'athena'}`);
  const files: string[] = [];
  let billing: Billing, query_id: string | null = null;
  const write = async (body: ByteStream) => {
    const path = `${directory}/part-${String(files.length).padStart(5, '0')}.parquet`;
    await save(path, body, options); files.push(path);
    console.info(`[datamulehub] Query result file complete: ${path}`);
  };
  if (is_fastest(sql)) {
    const result = await fastest(sql, options); billing = result.billing;
    // The JSON endpoint uses numbers, while the Parquet writer requires bigint
    // for physical INT64 columns. Convert lazily without copying the full table.
    const rows = (function* (): Generator<Row> {
      for (const row of result.rows) {
        const output: Row = {};
        for (const column of result.columns) {
          const value = row[column];
          if (value == null) output[column] = null;
          else if (column === 'detected_time') output[column] = BigInt(value as string | number);
          else if (column === 'ciks') {
            if (!Array.isArray(value)) throw new Error('Expected a ciks array from fastest metadata.');
            output[column] = value.map(item => item == null ? null : BigInt(item));
          } else output[column] = String(value);
        }
        yield output;
      }
    })();
    await write(parquet_stream(rows, fastest_schema(result.columns), options));
  } else {
    if (options.wait_seconds != null && (!Number.isFinite(options.wait_seconds) || options.wait_seconds < 0)) throw new RangeError('wait_seconds must be nonnegative or null.');
    const body: Record<string, unknown> = { query: sql, from_storage };
    if (options.wait_seconds !== null) body.wait_seconds = options.wait_seconds ?? 600;
    const response = await api_response('/v3/athena/download', options, body, 'POST', 'application/octet-stream');
    if (!response.body) throw new Error('Empty Athena query result.');
    const value = (header: string): number | null => {
      const text = response.headers.get(header);
      return text === null || text === 'unlimited' ? null : Number(text);
    };
    billing = {
      athena_scan: value('x-datamule-athena-scan-charge'), s3_download: value('x-datamule-s3-download-charge'),
      total_charge: value('x-datamule-total-charge') ?? 0,
      remaining_balance: value('x-datamule-remaining-balance'), remaining_api_key_credits: value('x-datamule-remaining-api-key-credits'),
    };
    query_id = response.headers.get('x-datamule-query-id');
    const result = await sniff(response.body);
    if (result.parquet) await write(result.body);
    else for await (const entry of tar_entries(result.body)) await write(entry.body);
  }
  const result = billing_result(directory, 0, [billing]);
  log_billing(`Query complete: files=${files.length} output_dir=${directory} query_id=${query_id ?? 'none'} elapsed=${((Date.now() - started) / 1000).toFixed(1)}s`, billing);
  return { output_dir: directory, files, query_id, cost: result.cost, remaining_balance: result.remaining_balance, remaining_api_key_credits: result.remaining_api_key_credits, billing };
}

export interface ReadQueryOptions extends Omit<QueryOptions, 'output_dir' | 'write_file'> { max_rows?: number }
export async function read_query(sql: string, options: ReadQueryOptions = {}): Promise<Row[]> {
  if (!['both', 'daily', 'delta'].includes(options.from_storage ?? 'both')) throw new TypeError('from_storage must be both, daily, or delta.');
  if (options.max_rows !== undefined) positive(options.max_rows, 'max_rows');
  if (is_fastest(sql)) {
    console.info('[datamulehub] Fastest filings metadata query started');
    const { rows, billing } = await fastest(sql, options);
    log_billing(`Fastest filings metadata query complete: rows=${rows.length}`, billing);
    if (options.max_rows !== undefined && rows.length > options.max_rows) throw new RangeError('Query exceeds max_rows.');
    return rows;
  }
  const rows: Row[] = [];
  await query(sql, { ...options, write_file: async (_path, body) => {
    const temporary = await spool(body, options);
    try {
      for await (const row of parquet_rows(await parquet_source(temporary.file), options)) {
        if (options.max_rows !== undefined && rows.length >= options.max_rows) throw new RangeError('Query exceeds max_rows; use query() for streamed file output.');
        rows.push(row);
      }
    } finally { await temporary.dispose(); }
  } });
  console.info(`[datamulehub] Query rows read: rows=${rows.length}`);
  return rows;
}
