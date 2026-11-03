import type { Accession, Many, RequestOptions } from './types.js';
import { checked_fetch, collect } from './core.js';

export function format_accession(accession: Accession, format: 'dash' | 'no-dash' | 'int' = 'no-dash'): string {
  if (typeof accession === 'number' && !Number.isSafeInteger(accession)) throw new RangeError('Unsafe accession number; pass a string or bigint.');
  const value = String(accession).trim();
  if (!/^(?:\d{1,18}|\d{10}-\d{2}-\d{6})$/.test(value)) throw new TypeError(`Invalid accession: ${value}`);
  const digits = value.replaceAll('-', '');
  if (BigInt(digits) <= 0n) throw new RangeError('Accession must be positive.');
  if (format === 'int') return BigInt(digits).toString();
  const padded = digits.padStart(18, '0');
  if (format === 'no-dash') return padded;
  if (format === 'dash') return `${padded.slice(0, 10)}-${padded.slice(10, 12)}-${padded.slice(12)}`;
  throw new TypeError('Unrecognized accession format.');
}

export function detect_accession_type(accession: Accession): 'dash' | 'no-dash' | 'int' {
  const value = String(accession);
  return value.includes('-') ? 'dash' : value.length === 18 ? 'no-dash' : 'int';
}

export function normalize_accessions(accession?: Many<Accession>): string[] | undefined {
  if (accession === undefined) return;
  const values = Array.isArray(accession) ? accession : [accession];
  if (!values.length) throw new RangeError('accession must not be empty.');
  return [...new Set(values.map(value => format_accession(value, 'no-dash')))];
}

// This cache is an optimization only; correctness never depends on isolate persistence.
let ticker_cache: { expires: number; map: Map<string, number[]> } | undefined;
let loading: Promise<Map<string, number[]>> | undefined;
const METADATA_URL = 'https://github.com/john-friedman/datamule-data/raw/master/data/filer_metadata/listed_filer_metadata.csv.gz';

function csv_rows(text: string): string[][] {
  const rows: string[][] = [];
  let row: string[] = [], cell = '', quoted = false;
  for (let i = 0; i < text.length; i++) {
    const c = text[i];
    if (c === '"') {
      if (quoted && text[i + 1] === '"') { cell += '"'; i++; }
      else quoted = !quoted;
    } else if (!quoted && (c === ',' || c === '\n')) {
      row.push(cell.replace(/\r$/, '')); cell = '';
      if (c === '\n') { rows.push(row); row = []; }
    } else cell += c;
  }
  if (quoted) throw new Error('Unterminated CSV field.');
  if (cell || row.length) { row.push(cell.replace(/\r$/, '')); rows.push(row); }
  return rows;
}

async function load_tickers(options: RequestOptions): Promise<Map<string, number[]>> {
  console.info('[datamulehub] Loading ticker metadata');
  const response = await checked_fetch(METADATA_URL, {}, options);
  if (!response.body) throw new Error('Empty ticker metadata response.');
  const data = await collect(response.body.pipeThrough(new DecompressionStream('gzip')), 16 * 1024 * 1024);
  const rows = csv_rows(new TextDecoder().decode(data).replace(/^\uFEFF/, ''));
  const headers = rows.shift() ?? [];
  const cik_index = headers.indexOf('cik'), ticker_index = headers.indexOf('tickers');
  if (cik_index < 0 || ticker_index < 0) throw new Error('Ticker metadata has no cik or tickers column.');
  const map = new Map<string, number[]>();
  for (const row of rows) {
    if (!row[cik_index]) continue;
    const cik = Number(row[cik_index]);
    if (!Number.isSafeInteger(cik) || cik <= 0) throw new Error('Invalid CIK in ticker metadata.');
    // Upstream CSV stores a Python literal list of exchange symbols, not executable expressions.
    const literal = row[ticker_index] ?? '';
    if (!/^\[\s*(?:(?:'[^'\\]*'|"[^"\\]*")\s*,?\s*)*\]$/.test(literal)) throw new Error('Invalid ticker list.');
    for (const match of literal.matchAll(/'([^']*)'|"([^"]*)"/g)) {
      const symbol = (match[1] ?? match[2] ?? '').trim().toUpperCase();
      if (!symbol) continue;
      const matches = map.get(symbol) ?? [];
      if (!matches.includes(cik)) matches.push(cik);
      map.set(symbol, matches);
    }
  }
  if (!map.size) throw new Error('Ticker metadata is empty.');
  ticker_cache = { map, expires: Date.now() + 86400000 };
  console.info(`[datamulehub] Ticker metadata loaded: symbols=${map.size}`);
  return map;
}

export async function resolve_cik_filter(cik: Many<string | number> | undefined, ticker: Many<string> | undefined, options: RequestOptions = {}): Promise<Many<string | number> | undefined> {
  if (ticker === undefined) return cik;
  if (cik !== undefined) throw new TypeError('Provide either cik or ticker, not both.');
  const symbols = typeof ticker === 'string' ? [ticker] : ticker;
  if (!symbols.length || symbols.some(s => typeof s !== 'string' || !s.trim())) throw new TypeError('ticker must contain non-empty strings.');
  let map = ticker_cache && ticker_cache.expires > Date.now() ? ticker_cache.map : undefined;
  if (!map) {
    // Requests with a caller signal/fetch are not shared across unrelated callers.
    if (options.signal || options.fetch) map = await load_tickers(options);
    else {
      loading ??= load_tickers(options).finally(() => { loading = undefined; });
      try { map = await loading; }
      catch (error) {
        if (!ticker_cache) throw error;
        console.info('[datamulehub] Unable to refresh ticker metadata; using cached copy');
        map = ticker_cache.map;
      }
    }
  }
  const ciks = new Set<number>();
  for (const symbol of symbols) {
    const values = map.get(symbol.trim().toUpperCase());
    if (!values) throw new Error(`No CIK found for ticker: ${symbol}`);
    for (const value of values) ciks.add(value);
  }
  return [...ciks];
}

