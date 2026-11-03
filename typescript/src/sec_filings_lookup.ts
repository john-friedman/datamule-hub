import { api_json, get_api_key, log_billing, object, positive } from './core.js';
import { format_accession, normalize_accessions, resolve_cik_filter } from './utils.js';
import type { Billing, Columns, FilingFilters } from './types.js';

const names: Record<string, string> = {
  accession: 'accessionNumber', submission_type: 'submissionType', filing_date: 'filingDate',
  report_date: 'reportDate', detected_time: 'detectedTime', contains_xbrl: 'containsXBRL', document_type: 'documentType',
};
const filter_names = ['cik', 'accession', 'submission_type', 'filing_date', 'report_date', 'detected_time', 'contains_xbrl', 'document_type', 'filename', 'sequence'] as const;

export function build_params(filters: FilingFilters): URLSearchParams {
  const params = new URLSearchParams();
  for (const key of filter_names) {
    const value = filters[key];
    if (value === undefined || value === null) continue;
    const name = names[key] ?? key;
    if (key === 'accession') {
      params.set(name, normalize_accessions(filters.accession)!.map(v => format_accession(v, 'int')).join(','));
    } else if (key === 'filing_date' || key === 'report_date' || key === 'detected_time') {
      if (typeof value === 'object') {
        const range = Array.isArray(value) ? { start: value[0], end: value[1] } : value as { start: unknown; end: unknown };
        if (Array.isArray(value) && value.length !== 2 || range.start === undefined || range.end === undefined) throw new TypeError(`${key} requires a two-item range.`);
        params.set(`${name}_START`, String(range.start)); params.set(`${name}_END`, String(range.end));
      } else params.set(name, String(value));
    } else params.set(name, Array.isArray(value) ? value.join(',') : String(value));
  }
  return params;
}

async function* stream(endpoint: string, options: FilingFilters): AsyncGenerator<Columns> {
  get_api_key(options.api_key);
  const page_size = positive(options.page_size ?? 25000, 'page_size');
  let page = positive(options.page ?? 1, 'page');
  const cik = await resolve_cik_filter(options.cik, options.ticker, options);
  const params = build_params({ ...options, cik });
  params.set('pageSize', String(page_size));
  const started = Date.now();
  let pages = 0, rows = 0, total_charge = 0;
  let summary: Billing = {};
  console.info(`[datamulehub] SEC filings lookup started: endpoint=${endpoint} page=${page} page_size=${page_size}`);
  while (true) {
    options.signal?.throwIfAborted();
    params.set('page', String(page));
    const payload = await api_json(`/v3/sec-filings-lookup/${endpoint}?${params}`, options);
    const data = object(payload.data ?? {});
    let length: number | undefined;
    for (const [column, values] of Object.entries(data)) {
      if (!Array.isArray(values)) throw new Error(`Expected column array: ${column}`);
      if (length !== undefined && values.length !== length) throw new Error('Lookup returned mismatched column lengths.');
      length = values.length;
    }
    const metadata = object(payload.metadata ?? {});
    const pagination = object(metadata.pagination ?? {});
    const billing = object(metadata.billing ?? {}) as Billing;
    pages++; rows += length ?? 0;
    if (typeof billing.total_charge === 'number') total_charge += billing.total_charge;
    summary = { ...summary, ...billing, total_charge };
    log_billing(`SEC filings lookup page fetched: endpoint=${endpoint} page=${page} rows=${length ?? 0} has_more=${Boolean(pagination.hasMore)} elapsed=${((Date.now() - started) / 1000).toFixed(1)}s`, billing);
    yield data as Columns;
    if (options.page !== undefined || !pagination.hasMore) {
      log_billing(`SEC filings lookup complete: endpoint=${endpoint} pages=${pages} rows=${rows} elapsed=${((Date.now() - started) / 1000).toFixed(1)}s`, summary);
      return;
    }
    if (!length) throw new Error('Lookup returned an empty page with hasMore=true.');
    page++;
  }
}

async function lookup(source: AsyncIterable<Columns>): Promise<Columns> {
  const result: Columns = {};
  for await (const page of source) {
    for (const [key, values] of Object.entries(page)) {
      const column = result[key] ??= [];
      // Avoid spread argument limits on large pages.
      for (const value of values) column.push(value);
    }
  }
  return result;
}

export function stream_sgml(options: FilingFilters = {}): AsyncGenerator<Columns> { return stream('sgml-lookup', options); }
export function stream_tar(options: FilingFilters = {}): AsyncGenerator<Columns> { return stream('tar-lookup', options); }
export function lookup_sgml(options: FilingFilters = {}): Promise<Columns> { return lookup(stream_sgml(options)); }
export function lookup_tar(options: FilingFilters = {}): Promise<Columns> { return lookup(stream_tar(options)); }

export interface Filing { filingDate: string; accession: string }
export async function* filings(options: FilingFilters, xbrl = false): AsyncGenerator<Filing> {
  const accessions = normalize_accessions(options.accession);
  const batches = accessions ? Math.ceil(accessions.length / 200) : 1;
  for (let batch = 0; batch < batches; batch++) {
    const selected = accessions?.slice(batch * 200, batch * 200 + 200);
    const allowed = selected ? new Set(selected) : undefined;
    for await (const page of stream_sgml({ ...options, accession: selected, ...(xbrl ? { contains_xbrl: true } : {}) })) {
      const numbers = page.accession ?? [], dates = page.filingDate ?? [];
      if (numbers.length !== dates.length) throw new Error('Lookup returned mismatched filing columns.');
      // Deduplicate within a page, without retaining the entire result set.
      const seen = new Set<string>();
      for (let i = 0; i < numbers.length; i++) {
        const accession = format_accession(numbers[i] as string);
        const filingDate = String(dates[i]);
        if (allowed && !allowed.has(accession)) throw new Error('Lookup returned an unrequested accession.');
        const identity = `${filingDate}/${accession}`;
        if (!seen.has(identity)) { seen.add(identity); yield { accession, filingDate }; }
      }
    }
  }
}
