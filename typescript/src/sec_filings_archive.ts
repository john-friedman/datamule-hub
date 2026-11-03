import { api_json, ApiError, checked_fetch, is_node, object, parallel, positive, save } from './core.js';
import { decompress_zstd } from './compression.js';
import { ByteReader, prepare_dir, safe_name, sized, spool } from './io.js';
import type { BufferOptions, Spool } from './io.js';
import { filings, stream_tar } from './sec_filings_lookup.js';
import { format_accession } from './utils.js';
import { TarBatches, tar_entries } from './tar.js';
import type { ByteStream, FilingFilters, OutputOptions, RequestOptions } from './types.js';

export const SGML_BASE_URL = 'https://sec-filings-archive.sgml.datamule.xyz';
export const TAR_BASE_URL = 'https://sec-filings-archive.tar.datamule.xyz';

export interface ArchiveOptions extends FilingFilters, OutputOptions, BufferOptions {
  output_dir?: string;
  max_workers?: number;
  /** Accepted for Python compatibility; JS decompression runs on the current thread. */
  decomp_workers?: number;
  decompress?: boolean;
  tar_max_size_mb?: number | null;
  overwrite?: boolean;
  sgml_base_url?: string;
  tar_base_url?: string;
}

interface Job { filingDate: string; accession: string; filename?: string; start?: number; end?: number }
interface Item { name: string; size?: number; body: ByteStream }

export async function fetch_body(url: string, options: RequestOptions, on_response?: (response: Response) => void): Promise<ByteStream> {
  const response = await checked_fetch(url, { headers: { 'Accept-Encoding': 'identity' } }, options);
  on_response?.(response);
  if (!response.body) throw new Error('Empty download response.');
  const size = response.headers.get('content-length');
  // Content-Encoding may be transparently decoded by fetch; its encoded length is then irrelevant.
  return sized(response.body, size !== null && !response.headers.has('content-encoding') ? Number(size) : undefined);
}

async function range(url: string, start: number, end: number, options: RequestOptions): Promise<ByteStream> {
  if (!Number.isSafeInteger(start) || !Number.isSafeInteger(end) || start < 0 || end <= start) throw new RangeError('Invalid TAR byte range.');
  const response = await checked_fetch(url, { headers: { Range: `bytes=${start}-${end - 1}`, 'Accept-Encoding': 'identity' } }, options);
  if (response.status !== 206 || !response.headers.get('content-range')?.startsWith(`bytes ${start}-${end - 1}/`) || !response.body) {
    await response.body?.cancel(); throw new Error('Archive server returned an invalid range response.');
  }
  return sized(response.body, end - start);
}

export async function xbrl_url(job: Job, options: RequestOptions): Promise<string> {
  const path = `sec-filings/xbrl/${job.filingDate}/${job.accession}.json.zst`;
  const payload = await api_json('/v3/get-s3-link', options, { path });
  const url = object(payload.data).download_url;
  if (typeof url !== 'string') throw new Error('Missing XBRL download URL.');
  return url;
}

async function* jobs(mode: string, options: ArchiveOptions): AsyncGenerator<Job> {
  const document_mode = mode === 'tar' && options.document_type !== 'metadata' &&
    (options.document_type !== undefined || options.filename !== undefined || options.sequence !== undefined);
  if (document_mode) {
    for await (const page of stream_tar(options)) {
      const numbers = page.accession ?? [];
      for (let i = 0; i < numbers.length; i++) {
        yield { accession: format_accession(numbers[i] as string), filingDate: String(page.filingDate![i]), filename: String(page.filename![i]), start: Number(page.start![i]), end: Number(page.end![i]) };
      }
    }
  } else yield* filings({ ...options, ...(mode === 'tar' && options.document_type === 'metadata' ? { document_type: undefined } : {}) }, mode === 'xbrl');
}

async function* items(mode: string, job: Job, options: ArchiveOptions): AsyncGenerator<Item> {
  if (!/^\d{4}-\d{2}-\d{2}$/.test(job.filingDate)) throw new Error('Invalid filing date from lookup.');
  const decompress = options.decompress !== false;
  if (mode === 'sgml' || mode === 'xbrl') {
    const suffix = mode === 'sgml' ? 'sgml' : 'json';
    let url: string;
    try { url = mode === 'xbrl' ? await xbrl_url(job, options) : `${options.sgml_base_url ?? SGML_BASE_URL}/${job.filingDate}/${job.accession}.sgml.zst`; }
    catch (error) {
      if (mode === 'xbrl' && error instanceof ApiError && error.status === 404) {
        console.info(`[datamulehub] XBRL object unavailable; skipping: filing_date=${job.filingDate} accession=${job.accession}`);
        return;
      }
      throw error;
    }
    const body = await fetch_body(url, options);
    yield { name: `${job.filingDate}/${job.accession}.${suffix}${decompress ? '' : '.zst'}`, body: decompress ? decompress_zstd(body, mode === 'sgml') : body };
    return;
  }
  const url = `${options.tar_base_url ?? TAR_BASE_URL}/${job.filingDate}/${job.accession}.tar`;
  const prefix = `${job.filingDate}/${job.accession}`;
  if (options.document_type === 'metadata') {
    const reader = new ByteReader(await range(url, 0, 512, options));
    let header: Uint8Array;
    try { header = await reader.read(512); } finally { await reader.close(); }
    const text = new TextDecoder();
    const name = text.decode(header.subarray(0, 100)).replace(/\0.*$/s, '');
    if (name !== 'metadata.json') throw new Error('Expected metadata.json as first TAR entry.');
    const size = parseInt(text.decode(header.subarray(124, 136)).replace(/\0.*$/s, '').trim(), 8);
    if (!Number.isSafeInteger(size) || size <= 0) throw new Error('Invalid metadata size.');
    yield { name: `${prefix}/metadata.json`, size, body: await range(url, 512, 512 + size, options) };
  } else if (job.filename !== undefined) {
    const name = safe_name(job.filename).split('/').at(-1)!;
    const body = await range(url, job.start!, job.end!, options);
    yield { name: `${prefix}/${decompress ? name.replace(/\.zst$/, '') : name.endsWith('.zst') ? name : `${name}.zst`}`, size: decompress ? undefined : job.end! - job.start!, body: decompress ? decompress_zstd(body) : body };
  } else {
    for await (const entry of tar_entries(await fetch_body(url, options))) {
      const name = entry.name.split('/').at(-1)!;
      const decode = decompress && name !== 'metadata.json';
      const output_name = name === 'metadata.json' ? name : decode ? name.replace(/\.zst$/, '') : name.endsWith('.zst') ? name : `${name}.zst`;
      yield { name: `${prefix}/${output_name}`, size: decode ? undefined : entry.size, body: decode ? decompress_zstd(entry.body) : entry.body };
    }
  }
}

async function download(mode: string, options: ArchiveOptions): Promise<string[]> {
  const directory = options.output_dir ?? 'downloads';
  const count = positive(options.max_workers ?? 4, 'max_workers');
  if (options.decomp_workers !== undefined) positive(options.decomp_workers, 'decomp_workers');
  const max_size = options.tar_max_size_mb === undefined ? 512 : options.tar_max_size_mb;
  if (max_size !== null && (!Number.isFinite(max_size) || max_size <= 0)) throw new RangeError('tar_max_size_mb must be positive or null.');
  await prepare_dir(directory, options.overwrite ?? false, options);
  const started = Date.now();
  let processed = 0, last_progress = started;
  const label = mode.toUpperCase();
  console.info(`[datamulehub] ${label} archive download started: output_dir=${directory}`);
  const progress = () => {
    processed++;
    const now = Date.now();
    if (now - last_progress >= 1000) {
      console.info(`[datamulehub] ${label} archive download progress: processed=${processed} elapsed=${((now - started) / 1000).toFixed(1)}s`);
      last_progress = now;
    }
  };
  const complete = (paths: string[]): string[] => {
    console.info(`[datamulehub] ${label} archive download complete: processed=${processed} files=${paths.length} output_dir=${directory} elapsed=${((Date.now() - started) / 1000).toFixed(1)}s`);
    return paths;
  };
  if (max_size === null) {
    const paths: string[] = [];
    for await (const completed of parallel(jobs(mode, options), count, async job => {
      const result: string[] = [];
      for await (const item of items(mode, job, options)) {
        const path = `${directory}/${safe_name(item.name)}`;
        await save(path, item.body, options); result.push(path);
      }
      return result;
    })) {
      for (const path of completed) paths.push(path);
      progress();
    }
    return complete(paths);
  }
  const batches = new TarBatches(directory, max_size * 1024 * 1024, options);
  try {
    if (is_node()) {
      type Prepared = { name: string; temporary: Spool }[];
      const dispose = async (prepared: Prepared) => { for (const item of prepared) await item.temporary.dispose(); };
      for await (const prepared of parallel(jobs(mode, options), count, async job => {
        const result: Prepared = [];
        try {
          for await (const item of items(mode, job, options)) result.push({ name: item.name, temporary: await spool(item.body, options) });
          return result;
        } catch (error) { await dispose(result); throw error; }
      }, dispose)) {
        for (const item of prepared) await batches.append(item.name, item.temporary.size, item.temporary.open());
        progress();
      }
    } else {
      // Without temporary disk, consume one entry at a time to bound memory.
      for await (const job of jobs(mode, options)) {
        for await (const item of items(mode, job, options)) {
          if (item.size !== undefined) await batches.append(item.name, item.size, item.body);
          else {
            const temporary = await spool(item.body, options);
            try { await batches.append(item.name, temporary.size, temporary.open()); }
            finally { await temporary.dispose(); }
          }
        }
        progress();
      }
    }
    await batches.close();
    return complete(batches.paths);
  } catch (error) {
    console.info(`[datamulehub] ${label} archive download failed: processed=${processed}`);
    await batches.abort(error); throw error;
  }
}

export function download_sgml(options: ArchiveOptions = {}): Promise<string[]> { return download('sgml', options); }
export function download_tar(options: ArchiveOptions = {}): Promise<string[]> {
  const document_type = typeof options.document_type === 'string' && options.document_type.toLowerCase() === 'metadata' ? 'metadata' : options.document_type;
  return download('tar', { ...options, document_type });
}
export function download_xbrl(options: ArchiveOptions = {}): Promise<string[]> { return download('xbrl', options); }
