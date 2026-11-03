import { AwsClient } from 'aws4fetch';
import { bytes, checked_fetch, chunks, is_node, node_fs, node_module, object, parallel, parse_json, request_signal, save, sleep } from './core.js';
import { get_link, resolve_path } from './datasets.js';
import { filings } from './sec_filings_lookup.js';
import { SGML_BASE_URL, TAR_BASE_URL } from './sec_filings_archive.js';
import { sized } from './io.js';
import type { ByteStream, FilingFilters, OutputOptions } from './types.js';

export interface S3Credentials {
  aws_access_key_id: string;
  aws_secret_access_key: string;
  aws_session_token?: string;
  region_name: string;
  bucket_name: string;
  /** Optional S3-compatible origin. */
  endpoint_url?: string;
}
export interface GCSCredentials {
  bucket_name: string;
  service_file?: string;
  service_account?: { client_email: string; private_key: string; token_uri?: string };
  access_token?: string | (() => Promise<string>);
}
export interface TransferOptions extends FilingFilters, OutputOptions {
  max_workers?: number;
  retry_errors?: number;
  prefix?: string;
  errors_json_filename?: string | null;
  chunk_size_mb?: number;
  max_buffer_bytes?: number;
  on_progress?: (progress: { completed: number; failed: number; size_bytes: number }) => void;
}
export interface TransferFailure { key: string; error: string }
export interface TransferResult { total: number; successful: number; size_bytes: number; failed: TransferFailure[] }

interface TransferItem { key: string; url?: string; path?: string }
type Upload = (key: string, body: ByteStream, size: number | undefined, content_type: string, source_url: string) => Promise<number>;

function escape_xml(value: string): string {
  return value.replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;').replaceAll('"', '&quot;').replaceAll("'", '&apos;');
}
function xml_value(xml: string, tag: string): string | undefined {
  const value = new RegExp(`<${tag}(?:\\s[^>]*)?>([\\s\\S]*?)</${tag}>`).exec(xml)?.[1];
  return value?.replaceAll('&quot;', '"').replaceAll('&apos;', "'").replaceAll('&lt;', '<').replaceAll('&gt;', '>').replaceAll('&amp;', '&');
}

async function* parts(body: ByteStream, size: number): AsyncGenerator<Uint8Array> {
  let buffer = new Uint8Array(size), offset = 0;
  for await (const chunk of chunks(body)) {
    for (let position = 0; position < chunk.length;) {
      const count = Math.min(size - offset, chunk.length - position);
      buffer.set(chunk.subarray(position, position + count), offset);
      offset += count; position += count;
      if (offset === size) { yield buffer; buffer = new Uint8Array(size); offset = 0; }
    }
  }
  if (offset) yield buffer.subarray(0, offset);
}

function part_size(options: TransferOptions, size?: number, s3 = true): number {
  const requested = options.chunk_size_mb ?? 8;
  if (!Number.isFinite(requested) || requested <= 0) throw new RangeError('chunk_size_mb must be positive.');
  const minimum = s3 ? 5 * 1024 * 1024 : 256 * 1024;
  let result = Math.max(minimum, Math.ceil(requested * 1024 * 1024 / 262144) * 262144);
  if (s3 && size !== undefined) result = Math.max(result, Math.ceil(size / 10000 / 262144) * 262144);
  if (result > (options.max_buffer_bytes ?? 32 * 1024 * 1024)) throw new RangeError('Multipart chunk exceeds max_buffer_bytes; raise it or reduce object size.');
  return result;
}

function s3_upload(credentials: S3Credentials, options: TransferOptions): Upload {
  if (!credentials.aws_access_key_id || !credentials.aws_secret_access_key || !credentials.bucket_name || !credentials.region_name) throw new TypeError('S3 credentials require keys, region_name, and bucket_name.');
  const client = new AwsClient({ accessKeyId: credentials.aws_access_key_id, secretAccessKey: credentials.aws_secret_access_key, sessionToken: credentials.aws_session_token, region: credentials.region_name, service: 's3', retries: 0 });
  const endpoint = credentials.endpoint_url ?? `https://s3.${credentials.region_name}.amazonaws.com`;
  return async (key, body, size, content_type, source_url) => {
    const url = `${endpoint.replace(/\/$/, '')}/${encodeURIComponent(credentials.bucket_name)}/${key.split('/').map(encodeURIComponent).join('/')}`;
    const metadata = { 'source-url': source_url, 'transfer-date': new Date().toISOString() };
    const send = async (method: string, suffix = '', payload?: Uint8Array | string, cleanup = false) => {
      const headers: Record<string, string> = { 'Content-Type': content_type };
      if (method === 'PUT' && !suffix || method === 'POST' && suffix === '?uploads=') {
        for (const [name, value] of Object.entries(metadata)) headers[`x-amz-meta-${name}`] = value;
      }
      const request = await client.sign(`${url}${suffix}`, {
        method, headers,
        ...(payload === undefined ? {} : { body: typeof payload === 'string' ? payload : payload as Uint8Array<ArrayBuffer> }),
      });
      return checked_fetch(request.url, { method, headers: request.headers, body: request.body ? payload as BodyInit : undefined }, cleanup ? { ...options, signal: undefined, timeout: 10 } : options);
    };
    let upload_id: string | undefined;
    let total = 0;
    const completed: { number: number; etag: string }[] = [];
    try {
      const chunk_size = part_size(options, size);
      for await (const chunk of parts(body, chunk_size)) {
        if (!upload_id && chunk.length < chunk_size) {
          const response = await send('PUT', '', chunk); await response.body?.cancel();
          return chunk.length;
        }
        if (!upload_id) {
          const response = await send('POST', '?uploads=');
          const xml = await response.text();
          upload_id = xml_value(xml, 'UploadId');
          if (!upload_id) throw new Error('S3 did not return UploadId.');
        }
        const number = completed.length + 1;
        if (number > 10000) throw new Error('S3 multipart upload exceeds 10,000 parts.');
        const response = await send('PUT', `?uploadId=${encodeURIComponent(upload_id)}&partNumber=${number}`, chunk);
        const etag = response.headers.get('etag'); await response.body?.cancel();
        if (!etag) throw new Error('S3 did not return a part ETag.');
        completed.push({ number, etag }); total += chunk.length;
      }
      if (!upload_id) { const response = await send('PUT', '', new Uint8Array(0)); await response.body?.cancel(); return 0; }
      const xml = `<CompleteMultipartUpload>${completed.map(p => `<Part><PartNumber>${p.number}</PartNumber><ETag>${escape_xml(p.etag)}</ETag></Part>`).join('')}</CompleteMultipartUpload>`;
      const response = await send('POST', `?uploadId=${encodeURIComponent(upload_id)}`, xml);
      const result = await response.text();
      if (/<(?:\w+:)?Error[\s>]/.test(result)) throw new Error(`S3 completion failed: ${xml_value(result, 'Message') ?? 'unknown error'}`);
      return total;
    } catch (error) {
      if (upload_id) {
        const response = await send('DELETE', `?uploadId=${encodeURIComponent(upload_id)}`, undefined, true).catch(() => undefined);
        await response?.body?.cancel().catch(() => {});
      }
      if (!body.locked) await body.cancel().catch(() => {});
      throw error;
    }
  };
}

function base64(data: Uint8Array): string {
  let text = '';
  for (const byte of data) text += String.fromCharCode(byte);
  return btoa(text).replaceAll('+', '-').replaceAll('/', '_').replace(/=+$/, '');
}
function gcs_token(credentials: GCSCredentials, options: TransferOptions): () => Promise<string> {
  let cached: { token: string; expires: number } | undefined;
  let account: Record<string, unknown> | undefined = credentials.service_account;
  return async () => {
    if (typeof credentials.access_token === 'function') return credentials.access_token();
    if (credentials.access_token) return credentials.access_token;
    if (cached && cached.expires > Date.now() + 60000) return cached.token;
    if (!account && is_node()) {
      const os = await node_module<{ homedir(): string }>('node:os');
      const path = credentials.service_file ?? process.env.GOOGLE_APPLICATION_CREDENTIALS ?? `${os.homedir()}/.config/gcloud/application_default_credentials.json`;
      try { account = object(parse_json(await (await node_fs()).readFile(path, 'utf8'))); }
      catch (error) { if (credentials.service_file || process.env.GOOGLE_APPLICATION_CREDENTIALS) throw error; }
    }
    if (!account) throw new Error('Provide gcs_credentials.access_token, service_account, or a local service_file/ADC file.');
    let body: URLSearchParams;
    const token_url = String(account.token_uri ?? 'https://oauth2.googleapis.com/token');
    if (account.type === 'authorized_user') {
      body = new URLSearchParams({ grant_type: 'refresh_token', refresh_token: String(account.refresh_token), client_id: String(account.client_id), client_secret: String(account.client_secret) });
    } else {
      const encoder = new TextEncoder();
      const now = Math.floor(Date.now() / 1000);
      const jwt = `${base64(encoder.encode(JSON.stringify({ alg: 'RS256', typ: 'JWT' })))}.${base64(encoder.encode(JSON.stringify({ iss: account.client_email, scope: 'https://www.googleapis.com/auth/devstorage.read_write', aud: token_url, iat: now, exp: now + 3600 })))}`;
      const pem = String(account.private_key).replace(/-----[^-]+-----|\s/g, '');
      const der = Uint8Array.from(atob(pem), c => c.charCodeAt(0));
      const key = await crypto.subtle.importKey('pkcs8', der, { name: 'RSASSA-PKCS1-v1_5', hash: 'SHA-256' }, false, ['sign']);
      const signature = new Uint8Array(await crypto.subtle.sign('RSASSA-PKCS1-v1_5', key, encoder.encode(jwt)));
      body = new URLSearchParams({ grant_type: 'urn:ietf:params:oauth:grant-type:jwt-bearer', assertion: `${jwt}.${base64(signature)}` });
    }
    const response = await checked_fetch(token_url, { method: 'POST', body }, options);
    const data = object(parse_json(await response.text()));
    if (typeof data.access_token !== 'string') throw new Error('GCS token response is missing access_token.');
    cached = { token: data.access_token, expires: Date.now() + Number(data.expires_in ?? 3600) * 1000 };
    return cached.token;
  };
}

function gcs_upload(credentials: GCSCredentials, options: TransferOptions): Upload {
  if (!credentials.bucket_name) throw new TypeError('GCS credentials require bucket_name.');
  const token = gcs_token(credentials, options);
  return async (key, body, _size, content_type, source_url) => {
    const response = await checked_fetch(`https://storage.googleapis.com/upload/storage/v1/b/${encodeURIComponent(credentials.bucket_name)}/o?uploadType=resumable`, {
      method: 'POST', headers: { Authorization: `Bearer ${await token()}`, 'Content-Type': 'application/json', 'X-Upload-Content-Type': content_type },
      body: JSON.stringify({ name: key, contentType: content_type, metadata: { 'source-url': source_url, 'transfer-date': new Date().toISOString() } }),
    }, options);
    const location = response.headers.get('location'); await response.body?.cancel();
    if (!location) throw new Error('GCS did not return an upload session URI.');
    let total = 0;
    const iterator = parts(body, part_size(options, undefined, false));
    try {
      let current = await iterator.next();
      while (!current.done) {
        const next = await iterator.next();
        const end = total + current.value.length;
        const result = await (options.fetch ?? globalThis.fetch)(location, {
          method: 'PUT', redirect: 'manual', signal: request_signal(options), body: current.value as Uint8Array<ArrayBuffer>,
          headers: { 'Content-Type': content_type, 'Content-Range': `bytes ${total}-${end - 1}/${next.done ? end : '*'}` },
        });
        await result.body?.cancel();
        if (next.done ? ![200, 201].includes(result.status) : result.status !== 308 || result.headers.get('range') !== `bytes=0-${end - 1}`) throw new Error(`GCS upload failed or acknowledged an unexpected range (${result.status}).`);
        total = end; current = next;
      }
      if (!total) {
        const result = await checked_fetch(location, { method: 'PUT', body: new Uint8Array(0), headers: { 'Content-Range': 'bytes */0' } }, options);
        await result.body?.cancel();
      }
      return total;
    } catch (error) {
      const cancelled = await (options.fetch ?? globalThis.fetch)(location, { method: 'DELETE', signal: AbortSignal.timeout(10000) }).catch(() => undefined);
      await cancelled?.body?.cancel().catch(() => {});
      throw error;
    } finally { await iterator.return(undefined); }
  };
}

async function* archive_items(archive: string, options: TransferOptions): AsyncGenerator<TransferItem> {
  for await (const filing of filings(options, archive === 'xbrl')) {
    const key = `${filing.filingDate}/${filing.accession}.${archive === 'sgml' ? 'sgml.zst' : archive === 'xbrl' ? 'json.zst' : 'tar'}`;
    yield archive === 'xbrl' ? { key, path: `sec-filings/xbrl/${key}` } : { key, url: `${archive === 'sgml' ? SGML_BASE_URL : TAR_BASE_URL}/${key}` };
  }
}

async function transfer(items: AsyncIterable<TransferItem> | Iterable<TransferItem>, upload: Upload, options: TransferOptions, dataset = false): Promise<TransferResult> {
  const retries = options.retry_errors ?? 3;
  if (!Number.isSafeInteger(retries) || retries < 0) throw new RangeError('retry_errors must be nonnegative.');
  part_size(options); // Validate before obtaining any paid links.
  const started = Date.now();
  let last_progress = started;
  const label = dataset ? 'Dataset transfer' : 'Archive transfer';
  console.info(`[datamulehub] ${label} started: max_workers=${options.max_workers ?? 4}`);
  const result: TransferResult = { total: 0, successful: 0, size_bytes: 0, failed: [] };
  for await (const item of parallel(items, options.max_workers ?? 4, async item => {
    const key = options.prefix ? `${options.prefix.replace(/\/+$/, '')}/${item.key}` : item.key;
    for (let attempt = 0; ; attempt++) {
      options.signal?.throwIfAborted();
      let body: ByteStream | undefined;
      try {
        const url = item.url ?? (await get_link(item.path!, options)).download_url;
        const response = await checked_fetch(url, { headers: { 'Accept-Encoding': 'identity' } }, options);
        if (!response.body) throw new Error('Empty transfer source response.');
        const length = response.headers.get('content-length');
        const size = length === null || response.headers.has('content-encoding') ? undefined : Number(length);
        body = sized(response.body, size);
        return { key, size: await upload(key, body, size, response.headers.get('content-type') ?? 'application/octet-stream', url) };
      } catch (error) {
        if (body && !body.locked) await body.cancel().catch(() => {});
        options.signal?.throwIfAborted();
        if (attempt >= retries) {
          console.info(`[datamulehub] Transfer failed: key=${key} attempts=${attempt + 1}`);
          return { key, error: error instanceof Error ? error.message : String(error) };
        }
        console.info(`[datamulehub] Transfer retry: key=${key} retry=${attempt + 1}/${retries} delay=${2 ** attempt}s`);
        await sleep(2 ** attempt, options.signal);
      }
    }
  })) {
    result.total++;
    if ('error' in item) result.failed.push({ key: item.key, error: item.error! });
    else { result.successful++; result.size_bytes += item.size; }
    const now = Date.now();
    if (now - last_progress >= 1000) {
      console.info(`[datamulehub] ${label} progress: completed=${result.total} successful=${result.successful} failed=${result.failed.length} size_bytes=${result.size_bytes}`);
      last_progress = now;
    }
    options.on_progress?.({ completed: result.total, failed: result.failed.length, size_bytes: result.size_bytes });
  }
  const error_file = options.errors_json_filename === undefined ? dataset ? 'errors_datasets_transfer.json' : 'errors_archive_transfer.json' : options.errors_json_filename;
  if (result.failed.length && error_file && (is_node() || options.write_file)) {
    await save(error_file, bytes(new TextEncoder().encode(JSON.stringify(result.failed, null, 2))), options);
    console.info(`[datamulehub] Transfer errors saved: count=${result.failed.length} filename=${error_file}`);
  }
  console.info(`[datamulehub] ${label} complete: ${result.successful}/${result.total} successful, ${result.failed.length} failed, size_bytes=${result.size_bytes} elapsed=${((Date.now() - started) / 1000).toFixed(1)}s`);
  return result;
}

export interface S3ArchiveOptions extends TransferOptions { s3_credentials: S3Credentials }
export interface GCSArchiveOptions extends TransferOptions { gcs_credentials: GCSCredentials }
function archive_s3(mode: string, credentials: S3Credentials | S3ArchiveOptions, options: TransferOptions): Promise<TransferResult> {
  const config = 's3_credentials' in credentials ? credentials : { ...options, s3_credentials: credentials };
  return transfer(archive_items(mode, config), s3_upload(config.s3_credentials, config), config);
}
function archive_gcs(mode: string, credentials: GCSCredentials | GCSArchiveOptions, options: TransferOptions): Promise<TransferResult> {
  const config = 'gcs_credentials' in credentials ? credentials : { ...options, gcs_credentials: credentials };
  return transfer(archive_items(mode, config), gcs_upload(config.gcs_credentials, config), config);
}
export function transfer_sgml_archive_s3(credentials: S3Credentials | S3ArchiveOptions, options: TransferOptions = {}): Promise<TransferResult> { return archive_s3('sgml', credentials, options); }
export function transfer_tar_archive_s3(credentials: S3Credentials | S3ArchiveOptions, options: TransferOptions = {}): Promise<TransferResult> { return archive_s3('tar', credentials, options); }
export function transfer_xbrl_archive_s3(credentials: S3Credentials | S3ArchiveOptions, options: TransferOptions = {}): Promise<TransferResult> { return archive_s3('xbrl', credentials, options); }
export function transfer_sgml_archive_gcs(credentials: GCSCredentials | GCSArchiveOptions, options: TransferOptions = {}): Promise<TransferResult> { return archive_gcs('sgml', credentials, options); }
export function transfer_tar_archive_gcs(credentials: GCSCredentials | GCSArchiveOptions, options: TransferOptions = {}): Promise<TransferResult> { return archive_gcs('tar', credentials, options); }
export function transfer_xbrl_archive_gcs(credentials: GCSCredentials | GCSArchiveOptions, options: TransferOptions = {}): Promise<TransferResult> { return archive_gcs('xbrl', credentials, options); }

export interface S3DatasetOptions extends S3ArchiveOptions { datasets: readonly string[] }
export interface GCSDatasetOptions extends GCSArchiveOptions { datasets: readonly string[] }
function dataset_items(datasets: readonly string[]): TransferItem[] {
  if (!Array.isArray(datasets) || !datasets.length) throw new TypeError('datasets must be a non-empty array.');
  return datasets.map(dataset => { const path = resolve_path(dataset); return { key: path, path }; });
}
export function transfer_dataset_s3(options: S3DatasetOptions): Promise<TransferResult>;
export function transfer_dataset_s3(datasets: readonly string[], credentials: S3Credentials, options?: TransferOptions): Promise<TransferResult>;
export function transfer_dataset_s3(datasets: readonly string[] | S3DatasetOptions, credentials?: S3Credentials, options: TransferOptions = {}): Promise<TransferResult> {
  const config = Array.isArray(datasets) ? { ...options, datasets, s3_credentials: credentials! } : datasets as S3DatasetOptions;
  return transfer(dataset_items(config.datasets), s3_upload(config.s3_credentials, config), config, true);
}
export function transfer_dataset_gcs(options: GCSDatasetOptions): Promise<TransferResult>;
export function transfer_dataset_gcs(datasets: readonly string[], credentials: GCSCredentials, options?: TransferOptions): Promise<TransferResult>;
export function transfer_dataset_gcs(datasets: readonly string[] | GCSDatasetOptions, credentials?: GCSCredentials, options: TransferOptions = {}): Promise<TransferResult> {
  const config = Array.isArray(datasets) ? { ...options, datasets, gcs_credentials: credentials! } : datasets as GCSDatasetOptions;
  return transfer(dataset_items(config.datasets), gcs_upload(config.gcs_credentials, config), config, true);
}
