import { parse } from 'lossless-json';
import type { Billing, ByteStream, OutputOptions, RequestOptions, FileResult } from './types.js';

export const API_URL = 'https://api.datamule.xyz';

/** Log only billing fields, never request headers, credentials, or signed URLs. */
export function log_billing(message: string, billing: Billing): void {
  const parts = [`[datamulehub] ${message}`];
  if (typeof billing.total_charge === 'number') parts.push(`Cost: $${billing.total_charge.toFixed(4)}`);
  if (typeof billing.remaining_balance === 'number') parts.push(`Remaining balance: $${billing.remaining_balance.toFixed(2)}`);
  if (billing.remaining_api_key_credits === null) parts.push('API key credits left: Unlimited');
  else if (typeof billing.remaining_api_key_credits === 'number') parts.push(`API key credits left: $${billing.remaining_api_key_credits.toFixed(4)}`);
  console.info(parts.join(' | '));
}

export class ApiError extends Error {
  constructor(public readonly status: number, message: string) {
    super(`API request failed (${status}): ${message}`);
    this.name = 'ApiError';
  }
}

export function get_api_key(api_key?: string): string {
  const key = api_key ?? (typeof process !== 'undefined' ? process.env.DATAMULE_API_KEY : undefined);
  if (!key) throw new Error('Provide api_key or set DATAMULE_API_KEY.');
  return key;
}

/** Preserve unsafe JSON integer tokens as strings, before they can be rounded. */
export function parse_json(text: string): unknown {
  return parse(text, undefined, (token: string) => {
    const number = Number(token);
    return /^-?\d+$/.test(token) && !Number.isSafeInteger(number) ? token : number;
  });
}

export function object(value: unknown): Record<string, unknown> {
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error('Expected an object response.');
  return value as Record<string, unknown>;
}

export function positive(value: number, name: string): number {
  if (!Number.isSafeInteger(value) || value <= 0) throw new RangeError(`${name} must be a positive safe integer.`);
  return value;
}

export function request_signal(options: RequestOptions): AbortSignal | undefined {
  if (options.timeout === undefined) return options.signal;
  if (!Number.isFinite(options.timeout) || options.timeout <= 0) throw new RangeError('timeout must be positive.');
  const timeout = AbortSignal.timeout(Math.ceil(options.timeout * 1000));
  return options.signal ? AbortSignal.any([options.signal, timeout]) : timeout;
}

export async function checked_fetch(url: string, init: RequestInit = {}, options: RequestOptions = {}): Promise<Response> {
  const response = await (options.fetch ?? globalThis.fetch)(url, {
    ...init, signal: request_signal(options) ?? init.signal,
  });
  if (!response.ok) {
    // Error bodies are normally tiny; cap them so an upstream HTML error cannot exhaust memory.
    const body = response.body ? await collect(response.body, 64 * 1024, true) : new Uint8Array();
    const text = new TextDecoder().decode(body);
    let message = text;
    try { message = String(object(parse_json(text)).error ?? text); } catch { /* Use the text body. */ }
    throw new ApiError(response.status, message || response.statusText);
  }
  return response;
}

export async function api_response(path: string, options: RequestOptions = {}, body?: unknown, method?: string, accept = 'application/json'): Promise<Response> {
  return checked_fetch(`${options.base_url ?? API_URL}${path}`, {
    method: method ?? (body === undefined ? 'GET' : 'POST'),
    headers: {
      Authorization: `Bearer ${get_api_key(options.api_key)}`,
      Accept: accept,
      ...(body === undefined ? {} : { 'Content-Type': 'application/json' }),
    },
    ...(body === undefined ? {} : { body: JSON.stringify(body) }),
  }, options);
}

export async function api_json(path: string, options: RequestOptions = {}, body?: unknown, method?: string): Promise<Record<string, unknown>> {
  const response = await api_response(path, options, body, method);
  const payload = object(parse_json(await response.text()));
  if (payload.success === false) throw new ApiError(response.status, String(payload.error ?? 'Unsuccessful response.'));
  return payload;
}

export async function* chunks(stream: ByteStream): AsyncGenerator<Uint8Array> {
  const reader = stream.getReader();
  let done = false;
  try {
    while (true) {
      const result = await reader.read();
      if (result.done) { done = true; return; }
      if (result.value.length) yield result.value;
    }
  } finally {
    if (!done) await reader.cancel().catch(() => {});
    reader.releaseLock();
  }
}

export function byte_stream(source: AsyncIterable<Uint8Array>): ByteStream {
  const iterator = source[Symbol.asyncIterator]();
  return new ReadableStream({
    async pull(controller) {
      try {
        const next = await iterator.next();
        if (next.done) controller.close(); else controller.enqueue(next.value);
      } catch (error) { controller.error(error); }
    },
    async cancel() { await iterator.return?.(); },
  }, { highWaterMark: 0 });
}

export function bytes(data: Uint8Array): ByteStream {
  return byte_stream((async function* () { yield data; })());
}

export async function collect(stream: ByteStream, limit = 64 * 1024 * 1024, truncate = false): Promise<Uint8Array> {
  const parts: Uint8Array[] = [];
  let length = 0;
  for await (const chunk of chunks(stream)) {
    if (length + chunk.length > limit) {
      if (!truncate) throw new RangeError(`Buffer exceeds ${limit} bytes. Use streaming output or increase max_buffer_bytes.`);
      parts.push(chunk.subarray(0, limit - length)); length = limit; break;
    }
    parts.push(chunk); length += chunk.length;
  }
  const result = new Uint8Array(length);
  let offset = 0;
  for (const part of parts) { result.set(part, offset); offset += part.length; }
  return result;
}

export async function sleep(seconds: number, signal?: AbortSignal): Promise<void> {
  signal?.throwIfAborted();
  await new Promise<void>((resolve, reject) => {
    const finish = () => { signal?.removeEventListener('abort', abort); resolve(); };
    const timer = setTimeout(finish, seconds * 1000);
    const abort = () => { clearTimeout(timer); signal?.removeEventListener('abort', abort); reject(signal?.reason); };
    signal?.addEventListener('abort', abort, { once: true });
  });
}

/** Bound both running operations and queued results; drain peers on failure. */
export async function* parallel<T, R>(source: AsyncIterable<T> | Iterable<T>, count: number, run: (item: T) => Promise<R>, dispose?: (item: R) => Promise<void>): AsyncGenerator<R> {
  positive(count, 'max_workers');
  const iterator = (async function* () { yield* source; })();
  type Task = Promise<{ task: Task; result?: R; error?: unknown }>;
  const pending = new Set<Task>();
  async function add(): Promise<boolean> {
    const next = await iterator.next();
    if (next.done) return false;
    const task: Task = Promise.resolve().then(() => run(next.value)).then(
      result => ({ task, result }), error => ({ task, error }),
    );
    pending.add(task); return true;
  }
  try {
    for (let i = 0; i < count; i++) if (!await add()) break;
    while (pending.size) {
      const settled = await Promise.race(pending);
      pending.delete(settled.task);
      if ('error' in settled) throw settled.error;
      try { yield settled.result as R; }
      finally { await dispose?.(settled.result as R); }
      await add();
    }
  } finally {
    await iterator.return(undefined);
    for (const settled of await Promise.all(pending)) if (!('error' in settled)) await dispose?.(settled.result as R);
  }
}

export function is_node(): boolean {
  return typeof process !== 'undefined' && !!process.versions?.node;
}

// Load optional runtime facilities only when needed.
export function node_module<T>(name: string): Promise<T> { return import(name) as Promise<T>; }
interface FileHandle {
  write(data: Uint8Array, offset: number, length: number): Promise<{ bytesWritten: number }>;
  read(data: Uint8Array, offset: number, length: number, position: number): Promise<{ bytesRead: number }>;
  close(): Promise<void>;
}
interface FileSystem {
  open(path: string, flags: string): Promise<FileHandle>;
  mkdir(path: string, options: { recursive: true }): Promise<unknown>;
  mkdtemp(prefix: string): Promise<string>;
  readFile(path: string, encoding: 'utf8'): Promise<string>;
  readdir(path: string): Promise<string[]>;
  rename(from: string, to: string): Promise<void>;
  unlink(path: string): Promise<void>;
  rmdir(path: string): Promise<void>;
}
export async function node_fs(): Promise<FileSystem> {
  return node_module<FileSystem>('node:fs/promises');
}

export function require_output(options: OutputOptions): void {
  if (!options.write_file && !is_node()) throw new Error('Local file output requires Node.js. Provide write_file(path, body) to use another destination.');
}

export async function save(path: string, body: ByteStream, options: OutputOptions = {}): Promise<number> {
  require_output(options);
  let length = 0;
  let consumed = false;
  const propagation = new AbortController();
  const counted = body.pipeThrough(new TransformStream<Uint8Array, Uint8Array>({
    transform(chunk, controller) { length += chunk.length; controller.enqueue(chunk); },
    flush() { consumed = true; },
  }), { signal: propagation.signal });
  if (options.write_file) {
    try {
      await options.write_file(path, counted);
      if (!consumed) throw new Error('write_file must consume the entire stream before resolving.');
    }
    finally {
      propagation.abort();
      if (!counted.locked) await counted.cancel().catch(() => {});
    }
    return length;
  }
  const temporary = `${path}.partial-${crypto.randomUUID()}`;
  let fs: Awaited<ReturnType<typeof node_fs>> | undefined;
  let file: FileHandle | undefined;
  try {
    fs = await node_fs();
    const parent = path.replaceAll('\\', '/').split('/').slice(0, -1).join('/');
    if (parent) await fs.mkdir(parent, { recursive: true });
    file = await fs.open(temporary, 'wx');
    for await (const chunk of chunks(counted)) {
      let offset = 0;
      while (offset < chunk.length) {
        const { bytesWritten } = await file.write(chunk, offset, chunk.length - offset);
        if (!bytesWritten) throw new Error('File write made no progress.');
        offset += bytesWritten;
      }
    }
    await file.close();
    await fs.rename(temporary, path);
  } catch (error) {
    await file?.close().catch(() => {});
    if (file) await fs?.unlink(temporary).catch(() => {});
    throw error;
  } finally { propagation.abort(); }
  return length;
}

export function billing_result(filename: string, size: number, bills: Billing[]): FileResult {
  const minimum = (key: 'remaining_balance' | 'remaining_api_key_credits') => {
    const values = bills.map(b => b[key]).filter((v): v is number => typeof v === 'number');
    return values.length ? Math.min(...values) : null;
  };
  const cost = bills.reduce((sum, b) => sum + (b.total_charge ?? 0), 0);
  const billing = { ...bills[0], total_charge: cost, remaining_balance: minimum('remaining_balance'), remaining_api_key_credits: minimum('remaining_api_key_credits') };
  return { filename, size_bytes: size, size_gb: size / 1e9, cost, remaining_balance: billing.remaining_balance, remaining_api_key_credits: billing.remaining_api_key_credits, billing };
}
