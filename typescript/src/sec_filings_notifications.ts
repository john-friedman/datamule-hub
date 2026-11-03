import { api_json, ApiError, get_api_key, is_node, log_billing, node_module, object, parse_json, sleep } from './core.js';
import type { RequestOptions, Row } from './types.js';

export type Source = 'Rss' | 'Efts' | 'anticipate';
export interface EndpointOptions extends RequestOptions {
  submission_types?: readonly string[];
  ciks?: readonly (number | string)[];
  sources?: readonly Source[];
}
export interface AddEndpointOptions extends EndpointOptions { endpoint: string }
export interface RemoveEndpointOptions extends RequestOptions { id?: string; endpoint?: string }

export function add_endpoint(options: AddEndpointOptions): Promise<Record<string, unknown>>;
export function add_endpoint(endpoint: string, options?: EndpointOptions): Promise<Record<string, unknown>>;
export async function add_endpoint(endpoint: string | AddEndpointOptions, options: EndpointOptions = {}): Promise<Record<string, unknown>> {
  const config = typeof endpoint === 'string' ? { ...options, endpoint } : endpoint;
  const sources = config.sources ?? ['Rss', 'Efts', 'anticipate'];
  if (!sources.length || sources.some(s => !['Rss', 'Efts', 'anticipate'].includes(s))) throw new TypeError('sources must contain Rss, Efts, or anticipate.');
  const url = new URL(config.endpoint);
  if (url.protocol !== 'https:') throw new TypeError('Webhook endpoint must use HTTPS.');
  const result = await api_json('/v3/sec-filings-webhooks', config, {
    endpoint: config.endpoint,
    mode: config.submission_types?.length || config.ciks?.length ? 'filter' : 'all', sources,
    ...(config.submission_types?.length ? { submission_types: config.submission_types } : {}),
    ...(config.ciks?.length ? { ciks: config.ciks } : {}),
  });
  console.info('[datamulehub] Webhook endpoint added');
  return result;
}

export async function list_endpoints(options: RequestOptions = {}): Promise<Record<string, unknown>> {
  const result = await api_json('/v3/sec-filings-webhooks', options);
  console.info(`[datamulehub] Webhook endpoints listed${Array.isArray(result.data) ? `: count=${result.data.length}` : ''}`);
  return result;
}

export async function remove_endpoint(options: RemoveEndpointOptions): Promise<Record<string, unknown>> {
  if ((options.id === undefined) === (options.endpoint === undefined)) throw new TypeError('Provide exactly one of id or endpoint.');
  const result = await api_json(`/v3/sec-filings-webhooks${options.id === undefined ? '' : `/${encodeURIComponent(options.id)}`}`, options,
    options.id === undefined ? { endpoint: options.endpoint } : undefined, 'DELETE');
  console.info('[datamulehub] Webhook endpoint removed');
  return result;
}

/** Minimal socket interface for notification streams. */
export interface FilingSocket {
  readyState: number;
  close(code?: number, reason?: string): void;
  addEventListener(type: string, listener: (event: { data?: unknown; message?: string; code?: number }) => void): void;
  removeEventListener(type: string, listener: (event: { data?: unknown; message?: string; code?: number }) => void): void;
  ping?: () => void;
  on_pong?: (listener: () => void) => (() => void);
}
export type Connect = (url: string, headers: Record<string, string>, signal?: AbortSignal) => Promise<FilingSocket>;
export interface StreamOptions extends RequestOptions {
  idle_timeout?: number | null;
  ping_interval?: number | null;
  ping_timeout?: number;
  recv_timeout?: number;
  reconnect_delay?: number;
  /** Bound queued filing events if the consumer falls behind. */
  max_queue?: number;
  connect?: Connect;
}

async function connect_socket(url: string, credential: string, options: StreamOptions): Promise<FilingSocket> {
  const headers = { Authorization: `Bearer ${credential}` };
  if (options.connect) return options.connect(url, headers, options.signal);
  if (is_node()) {
    const { default: WebSocket } = await node_module<{ default: new (url: string, options: { headers: Record<string, string>; handshakeTimeout: number; maxPayload: number }) => FilingSocket & { on(type: string, listener: (...args: unknown[]) => void): void; off(type: string, listener: (...args: unknown[]) => void): void; terminate(): void } }>('ws');
    const socket = new WebSocket(url, { headers, handshakeTimeout: (options.timeout ?? 30) * 1000, maxPayload: 1024 * 1024 });
    // ws can emit an error after terminate() during a failed handshake.
    socket.on('error', () => {});
    socket.on_pong = listener => { socket.on('pong', listener); return () => socket.off('pong', listener); };
    await new Promise<void>((resolve, reject) => {
      const cleanup = () => {
        socket.removeEventListener('open', open); socket.removeEventListener('error', error);
        socket.off('unexpected-response', unexpected); options.signal?.removeEventListener('abort', abort);
      };
      const open = () => { cleanup(); resolve(); };
      const error = (event: { message?: string }) => { cleanup(); reject(new TypeError(event.message ?? 'WebSocket connection failed.')); };
      const unexpected = (...args: unknown[]) => {
        const response = args[1] as { statusCode: number; destroy(): void };
        cleanup(); response.destroy(); socket.terminate(); reject(new ApiError(response.statusCode, 'WebSocket authentication/upgrade failed.'));
      };
      const abort = () => { cleanup(); socket.terminate(); reject(options.signal?.reason); };
      socket.addEventListener('open', open); socket.addEventListener('error', error);
      socket.on('unexpected-response', unexpected); options.signal?.addEventListener('abort', abort, { once: true });
      if (options.signal?.aborted) abort();
    });
    return socket;
  }
  throw new Error('Provide a connect adapter for authenticated WebSockets in this runtime.');
}

async function* events(socket: FilingSocket, options: StreamOptions, fastest: boolean, expires?: number): AsyncGenerator<Row> {
  const queue: Row[] = [];
  let failure: unknown, closed = false, wake: (() => void) | undefined;
  let last_activity = Date.now();
  let next_ping = Date.now() + (options.ping_interval ?? 60) * 1000;
  let pong_deadline: number | undefined;
  const remove_pong = socket.on_pong?.(() => { pong_deadline = undefined; });
  const notify = () => { wake?.(); wake = undefined; };
  const onmessage = (event: { data?: unknown }) => {
    try {
      last_activity = Date.now();
      const data = typeof event.data === 'string' ? event.data : event.data instanceof ArrayBuffer ? new TextDecoder().decode(event.data) : ArrayBuffer.isView(event.data) ? new TextDecoder().decode(event.data) : undefined;
      if (data === undefined) throw new Error('Unexpected WebSocket message type.');
      const item = object(parse_json(data));
      if (item.type === 'error') throw new WebSocketServerError(String(item.error ?? 'WebSocket error'));
      const filing = fastest ? item.accession !== undefined ? item : undefined : item.type === 'filing' ? object(item.item) : undefined;
      if (filing) {
        if (queue.length >= (options.max_queue ?? 1024)) throw new Error('Filing queue overflow; consumer must keep up with the stream.');
        queue.push(filing);
      }
    } catch (error) { failure = error; }
    notify();
  };
  const onclose = () => { closed = true; notify(); };
  const onerror = () => { failure = new Error('WebSocket transport error.'); notify(); };
  const onabort = () => { failure = options.signal?.reason; closed = true; notify(); };
  socket.addEventListener('message', onmessage); socket.addEventListener('close', onclose); socket.addEventListener('error', onerror);
  options.signal?.addEventListener('abort', onabort, { once: true });
  try {
    while (true) {
      options.signal?.throwIfAborted();
      if (failure) throw failure;
      if (pong_deadline !== undefined && Date.now() >= pong_deadline) throw new WebSocketPingTimeout('WebSocket pong timeout.');
      if (!closed && socket.readyState === 1 && socket.ping && socket.on_pong && options.ping_interval !== null && options.ping_interval !== 0 && !pong_deadline && Date.now() >= next_ping) {
        socket.ping(); pong_deadline = Date.now() + (options.ping_timeout ?? 10) * 1000;
        next_ping = Date.now() + (options.ping_interval ?? 60) * 1000;
      }
      if (queue.length) {
        console.info(`[datamulehub] ${fastest ? 'Fastest filings ' : ''}WebSocket emitted filing`);
        yield queue.shift()!; continue;
      }
      if (closed || socket.readyState >= 2 || expires !== undefined && Date.now() >= expires) return;
      const idle = options.idle_timeout === undefined ? fastest ? null : 300 : options.idle_timeout;
      if (idle && Date.now() - last_activity >= idle * 1000) throw new WebSocketIdleTimeout('WebSocket idle timeout.');
      await new Promise<void>(resolve => {
        const timer = setTimeout(() => { wake = undefined; resolve(); }, 1000);
        wake = () => { clearTimeout(timer); resolve(); };
      });
    }
  } finally {
    options.signal?.removeEventListener('abort', onabort);
    remove_pong?.();
    socket.removeEventListener('message', onmessage); socket.removeEventListener('close', onclose); socket.removeEventListener('error', onerror);
    socket.close(1000, 'Consumer closed');
    console.info(`[datamulehub] ${fastest ? 'Fastest filings ' : ''}WebSocket disconnected`);
  }
}

export class WebSocketIdleTimeout extends Error { override name = 'WebSocketIdleTimeout'; }
export class WebSocketPingTimeout extends Error { override name = 'WebSocketPingTimeout'; }
export class WebSocketServerError extends Error { override name = 'WebSocketServerError'; }

async function* stream(options: StreamOptions, fastest: boolean): AsyncGenerator<Row> {
  if (!options.connect && !is_node()) throw new Error('Provide a connect adapter for authenticated WebSockets in this runtime.');
  const key = get_api_key(options.api_key);
  for (const [name, value] of Object.entries({ idle_timeout: options.idle_timeout, reconnect_delay: options.reconnect_delay, recv_timeout: options.recv_timeout, ping_interval: options.ping_interval, ping_timeout: options.ping_timeout })) {
    if (value != null && (!Number.isFinite(value) || value < 0)) throw new RangeError(`${name} must be nonnegative.`);
  }
  if (options.max_queue !== undefined && (!Number.isSafeInteger(options.max_queue) || options.max_queue <= 0)) throw new RangeError('max_queue must be positive.');
  let access: { url: string; token: string; expires: number } | undefined;
  while (true) {
    options.signal?.throwIfAborted();
    // Token purchases are outside the reconnect catch, so a failed purchase is
    // surfaced rather than automatically making repeated chargeable requests.
    if (fastest && (!access || Date.now() >= access.expires)) {
      console.info('[datamulehub] Requesting fastest filings access');
      const payload = await api_json('/v3/auth/service-token?service=fastest-sec-filings-websocket', options, undefined, 'POST');
      const data = object(payload.data);
      const expires = Date.parse(String(data.expires_at));
      if (typeof data.token !== 'string' || typeof data.websocket_url !== 'string' || !Number.isFinite(expires) || expires <= Date.now()) throw new Error('Invalid fastest-filings token response.');
      access = { url: data.websocket_url, token: data.token, expires };
      log_billing(`Fastest filings access issued: expires_at=${new Date(expires).toISOString()}`, object(object(payload.metadata ?? {}).billing ?? {}));
    }
    try {
      const url = access?.url ?? `${options.base_url ?? 'https://api.datamule.xyz'}/v3/websocket`.replace(/^http/, 'ws');
      const socket = await connect_socket(url, access?.token ?? key, options);
      console.info(`[datamulehub] ${fastest ? 'Fastest filings ' : ''}WebSocket connected`);
      yield* events(socket, options, fastest, access?.expires);
      console.info(`[datamulehub] WebSocket closed; reconnecting in ${options.reconnect_delay ?? 1}s`);
    } catch (error) {
      options.signal?.throwIfAborted();
      if (error instanceof ApiError && (error.status === 401 || error.status === 403)) {
        // Match Python's allowance for server/client clock skew at expiration.
        if (access && Date.now() + 5000 >= access.expires) {
          access = undefined;
          continue;
        }
        console.info(`[datamulehub] WebSocket authentication failed: status=${error.status}`);
        throw error;
      }
      const retry = error instanceof WebSocketIdleTimeout || error instanceof WebSocketPingTimeout || error instanceof TypeError ||
        error instanceof ApiError && (error.status >= 500 || error.status === 429) ||
        error instanceof Error && error.message === 'WebSocket transport error.';
      if (!retry) throw error;
      console.info(`[datamulehub] WebSocket stream interrupted; reconnecting in ${options.reconnect_delay ?? 1}s (${error instanceof Error ? error.name : 'transport error'})`);
    }
    await sleep(options.reconnect_delay ?? 1, options.signal);
  }
}

export function stream_filings(options: StreamOptions = {}): AsyncGenerator<Row> { return stream(options, false); }
export function stream_fastest_filings(options: StreamOptions = {}): AsyncGenerator<Row> { return stream(options, true); }
