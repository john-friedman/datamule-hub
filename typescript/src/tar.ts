import { byte_stream, chunks, collect, save } from './core.js';
import { ByteReader, safe_name } from './io.js';
import type { ByteStream, OutputOptions } from './types.js';

const encoder = new TextEncoder();
const decoder = new TextDecoder();
const padding = (size: number) => (512 - size % 512) % 512;
const text = (data: Uint8Array) => decoder.decode(data).replace(/\0.*$/s, '');
function number(data: Uint8Array): number {
  if (data[0]! & 128) {
    let value = BigInt(data[0]! & 127);
    for (let i = 1; i < data.length; i++) value = value * 256n + BigInt(data[i]!);
    if (value > BigInt(Number.MAX_SAFE_INTEGER)) throw new Error('TAR size exceeds safe integer range.');
    return Number(value);
  }
  const value = parseInt(text(data).trim() || '0', 8);
  if (!Number.isSafeInteger(value) || value < 0) throw new Error('Invalid TAR number.');
  return value;
}

export interface TarEntry { name: string; size: number; body: ByteStream }
export async function* tar_entries(body: ByteStream): AsyncGenerator<TarEntry> {
  const reader = new ByteReader(body);
  let attributes: Record<string, string> = {}, global: Record<string, string> = {};
  try {
    while (true) {
      const header = await reader.read(512);
      if (header.every(value => value === 0)) return;
      const checksum = number(header.subarray(148, 156));
      let sum = 256;
      for (let i = 0; i < 512; i++) if (i < 148 || i >= 156) sum += header[i]!;
      if (sum !== checksum) throw new Error('Invalid TAR header checksum.');
      const type = String.fromCharCode(header[156]!);
      let size = number(header.subarray(124, 136));
      let name = text(header.subarray(0, 100));
      const prefix = header[262] === 0 ? text(header.subarray(345, 500)) : '';
      if (prefix) name = `${prefix}/${name}`;
      if (type === 'x' || type === 'g' || type === 'L') {
        if (size > 1024 * 1024) throw new Error('TAR extended header exceeds 1 MiB.');
        const data = await reader.read(size);
        if (type === 'L') attributes.path = text(data).replace(/\n$/, '');
        else {
          const target = type === 'g' ? global : attributes;
          let offset = 0;
          while (offset < data.length) {
            const space = data.indexOf(32, offset);
            const length = Number(decoder.decode(data.subarray(offset, space)));
            if (space < offset || !Number.isSafeInteger(length) || length <= space - offset + 1 || offset + length > data.length) throw new Error('Invalid PAX record.');
            const record = decoder.decode(data.subarray(space + 1, offset + length - 1));
            const equal = record.indexOf('=');
            if (equal > 0) target[record.slice(0, equal)] = record.slice(equal + 1);
            offset += length;
          }
        }
        await reader.skip(padding(size)); continue;
      }
      const effective = { ...global, ...attributes }; attributes = {};
      name = (effective.path ?? name).replace(/^(?:\.\/)+/, '');
      if (effective.size !== undefined) {
        size = Number(effective.size);
        if (!Number.isSafeInteger(size) || size < 0) throw new Error('Invalid PAX file size.');
      }
      if (type === '\0' || type === '0') {
        safe_name(name);
        let consumed = 0;
        const entry_body = byte_stream((async function* () {
          for await (const chunk of reader.take(size)) { consumed += chunk.length; yield chunk; }
        })());
        yield { name, size, body: entry_body };
        if (entry_body.locked) throw new Error('Consume each TAR entry before requesting the next.');
        await entry_body.cancel().catch(() => {});
        if (consumed < size) await reader.skip(size - consumed);
      } else await reader.skip(size); // Never follow links or create device entries.
      await reader.skip(padding(size));
    }
  } finally { await reader.close(); }
}

function header(name: string, size: number, type = '0'): Uint8Array {
  safe_name(name);
  const result = new Uint8Array(512);
  const put = (offset: number, length: number, value: string) => {
    const data = encoder.encode(value);
    if (data.length > length) throw new Error(`TAR field too long: ${value}`);
    result.set(data, offset);
  };
  if (encoder.encode(name).length <= 100) put(0, 100, name);
  else {
    const split = name.lastIndexOf('/');
    if (split < 0) throw new Error(`TAR filename too long: ${name}`);
    put(0, 100, name.slice(split + 1)); put(345, 155, name.slice(0, split));
  }
  put(100, 8, '0000644\0'); put(108, 8, '0000000\0'); put(116, 8, '0000000\0');
  if (size < 8 ** 11) put(124, 12, size.toString(8).padStart(11, '0') + '\0');
  else {
    let value = BigInt(size);
    for (let i = 135; i >= 124; i--) { result[i] = Number(value & 255n); value >>= 8n; }
    result[124] = result[124]! | 128;
  }
  put(136, 12, '00000000000\0'); result.fill(32, 148, 156); result[156] = type.charCodeAt(0);
  put(257, 6, 'ustar\0'); put(263, 2, '00');
  const sum = result.reduce((a, b) => a + b, 0);
  put(148, 8, sum.toString(8).padStart(6, '0') + '\0 ');
  return result;
}

function entry_headers(name: string, size: number): Uint8Array[] {
  const split = name.lastIndexOf('/');
  if (encoder.encode(name).length <= 100 || split >= 0 && encoder.encode(name.slice(0, split)).length <= 155 && encoder.encode(name.slice(split + 1)).length <= 100) return [header(name, size)];
  // POSIX PAX paths retain arbitrary UTF-8 names without truncating them.
  const value = ` path=${name}\n`;
  let length = encoder.encode(value).length + 1;
  while (encoder.encode(`${length}${value}`).length !== length) length = encoder.encode(`${length}${value}`).length;
  const record = encoder.encode(`${length}${value}`);
  return [header('PaxHeaders/path', record.length, 'x'), record, new Uint8Array(padding(record.length)), header('entry', size)];
}

/** One shard at a time, with backpressure propagated to the source. */
export class TarBatches {
  readonly paths: string[] = [];
  private writer?: WritableStreamDefaultWriter<Uint8Array>;
  private completion?: Promise<void>;
  private failure?: unknown;
  private size = 0;
  constructor(private directory: string, private max_bytes: number, private output: OutputOptions) {}
  private async start(): Promise<void> {
    const transform = new TransformStream<Uint8Array, Uint8Array>();
    this.writer = transform.writable.getWriter();
    const path = `${this.directory}/batch_${String(this.paths.length + 1).padStart(6, '0')}.tar`;
    this.paths.push(path); this.size = 0;
    // Attach rejection immediately; failed consumers must unblock the writer.
    this.completion = save(path, transform.readable, this.output).then(() => {}, error => {
      this.failure = error;
      void this.writer?.abort(error).catch(() => {});
    });
  }
  async append(name: string, size: number, body: ByteStream): Promise<void> {
    if (!Number.isSafeInteger(size) || size < 0) throw new RangeError('Invalid TAR entry size.');
    const headers = entry_headers(name, size);
    const required = headers.reduce((sum, part) => sum + part.length, 0) + size + padding(size);
    if (this.writer && this.size && this.size + required + 1024 > this.max_bytes) await this.close();
    if (!this.writer) await this.start();
    if (this.failure) throw this.failure;
    for (const part of headers) if (part.length) await this.writer!.write(part);
    let actual = 0;
    for await (const chunk of chunks(body)) { actual += chunk.length; await this.writer!.write(chunk); }
    if (actual !== size) throw new Error('TAR entry size changed while writing.');
    if (padding(size)) await this.writer!.write(new Uint8Array(padding(size)));
    this.size += required;
  }
  async close(): Promise<void> {
    if (!this.writer) return;
    await this.writer.write(new Uint8Array(1024)); await this.writer.close();
    await this.completion;
    this.writer = undefined;
    if (this.failure) throw this.failure;
  }
  async abort(error: unknown): Promise<void> {
    await this.writer?.abort(error).catch(() => {});
    await this.completion;
    this.writer = undefined;
  }
}
