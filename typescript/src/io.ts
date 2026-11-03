import type { AsyncBuffer } from 'hyparquet';
import { byte_stream, bytes, chunks, collect, is_node, node_fs, node_module, require_output, save } from './core.js';
import type { ByteStream, OutputOptions } from './types.js';

export interface BufferOptions { max_buffer_bytes?: number }
export interface Spool {
  size: number;
  file: AsyncBuffer;
  open(): ByteStream;
  dispose(): Promise<void>;
}

export async function spool(body: ByteStream, options: BufferOptions = {}): Promise<Spool> {
  if (!is_node()) {
    const data = await collect(body, options.max_buffer_bytes ?? 32 * 1024 * 1024);
    return { size: data.length, file: { byteLength: data.length, slice: (a, b) => data.slice(a, b).buffer }, open: () => bytes(data), async dispose() {} };
  }
  const fs = await node_fs();
  const os = await node_module<{ tmpdir(): string }>('node:os');
  const dir = await fs.mkdtemp(`${os.tmpdir()}/datamulehub-`);
  const path = `${dir}/data`;
  let handle: Awaited<ReturnType<typeof fs.open>> | undefined;
  try {
    const size = await save(path, body);
    handle = await fs.open(path, 'r');
    const file: AsyncBuffer = {
      byteLength: size,
      async slice(start, end = size) {
        if (start < 0) start += size;
        if (end < 0) end += size;
        if (start < 0 || end < start || end > size) throw new RangeError('Invalid file byte range.');
        const data = new Uint8Array(end - start);
        let offset = 0;
        while (offset < data.length) {
          const { bytesRead } = await handle!.read(data, offset, data.length - offset, start + offset);
          if (!bytesRead) throw new Error('Unexpected end of file.');
          offset += bytesRead;
        }
        return data.buffer;
      },
    };
    return {
      size, file,
      open: () => byte_stream((async function* () {
        for (let start = 0; start < size; start += 65536) yield new Uint8Array(await file.slice(start, Math.min(start + 65536, size)));
      })()),
      async dispose() { await handle!.close(); await fs.unlink(path); await fs.rmdir(dir); },
    };
  } catch (error) {
    await handle?.close().catch(() => {});
    await fs.unlink(path).catch(() => {}); await fs.rmdir(dir).catch(() => {});
    throw error;
  }
}

export async function prepare_dir(path: string, overwrite: boolean, options: OutputOptions): Promise<void> {
  require_output(options);
  if (options.write_file) return;
  const fs = await node_fs();
  try {
    const entries = await fs.readdir(path);
    if (entries.length && !overwrite) throw new Error(`Output directory is not empty: ${path}. Set overwrite=true to replace matching files.`);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error;
  }
  await fs.mkdir(path, { recursive: true });
}

export function safe_name(name: string): string {
  if (!name || name.includes('\0') || name.includes('\\') || name.startsWith('/') || /^[a-z]:/i.test(name) || name.split('/').some(p => p === '..' || p === '.')) throw new Error(`Unsafe output name: ${name}`);
  return name;
}

export function sized(body: ByteStream, expected?: number): ByteStream {
  if (expected === undefined) return body;
  let actual = 0;
  return body.pipeThrough(new TransformStream<Uint8Array, Uint8Array>({
    transform(chunk, controller) {
      actual += chunk.length;
      if (actual > expected) throw new Error(`Response exceeds expected length ${expected}.`);
      controller.enqueue(chunk);
    },
    flush() { if (actual !== expected) throw new Error(`Incomplete response: expected ${expected} bytes, received ${actual}.`); },
  }));
}

/** Read an input incrementally, retaining at most one upstream chunk. */
export class ByteReader {
  private readonly iterator: AsyncGenerator<Uint8Array>;
  private chunk: Uint8Array = new Uint8Array(0);
  private offset = 0;
  constructor(body: ByteStream) { this.iterator = chunks(body); }
  async *take(size: number): AsyncGenerator<Uint8Array> {
    while (size > 0) {
      if (this.offset === this.chunk.length) {
        const next = await this.iterator.next();
        if (next.done) throw new Error('Unexpected end of stream.');
        this.chunk = next.value; this.offset = 0;
      }
      const length = Math.min(size, this.chunk.length - this.offset);
      const piece = this.chunk.subarray(this.offset, this.offset + length);
      this.offset += length; size -= length;
      yield piece;
    }
  }
  async read(size: number): Promise<Uint8Array> {
    return collect(byte_stream(this.take(size)), size);
  }
  async skip(size: number): Promise<void> { for await (const _ of this.take(size)) { /* Drain. */ } }
  async close(): Promise<void> { await this.iterator.return(undefined); }
}
