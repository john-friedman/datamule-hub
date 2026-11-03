import { parquetMetadataAsync, parquetReadObjects, parquetSchema } from 'hyparquet/src/index.js';
import type { AsyncBuffer, FileMetaData, SchemaElement, SchemaTree } from 'hyparquet';
import { ByteWriter, parquetWriteRows } from 'hyparquet-writer/src/index.js';
import { decompress } from 'fzstd';
import { checked_fetch, collect, object, positive } from './core.js';
import type { BufferOptions } from './io.js';
import type { ByteStream, RequestOptions, Row } from './types.js';

export interface ParquetOptions extends BufferOptions {
  /** Row count buffered by the output writer. Input decoding is by row group. */
  row_group_size?: number;
}
export interface ParquetSource { file: AsyncBuffer; metadata: FileMetaData; key?: string }

export async function remote_file(url: string, size: number | undefined, options: RequestOptions & BufferOptions): Promise<AsyncBuffer> {
  if (size === undefined) {
    const response = await checked_fetch(url, { method: 'HEAD' }, options);
    const length = response.headers.get('content-length');
    if (length === null) throw new Error('Parquet range reading requires Content-Length.');
    size = Number(length);
  }
  positive(size, 'Parquet file size');
  const byteLength = size;
  return {
    byteLength,
    async slice(start, end = byteLength) {
      if (start < 0) start += byteLength;
      if (end < 0) end += byteLength;
      if (!Number.isSafeInteger(start) || !Number.isSafeInteger(end) || start < 0 || end < start || end > byteLength) throw new RangeError('Invalid Parquet range.');
      if (start === end) return new ArrayBuffer(0);
      const limit = options.max_buffer_bytes ?? 32 * 1024 * 1024;
      if (end - start > limit) throw new RangeError(`Parquet byte range exceeds max_buffer_bytes (${limit}).`);
      const response = await checked_fetch(url, { headers: { Range: `bytes=${start}-${end - 1}`, 'Accept-Encoding': 'identity' } }, options);
      if (response.status !== 206 || !response.headers.get('content-range')?.startsWith(`bytes ${start}-${end - 1}/`) || !response.body) {
        await response.body?.cancel(); throw new Error('Parquet server must honor byte ranges.');
      }
      const data = await collect(response.body, end - start);
      if (data.length !== end - start) throw new Error('Truncated Parquet byte range.');
      return data.buffer as ArrayBuffer;
    },
  };
}

export async function parquet_source(file: AsyncBuffer, key?: string): Promise<ParquetSource> {
  return { file, metadata: await parquetMetadataAsync(file), key };
}

export function column_names(source: ParquetSource): string[] {
  return parquetSchema(source.metadata).children.map(child => child.element.name);
}

export async function* parquet_rows(source: ParquetSource, options: ParquetOptions = {}, columns?: string[], raw = false): AsyncGenerator<Row> {
  let start = 0;
  const limit = options.max_buffer_bytes ?? 32 * 1024 * 1024;
  const trees = parquetSchema(source.metadata).children;
  // Read decimal physical values without converting through float64, which can
  // round identifiers and high-precision decimal values during a dataset merge.
  const metadata = !raw ? source.metadata : {
    ...source.metadata,
    schema: source.metadata.schema.map(element => {
      if (element.type === 'INT96') throw new Error('Merging legacy INT96 Parquet timestamps is unsupported.');
      const field = { ...element };
      if (field.converted_type === 'DECIMAL') delete field.converted_type;
      if (field.logical_type?.type === 'DECIMAL') delete field.logical_type;
      return field;
    }),
  };
  for (const group of source.metadata.row_groups) {
    const end = start + Number(group.num_rows);
    const selected = group.columns.filter(c => !columns || columns.includes(c.meta_data?.path_in_schema[0] ?? ''));
    const decoded_bytes = selected.reduce((sum, c) => sum + Number(c.meta_data?.total_uncompressed_size ?? 0), 0);
    if (decoded_bytes > limit) throw new RangeError(`Parquet row group needs ${decoded_bytes} uncompressed bytes, above max_buffer_bytes (${limit}). Raise the limit if enough memory is available.`);
    const rows = await parquetReadObjects({
      file: source.file, metadata, rowStart: start, rowEnd: end, columns, utf8: !raw,
      compressors: { ZSTD: (input, outputLength) => {
        if (outputLength > limit) throw new RangeError('Zstandard page exceeds max_buffer_bytes.');
        return decompress(input);
      } },
      ...(raw ? { parsers: {
        dateFromDays: (days: number) => days,
        timestampFromMilliseconds: (value: bigint) => value,
        timestampFromMicroseconds: (value: bigint) => value,
        timestampFromNanoseconds: (value: bigint) => value,
      } } : {}),
    });
    for (const row of rows) {
      if (raw) for (const tree of trees) if (tree.element.name in row) row[tree.element.name] = restore_decimals(tree, row[tree.element.name]);
      yield row;
    }
    start = end;
  }
}

function restore_decimals(tree: SchemaTree, value: unknown): unknown {
  if (value == null) return value;
  const element = tree.element;
  if (element.converted_type === 'MAP' || element.logical_type?.type === 'MAP') {
    const key = tree.children[0]?.children[0], item = tree.children[0]?.children[1];
    if (!key || !item) throw new Error('Invalid Parquet MAP schema.');
    const entries = value instanceof Map ? [...value] : Array.isArray(value) ? value.map(entry => Array.isArray(entry) ? entry : [object(entry).key, object(entry).value]) : Object.entries(object(value));
    return entries.map(([k, v]) => ({ key: restore_decimals(key, k), value: restore_decimals(item, v) }));
  }
  if (Array.isArray(value)) {
    const child = element.converted_type === 'LIST' || element.logical_type?.type === 'LIST' ? tree.children[0]?.children[0] : tree;
    return child ? value.map(item => child === tree && tree.children.length ? restore_struct(tree, item) : restore_decimals(child, item)) : value;
  }
  if (element.converted_type === 'DECIMAL' || element.logical_type?.type === 'DECIMAL') {
    if (value instanceof Uint8Array) {
      let result = 0n;
      for (const byte of value) result = result * 256n + BigInt(byte);
      if (value[0]! & 128) result -= 1n << BigInt(value.length * 8);
      return result;
    }
    return BigInt(value as number | bigint);
  }
  return restore_struct(tree, value);
}
function restore_struct(tree: SchemaTree, value: unknown): unknown {
  if (value && typeof value === 'object' && !(value instanceof Uint8Array)) {
    const record = value as Row;
    for (const child of tree.children) if (child.element.name in record) record[child.element.name] = restore_decimals(child, record[child.element.name]);
  }
  return value;
}

function top_fields(schema: SchemaElement[]): SchemaElement[][] {
  let index = 1;
  function subtree(): SchemaElement[] {
    const field = schema[index++]!;
    const result = [{ ...field }];
    for (let i = 0; i < (field.num_children ?? 0); i++) result.push(...subtree());
    return result;
  }
  const result: SchemaElement[][] = [];
  for (let i = 0; i < (schema[0]?.num_children ?? 0); i++) result.push(subtree());
  return result;
}

export function merge_schema(sources: ParquetSource[], add_source = true, add_date = false): SchemaElement[] {
  const fields = new Map<string, SchemaElement[]>();
  for (const source of sources) for (const tree of top_fields(source.metadata.schema)) {
    tree[0]!.repetition_type = 'OPTIONAL';
    const name = tree[0]!.name;
    const existing = fields.get(name);
    // Compare normalized property order, since equivalent schemas can come from different writers.
    const signature = (value: unknown): string => JSON.stringify(value, (_key, item: unknown) => item && typeof item === 'object' && !Array.isArray(item) ? Object.fromEntries(Object.entries(item).sort(([a], [b]) => a.localeCompare(b))) : item);
    if (existing && signature(existing) !== signature(tree)) throw new Error(`Conflicting Parquet types for column ${name}.`);
    fields.set(name, tree);
  }
  const add_string = (name: string) => { if (!fields.has(name)) fields.set(name, [{ name, type: 'BYTE_ARRAY', converted_type: 'UTF8', repetition_type: 'OPTIONAL' }]); };
  if (add_source) add_string('_sourceKey');
  if (add_date) add_string('filingDate');
  return [{ name: 'root', num_children: fields.size }, ...Array.from(fields.values()).flat()];
}

/** Encodes and flushes one row group at a time, including downstream backpressure. */
export function parquet_stream(rows: AsyncIterable<Row> | Iterable<Row>, schema: SchemaElement[], options: ParquetOptions = {}): ByteStream {
  const transform = new TransformStream<Uint8Array, Uint8Array>();
  const sink = transform.writable.getWriter();
  const limit = options.max_buffer_bytes ?? 32 * 1024 * 1024;
  class StreamWriter extends ByteWriter {
    override ensure(size: number): void {
      if (this.index + size > limit) throw new RangeError('Encoded Parquet row group exceeds max_buffer_bytes.');
      super.ensure(size);
    }
    async flush(): Promise<void> {
      if (this.index) { await sink.write(this.getBytes().slice()); this.index = 0; }
    }
    override async finish(): Promise<void> { await this.flush(); }
  }
  const columns = top_fields(schema).map(tree => ({ name: tree[0]!.name, offsetIndex: false }));
  const rowGroupSize = positive(options.row_group_size ?? 4096, 'row_group_size');
  void (async () => {
    await parquetWriteRows({ writer: new StreamWriter(), rows, columns, schema, rowGroupSize, codec: 'SNAPPY', dictionarySize: 1024 * 1024 });
    await sink.close();
  })().catch(async error => { await sink.abort(error).catch(() => {}); });
  return transform.readable;
}
