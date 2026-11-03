import { Decompress } from 'fzstd';
import { byte_stream, chunks } from './core.js';
import type { ByteStream } from './types.js';

/** Slice compressed input to limit the decoder's synchronous output queue. */
export function decompress_zstd(body: ByteStream, allow_plain = false): ByteStream {
  return byte_stream((async function* () {
    const output: Uint8Array[] = [];
    let decoder: Decompress | undefined, prefix = new Uint8Array(0), plain = false;
    for await (const chunk of chunks(body)) {
      let input = chunk;
      if (!decoder && !plain) {
        const joined = new Uint8Array(prefix.length + input.length);
        joined.set(prefix); joined.set(input, prefix.length);
        if (joined.length < 4) { prefix = joined; continue; }
        input = joined; prefix = new Uint8Array(0);
        const magic = new DataView(joined.buffer).getUint32(0, true);
        plain = allow_plain && magic !== 0xfd2fb528 && (magic & 0xfffffff0) !== 0x184d2a50;
        if (!plain) decoder = new Decompress(data => { if (data.length) output.push(data.slice()); });
      }
      if (plain) { yield input; continue; }
      for (let offset = 0; offset < input.length; offset += 4096) {
        decoder!.push(input.subarray(offset, offset + 4096));
        yield* output; output.length = 0;
      }
    }
    if (decoder) { decoder.push(new Uint8Array(0), true); yield* output; }
    else if (allow_plain) { if (prefix.length) yield prefix; }
    else throw new Error('Empty or truncated Zstandard stream.');
  })());
}
