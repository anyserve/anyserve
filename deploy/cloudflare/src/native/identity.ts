/**
 * Canonical fingerprints, scoped `(namespace, job_id)` bijection, UTF-8 byte-order
 * map sorting, and incremental/streaming SHA-256 with byte-exact parity against
 * `crates/anyserve-core/src/runtime/identity.rs`.
 *
 * Runs in both Cloudflare `workerd` (uses `crypto.DigestStream` when streaming)
 * and plain Node 24 Vitest (falls back to a zero-concatenation incremental SHA-256),
 * with zero host-specific runtime imports.
 */

import {
  Durability,
  type ErrorInfoInput,
  type JobSpecInput,
  type ProtoObjectInput,
  utf8ByteLen,
} from "./proto";

export const DEFAULT_NAMESPACE = "default";
export const SCOPED_PREFIX = "@ns:";
export const OBJECT_OVERHEAD = 64;

export const IDENTITY_EXCLUDED_METADATA_KEYS = [
  "traceparent",
  "tracestate",
  "baggage",
  "x-request-id",
] as const;

export const MAX_TRACE_HEADER_BYTES = 512;

export function extractTraceHeaders(headers: Headers): Record<string, string> {
  const out: Record<string, string> = {};
  for (const key of IDENTITY_EXCLUDED_METADATA_KEYS) {
    const raw = headers.get(key);
    if (!raw) continue;
    const trimmed = raw.trim();
    if (trimmed.length > 0 && utf8ByteLen(trimmed) <= MAX_TRACE_HEADER_BYTES) {
      out[key] = trimmed;
    }
  }
  return out;
}

export function mergeTraceHeaders(
  metadata: Record<string, string>,
  headers: Record<string, string>,
): void {
  for (const key of IDENTITY_EXCLUDED_METADATA_KEYS) {
    const existing = metadata[key];
    if (existing !== undefined && existing.trim().length > 0) {
      continue;
    }
    const incoming = headers[key];
    if (!incoming) continue;
    const trimmed = incoming.trim();
    if (trimmed.length > 0 && utf8ByteLen(trimmed) <= MAX_TRACE_HEADER_BYTES) {
      metadata[key] = trimmed;
    }
  }
}

const EXCLUDED_METADATA_SET: ReadonlySet<string> = new Set(
  IDENTITY_EXCLUDED_METADATA_KEYS,
);

export const OBJECT_SHA256_METADATA_KEY = "anyserve.sha256";
export const OBJECT_SIZE_METADATA_KEY = "anyserve.size_bytes";

export const JOB_SPEC_HASH_DOMAIN = "anyserve.job.v1";
export const OUTCOME_DIGEST_DOMAIN = "anyserve.result.v1";

const utf8Encoder = new TextEncoder();
const HEX_CHARS = "0123456789abcdef";

// ---------- Bijective Scoped Job ID ----------

/**
 * Bijective mapping between `(namespace, rawJobId)` and the internal storage key:
 * - `"default"` namespace keeps `rawJobId` bare UNLESS `rawJobId` starts with `"@ns:"`.
 * - Everything else encodes as `"@ns:<namespace>:<rawJobId>"`.
 *
 * Thus `("default", "@ns:team:j") => "@ns:default:@ns:team:j"`, which differs from
 * `("team", "j") => "@ns:team:j"`.
 */
export function encodeScopedId(namespace: string, rawJobId: string): string {
  if (namespace === DEFAULT_NAMESPACE && !rawJobId.startsWith(SCOPED_PREFIX)) {
    return rawJobId;
  }
  return `${SCOPED_PREFIX}${namespace}:${rawJobId}`;
}

export function decodeScopedId(
  scopedJobId: string,
): [namespace: string, rawJobId: string] {
  if (scopedJobId.startsWith(SCOPED_PREFIX)) {
    const rest = scopedJobId.slice(SCOPED_PREFIX.length);
    const colonIdx = rest.indexOf(":");
    if (colonIdx !== -1) {
      return [rest.slice(0, colonIdx), rest.slice(colonIdx + 1)];
    }
  }
  return [DEFAULT_NAMESPACE, scopedJobId];
}

// ---------- UTF-8 Byte-Order Sorting ----------

export function compareBytes(a: Uint8Array, b: Uint8Array): number {
  const min = a.byteLength < b.byteLength ? a.byteLength : b.byteLength;
  for (let i = 0; i < min; i++) {
    const diff = a[i]! - b[i]!;
    if (diff !== 0) return diff;
  }
  return a.byteLength - b.byteLength;
}

/**
 * Compares two strings in unsigned UTF-8 byte order (matching Rust `str::cmp` /
 * `BTreeMap<String, _>`), NOT JavaScript's UTF-16 code-unit order.
 */
export function compareUtf8(a: string, b: string): number {
  if (a === b) return 0;
  const n = a.length < b.length ? a.length : b.length;
  // ASCII fast path: the first differing code unit decides when both are ASCII.
  for (let i = 0; i < n; i++) {
    const ca = a.charCodeAt(i);
    const cb = b.charCodeAt(i);
    if (ca === cb) continue;
    if (ca < 0x80 && cb < 0x80) return ca - cb;
    return compareBytes(utf8Encoder.encode(a), utf8Encoder.encode(b));
  }
  // A UTF-16 prefix is a UTF-8 prefix unless it ends in a lone high surrogate.
  if (n > 0 && (a.charCodeAt(n - 1) & 0xfc00) === 0xd800) {
    return compareBytes(utf8Encoder.encode(a), utf8Encoder.encode(b));
  }
  return a.length - b.length;
}

export interface EncodedMapEntry<V> {
  key: string;
  keyBytes: Uint8Array;
  value: V;
}

export function sortMapByUtf8Bytes<V>(
  map: Record<string, V> | ReadonlyMap<string, V> | undefined,
): EncodedMapEntry<V>[] {
  if (!map) return [];
  const entries: EncodedMapEntry<V>[] = [];
  if (map instanceof Map) {
    for (const [key, value] of map.entries()) {
      entries.push({ key, keyBytes: utf8Encoder.encode(key), value });
    }
  } else {
    const keys = Object.keys(map as Record<string, V>);
    for (let i = 0; i < keys.length; i++) {
      const key = keys[i]!;
      entries.push({
        key,
        keyBytes: utf8Encoder.encode(key),
        value: (map as Record<string, V>)[key]!,
      });
    }
  }
  entries.sort((x, y) => compareBytes(x.keyBytes, y.keyBytes));
  return entries;
}

// ---------- Incremental FIPS 180-4 SHA-256 ----------

const SHA256_K = new Uint32Array([
  0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1,
  0x923f82a4, 0xab1c5ed5, 0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3,
  0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174, 0xe49b69c1, 0xefbe4786,
  0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
  0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147,
  0x06ca6351, 0x14292967, 0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13,
  0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85, 0xa2bfe8a1, 0xa81a664b,
  0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
  0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a,
  0x5b9cca4f, 0x682e6ff3, 0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208,
  0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2,
]);

export function bytesToHex(bytes: Uint8Array): string {
  let out = "";
  for (let i = 0; i < bytes.byteLength; i++) {
    const b = bytes[i]!;
    out += HEX_CHARS[b >>> 4]! + HEX_CHARS[b & 0x0f]!;
  }
  return out;
}

/**
 * Pure incremental SHA-256 state machine: processes arbitrary byte streams in
 * 64-byte blocks with zero payload concatenation.
 */
export class IncrementalSha256 {
  private h0 = 0x6a09e667;
  private h1 = 0xbb67ae85;
  private h2 = 0x3c6ef372;
  private h3 = 0xa54ff53a;
  private h4 = 0x510e527f;
  private h5 = 0x9b05688c;
  private h6 = 0x1f83d9ab;
  private h7 = 0x5be0cd19;

  private readonly block = new Uint8Array(64);
  private readonly w = new Uint32Array(64);
  private blockLen = 0;
  private totalBytes = 0;
  private finalized = false;

  update(data: Uint8Array): this {
    if (this.finalized) {
      throw new Error("IncrementalSha256 already finalized");
    }
    let offset = 0;
    let remaining = data.byteLength;
    if (remaining === 0) return this;
    this.totalBytes += remaining;

    if (this.blockLen > 0) {
      const need = 64 - this.blockLen;
      if (remaining < need) {
        this.block.set(data, this.blockLen);
        this.blockLen += remaining;
        return this;
      }
      this.block.set(data.subarray(0, need), this.blockLen);
      this.compressBlock(this.block, 0);
      this.blockLen = 0;
      offset += need;
      remaining -= need;
    }

    while (remaining >= 64) {
      this.compressBlock(data, offset);
      offset += 64;
      remaining -= 64;
    }

    if (remaining > 0) {
      this.block.set(data.subarray(offset, offset + remaining), 0);
      this.blockLen = remaining;
    }
    return this;
  }

  private compressBlock(buf: Uint8Array, offset: number): void {
    const w = this.w;
    for (let i = 0; i < 16; i++) {
      const j = offset + i * 4;
      w[i] =
        ((buf[j]! << 24) |
          (buf[j + 1]! << 16) |
          (buf[j + 2]! << 8) |
          buf[j + 3]!) >>>
        0;
    }
    for (let i = 16; i < 64; i++) {
      const w15 = w[i - 15]!;
      const s0 =
        (((w15 >>> 7) | (w15 << 25)) ^
          ((w15 >>> 18) | (w15 << 14)) ^
          (w15 >>> 3)) >>>
        0;
      const w2 = w[i - 2]!;
      const s1 =
        (((w2 >>> 17) | (w2 << 15)) ^
          ((w2 >>> 19) | (w2 << 13)) ^
          (w2 >>> 10)) >>>
        0;
      w[i] = (w[i - 16]! + s0 + w[i - 7]! + s1) >>> 0;
    }

    let a = this.h0;
    let b = this.h1;
    let c = this.h2;
    let d = this.h3;
    let e = this.h4;
    let f = this.h5;
    let g = this.h6;
    let h = this.h7;

    for (let i = 0; i < 64; i++) {
      const S1 =
        (((e >>> 6) | (e << 26)) ^
          ((e >>> 11) | (e << 21)) ^
          ((e >>> 25) | (e << 7))) >>>
        0;
      const ch = ((e & f) ^ (~e & g)) >>> 0;
      const temp1 = (h + S1 + ch + SHA256_K[i]! + w[i]!) >>> 0;
      const S0 =
        (((a >>> 2) | (a << 30)) ^
          ((a >>> 13) | (a << 19)) ^
          ((a >>> 22) | (a << 10))) >>>
        0;
      const maj = ((a & b) ^ (a & c) ^ (b & c)) >>> 0;
      const temp2 = (S0 + maj) >>> 0;

      h = g;
      g = f;
      f = e;
      e = (d + temp1) >>> 0;
      d = c;
      c = b;
      b = a;
      a = (temp1 + temp2) >>> 0;
    }

    this.h0 = (this.h0 + a) >>> 0;
    this.h1 = (this.h1 + b) >>> 0;
    this.h2 = (this.h2 + c) >>> 0;
    this.h3 = (this.h3 + d) >>> 0;
    this.h4 = (this.h4 + e) >>> 0;
    this.h5 = (this.h5 + f) >>> 0;
    this.h6 = (this.h6 + g) >>> 0;
    this.h7 = (this.h7 + h) >>> 0;
  }

  digest(): Uint8Array {
    if (this.finalized) {
      throw new Error("IncrementalSha256 already finalized");
    }
    this.finalized = true;

    const totalBits = BigInt(this.totalBytes) << 3n;
    this.block[this.blockLen++] = 0x80;
    if (this.blockLen > 56) {
      this.block.fill(0, this.blockLen, 64);
      this.compressBlock(this.block, 0);
      this.blockLen = 0;
    }
    this.block.fill(0, this.blockLen, 56);
    const dv = new DataView(
      this.block.buffer,
      this.block.byteOffset,
      this.block.byteLength,
    );
    dv.setBigUint64(56, totalBits, false);
    this.compressBlock(this.block, 0);

    const out = new Uint8Array(32);
    const outDv = new DataView(out.buffer);
    outDv.setUint32(0, this.h0, false);
    outDv.setUint32(4, this.h1, false);
    outDv.setUint32(8, this.h2, false);
    outDv.setUint32(12, this.h3, false);
    outDv.setUint32(16, this.h4, false);
    outDv.setUint32(20, this.h5, false);
    outDv.setUint32(24, this.h6, false);
    outDv.setUint32(28, this.h7, false);
    return out;
  }

  digestHex(): string {
    return bytesToHex(this.digest());
  }
}

// ---------- Streaming SHA-256 Sink (`crypto.DigestStream` in workerd, incremental fallback in Node) ----------

interface DigestStreamLike extends WritableStream<BufferSource> {
  readonly digest: Promise<ArrayBuffer>;
}

type DigestStreamConstructor = new (algorithm: string) => DigestStreamLike;

export interface AsyncSha256Sink {
  readonly usesDigestStream: boolean;
  update(chunk: Uint8Array): Promise<void>;
  digestHex(): Promise<string>;
}

/**
 * Creates a streaming SHA-256 sink. In Cloudflare `workerd`, delegates to native
 * `crypto.DigestStream("SHA-256")`; in plain Node 24, falls back to `IncrementalSha256`
 * so multi-MiB payloads are hashed chunk-by-chunk without concatenation.
 */
export function createAsyncSha256Sink(options?: {
  preferDigestStream?: boolean;
}): AsyncSha256Sink {
  const prefer = options?.preferDigestStream ?? true;
  const DigestStreamCtor = prefer
    ? (
        globalThis as unknown as {
          crypto?: { DigestStream?: DigestStreamConstructor };
        }
      ).crypto?.DigestStream
    : undefined;

  if (typeof DigestStreamCtor === "function") {
    const ds = new DigestStreamCtor("SHA-256");
    const writer = ds.getWriter();
    return {
      usesDigestStream: true,
      async update(chunk: Uint8Array): Promise<void> {
        if (chunk.byteLength > 0) {
          await writer.write(chunk);
        }
      },
      async digestHex(): Promise<string> {
        await writer.close();
        const buf = await ds.digest;
        return bytesToHex(new Uint8Array(buf));
      },
    };
  }

  const fallback = new IncrementalSha256();
  return {
    usesDigestStream: false,
    async update(chunk: Uint8Array): Promise<void> {
      fallback.update(chunk);
    },
    async digestHex(): Promise<string> {
      return fallback.digestHex();
    },
  };
}

export type ByteChunkSource =
  | Uint8Array
  | Iterable<Uint8Array>
  | AsyncIterable<Uint8Array>
  | ReadableStream<Uint8Array>;

async function* iterateChunks(
  source: ByteChunkSource,
): AsyncGenerator<Uint8Array, void, undefined> {
  if (source instanceof Uint8Array) {
    if (source.byteLength > 0) yield source;
    return;
  }
  if (
    typeof ReadableStream !== "undefined" &&
    source instanceof ReadableStream
  ) {
    const reader = source.getReader();
    try {
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        if (value && value.byteLength > 0) {
          yield value;
        }
      }
    } finally {
      reader.releaseLock();
    }
    return;
  }
  if (Symbol.asyncIterator in source) {
    for await (const chunk of source as AsyncIterable<Uint8Array>) {
      if (chunk.byteLength > 0) {
        yield chunk;
      }
    }
    return;
  }
  for (const chunk of source as Iterable<Uint8Array>) {
    if (chunk.byteLength > 0) {
      yield chunk;
    }
  }
}

/** Incrementally hashes a chunk stream into a 64-character lowercase SHA-256 hex digest. */
export async function sha256HexStream(
  source: ByteChunkSource,
  options?: { preferDigestStream?: boolean },
): Promise<string> {
  const sink = createAsyncSha256Sink(options);
  for await (const chunk of iterateChunks(source)) {
    await sink.update(chunk);
  }
  return sink.digestHex();
}

export function sha256Hex(data: Uint8Array | string): string {
  const bytes = typeof data === "string" ? utf8Encoder.encode(data) : data;
  return new IncrementalSha256().update(bytes).digestHex();
}

// ---------- Synchronous Canonical Fingerprint Builder ----------

function makeU64Le(value: bigint | number): Uint8Array {
  const buf = new Uint8Array(8);
  const dv = new DataView(buf.buffer);
  dv.setBigUint64(
    0,
    BigInt.asUintN(64, typeof value === "bigint" ? value : BigInt(value)),
    true,
  );
  return buf;
}

class SyncCanonical {
  private readonly sha = new IncrementalSha256();
  private readonly u64Buf = new Uint8Array(8);
  private readonly u64Dv = new DataView(this.u64Buf.buffer);

  constructor(domain: string) {
    this.str(domain);
  }

  u64(value: bigint | number): void {
    this.u64Dv.setBigUint64(
      0,
      BigInt.asUintN(64, typeof value === "bigint" ? value : BigInt(value)),
      true,
    );
    this.sha.update(this.u64Buf);
  }

  bytes(value: Uint8Array): void {
    this.u64(value.byteLength);
    this.sha.update(value);
  }

  rawBytesWithLength(
    byteLength: bigint | number,
    chunks: readonly Uint8Array[],
  ): void {
    const expected =
      typeof byteLength === "bigint" ? byteLength : BigInt(byteLength);
    this.u64(expected);
    let actual = 0n;
    for (let i = 0; i < chunks.length; i++) {
      const c = chunks[i]!;
      actual += BigInt(c.byteLength);
      this.sha.update(c);
    }
    if (actual !== expected) {
      throw new Error(
        `canonical inline object length mismatch: declared ${expected}, got ${actual}`,
      );
    }
  }

  str(value: string): void {
    this.bytes(utf8Encoder.encode(value));
  }

  strPreEncoded(keyBytes: Uint8Array): void {
    this.u64(keyBytes.byteLength);
    this.sha.update(keyBytes);
  }

  private entries(sorted: EncodedMapEntry<string>[]): void {
    this.u64(sorted.length);
    for (let i = 0; i < sorted.length; i++) {
      const entry = sorted[i]!;
      this.strPreEncoded(entry.keyBytes);
      this.str(entry.value);
    }
  }

  map(record: Record<string, string> | undefined): void {
    this.entries(sortMapByUtf8Bytes(record));
  }

  filteredMap(
    record: Record<string, string> | undefined,
    include: (key: string) => boolean,
  ): void {
    this.entries(sortMapByUtf8Bytes(record).filter((e) => include(e.key)));
  }

  object(obj: CanonicalObjectInput): void {
    const norm = normalizeSyncObject(obj);
    if (norm.kind === "inline") {
      this.str("inline");
      if (norm.chunks) {
        this.rawBytesWithLength(norm.byteLength, norm.chunks);
      } else {
        this.bytes(norm.data);
      }
      this.map(norm.metadata);
    } else {
      const shaMeta = norm.metadata?.[OBJECT_SHA256_METADATA_KEY];
      if (shaMeta !== undefined && shaMeta.length > 0) {
        this.str("uri_digest");
        this.str(shaMeta);
        this.str(norm.metadata?.[OBJECT_SIZE_METADATA_KEY] ?? "");
        this.filteredMap(
          norm.metadata,
          (key) =>
            key !== OBJECT_SHA256_METADATA_KEY &&
            key !== OBJECT_SIZE_METADATA_KEY,
        );
      } else {
        this.str("uri");
        this.str(norm.uri);
        this.map(norm.metadata);
      }
    }
  }

  finish(): string {
    return this.sha.digestHex();
  }
}

// ---------- Object Input Normalization (Sync & Streaming) ----------

export type CanonicalObjectInput =
  | ProtoObjectInput
  | {
      kind: "inline";
      data: Uint8Array;
      metadata?: Record<string, string>;
    }
  | {
      kind: "inline_chunks";
      byteLength: number | bigint;
      chunks: readonly Uint8Array[];
      metadata?: Record<string, string>;
    }
  | {
      kind: "uri";
      uri: string;
      metadata?: Record<string, string>;
    };

export type StreamingCanonicalObjectInput =
  | CanonicalObjectInput
  | {
      kind: "inline_stream";
      byteLength: number | bigint;
      chunks: ByteChunkSource;
      metadata?: Record<string, string>;
    };

type NormalizedSyncObject =
  | {
      kind: "inline";
      data: Uint8Array;
      chunks?: readonly Uint8Array[];
      byteLength: number | bigint;
      metadata: Record<string, string> | undefined;
    }
  | {
      kind: "uri";
      uri: string;
      metadata: Record<string, string> | undefined;
    };

function normalizeSyncObject(obj: CanonicalObjectInput): NormalizedSyncObject {
  if ("kind" in obj) {
    if (obj.kind === "inline") {
      return {
        kind: "inline",
        data: obj.data,
        byteLength: obj.data.byteLength,
        metadata: obj.metadata,
      };
    }
    if (obj.kind === "inline_chunks") {
      return {
        kind: "inline",
        data: new Uint8Array(0),
        chunks: obj.chunks,
        byteLength: obj.byteLength,
        metadata: obj.metadata,
      };
    }
    return {
      kind: "uri",
      uri: obj.uri,
      metadata: obj.metadata,
    };
  }
  if (!obj.body) {
    throw new Error("Object body is required for canonical hashing");
  }
  if (obj.body.case === "inline") {
    return {
      kind: "inline",
      data: obj.body.value,
      byteLength: obj.body.value.byteLength,
      metadata: obj.metadata,
    };
  }
  return {
    kind: "uri",
    uri: obj.body.value,
    metadata: obj.metadata,
  };
}

export type ResolvedDurability =
  | Durability.EPHEMERAL
  | Durability.DURABLE
  | "ephemeral"
  | "durable";

function durabilityLabel(durability: ResolvedDurability): "ephemeral" | "durable" {
  if (durability === Durability.EPHEMERAL || durability === "ephemeral") {
    return "ephemeral";
  }
  if (durability === Durability.DURABLE || durability === "durable") {
    return "durable";
  }
  throw new Error(`Invalid resolved durability: ${String(durability)}`);
}

// ---------- `specHash` ("anyserve.job.v1") ----------

/**
 * Computes the canonical idempotency fingerprint (`"anyserve.job.v1"`) with
 * byte-exact parity against `crates/anyserve-core/src/runtime/identity.rs::spec_hash`.
 */
export function specHash(
  spec: JobSpecInput,
  durability: ResolvedDurability,
): string {
  const hash = new SyncCanonical(JOB_SPEC_HASH_DOMAIN);
  const demand = spec.demand;
  hash.str(demand?.interface ?? "");
  hash.map(demand?.requiredAttributes);
  hash.map(demand?.preferredAttributes);

  const capSorted = sortMapByUtf8Bytes(demand?.requiredCapacity);
  hash.u64(capSorted.length);
  for (let i = 0; i < capSorted.length; i++) {
    const entry = capSorted[i]!;
    hash.strPreEncoded(entry.keyBytes);
    hash.u64(entry.value);
  }

  const inputs = spec.inputs ?? [];
  hash.u64(inputs.length);
  for (let i = 0; i < inputs.length; i++) {
    hash.object(inputs[i]!);
  }

  hash.bytes(spec.params ?? new Uint8Array(0));

  const policy = spec.policy;
  hash.str(durabilityLabel(durability));
  hash.u64((policy?.priority ?? 0) >>> 0);
  hash.u64(policy?.queueTimeoutMs ?? 0n);
  hash.u64(policy?.executionTimeoutMs ?? 0n);
  hash.u64(policy?.totalTimeoutMs ?? 0n);
  hash.u64(policy?.spilloverAfterMs ?? 0n);
  hash.u64((policy?.maxAttempts ?? 0) >>> 0);

  hash.filteredMap(spec.metadata, (key) => !EXCLUDED_METADATA_SET.has(key));
  return hash.finish();
}

// ---------- `outcomeDigest` ("anyserve.result.v1") ----------

/**
 * Computes the canonical outcome digest (`"anyserve.result.v1"`) with byte-exact
 * parity against `crates/anyserve-core/src/runtime/identity.rs::outcome_digest`.
 */
export function outcomeDigest(
  outputs: readonly CanonicalObjectInput[],
  error?: ErrorInfoInput | null,
): string {
  const hash = new SyncCanonical(OUTCOME_DIGEST_DOMAIN);
  if (!error) {
    hash.str("succeeded");
    hash.u64(outputs.length);
    for (let i = 0; i < outputs.length; i++) {
      hash.object(outputs[i]!);
    }
  } else {
    hash.str("failed");
    hash.str(error.code ?? "");
    hash.str(error.message ?? "");
    hash.u64(error.retryable ? 1n : 0n);
  }
  return hash.finish();
}

// ---------- Async Streaming Canonical Builder (`specHashStream` / `outcomeDigestStream`) ----------

class AsyncCanonical {
  private readonly sink: AsyncSha256Sink;

  constructor(options?: { preferDigestStream?: boolean }) {
    this.sink = createAsyncSha256Sink(options);
  }

  get usesDigestStream(): boolean {
    return this.sink.usesDigestStream;
  }

  async init(domain: string): Promise<void> {
    await this.str(domain);
  }

  async u64(value: bigint | number): Promise<void> {
    await this.sink.update(makeU64Le(value));
  }

  async bytes(value: Uint8Array): Promise<void> {
    await this.u64(value.byteLength);
    if (value.byteLength > 0) {
      await this.sink.update(value);
    }
  }

  async str(value: string): Promise<void> {
    await this.bytes(utf8Encoder.encode(value));
  }

  async strPreEncoded(keyBytes: Uint8Array): Promise<void> {
    await this.u64(keyBytes.byteLength);
    if (keyBytes.byteLength > 0) {
      await this.sink.update(keyBytes);
    }
  }

  private async entries(sorted: EncodedMapEntry<string>[]): Promise<void> {
    await this.u64(sorted.length);
    for (let i = 0; i < sorted.length; i++) {
      const entry = sorted[i]!;
      await this.strPreEncoded(entry.keyBytes);
      await this.str(entry.value);
    }
  }

  async map(record: Record<string, string> | undefined): Promise<void> {
    await this.entries(sortMapByUtf8Bytes(record));
  }

  async filteredMap(
    record: Record<string, string> | undefined,
    include: (key: string) => boolean,
  ): Promise<void> {
    await this.entries(sortMapByUtf8Bytes(record).filter((e) => include(e.key)));
  }

  async streamBytesWithLength(
    byteLength: number | bigint,
    source: ByteChunkSource,
  ): Promise<void> {
    const expected =
      typeof byteLength === "bigint" ? byteLength : BigInt(byteLength);
    await this.u64(expected);
    let actual = 0n;
    for await (const chunk of iterateChunks(source)) {
      actual += BigInt(chunk.byteLength);
      await this.sink.update(chunk);
    }
    if (actual !== expected) {
      throw new Error(
        `streaming canonical object length mismatch: declared ${expected}, got ${actual}`,
      );
    }
  }

  async object(obj: StreamingCanonicalObjectInput): Promise<void> {
    if ("kind" in obj && obj.kind === "inline_stream") {
      await this.str("inline");
      await this.streamBytesWithLength(obj.byteLength, obj.chunks);
      await this.map(obj.metadata);
      return;
    }
    const norm = normalizeSyncObject(obj);
    if (norm.kind === "inline") {
      await this.str("inline");
      if (norm.chunks) {
        await this.streamBytesWithLength(norm.byteLength, norm.chunks);
      } else {
        await this.bytes(norm.data);
      }
      await this.map(norm.metadata);
    } else {
      const shaMeta = norm.metadata?.[OBJECT_SHA256_METADATA_KEY];
      if (shaMeta !== undefined && shaMeta.length > 0) {
        await this.str("uri_digest");
        await this.str(shaMeta);
        await this.str(norm.metadata?.[OBJECT_SIZE_METADATA_KEY] ?? "");
        await this.filteredMap(
          norm.metadata,
          (key) =>
            key !== OBJECT_SHA256_METADATA_KEY &&
            key !== OBJECT_SIZE_METADATA_KEY,
        );
      } else {
        await this.str("uri");
        await this.str(norm.uri);
        await this.map(norm.metadata);
      }
    }
  }

  async finish(): Promise<string> {
    return this.sink.digestHex();
  }
}

export interface StreamingJobSpecInput {
  demand?: JobSpecInput["demand"];
  inputs?: readonly StreamingCanonicalObjectInput[];
  params?: Uint8Array | { byteLength: number | bigint; chunks: ByteChunkSource };
  policy?: JobSpecInput["policy"];
  metadata?: Record<string, string>;
}

/**
 * Streaming `specHash` ("anyserve.job.v1") over chunked/R2-backed inputs without
 * concatenating large payloads in memory. Uses `crypto.DigestStream` in workerd.
 */
export async function specHashStream(
  spec: StreamingJobSpecInput,
  durability: ResolvedDurability,
  options?: { preferDigestStream?: boolean },
): Promise<string> {
  const hash = new AsyncCanonical(options);
  await hash.init(JOB_SPEC_HASH_DOMAIN);
  const demand = spec.demand;
  await hash.str(demand?.interface ?? "");
  await hash.map(demand?.requiredAttributes);
  await hash.map(demand?.preferredAttributes);

  const capSorted = sortMapByUtf8Bytes(demand?.requiredCapacity);
  await hash.u64(capSorted.length);
  for (let i = 0; i < capSorted.length; i++) {
    const entry = capSorted[i]!;
    await hash.strPreEncoded(entry.keyBytes);
    await hash.u64(entry.value);
  }

  const inputs = spec.inputs ?? [];
  await hash.u64(inputs.length);
  for (let i = 0; i < inputs.length; i++) {
    await hash.object(inputs[i]!);
  }

  if (!spec.params) {
    await hash.bytes(new Uint8Array(0));
  } else if (spec.params instanceof Uint8Array) {
    await hash.bytes(spec.params);
  } else {
    await hash.streamBytesWithLength(
      spec.params.byteLength,
      spec.params.chunks,
    );
  }

  const policy = spec.policy;
  await hash.str(durabilityLabel(durability));
  await hash.u64((policy?.priority ?? 0) >>> 0);
  await hash.u64(policy?.queueTimeoutMs ?? 0n);
  await hash.u64(policy?.executionTimeoutMs ?? 0n);
  await hash.u64(policy?.totalTimeoutMs ?? 0n);
  await hash.u64(policy?.spilloverAfterMs ?? 0n);
  await hash.u64((policy?.maxAttempts ?? 0) >>> 0);

  await hash.filteredMap(
    spec.metadata,
    (key) => !EXCLUDED_METADATA_SET.has(key),
  );
  return hash.finish();
}

/**
 * Streaming `outcomeDigest` ("anyserve.result.v1") over chunked/segmented output
 * objects (such as multi-MiB R2 upload segments) without concatenating them in JS.
 * Uses `crypto.DigestStream` in workerd and incremental SHA-256 fallback in Node.
 */
export async function outcomeDigestStream(
  outputs: readonly StreamingCanonicalObjectInput[],
  error?: ErrorInfoInput | null,
  options?: { preferDigestStream?: boolean },
): Promise<string> {
  const hash = new AsyncCanonical(options);
  await hash.init(OUTCOME_DIGEST_DOMAIN);
  if (!error) {
    await hash.str("succeeded");
    await hash.u64(outputs.length);
    for (let i = 0; i < outputs.length; i++) {
      await hash.object(outputs[i]!);
    }
  } else {
    await hash.str("failed");
    await hash.str(error.code ?? "");
    await hash.str(error.message ?? "");
    await hash.u64(error.retryable ? 1n : 0n);
  }
  return hash.finish();
}

/**
 * Computes the `ResultAssembler` byte accounting for one result object matching
 * `crates/anyserve-core/src/worker_service.rs`:
 * `OBJECT_OVERHEAD (64) + sum(utf8(k).len + utf8(v).len) + utf8(uri).len + dataBytes`.
 */
export function resultObjectAccountedBytes(params: {
  metadata?: Record<string, string>;
  uri?: string;
  dataByteLength?: number | bigint;
}): bigint {
  let metadataBytes = 0n;
  if (params.metadata) {
    const keys = Object.keys(params.metadata);
    for (let i = 0; i < keys.length; i++) {
      const k = keys[i]!;
      const v = params.metadata[k]!;
      metadataBytes += BigInt(utf8ByteLen(k) + utf8ByteLen(v));
    }
  }
  const uriBytes = params.uri ? BigInt(utf8ByteLen(params.uri)) : 0n;
  const dataBytes =
    params.dataByteLength !== undefined
      ? typeof params.dataByteLength === "bigint"
        ? params.dataByteLength
        : BigInt(params.dataByteLength)
      : 0n;
  return BigInt(OBJECT_OVERHEAD) + metadataBytes + uriBytes + dataBytes;
}
