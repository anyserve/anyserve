// Single-buffer protobuf wire helpers for streaming large `ExecutionFinished` frames.

import { sortMapByUtf8Bytes } from "./identity";
import { type BundleManifestEntry } from "./store";
import { utf8ByteLen } from "./utf8";

const utf8Encoder = new TextEncoder();

// ---------------------------------------------------------------------------
// Single-Buffer Protobuf Wire Helpers for Streaming Large `ExecutionFinished`
// ---------------------------------------------------------------------------

export function varintLen32(v: number): number {
  const u = v >>> 0;
  if (u < 0x80) return 1;
  if (u < 0x4000) return 2;
  if (u < 0x200000) return 3;
  if (u < 0x10000000) return 4;
  return 5;
}

export function varintLen64(v: bigint): number {
  let u = BigInt.asUintN(64, v);
  let n = 1;
  while (u >= 0x80n) {
    u >>= 7n;
    n += 1;
  }
  return n;
}

export function writeVarint32At(buf: Uint8Array, pos: number, v: number): number {
  let u = v >>> 0;
  while (u >= 0x80) {
    buf[pos++] = (u & 0x7f) | 0x80;
    u >>>= 7;
  }
  buf[pos++] = u;
  return pos;
}

export function writeVarint64At(buf: Uint8Array, pos: number, v: bigint): number {
  let u = BigInt.asUintN(64, v);
  while (u >= 0x80n) {
    buf[pos++] = Number(u & 0x7fn) | 0x80;
    u >>= 7n;
  }
  buf[pos++] = Number(u);
  return pos;
}

export function lenDelimitedSize(tagBytes: number, bodyLen: number): number {
  return tagBytes + varintLen32(bodyLen) + bodyLen;
}

export function stringMapEncodedSize(
  tagBytes: number,
  map: Record<string, string> | undefined,
): number {
  if (!map) return 0;
  const sorted = sortMapByUtf8Bytes(map);
  let total = 0;
  for (const e of sorted) {
    const valBytes = utf8ByteLen(e.value);
    const entryLen =
      (e.keyBytes.byteLength > 0 ? lenDelimitedSize(1, e.keyBytes.byteLength) : 0) +
      (valBytes > 0 ? lenDelimitedSize(1, valBytes) : 0);
    total += lenDelimitedSize(tagBytes, entryLen);
  }
  return total;
}

export function writeStringMapAt(
  buf: Uint8Array,
  pos: number,
  fieldTag: number,
  map: Record<string, string> | undefined,
): number {
  if (!map) return pos;
  const sorted = sortMapByUtf8Bytes(map);
  for (const e of sorted) {
    const valBytes = utf8Encoder.encode(e.value);
    const entryLen =
      (e.keyBytes.byteLength > 0 ? lenDelimitedSize(1, e.keyBytes.byteLength) : 0) +
      (valBytes.byteLength > 0 ? lenDelimitedSize(1, valBytes.byteLength) : 0);
    pos = writeVarint32At(buf, pos, fieldTag);
    pos = writeVarint32At(buf, pos, entryLen);
    if (e.keyBytes.byteLength > 0) {
      pos = writeVarint32At(buf, pos, (1 << 3) | 2);
      pos = writeVarint32At(buf, pos, e.keyBytes.byteLength);
      buf.set(e.keyBytes, pos);
      pos += e.keyBytes.byteLength;
    }
    if (valBytes.byteLength > 0) {
      pos = writeVarint32At(buf, pos, (2 << 3) | 2);
      pos = writeVarint32At(buf, pos, valBytes.byteLength);
      buf.set(valBytes, pos);
      pos += valBytes.byteLength;
    }
  }
  return pos;
}

export interface OutputManifestItemRef {
  seq: number;
  entry: BundleManifestEntry;
}

export function encodedObjectLenFromManifestEntry(entry: BundleManifestEntry): number {
  if (entry.t === "u") {
    const uLen = utf8ByteLen(entry.u);
    return lenDelimitedSize(1, uLen) + stringMapEncodedSize(1, entry.m);
  }
  const meta = "m" in entry ? entry.m : undefined;
  return lenDelimitedSize(1, entry.l) + stringMapEncodedSize(1, meta);
}
