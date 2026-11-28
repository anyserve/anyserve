/**
 * Hand-written zero-eval Protobuf v3 codec for `crates/anyserve-proto/protos/execution.proto`
 * (`package anyserve.v2`), plus AnyServe WebSocket binary frame helpers.
 *
 * Compatible with both Cloudflare `workerd` (strict CSP: no dynamic code generation)
 * and plain Node 24, with zero host-specific runtime imports.
 *
 * Messages are described once by compact field descriptors (`def`) that drive a shared
 * length/write/read interpreter (no `eval` / `new Function`). Hot wire messages
 * (Frame, ResultChunk, InputData, Ack, Object) use hand-written direct codecs.
 * Encoding is two-pass: `lenMsg` pre-sizes the single output buffer and records every
 * nested message length so `writeMsg` never recomputes them; map keys are sorted once,
 * only while writing.
 */

const WIRE_VARINT = 0;
const WIRE_64BIT = 1;
const WIRE_LEN = 2;
const WIRE_32BIT = 5;

const utf8Encoder = new TextEncoder();
const fatalUtf8Decoder = new TextDecoder("utf-8", { fatal: true, ignoreBOM: false });
const EMPTY_BYTES = new Uint8Array(0);
const MAX_SAFE_BIG = 0x1fffffffffffffn;
// Module-level 8-byte scratch for IEEE-754 float64 reads/writes without per-message DataView allocation.
const F64_BYTES = new Uint8Array(8);
const F64_VIEW = new DataView(F64_BYTES.buffer);

export class ProtoDecodeError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "ProtoDecodeError";
  }
}

// ---------- Enums ----------

export enum Durability {
  DURABILITY_UNSPECIFIED = 0,
  EPHEMERAL = 1,
  DURABLE = 2,
}

export enum JobState {
  JOB_STATE_UNSPECIFIED = 0,
  QUEUED = 1,
  RUNNING = 2,
  SUCCEEDED = 3,
  FAILED = 4,
  CANCELLED = 5,
}

export enum ConflictPolicy {
  CONFLICT_POLICY_UNSPECIFIED = 0,
  FAIL = 1,
  USE_EXISTING = 2,
}

export enum AttemptFinishedStatus {
  STATUS_UNSPECIFIED = 0,
  COMMITTED = 1,
  FENCED = 2,
  REJECTED = 3,
}

export { AttemptFinishedStatus as AttemptFinished_Status };

// ---------- Message Interfaces (Decoded & Input) ----------

export type ObjectBody = { case: "inline"; value: Uint8Array } | { case: "uri"; value: string };
export interface ProtoObject { body?: ObjectBody; metadata: Record<string, string> }
export type { ProtoObject as Object };
export interface ProtoObjectInput { body?: ObjectBody; metadata?: Record<string, string> }

export interface Demand {
  interface: string;
  requiredAttributes: Record<string, string>;
  preferredAttributes: Record<string, string>;
  requiredCapacity: Record<string, bigint>;
  affinityKey: string;
}
export interface DemandInput {
  interface?: string;
  requiredAttributes?: Record<string, string>;
  preferredAttributes?: Record<string, string>;
  requiredCapacity?: Record<string, bigint>;
  affinityKey?: string;
}

export interface ExecutionPolicy {
  durability: Durability;
  priority: number;
  queueTimeoutMs: bigint;
  executionTimeoutMs: bigint;
  totalTimeoutMs: bigint;
  spilloverAfterMs: bigint;
  maxAttempts: number;
  fairnessKey: string;
  concurrencyKey: string;
}
export interface ExecutionPolicyInput {
  durability?: Durability;
  priority?: number;
  queueTimeoutMs?: bigint;
  executionTimeoutMs?: bigint;
  totalTimeoutMs?: bigint;
  spilloverAfterMs?: bigint;
  maxAttempts?: number;
  fairnessKey?: string;
  concurrencyKey?: string;
}

export interface JobSpec {
  demand?: Demand;
  inputs: ProtoObject[];
  params: Uint8Array;
  policy?: ExecutionPolicy;
  metadata: Record<string, string>;
}
export interface JobSpecInput {
  demand?: DemandInput;
  inputs?: readonly ProtoObjectInput[];
  params?: Uint8Array;
  policy?: ExecutionPolicyInput;
  metadata?: Record<string, string>;
}

export interface ErrorInfo { code: string; message: string; retryable: boolean }
export interface ErrorInfoInput { code?: string; message?: string; retryable?: boolean }

export interface Job {
  jobId: string;
  state: JobState;
  durability: Durability;
  interface: string;
  attemptCount: number;
  currentAttemptId: string;
  currentWorkerId: string;
  createdAtMs: bigint;
  updatedAtMs: bigint;
  error?: ErrorInfo;
  outputs: ProtoObject[];
  metadata: Record<string, string>;
  specHash: string;
  namespace: string;
}
export interface JobInput {
  jobId?: string;
  state?: JobState;
  durability?: Durability;
  interface?: string;
  attemptCount?: number;
  currentAttemptId?: string;
  currentWorkerId?: string;
  createdAtMs?: bigint;
  updatedAtMs?: bigint;
  error?: ErrorInfoInput;
  outputs?: readonly ProtoObjectInput[];
  metadata?: Record<string, string>;
  specHash?: string;
  namespace?: string;
}

export interface SubmitRequest { jobId: string; conflictPolicy: ConflictPolicy; spec?: JobSpec }
export interface SubmitRequestInput { jobId?: string; conflictPolicy?: ConflictPolicy; spec?: JobSpecInput }
export interface SubmitResponse { job?: Job; created: boolean }
export interface SubmitResponseInput { job?: JobInput; created?: boolean }

export interface InvokeStart { spec?: JobSpec; streamingInput: boolean }
export interface InvokeStartInput { spec?: JobSpecInput; streamingInput?: boolean }
export interface InputChunk { data: Uint8Array; metadata: Record<string, string> }
export interface InputChunkInput { data?: Uint8Array; metadata?: Record<string, string> }
export interface InputEnd {}

export type InvokeRequestMsg =
  | { case: "start"; value: InvokeStart }
  | { case: "input"; value: InputChunk }
  | { case: "inputEnd"; value: InputEnd };
export type InvokeRequestMsgInput =
  | { case: "start"; value: InvokeStartInput }
  | { case: "input"; value: InputChunkInput }
  | { case: "inputEnd"; value?: InputEnd };
export interface InvokeRequest { msg?: InvokeRequestMsg }
export interface InvokeRequestInput { msg?: InvokeRequestMsgInput }

export interface Accepted {}
export interface Assigned { workerId: string; attemptNumber: number }
export interface AssignedInput { workerId?: string; attemptNumber?: number }
export interface Output { data: Uint8Array; metadata: Record<string, string>; progress: boolean }
export interface OutputInput { data?: Uint8Array; metadata?: Record<string, string>; progress?: boolean }
export interface AttemptRestarted { previousAttemptId: string; reason?: ErrorInfo }
export interface AttemptRestartedInput { previousAttemptId?: string; reason?: ErrorInfoInput }
export interface ExecutionFinished { state: JobState; outputs: ProtoObject[]; error?: ErrorInfo }

/**
 * Allows segmented/chunked inline bytes (`readonly Uint8Array[]`) when pre-sizing
 * and writing large `ExecutionFinished` frames directly into one buffer without
 * concatenating multi-MiB objects first.
 */
export type SegmentedObjectBody =
  | { case: "inline"; value: Uint8Array | readonly Uint8Array[] }
  | { case: "uri"; value: string };
export interface SegmentedObjectInput { body?: SegmentedObjectBody; metadata?: Record<string, string> }
export interface ExecutionFinishedInput {
  state?: JobState;
  outputs?: readonly SegmentedObjectInput[];
  error?: ErrorInfoInput;
}

export type ExecutionEventVariant =
  | { case: "accepted"; value: Accepted }
  | { case: "assigned"; value: Assigned }
  | { case: "output"; value: Output }
  | { case: "attemptRestarted"; value: AttemptRestarted }
  | { case: "finished"; value: ExecutionFinished };
export type ExecutionEventVariantInput =
  | { case: "accepted"; value?: Accepted }
  | { case: "assigned"; value: AssignedInput }
  | { case: "output"; value: OutputInput }
  | { case: "attemptRestarted"; value: AttemptRestartedInput }
  | { case: "finished"; value: ExecutionFinishedInput };

export interface ExecutionEvent {
  jobId: string;
  sequence: bigint;
  attemptId: string;
  atMs: bigint;
  event?: ExecutionEventVariant;
}
export interface ExecutionEventInput {
  jobId?: string;
  sequence?: bigint;
  attemptId?: string;
  atMs?: bigint;
  event?: ExecutionEventVariantInput;
}
export interface ExecutionFinishedFrameInput {
  jobId?: string;
  sequence?: bigint;
  attemptId?: string;
  atMs?: bigint;
  state?: JobState;
  outputs?: readonly SegmentedObjectInput[];
  error?: ErrorInfoInput;
}

export interface WatchRequest { jobId: string; afterSequence: bigint }
export interface WatchRequestInput { jobId?: string; afterSequence?: bigint }
export interface GetJobRequest { jobId: string; includeOutputs: boolean }
export interface GetJobRequestInput { jobId?: string; includeOutputs?: boolean }
export interface CancelRequest { jobId: string; reason: string }
export interface CancelRequestInput { jobId?: string; reason?: string }
export interface ListJobsRequest { states: JobState[]; limit: number; pageToken: string }
export interface ListJobsRequestInput { states?: readonly JobState[]; limit?: number; pageToken?: string }
export interface ListJobsResponse { jobs: Job[]; nextPageToken: string }
export interface ListJobsResponseInput { jobs?: readonly JobInput[]; nextPageToken?: string }

export interface Hello {
  workerId: string;
  resumeSessionId: string;
  interfaces: string[];
  attributes: Record<string, string>;
  totalCapacity: Record<string, bigint>;
  maxConcurrency: number;
  sdkVersion: string;
  heldLeaseIds: string[];
  metadata: Record<string, string>;
  pool: string;
}
export interface HelloInput {
  workerId?: string;
  resumeSessionId?: string;
  interfaces?: readonly string[];
  attributes?: Record<string, string>;
  totalCapacity?: Record<string, bigint>;
  maxConcurrency?: number;
  sdkVersion?: string;
  heldLeaseIds?: readonly string[];
  metadata?: Record<string, string>;
  pool?: string;
}

export interface Credit { freeSlots: number }
export interface CreditInput { freeSlots?: number }
export interface Load { queued: number; running: number; utilization: number; custom: Record<string, number> }
export interface LoadInput { queued?: number; running?: number; utilization?: number; custom?: Record<string, number> }
export interface Heartbeat { load?: Load }
export interface HeartbeatInput { load?: LoadInput }
export interface AssignReply { leaseId: string; accepted: boolean; rejectReason: string }
export interface AssignReplyInput { leaseId?: string; accepted?: boolean; rejectReason?: string }
export interface CancelAck { leaseId: string }
export interface CancelAckInput { leaseId?: string }
export interface Draining { preempting: boolean }
export interface DrainingInput { preempting?: boolean }

export type WorkerMessageVariant =
  | { case: "hello"; value: Hello }
  | { case: "credit"; value: Credit }
  | { case: "heartbeat"; value: Heartbeat }
  | { case: "assignReply"; value: AssignReply }
  | { case: "cancelAck"; value: CancelAck }
  | { case: "draining"; value: Draining };
export type WorkerMessageVariantInput =
  | { case: "hello"; value: HelloInput }
  | { case: "credit"; value: CreditInput }
  | { case: "heartbeat"; value: HeartbeatInput }
  | { case: "assignReply"; value: AssignReplyInput }
  | { case: "cancelAck"; value: CancelAckInput }
  | { case: "draining"; value: DrainingInput };
export interface WorkerMessage { msg?: WorkerMessageVariant }
export interface WorkerMessageInput { msg?: WorkerMessageVariantInput }

export interface Limits {
  maxControlMessageBytes: number;
  maxChunkBytes: number;
  attemptWindowBytes: number;
  assignInlineLimit: number;
}
export interface LimitsInput {
  maxControlMessageBytes?: number;
  maxChunkBytes?: number;
  attemptWindowBytes?: number;
  assignInlineLimit?: number;
}

export interface Welcome {
  sessionId: string;
  generation: bigint;
  heartbeatIntervalMs: number;
  heartbeatTimeoutMs: number;
  limits?: Limits;
  resumedLeaseIds: string[];
}
export interface WelcomeInput {
  sessionId?: string;
  generation?: bigint;
  heartbeatIntervalMs?: number;
  heartbeatTimeoutMs?: number;
  limits?: LimitsInput;
  resumedLeaseIds?: readonly string[];
}

export interface Assign {
  leaseId: string;
  attemptId: string;
  jobId: string;
  attemptNumber: number;
  generation: bigint;
  interface: string;
  params: Uint8Array;
  reservedCapacity: Record<string, bigint>;
  metadata: Record<string, string>;
  inlineInputs: ProtoObject[];
  inputsViaAttempt: boolean;
  streamingInput: boolean;
  executionDeadlineMs: bigint;
  totalDeadlineMs: bigint;
}
export interface AssignInput {
  leaseId?: string;
  attemptId?: string;
  jobId?: string;
  attemptNumber?: number;
  generation?: bigint;
  interface?: string;
  params?: Uint8Array;
  reservedCapacity?: Record<string, bigint>;
  metadata?: Record<string, string>;
  inlineInputs?: readonly ProtoObjectInput[];
  inputsViaAttempt?: boolean;
  streamingInput?: boolean;
  executionDeadlineMs?: bigint;
  totalDeadlineMs?: bigint;
}

export interface Cancel { leaseId: string; reason: string }
export interface CancelInput { leaseId?: string; reason?: string }
export interface Fence { leaseId: string; reason: string }
export interface FenceInput { leaseId?: string; reason?: string }
export interface Drain {}

export type ServerMessageVariant =
  | { case: "welcome"; value: Welcome }
  | { case: "assign"; value: Assign }
  | { case: "cancel"; value: Cancel }
  | { case: "fence"; value: Fence }
  | { case: "drain"; value: Drain };
export type ServerMessageVariantInput =
  | { case: "welcome"; value: WelcomeInput }
  | { case: "assign"; value: AssignInput }
  | { case: "cancel"; value: CancelInput }
  | { case: "fence"; value: FenceInput }
  | { case: "drain"; value?: Drain };
export interface ServerMessage { msg?: ServerMessageVariant }
export interface ServerMessageInput { msg?: ServerMessageVariantInput }

export interface AttemptOpen { leaseId: string; generation: bigint; resumeAfterSequence: bigint }
export interface AttemptOpenInput { leaseId?: string; generation?: bigint; resumeAfterSequence?: bigint }
export interface Frame { sequence: bigint; data: Uint8Array; metadata: Record<string, string>; progress: boolean }
export interface FrameInput { sequence?: bigint; data?: Uint8Array; metadata?: Record<string, string>; progress?: boolean }
export interface ResultChunk {
  index: number;
  data: Uint8Array;
  last: boolean;
  metadata: Record<string, string>;
  uri: string;
}
export interface ResultChunkInput {
  index?: number;
  data?: Uint8Array;
  last?: boolean;
  metadata?: Record<string, string>;
  uri?: string;
}

export interface Succeeded {}
export interface Cancelled {}
export type FinishOutcome =
  | { case: "succeeded"; value: Succeeded }
  | { case: "failed"; value: ErrorInfo }
  | { case: "cancelled"; value: Cancelled };
export type FinishOutcomeInput =
  | { case: "succeeded"; value?: Succeeded }
  | { case: "failed"; value: ErrorInfoInput }
  | { case: "cancelled"; value?: Cancelled };
export interface Finish { outcome?: FinishOutcome; handlerDurationMs?: bigint }
export interface FinishInput { outcome?: FinishOutcomeInput; handlerDurationMs?: bigint }

/** Note: `AttemptUp` oneof field numbers are open=1, frame=2, result=3, finish=5. */
export type AttemptUpVariant =
  | { case: "open"; value: AttemptOpen }
  | { case: "frame"; value: Frame }
  | { case: "result"; value: ResultChunk }
  | { case: "finish"; value: Finish };
export type AttemptUpVariantInput =
  | { case: "open"; value: AttemptOpenInput }
  | { case: "frame"; value: FrameInput }
  | { case: "result"; value: ResultChunkInput }
  | { case: "finish"; value: FinishInput };
export interface AttemptUp { msg?: AttemptUpVariant }
export interface AttemptUpInput { msg?: AttemptUpVariantInput }

export interface AttemptOpened { resumeFromSequence: bigint; terminalReplay: boolean }
export interface AttemptOpenedInput { resumeFromSequence?: bigint; terminalReplay?: boolean }

export interface InputData {
  sequence: bigint;
  data: Uint8Array;
  last: boolean;
  metadata: Record<string, string>;
  /** Proto3 `optional`: `0` emits `28 00`, `undefined` omits field 5. */
  objectIndex?: number;
  uri: string;
  parameterChunk: boolean;
  objectEnd: boolean;
}
export interface InputDataInput {
  sequence?: bigint;
  data?: Uint8Array;
  last?: boolean;
  metadata?: Record<string, string>;
  objectIndex?: number;
  uri?: string;
  parameterChunk?: boolean;
  objectEnd?: boolean;
}

export interface Ack { upToSequence: bigint; resultBytes: bigint }
export interface AckInput { upToSequence?: bigint; resultBytes?: bigint }
export interface AttemptFinished { status: AttemptFinishedStatus; reason: string }
export interface AttemptFinishedInput { status?: AttemptFinishedStatus; reason?: string }

export type AttemptDownVariant =
  | { case: "opened"; value: AttemptOpened }
  | { case: "input"; value: InputData }
  | { case: "ack"; value: Ack }
  | { case: "finished"; value: AttemptFinished };
export type AttemptDownVariantInput =
  | { case: "opened"; value: AttemptOpenedInput }
  | { case: "input"; value: InputDataInput }
  | { case: "ack"; value: AckInput }
  | { case: "finished"; value: AttemptFinishedInput };
export interface AttemptDown { msg?: AttemptDownVariant }
export interface AttemptDownInput { msg?: AttemptDownVariantInput }

// ---------- Wire Primitives ----------

/** Zero-allocation exact UTF-8 byte length of `str` (lone surrogates count as 3 bytes, like `TextEncoder`). */
export function utf8ByteLen(str: string): number {
  const len = str.length;
  let bytes = len;
  for (let i = 0; i < len; i++) {
    const code = str.charCodeAt(i);
    if (code < 0x80) continue;
    if (code < 0x800) bytes += 1;
    else if (code >= 0xd800 && code <= 0xdbff && i + 1 < len && (str.charCodeAt(i + 1) & 0xfc00) === 0xdc00) {
      bytes += 2; // 4 bytes for the pair = 2 units + 2 extra
      i++;
    } else bytes += 2;
  }
  return bytes;
}

function varint32Len(value: number): number {
  const u = value >>> 0;
  return u < 0x80 ? 1 : u < 0x4000 ? 2 : u < 0x200000 ? 3 : u < 0x10000000 ? 4 : 5;
}

/** `value` must already be in `[0, 2^64)`. */
function varint64Len(value: bigint): number {
  if (value <= MAX_SAFE_BIG) {
    let n = Number(value);
    let c = 1;
    while (n >= 0x80) {
      n = Math.floor(n / 128);
      c++;
    }
    return c;
  }
  let u = value >> 49n;
  let c = 8;
  while (u >= 0x80n) {
    u >>= 7n;
    c++;
  }
  return c;
}

const tagLen = (no: number): number => varint32Len(no << 3);
const lenTotal = (no: number, inner: number): number => tagLen(no) + varint32Len(inner) + inner;
const enumVarintLen = (v: number): number => ((v | 0) < 0 ? 10 : varint32Len(v));
/** Normalises a uint64 to `[0, 2^64)`, returning 0n when it wraps to zero. */
const norm64 = (v: bigint | undefined): bigint =>
  !v ? 0n : v >= 0n && v <= MAX_SAFE_BIG ? v : BigInt.asUintN(64, v);

const strLen = (no: number, v: string | undefined): number => (v ? lenTotal(no, utf8ByteLen(v)) : 0);
const bytesLen = (no: number, v: Uint8Array | undefined): number => (v && v.byteLength ? lenTotal(no, v.byteLength) : 0);
const u32Len = (no: number, v: number | undefined): number => ((v ?? 0) >>> 0 ? tagLen(no) + varint32Len(v!) : 0);
const u64Len = (no: number, v: bigint | undefined): number => {
  const u = norm64(v);
  return u === 0n ? 0 : tagLen(no) + varint64Len(u);
};
const boolLen = (no: number, v: boolean | undefined): number => (v ? tagLen(no) + 1 : 0);
const enumLen = (no: number, v: number | undefined): number => ((v ?? 0) | 0 ? tagLen(no) + enumVarintLen(v!) : 0);
const dblLen = (no: number, v: number | undefined): number => (v === undefined || v === 0 ? 0 : tagLen(no) + 8);
const rstrLen = (no: number, items: readonly string[] | undefined): number => {
  let n = 0;
  if (items) for (let i = 0; i < items.length; i++) n += lenTotal(no, utf8ByteLen(items[i]!));
  return n;
};
const renumLen = (no: number, items: readonly number[] | undefined): number => {
  if (!items || items.length === 0) return 0;
  let n = 0;
  for (let i = 0; i < items.length; i++) n += enumVarintLen(items[i]!);
  return lenTotal(no, n);
};
/** String-keyed map length (`vk` selects the value codec); unsorted, never allocates. */
function mapLen(no: number, map: Record<string, any> | undefined, vk: number): number {
  if (!map) return 0;
  let total = 0;
  for (const k in map) {
    total += lenTotal(no, strLen(1, k) + mapValLen(map[k], vk));
  }
  return total;
}
function mapValLen(v: any, vk: number): number {
  return vk === SMAP ? strLen(2, v) : vk === IMAP ? u64Len(2, v) : dblLen(2, v);
}

function segLen(value: Uint8Array | readonly Uint8Array[]): number {
  if (value instanceof Uint8Array) return value.byteLength;
  let sum = 0;
  for (let i = 0; i < value.length; i++) sum += value[i]!.byteLength;
  return sum;
}

function compareBytesLex(a: Uint8Array, b: Uint8Array): number {
  const min = a.byteLength < b.byteLength ? a.byteLength : b.byteLength;
  for (let i = 0; i < min; i++) {
    const diff = a[i]! - b[i]!;
    if (diff !== 0) return diff;
  }
  return a.byteLength - b.byteLength;
}

/** Map keys in UTF-8 byte order; 0/1-entry maps return without sorting. Called once per map per encode. */
function sortedMapKeys(record: Record<string, unknown>): string[] {
  const keys = globalThis.Object.keys(record);
  if (keys.length <= 1) return keys;
  for (let i = 0; i < keys.length; i++) {
    const k = keys[i]!;
    for (let j = 0; j < k.length; j++) {
      if (k.charCodeAt(j) >= 0x80) {
        const enc = keys.map((key) => ({ key, b: utf8Encoder.encode(key) }));
        enc.sort((x, y) => compareBytesLex(x.b, y.b));
        return enc.map((e) => e.key);
      }
    }
  }
  return keys.sort(); // all ASCII: UTF-16 order === UTF-8 byte order
}

function setMapProp<V>(target: Record<string, V>, key: string, value: V): void {
  if (key === "__proto__") {
    globalThis.Object.defineProperty(target, key, { value, enumerable: true, writable: true, configurable: true });
  } else {
    target[key] = value;
  }
}

class ProtoWriter {
  readonly buf: Uint8Array;
  readonly lens: number[] | null;
  pos = 0;
  li = 0;

  constructor(size: number, lens: number[] | null) {
    this.buf = new Uint8Array(size);
    this.lens = lens;
  }

  writeByte(b: number): void {
    this.buf[this.pos++] = b & 0xff;
  }

  v32(value: number): void {
    let u = value >>> 0;
    while (u >= 0x80) {
      this.buf[this.pos++] = (u & 0x7f) | 0x80;
      u >>>= 7;
    }
    this.buf[this.pos++] = u;
  }

  /** `value` must already be in `[0, 2^64)`. */
  v64(value: bigint): void {
    if (value <= MAX_SAFE_BIG) {
      let n = Number(value);
      while (n >= 0x80) {
        this.buf[this.pos++] = (n % 128) | 0x80;
        n = Math.floor(n / 128);
      }
      this.buf[this.pos++] = n;
      return;
    }
    let u = value;
    while (u >= 0x80n) {
      this.buf[this.pos++] = Number(u & 0x7fn) | 0x80;
      u >>= 7n;
    }
    this.buf[this.pos++] = Number(u);
  }

  enumVal(value: number): void {
    const v = value | 0;
    if (v < 0) this.v64(BigInt.asUintN(64, BigInt(v)));
    else this.v32(v);
  }

  tag(no: number, wt: number): void {
    this.v32((no << 3) | wt);
  }

  hdr(no: number, len: number): void {
    this.v32((no << 3) | WIRE_LEN);
    this.v32(len);
  }

  raw(bytes: Uint8Array): void {
    if (bytes.byteLength > 0) {
      this.buf.set(bytes, this.pos);
      this.pos += bytes.byteLength;
    }
  }

  rawStr(str: string): void {
    const n = str.length;
    if (n === 0) return;
    const buf = this.buf;
    if (n <= 48) {
      let p = this.pos;
      let i = 0;
      for (; i < n; i++) {
        const c = str.charCodeAt(i);
        if (c >= 0x80) break;
        buf[p++] = c;
      }
      if (i === n) {
        this.pos = p;
        return;
      }
    }
    this.pos += utf8Encoder.encodeInto(str, buf.subarray(this.pos)).written;
  }

  u32(no: number, value: number | undefined): void {
    const u = (value ?? 0) >>> 0;
    if (u === 0) return;
    this.tag(no, WIRE_VARINT);
    this.v32(u);
  }

  u64(no: number, value: bigint | undefined): void {
    const u = norm64(value);
    if (u === 0n) return;
    this.tag(no, WIRE_VARINT);
    this.v64(u);
  }

  bool(no: number, value: boolean | undefined): void {
    if (!value) return;
    this.tag(no, WIRE_VARINT);
    this.buf[this.pos++] = 1;
  }

  enm(no: number, value: number | undefined): void {
    const v = (value ?? 0) | 0;
    if (v === 0) return;
    this.tag(no, WIRE_VARINT);
    this.enumVal(v);
  }

  dbl(no: number, value: number | undefined): void {
    if (value === undefined || value === 0) return;
    this.tag(no, WIRE_64BIT);
    F64_VIEW.setFloat64(0, value, true);
    const buf = this.buf;
    const p = this.pos;
    buf[p] = F64_BYTES[0]!;
    buf[p + 1] = F64_BYTES[1]!;
    buf[p + 2] = F64_BYTES[2]!;
    buf[p + 3] = F64_BYTES[3]!;
    buf[p + 4] = F64_BYTES[4]!;
    buf[p + 5] = F64_BYTES[5]!;
    buf[p + 6] = F64_BYTES[6]!;
    buf[p + 7] = F64_BYTES[7]!;
    this.pos = p + 8;
  }

  str(no: number, value: string | undefined): void {
    if (value) this.astr(no, value);
  }

  /** Always-emitted string (oneof / repeated members). Single-pass fast path for ASCII < 128 bytes. */
  astr(no: number, value: string): void {
    const n = value.length;
    if (n === 0) {
      this.hdr(no, 0);
      return;
    }
    this.v32((no << 3) | WIRE_LEN);
    if (n < 128) {
      const buf = this.buf;
      const lenPos = this.pos;
      let p = lenPos + 1;
      let i = 0;
      for (; i < n; i++) {
        const c = value.charCodeAt(i);
        if (c >= 0x80) break;
        buf[p++] = c;
      }
      if (i === n) {
        buf[lenPos] = n;
        this.pos = p;
        return;
      }
      // Non-ASCII fallback: rewind to lenPos and write UTF-8 length + bytes.
      this.pos = lenPos;
    }
    const bl = utf8ByteLen(value);
    this.v32(bl);
    this.pos += utf8Encoder.encodeInto(value, this.buf.subarray(this.pos)).written;
  }

  bytes(no: number, value: Uint8Array | undefined): void {
    if (!value || value.byteLength === 0) return;
    this.hdr(no, value.byteLength);
    this.raw(value);
  }

  /** Always-emitted bytes, optionally segmented. */
  abytes(no: number, value: Uint8Array | readonly Uint8Array[]): void {
    this.hdr(no, segLen(value));
    if (value instanceof Uint8Array) this.raw(value);
    else for (let i = 0; i < value.length; i++) this.raw(value[i]!);
  }

  rstr(no: number, items: readonly string[] | undefined): void {
    if (items) for (let i = 0; i < items.length; i++) this.astr(no, items[i]!);
  }

  renum(no: number, items: readonly number[] | undefined): void {
    if (!items || items.length === 0) return;
    let n = 0;
    for (let i = 0; i < items.length; i++) n += enumVarintLen(items[i]!);
    this.hdr(no, n);
    for (let i = 0; i < items.length; i++) this.enumVal(items[i]!);
  }

  /** Writes a string-keyed map sorted once by UTF-8 byte order of the keys. */
  map(no: number, map: Record<string, any> | undefined, vk: number): void {
    if (!map) return;
    let firstKey: string | undefined;
    let multi = false;
    for (const k in map) {
      if (firstKey === undefined) {
        firstKey = k;
      } else {
        multi = true;
        break;
      }
    }
    if (firstKey === undefined) return;
    if (!multi) {
      const v = map[firstKey];
      this.hdr(no, strLen(1, firstKey) + mapValLen(v, vk));
      this.str(1, firstKey);
      if (vk === SMAP) this.str(2, v);
      else if (vk === IMAP) this.u64(2, v);
      else this.dbl(2, v);
      return;
    }
    const keys = sortedMapKeys(map);
    for (let i = 0; i < keys.length; i++) {
      const k = keys[i]!;
      const v = map[k];
      this.hdr(no, strLen(1, k) + mapValLen(v, vk));
      this.str(1, k);
      if (vk === SMAP) this.str(2, v);
      else if (vk === IMAP) this.u64(2, v);
      else this.dbl(2, v);
    }
  }
}

class ProtoReader {
  readonly buf: Uint8Array;
  pos = 0;
  /** Field number / wire type of the tag most recently returned by `readTag`. */
  fn = 0;
  wt = 0;

  constructor(buf: Uint8Array) {
    this.buf = buf;
  }

  readVarint64(end: number): bigint {
    const buf = this.buf;
    let p = this.pos;
    if (p >= end) throw new ProtoDecodeError("unexpected EOF while reading varint");
    let b = buf[p++]!;
    if (b < 0x80) {
      this.pos = p;
      return b === 0 ? 0n : BigInt(b);
    }
    let lo = b & 0x7f;
    if (p >= end) throw new ProtoDecodeError("unexpected EOF while reading varint");
    b = buf[p++]!;
    lo |= (b & 0x7f) << 7;
    if (b < 0x80) {
      this.pos = p;
      return BigInt(lo);
    }
    if (p >= end) throw new ProtoDecodeError("unexpected EOF while reading varint");
    b = buf[p++]!;
    lo |= (b & 0x7f) << 14;
    if (b < 0x80) {
      this.pos = p;
      return BigInt(lo);
    }
    if (p >= end) throw new ProtoDecodeError("unexpected EOF while reading varint");
    b = buf[p++]!;
    lo |= (b & 0x7f) << 21;
    if (b < 0x80) {
      this.pos = p;
      return BigInt(lo);
    }
    // Bytes 4..6 (bits 28..48) fit exactly in JS float64 integer precision (<= 2^49 - 1).
    let val = lo;
    if (p >= end) throw new ProtoDecodeError("unexpected EOF while reading varint");
    b = buf[p++]!;
    val += (b & 0x7f) * 0x10000000;
    if (b < 0x80) {
      this.pos = p;
      return BigInt(val);
    }
    if (p >= end) throw new ProtoDecodeError("unexpected EOF while reading varint");
    b = buf[p++]!;
    val += (b & 0x7f) * 0x800000000;
    if (b < 0x80) {
      this.pos = p;
      return BigInt(val);
    }
    if (p >= end) throw new ProtoDecodeError("unexpected EOF while reading varint");
    b = buf[p++]!;
    val += (b & 0x7f) * 0x40000000000;
    if (b < 0x80) {
      this.pos = p;
      return BigInt(val);
    }
    this.pos = p;
    let result = BigInt(val);
    for (let i = 7; i < 10; i++) {
      if (this.pos >= end) throw new ProtoDecodeError("unexpected EOF while reading varint");
      b = buf[this.pos++]!;
      if (i === 9 && b > 0x01) throw new ProtoDecodeError("varint exceeds 64-bit integer range");
      result |= BigInt(b & 0x7f) << BigInt(i * 7);
      if ((b & 0x80) === 0) return result;
    }
    throw new ProtoDecodeError("varint exceeds 10 bytes");
  }

  readTag(end: number): void {
    const buf = this.buf;
    const p = this.pos;
    let tag: number;
    if (p < end && buf[p]! < 0x80) {
      tag = buf[p]!;
      this.pos = p + 1;
    } else if (p + 1 < end && buf[p + 1]! < 0x80) {
      tag = (buf[p]! & 0x7f) | (buf[p + 1]! << 7);
      this.pos = p + 2;
    } else {
      const raw = this.readVarint64(end);
      if (raw > 0xffffffffn) throw new ProtoDecodeError("field tag exceeds 32 bits");
      tag = Number(raw) >>> 0;
    }
    const fn = tag >>> 3;
    const wt = tag & 0x07;
    if (fn === 0) throw new ProtoDecodeError("invalid protobuf field number 0");
    if (wt !== WIRE_VARINT && wt !== WIRE_64BIT && wt !== WIRE_LEN && wt !== WIRE_32BIT) {
      throw new ProtoDecodeError(`invalid protobuf wire type ${wt}`);
    }
    this.fn = fn;
    this.wt = wt;
  }

  want(expected: number): void {
    if (this.wt !== expected) {
      throw new ProtoDecodeError(`unexpected wire type ${this.wt} (expected ${expected})`);
    }
  }

  readSubEnd(end: number): number {
    let len: number;
    const p = this.pos;
    if (p < end && this.buf[p]! < 0x80) {
      len = this.buf[p]!;
      this.pos = p + 1;
    } else {
      const len64 = this.readVarint64(end);
      if (len64 > BigInt(end - this.pos)) {
        throw new ProtoDecodeError("length-delimited field exceeds buffer bounds");
      }
      len = Number(len64);
    }
    if (len > end - this.pos) throw new ProtoDecodeError("length-delimited field exceeds buffer bounds");
    return this.pos + len;
  }

  readUint32(end: number): number {
    const p = this.pos;
    if (p < end && this.buf[p]! < 0x80) {
      this.pos = p + 1;
      return this.buf[p]!;
    }
    return Number(this.readVarint64(end) & 0xffffffffn) >>> 0;
  }

  readUint64(end: number): bigint {
    return this.readVarint64(end);
  }

  readInt64(end: number): bigint {
    return BigInt.asIntN(64, this.readVarint64(end));
  }

  readEnum(end: number): number {
    const p = this.pos;
    if (p < end && this.buf[p]! < 0x80) {
      this.pos = p + 1;
      return this.buf[p]!;
    }
    return Number(BigInt.asIntN(32, this.readVarint64(end)));
  }

  readBool(end: number): boolean {
    const p = this.pos;
    if (p < end && this.buf[p]! < 0x80) {
      this.pos = p + 1;
      return this.buf[p]! !== 0;
    }
    return this.readVarint64(end) !== 0n;
  }

  readDouble(end: number): number {
    const p = this.pos;
    if (p + 8 > end) throw new ProtoDecodeError("unexpected EOF while reading 64-bit double");
    const buf = this.buf;
    F64_BYTES[0] = buf[p]!;
    F64_BYTES[1] = buf[p + 1]!;
    F64_BYTES[2] = buf[p + 2]!;
    F64_BYTES[3] = buf[p + 3]!;
    F64_BYTES[4] = buf[p + 4]!;
    F64_BYTES[5] = buf[p + 5]!;
    F64_BYTES[6] = buf[p + 6]!;
    F64_BYTES[7] = buf[p + 7]!;
    this.pos = p + 8;
    return F64_VIEW.getFloat64(0, true);
  }

  readBytes(end: number): Uint8Array {
    const subEnd = this.readSubEnd(end);
    if (subEnd === this.pos) return EMPTY_BYTES;
    const out = this.buf.slice(this.pos, subEnd);
    this.pos = subEnd;
    return out;
  }

  readString(end: number): string {
    const subEnd = this.readSubEnd(end);
    const start = this.pos;
    if (subEnd === start) return "";
    this.pos = subEnd;
    const buf = this.buf;
    if (subEnd - start <= 24) {
      let s = "";
      let i = start;
      for (; i < subEnd; i++) {
        const c = buf[i]!;
        if (c >= 0x80) break;
        s += String.fromCharCode(c);
      }
      if (i === subEnd) return s;
    }
    try {
      return fatalUtf8Decoder.decode(buf.subarray(start, subEnd));
    } catch {
      throw new ProtoDecodeError("invalid UTF-8 in protobuf string field");
    }
  }

  skipField(wireType: number, end: number): void {
    switch (wireType) {
      case WIRE_VARINT:
        this.readVarint64(end);
        break;
      case WIRE_64BIT:
        if (this.pos + 8 > end) throw new ProtoDecodeError("unexpected EOF skipping 64-bit field");
        this.pos += 8;
        break;
      case WIRE_LEN:
        this.pos = this.readSubEnd(end);
        break;
      case WIRE_32BIT:
        if (this.pos + 4 > end) throw new ProtoDecodeError("unexpected EOF skipping 32-bit field");
        this.pos += 4;
        break;
      default:
        throw new ProtoDecodeError(`unsupported wire type ${wireType}`);
    }
  }

  /** Reads one `map<string, V>` entry (`vk` = SMAP / IMAP / DMAP value codec) into `target`. */
  readMapEntry(end: number, target: Record<string, any>, vk: number): void {
    const subEnd = this.readSubEnd(end);
    let key = "";
    let val: any = vk === SMAP ? "" : vk === IMAP ? 0n : 0;
    while (this.pos < subEnd) {
      this.readTag(subEnd);
      if (this.fn === 1) {
        this.want(WIRE_LEN);
        key = this.readString(subEnd);
      } else if (this.fn === 2) {
        if (vk === SMAP) {
          this.want(WIRE_LEN);
          val = this.readString(subEnd);
        } else if (vk === IMAP) {
          this.want(WIRE_VARINT);
          val = this.readInt64(subEnd);
        } else {
          this.want(WIRE_64BIT);
          val = this.readDouble(subEnd);
        }
      } else {
        this.skipField(this.wt, subEnd);
      }
    }
    setMapProp(target, key, val);
  }
}

// ---------- Schema-driven codec (zero-eval field descriptors) ----------

const STR = 0;
const BYTES = 1;
const U32 = 2;
const U64 = 3;
const BOOL = 4;
const ENUM = 5;
const DBL = 6;
const RSTR = 7;
const SMAP = 8; // map<string,string>
const IMAP = 9; // map<string,int64/uint64>
const DMAP = 10; // map<string,double>
const MSG = 11;
const RMSG = 12;
const RENUM = 13; // packed repeated enum
const OPT32 = 14; // proto3 `optional uint32`
const ONEOF = 15; // `[no, prop, ONEOF, [[no, case, schema], ...]]`
const VARIANT = 16; // internal: decode-side entry for one oneof member
const OPT64 = 17; // proto3 `optional uint64`

/** Expected wire type per kind (index = kind). */
const WIRE_OF = [2, 2, 0, 0, 0, 0, 1, 2, 2, 2, 2, 2, 2, 2, 0, 2, 2, 0];

interface Field {
  no: number;
  name: string;
  kind: number;
  sub: Schema | null;
  /** ONEOF: members by case name; VARIANT: the case name. */
  cases?: Record<string, Field>;
  case?: string;
}

interface Codec {
  len(m: any): number;
  write(w: ProtoWriter, m: any): void;
  read(r: ProtoReader, end: number, out?: any): any;
}

interface Schema {
  fields: Field[];
  byNo: (Field | undefined)[];
  /** Default-value template; containers are replaced with fresh ones by `mk`. */
  tpl: Record<string, unknown>;
  containers: Field[];
  c: Codec | null;
}

type FieldDef = [no: number, name: string, kind: number, sub?: Schema | [no: number, name: string, sub: Schema][]];

function def(defs: FieldDef[], c: Codec | null = null): Schema {
  const s: Schema = { fields: [], byNo: [], tpl: {}, containers: [], c };
  for (const [no, name, kind, sub] of defs) {
    if (kind === ONEOF) {
      const cases: Record<string, Field> = globalThis.Object.create(null);
      for (const [vno, vcase, vsub] of sub as [number, string, Schema][]) {
        cases[vcase] = s.byNo[vno] = { no: vno, name, kind: VARIANT, sub: vsub, case: vcase };
      }
      s.fields.push({ no, name, kind, sub: null, cases });
      continue;
    }
    const f: Field = { no, name, kind, sub: (sub as Schema | undefined) ?? null };
    s.fields.push(f);
    s.byNo[no] = f;
    if (kind === MSG || kind === OPT32 || kind === OPT64) continue;
    if (kind >= RSTR && kind <= RENUM) {
      s.tpl[name] = undefined;
      s.containers.push(f);
    } else s.tpl[name] = kind === STR ? "" : kind === BYTES ? EMPTY_BYTES : kind === U64 ? 0n : kind === BOOL ? false : 0;
  }
  return s;
}

function mk(s: Schema): any {
  const o: any = { ...s.tpl };
  for (const f of s.containers) o[f.name] = f.kind === RSTR || f.kind === RMSG || f.kind === RENUM ? [] : {};
  return o;
}

function lenSub(no: number, s: Schema, m: any, L: number[] | null): number {
  let slot = 0;
  if (L) {
    slot = L.length;
    L.push(0);
  }
  const n = lenMsg(s, m, L);
  if (L) L[slot] = n;
  return lenTotal(no, n);
}

function writeSub(w: ProtoWriter, no: number, s: Schema, m: any): void {
  w.hdr(no, w.lens![w.li++]!);
  writeMsg(w, s, m);
}

/** Exact encoded length; when `L` is given, records nested message lengths in pre-order for `writeMsg`. */
function lenMsg(s: Schema, m: any, L: number[] | null): number {
  if (s.c) return s.c.len(m);
  let n = 0;
  const fields = s.fields;
  for (let i = 0; i < fields.length; i++) {
    const f = fields[i]!;
    const v = m[f.name];
    if (v === undefined) continue;
    const no = f.no;
    switch (f.kind) {
      case STR: n += strLen(no, v); break;
      case BYTES: n += bytesLen(no, v); break;
      case U32: n += u32Len(no, v); break;
      case U64: n += u64Len(no, v); break;
      case BOOL: n += boolLen(no, v); break;
      case ENUM: n += enumLen(no, v); break;
      case DBL: n += dblLen(no, v); break;
      case RSTR: n += rstrLen(no, v); break;
      case SMAP: case IMAP: case DMAP: n += mapLen(no, v, f.kind); break;
      case RENUM: n += renumLen(no, v); break;
      case OPT32: n += tagLen(no) + varint32Len(v >>> 0); break;
      case OPT64: n += tagLen(no) + varint64Len(BigInt(v)); break;
      case MSG: n += lenSub(no, f.sub!, v, L); break;
      case RMSG: for (let j = 0; j < v.length; j++) n += lenSub(no, f.sub!, v[j], L); break;
      case ONEOF: {
        const vf = f.cases![v.case];
        if (vf) n += lenSub(vf.no, vf.sub!, v.value, L);
        break;
      }
    }
  }
  return n;
}

function writeMsg(w: ProtoWriter, s: Schema, m: any): void {
  if (s.c) return s.c.write(w, m);
  const fields = s.fields;
  for (let i = 0; i < fields.length; i++) {
    const f = fields[i]!;
    const v = m[f.name];
    if (v === undefined) continue;
    const no = f.no;
    switch (f.kind) {
      case STR: w.str(no, v); break;
      case BYTES: w.bytes(no, v); break;
      case U32: w.u32(no, v); break;
      case U64: w.u64(no, v); break;
      case BOOL: w.bool(no, v); break;
      case ENUM: w.enm(no, v); break;
      case DBL: w.dbl(no, v); break;
      case RSTR: w.rstr(no, v); break;
      case SMAP: case IMAP: case DMAP: w.map(no, v, f.kind); break;
      case RENUM: w.renum(no, v); break;
      case OPT32: w.tag(no, WIRE_VARINT); w.v32(v >>> 0); break;
      case OPT64: w.tag(no, WIRE_VARINT); w.v64(BigInt(v)); break;
      case MSG: writeSub(w, no, f.sub!, v); break;
      case RMSG: for (let j = 0; j < v.length; j++) writeSub(w, no, f.sub!, v[j]); break;
      case ONEOF: {
        const vf = f.cases![v.case];
        if (vf) writeSub(w, vf.no, vf.sub!, v.value);
        break;
      }
    }
  }
}

function readMsg(s: Schema, r: ProtoReader, end: number, out?: any): any {
  if (s.c) return s.c.read(r, end, out);
  out ??= mk(s);
  while (r.pos < end) {
    r.readTag(end);
    const wt = r.wt;
    const f = s.byNo[r.fn];
    if (!f) {
      r.skipField(wt, end);
      continue;
    }
    const kind = f.kind;
    if (kind === RENUM) {
      if (wt === WIRE_LEN) {
        const subEnd = r.readSubEnd(end);
        while (r.pos < subEnd) out[f.name].push(r.readEnum(subEnd));
      } else if (wt === WIRE_VARINT) out[f.name].push(r.readEnum(end));
      else r.want(WIRE_LEN);
      continue;
    }
    r.want(WIRE_OF[kind]!);
    const name = f.name;
    switch (kind) {
      case STR: out[name] = r.readString(end); break;
      case BYTES: out[name] = r.readBytes(end); break;
      case U32: case OPT32: out[name] = r.readUint32(end); break;
      case U64: case OPT64: out[name] = r.readUint64(end); break;
      case BOOL: out[name] = r.readBool(end); break;
      case ENUM: out[name] = r.readEnum(end); break;
      case DBL: out[name] = r.readDouble(end); break;
      case RSTR: out[name].push(r.readString(end)); break;
      case SMAP: case IMAP: case DMAP: r.readMapEntry(end, out[name], kind); break;
      case MSG: out[name] = readMsg(f.sub!, r, r.readSubEnd(end), out[name]); break;
      case RMSG: out[name].push(readMsg(f.sub!, r, r.readSubEnd(end))); break;
      case VARIANT: {
        const prev = out[name];
        out[name] = { case: f.case, value: readMsg(f.sub!, r, r.readSubEnd(end), prev?.case === f.case ? prev.value : undefined) };
        break;
      }
    }
  }
  return out;
}

function enc(s: Schema, m: any): Uint8Array {
  const L = s.c ? null : [];
  const n = lenMsg(s, m, L);
  if (n === 0) return new Uint8Array(0);
  const w = new ProtoWriter(n, L);
  writeMsg(w, s, m);
  return w.buf;
}

const dec = (s: Schema, buf: Uint8Array): any => readMsg(s, new ProtoReader(buf), buf.byteLength);

type Triple<I, O> = [(m: I) => number, (m: I) => Uint8Array, (buf: Uint8Array) => O];
type EmptyTriple<O> = [(m?: O) => number, (m?: O) => Uint8Array, (buf: Uint8Array) => O];
const codec = <I, O>(s: Schema): Triple<I, O> => [(m) => lenMsg(s, m, null), (m) => enc(s, m), (b) => dec(s, b)];
const emptyCodec = <O>(s: Schema): EmptyTriple<O> => codec<O, O>(s) as unknown as EmptyTriple<O>;

// ---------- Hot-path direct codecs ----------

/** `Object { oneof body { bytes inline = 1; string uri = 2 }; map<string,string> metadata = 3 }` incl. segmented inline. */
const objectCodec: Codec = {
  len(m: SegmentedObjectInput): number {
    const b = m.body;
    const body = !b ? 0 : b.case === "inline" ? lenTotal(1, segLen(b.value)) : lenTotal(2, utf8ByteLen(b.value));
    return body + mapLen(3, m.metadata, SMAP);
  },
  write(w, m: SegmentedObjectInput) {
    const b = m.body;
    if (b) {
      if (b.case === "inline") w.abytes(1, b.value);
      else w.astr(2, b.value);
    }
    w.map(3, m.metadata, SMAP);
  },
  read(r, end, out: ProtoObject = { metadata: {} }) {
    while (r.pos < end) {
      r.readTag(end);
      switch (r.fn) {
        case 1: r.want(WIRE_LEN); out.body = { case: "inline", value: r.readBytes(end) }; break;
        case 2: r.want(WIRE_LEN); out.body = { case: "uri", value: r.readString(end) }; break;
        case 3: r.want(WIRE_LEN); r.readMapEntry(end, out.metadata, SMAP); break;
        default: r.skipField(r.wt, end);
      }
    }
    return out;
  },
};

const frameCodec: Codec = {
  len: (m: FrameInput) => u64Len(1, m.sequence) + bytesLen(2, m.data) + mapLen(3, m.metadata, SMAP) + boolLen(4, m.progress),
  write(w, m: FrameInput) {
    w.u64(1, m.sequence);
    w.bytes(2, m.data);
    w.map(3, m.metadata, SMAP);
    w.bool(4, m.progress);
  },
  read(r, end, out: Frame = { sequence: 0n, data: EMPTY_BYTES, metadata: {}, progress: false }) {
    while (r.pos < end) {
      r.readTag(end);
      switch (r.fn) {
        case 1: r.want(WIRE_VARINT); out.sequence = r.readUint64(end); break;
        case 2: r.want(WIRE_LEN); out.data = r.readBytes(end); break;
        case 3: r.want(WIRE_LEN); r.readMapEntry(end, out.metadata, SMAP); break;
        case 4: r.want(WIRE_VARINT); out.progress = r.readBool(end); break;
        default: r.skipField(r.wt, end);
      }
    }
    return out;
  },
};

const resultChunkCodec: Codec = {
  len: (m: ResultChunkInput) =>
    u32Len(1, m.index) + bytesLen(2, m.data) + boolLen(3, m.last) + mapLen(4, m.metadata, SMAP) + strLen(5, m.uri),
  write(w, m: ResultChunkInput) {
    w.u32(1, m.index);
    w.bytes(2, m.data);
    w.bool(3, m.last);
    w.map(4, m.metadata, SMAP);
    w.str(5, m.uri);
  },
  read(r, end, out: ResultChunk = { index: 0, data: EMPTY_BYTES, last: false, metadata: {}, uri: "" }) {
    while (r.pos < end) {
      r.readTag(end);
      switch (r.fn) {
        case 1: r.want(WIRE_VARINT); out.index = r.readUint32(end); break;
        case 2: r.want(WIRE_LEN); out.data = r.readBytes(end); break;
        case 3: r.want(WIRE_VARINT); out.last = r.readBool(end); break;
        case 4: r.want(WIRE_LEN); r.readMapEntry(end, out.metadata, SMAP); break;
        case 5: r.want(WIRE_LEN); out.uri = r.readString(end); break;
        default: r.skipField(r.wt, end);
      }
    }
    return out;
  },
};

const inputDataCodec: Codec = {
  len: (m: InputDataInput) =>
    u64Len(1, m.sequence) +
    bytesLen(2, m.data) +
    boolLen(3, m.last) +
    mapLen(4, m.metadata, SMAP) +
    (m.objectIndex !== undefined ? tagLen(5) + varint32Len(m.objectIndex >>> 0) : 0) +
    strLen(6, m.uri) +
    boolLen(7, m.parameterChunk) +
    boolLen(8, m.objectEnd),
  write(w, m: InputDataInput) {
    w.u64(1, m.sequence);
    w.bytes(2, m.data);
    w.bool(3, m.last);
    w.map(4, m.metadata, SMAP);
    if (m.objectIndex !== undefined) {
      w.tag(5, WIRE_VARINT);
      w.v32(m.objectIndex >>> 0);
    }
    w.str(6, m.uri);
    w.bool(7, m.parameterChunk);
    w.bool(8, m.objectEnd);
  },
  read(
    r,
    end,
    out: InputData = { sequence: 0n, data: EMPTY_BYTES, last: false, metadata: {}, uri: "", parameterChunk: false, objectEnd: false },
  ) {
    while (r.pos < end) {
      r.readTag(end);
      switch (r.fn) {
        case 1: r.want(WIRE_VARINT); out.sequence = r.readUint64(end); break;
        case 2: r.want(WIRE_LEN); out.data = r.readBytes(end); break;
        case 3: r.want(WIRE_VARINT); out.last = r.readBool(end); break;
        case 4: r.want(WIRE_LEN); r.readMapEntry(end, out.metadata, SMAP); break;
        case 5: r.want(WIRE_VARINT); out.objectIndex = r.readUint32(end); break;
        case 6: r.want(WIRE_LEN); out.uri = r.readString(end); break;
        case 7: r.want(WIRE_VARINT); out.parameterChunk = r.readBool(end); break;
        case 8: r.want(WIRE_VARINT); out.objectEnd = r.readBool(end); break;
        default: r.skipField(r.wt, end);
      }
    }
    return out;
  },
};

const ackCodec: Codec = {
  len: (m: AckInput) => u64Len(1, m.upToSequence) + u64Len(2, m.resultBytes),
  write(w, m: AckInput) {
    w.u64(1, m.upToSequence);
    w.u64(2, m.resultBytes);
  },
  read(r, end, out: Ack = { upToSequence: 0n, resultBytes: 0n }) {
    while (r.pos < end) {
      r.readTag(end);
      switch (r.fn) {
        case 1: r.want(WIRE_VARINT); out.upToSequence = r.readUint64(end); break;
        case 2: r.want(WIRE_VARINT); out.resultBytes = r.readUint64(end); break;
        default: r.skipField(r.wt, end);
      }
    }
    return out;
  },
};

const emptyCodecObj: Codec = {
  len: () => 0,
  write() {},
  read(r, end, out: Record<string, never> = {}) {
    while (r.pos < end) {
      r.readTag(end);
      r.skipField(r.wt, end);
    }
    return out;
  },
};

const errorInfoCodec: Codec = {
  len: (m: ErrorInfoInput) => strLen(1, m.code) + strLen(2, m.message) + boolLen(3, m.retryable),
  write(w, m: ErrorInfoInput) {
    w.str(1, m.code);
    w.str(2, m.message);
    w.bool(3, m.retryable);
  },
  read(r, end, out: ErrorInfo = { code: "", message: "", retryable: false }) {
    while (r.pos < end) {
      r.readTag(end);
      switch (r.fn) {
        case 1: r.want(WIRE_LEN); out.code = r.readString(end); break;
        case 2: r.want(WIRE_LEN); out.message = r.readString(end); break;
        case 3: r.want(WIRE_VARINT); out.retryable = r.readBool(end); break;
        default: r.skipField(r.wt, end);
      }
    }
    return out;
  },
};

const loadCodec: Codec = {
  len: (m: LoadInput) =>
    u32Len(1, m.queued) + u32Len(2, m.running) + dblLen(3, m.utilization) + mapLen(4, m.custom, DMAP),
  write(w, m: LoadInput) {
    w.u32(1, m.queued);
    w.u32(2, m.running);
    w.dbl(3, m.utilization);
    w.map(4, m.custom, DMAP);
  },
  read(r, end, out: Load = { queued: 0, running: 0, utilization: 0, custom: {} }) {
    while (r.pos < end) {
      r.readTag(end);
      switch (r.fn) {
        case 1: r.want(WIRE_VARINT); out.queued = r.readUint32(end); break;
        case 2: r.want(WIRE_VARINT); out.running = r.readUint32(end); break;
        case 3: r.want(WIRE_64BIT); out.utilization = r.readDouble(end); break;
        case 4: r.want(WIRE_LEN); r.readMapEntry(end, out.custom, DMAP); break;
        default: r.skipField(r.wt, end);
      }
    }
    return out;
  },
};

const heartbeatCodec: Codec = {
  len: (m: HeartbeatInput) => (m.load !== undefined ? lenTotal(1, loadCodec.len(m.load)) : 0),
  write(w, m: HeartbeatInput) {
    if (m.load !== undefined) {
      w.hdr(1, loadCodec.len(m.load));
      loadCodec.write(w, m.load);
    }
  },
  read(r, end, out: Heartbeat = {}) {
    while (r.pos < end) {
      r.readTag(end);
      if (r.fn === 1) {
        r.want(WIRE_LEN);
        out.load = loadCodec.read(r, r.readSubEnd(end), out.load);
      } else {
        r.skipField(r.wt, end);
      }
    }
    return out;
  },
};

const creditCodec: Codec = {
  len: (m: CreditInput) => u32Len(1, m.freeSlots),
  write(w, m: CreditInput) {
    w.u32(1, m.freeSlots);
  },
  read(r, end, out: Credit = { freeSlots: 0 }) {
    while (r.pos < end) {
      r.readTag(end);
      if (r.fn === 1) {
        r.want(WIRE_VARINT);
        out.freeSlots = r.readUint32(end);
      } else {
        r.skipField(r.wt, end);
      }
    }
    return out;
  },
};

const assignReplyCodec: Codec = {
  len: (m: AssignReplyInput) => strLen(1, m.leaseId) + boolLen(2, m.accepted) + strLen(3, m.rejectReason),
  write(w, m: AssignReplyInput) {
    w.str(1, m.leaseId);
    w.bool(2, m.accepted);
    w.str(3, m.rejectReason);
  },
  read(r, end, out: AssignReply = { leaseId: "", accepted: false, rejectReason: "" }) {
    while (r.pos < end) {
      r.readTag(end);
      switch (r.fn) {
        case 1: r.want(WIRE_LEN); out.leaseId = r.readString(end); break;
        case 2: r.want(WIRE_VARINT); out.accepted = r.readBool(end); break;
        case 3: r.want(WIRE_LEN); out.rejectReason = r.readString(end); break;
        default: r.skipField(r.wt, end);
      }
    }
    return out;
  },
};

const cancelAckCodec: Codec = {
  len: (m: CancelAckInput) => strLen(1, m.leaseId),
  write(w, m: CancelAckInput) {
    w.str(1, m.leaseId);
  },
  read(r, end, out: CancelAck = { leaseId: "" }) {
    while (r.pos < end) {
      r.readTag(end);
      if (r.fn === 1) {
        r.want(WIRE_LEN);
        out.leaseId = r.readString(end);
      } else {
        r.skipField(r.wt, end);
      }
    }
    return out;
  },
};

const drainingCodec: Codec = {
  len: (m: DrainingInput) => boolLen(1, m.preempting),
  write(w, m: DrainingInput) {
    w.bool(1, m.preempting);
  },
  read(r, end, out: Draining = { preempting: false }) {
    while (r.pos < end) {
      r.readTag(end);
      if (r.fn === 1) {
        r.want(WIRE_VARINT);
        out.preempting = r.readBool(end);
      } else {
        r.skipField(r.wt, end);
      }
    }
    return out;
  },
};

const assignCodec: Codec = {
  len(m: AssignInput): number {
    let n =
      strLen(1, m.leaseId) +
      strLen(2, m.attemptId) +
      strLen(3, m.jobId) +
      u32Len(4, m.attemptNumber) +
      u64Len(5, m.generation) +
      strLen(6, m.interface) +
      bytesLen(7, m.params) +
      mapLen(8, m.reservedCapacity, IMAP) +
      mapLen(9, m.metadata, SMAP) +
      boolLen(11, m.inputsViaAttempt) +
      boolLen(12, m.streamingInput) +
      u64Len(13, m.executionDeadlineMs) +
      u64Len(14, m.totalDeadlineMs);
    const items = m.inlineInputs;
    if (items) {
      for (let i = 0; i < items.length; i++) {
        n += lenTotal(10, objectCodec.len(items[i]!));
      }
    }
    return n;
  },
  write(w, m: AssignInput) {
    w.str(1, m.leaseId);
    w.str(2, m.attemptId);
    w.str(3, m.jobId);
    w.u32(4, m.attemptNumber);
    w.u64(5, m.generation);
    w.str(6, m.interface);
    w.bytes(7, m.params);
    w.map(8, m.reservedCapacity, IMAP);
    w.map(9, m.metadata, SMAP);
    const items = m.inlineInputs;
    if (items) {
      for (let i = 0; i < items.length; i++) {
        const item = items[i]!;
        w.hdr(10, objectCodec.len(item));
        objectCodec.write(w, item);
      }
    }
    w.bool(11, m.inputsViaAttempt);
    w.bool(12, m.streamingInput);
    w.u64(13, m.executionDeadlineMs);
    w.u64(14, m.totalDeadlineMs);
  },
  read(
    r,
    end,
    out: Assign = {
      leaseId: "",
      attemptId: "",
      jobId: "",
      attemptNumber: 0,
      generation: 0n,
      interface: "",
      params: EMPTY_BYTES,
      reservedCapacity: {},
      metadata: {},
      inlineInputs: [],
      inputsViaAttempt: false,
      streamingInput: false,
      executionDeadlineMs: 0n,
      totalDeadlineMs: 0n,
    },
  ) {
    while (r.pos < end) {
      r.readTag(end);
      switch (r.fn) {
        case 1: r.want(WIRE_LEN); out.leaseId = r.readString(end); break;
        case 2: r.want(WIRE_LEN); out.attemptId = r.readString(end); break;
        case 3: r.want(WIRE_LEN); out.jobId = r.readString(end); break;
        case 4: r.want(WIRE_VARINT); out.attemptNumber = r.readUint32(end); break;
        case 5: r.want(WIRE_VARINT); out.generation = r.readUint64(end); break;
        case 6: r.want(WIRE_LEN); out.interface = r.readString(end); break;
        case 7: r.want(WIRE_LEN); out.params = r.readBytes(end); break;
        case 8: r.want(WIRE_LEN); r.readMapEntry(end, out.reservedCapacity, IMAP); break;
        case 9: r.want(WIRE_LEN); r.readMapEntry(end, out.metadata, SMAP); break;
        case 10: r.want(WIRE_LEN); out.inlineInputs.push(objectCodec.read(r, r.readSubEnd(end))); break;
        case 11: r.want(WIRE_VARINT); out.inputsViaAttempt = r.readBool(end); break;
        case 12: r.want(WIRE_VARINT); out.streamingInput = r.readBool(end); break;
        case 13: r.want(WIRE_VARINT); out.executionDeadlineMs = r.readUint64(end); break;
        case 14: r.want(WIRE_VARINT); out.totalDeadlineMs = r.readUint64(end); break;
        default: r.skipField(r.wt, end);
      }
    }
    return out;
  },
};

const cancelCodec: Codec = {
  len: (m: CancelInput) => strLen(1, m.leaseId) + strLen(2, m.reason),
  write(w, m: CancelInput) {
    w.str(1, m.leaseId);
    w.str(2, m.reason);
  },
  read(r, end, out: Cancel = { leaseId: "", reason: "" }) {
    while (r.pos < end) {
      r.readTag(end);
      switch (r.fn) {
        case 1: r.want(WIRE_LEN); out.leaseId = r.readString(end); break;
        case 2: r.want(WIRE_LEN); out.reason = r.readString(end); break;
        default: r.skipField(r.wt, end);
      }
    }
    return out;
  },
};

const limitsCodec: Codec = {
  len: (m: LimitsInput) =>
    u32Len(1, m.maxControlMessageBytes) +
    u32Len(2, m.maxChunkBytes) +
    u32Len(3, m.attemptWindowBytes) +
    u32Len(4, m.assignInlineLimit),
  write(w, m: LimitsInput) {
    w.u32(1, m.maxControlMessageBytes);
    w.u32(2, m.maxChunkBytes);
    w.u32(3, m.attemptWindowBytes);
    w.u32(4, m.assignInlineLimit);
  },
  read(
    r,
    end,
    out: Limits = { maxControlMessageBytes: 0, maxChunkBytes: 0, attemptWindowBytes: 0, assignInlineLimit: 0 },
  ) {
    while (r.pos < end) {
      r.readTag(end);
      switch (r.fn) {
        case 1: r.want(WIRE_VARINT); out.maxControlMessageBytes = r.readUint32(end); break;
        case 2: r.want(WIRE_VARINT); out.maxChunkBytes = r.readUint32(end); break;
        case 3: r.want(WIRE_VARINT); out.attemptWindowBytes = r.readUint32(end); break;
        case 4: r.want(WIRE_VARINT); out.assignInlineLimit = r.readUint32(end); break;
        default: r.skipField(r.wt, end);
      }
    }
    return out;
  },
};

const welcomeCodec: Codec = {
  len: (m: WelcomeInput) =>
    strLen(1, m.sessionId) +
    u64Len(2, m.generation) +
    u32Len(3, m.heartbeatIntervalMs) +
    u32Len(4, m.heartbeatTimeoutMs) +
    (m.limits !== undefined ? lenTotal(5, limitsCodec.len(m.limits)) : 0) +
    rstrLen(6, m.resumedLeaseIds),
  write(w, m: WelcomeInput) {
    w.str(1, m.sessionId);
    w.u64(2, m.generation);
    w.u32(3, m.heartbeatIntervalMs);
    w.u32(4, m.heartbeatTimeoutMs);
    if (m.limits !== undefined) {
      w.hdr(5, limitsCodec.len(m.limits));
      limitsCodec.write(w, m.limits);
    }
    w.rstr(6, m.resumedLeaseIds);
  },
  read(
    r,
    end,
    out: Welcome = {
      sessionId: "",
      generation: 0n,
      heartbeatIntervalMs: 0,
      heartbeatTimeoutMs: 0,
      resumedLeaseIds: [],
    },
  ) {
    while (r.pos < end) {
      r.readTag(end);
      switch (r.fn) {
        case 1: r.want(WIRE_LEN); out.sessionId = r.readString(end); break;
        case 2: r.want(WIRE_VARINT); out.generation = r.readUint64(end); break;
        case 3: r.want(WIRE_VARINT); out.heartbeatIntervalMs = r.readUint32(end); break;
        case 4: r.want(WIRE_VARINT); out.heartbeatTimeoutMs = r.readUint32(end); break;
        case 5: r.want(WIRE_LEN); out.limits = limitsCodec.read(r, r.readSubEnd(end), out.limits); break;
        case 6: r.want(WIRE_LEN); out.resumedLeaseIds.push(r.readString(end)); break;
        default: r.skipField(r.wt, end);
      }
    }
    return out;
  },
};

const serverMessageCodec: Codec = {
  len(m: ServerMessageInput): number {
    const v = m.msg;
    if (!v) return 0;
    switch (v.case) {
      case "assign": return lenTotal(2, assignCodec.len(v.value));
      case "welcome": return lenTotal(1, welcomeCodec.len(v.value));
      case "cancel": return lenTotal(3, cancelCodec.len(v.value));
      case "fence": return lenTotal(4, cancelCodec.len(v.value));
      case "drain": return lenTotal(5, 0);
    }
  },
  write(w, m: ServerMessageInput) {
    const v = m.msg;
    if (!v) return;
    switch (v.case) {
      case "assign":
        w.hdr(2, assignCodec.len(v.value));
        assignCodec.write(w, v.value);
        break;
      case "welcome":
        w.hdr(1, welcomeCodec.len(v.value));
        welcomeCodec.write(w, v.value);
        break;
      case "cancel":
        w.hdr(3, cancelCodec.len(v.value));
        cancelCodec.write(w, v.value);
        break;
      case "fence":
        w.hdr(4, cancelCodec.len(v.value));
        cancelCodec.write(w, v.value);
        break;
      case "drain":
        w.hdr(5, 0);
        break;
    }
  },
  read(r, end, out: ServerMessage = {}) {
    while (r.pos < end) {
      r.readTag(end);
      switch (r.fn) {
        case 2:
          r.want(WIRE_LEN);
          out.msg = {
            case: "assign",
            value: assignCodec.read(r, r.readSubEnd(end), out.msg?.case === "assign" ? out.msg.value : undefined),
          };
          break;
        case 1:
          r.want(WIRE_LEN);
          out.msg = {
            case: "welcome",
            value: welcomeCodec.read(r, r.readSubEnd(end), out.msg?.case === "welcome" ? out.msg.value : undefined),
          };
          break;
        case 3:
          r.want(WIRE_LEN);
          out.msg = {
            case: "cancel",
            value: cancelCodec.read(r, r.readSubEnd(end), out.msg?.case === "cancel" ? out.msg.value : undefined),
          };
          break;
        case 4:
          r.want(WIRE_LEN);
          out.msg = {
            case: "fence",
            value: cancelCodec.read(r, r.readSubEnd(end), out.msg?.case === "fence" ? out.msg.value : undefined),
          };
          break;
        case 5:
          r.want(WIRE_LEN);
          out.msg = {
            case: "drain",
            value: emptyCodecObj.read(r, r.readSubEnd(end), out.msg?.case === "drain" ? out.msg.value : undefined),
          };
          break;
        default:
          r.skipField(r.wt, end);
      }
    }
    return out;
  },
};

const assignedCodec: Codec = {
  len: (m: AssignedInput) => strLen(1, m.workerId) + u32Len(2, m.attemptNumber),
  write(w, m: AssignedInput) {
    w.str(1, m.workerId);
    w.u32(2, m.attemptNumber);
  },
  read(r, end, out: Assigned = { workerId: "", attemptNumber: 0 }) {
    while (r.pos < end) {
      r.readTag(end);
      switch (r.fn) {
        case 1: r.want(WIRE_LEN); out.workerId = r.readString(end); break;
        case 2: r.want(WIRE_VARINT); out.attemptNumber = r.readUint32(end); break;
        default: r.skipField(r.wt, end);
      }
    }
    return out;
  },
};

const outputCodec: Codec = {
  len: (m: OutputInput) => bytesLen(1, m.data) + mapLen(2, m.metadata, SMAP) + boolLen(3, m.progress),
  write(w, m: OutputInput) {
    w.bytes(1, m.data);
    w.map(2, m.metadata, SMAP);
    w.bool(3, m.progress);
  },
  read(r, end, out: Output = { data: EMPTY_BYTES, metadata: {}, progress: false }) {
    while (r.pos < end) {
      r.readTag(end);
      switch (r.fn) {
        case 1: r.want(WIRE_LEN); out.data = r.readBytes(end); break;
        case 2: r.want(WIRE_LEN); r.readMapEntry(end, out.metadata, SMAP); break;
        case 3: r.want(WIRE_VARINT); out.progress = r.readBool(end); break;
        default: r.skipField(r.wt, end);
      }
    }
    return out;
  },
};

const attemptRestartedCodec: Codec = {
  len: (m: AttemptRestartedInput) =>
    strLen(1, m.previousAttemptId) + (m.reason !== undefined ? lenTotal(2, errorInfoCodec.len(m.reason)) : 0),
  write(w, m: AttemptRestartedInput) {
    w.str(1, m.previousAttemptId);
    if (m.reason !== undefined) {
      w.hdr(2, errorInfoCodec.len(m.reason));
      errorInfoCodec.write(w, m.reason);
    }
  },
  read(r, end, out: AttemptRestarted = { previousAttemptId: "" }) {
    while (r.pos < end) {
      r.readTag(end);
      switch (r.fn) {
        case 1: r.want(WIRE_LEN); out.previousAttemptId = r.readString(end); break;
        case 2: r.want(WIRE_LEN); out.reason = errorInfoCodec.read(r, r.readSubEnd(end), out.reason); break;
        default: r.skipField(r.wt, end);
      }
    }
    return out;
  },
};

const executionFinishedCodec: Codec = {
  len(m: ExecutionFinishedInput): number {
    let n = enumLen(1, m.state);
    const items = m.outputs;
    if (items) {
      for (let i = 0; i < items.length; i++) {
        n += lenTotal(2, objectCodec.len(items[i]!));
      }
    }
    if (m.error !== undefined) {
      n += lenTotal(3, errorInfoCodec.len(m.error));
    }
    return n;
  },
  write(w, m: ExecutionFinishedInput) {
    w.enm(1, m.state);
    const items = m.outputs;
    if (items) {
      for (let i = 0; i < items.length; i++) {
        const item = items[i]!;
        w.hdr(2, objectCodec.len(item));
        objectCodec.write(w, item);
      }
    }
    if (m.error !== undefined) {
      w.hdr(3, errorInfoCodec.len(m.error));
      errorInfoCodec.write(w, m.error);
    }
  },
  read(r, end, out: ExecutionFinished = { state: 0, outputs: [] }) {
    while (r.pos < end) {
      r.readTag(end);
      switch (r.fn) {
        case 1: r.want(WIRE_VARINT); out.state = r.readEnum(end); break;
        case 2: r.want(WIRE_LEN); out.outputs.push(objectCodec.read(r, r.readSubEnd(end))); break;
        case 3: r.want(WIRE_LEN); out.error = errorInfoCodec.read(r, r.readSubEnd(end), out.error); break;
        default: r.skipField(r.wt, end);
      }
    }
    return out;
  },
};

const executionEventCodec: Codec = {
  len(m: ExecutionEventInput): number {
    let n = strLen(1, m.jobId) + u64Len(2, m.sequence) + strLen(3, m.attemptId) + u64Len(4, m.atMs);
    const ev = m.event;
    if (ev) {
      switch (ev.case) {
        case "output": n += lenTotal(12, outputCodec.len(ev.value)); break;
        case "assigned": n += lenTotal(11, assignedCodec.len(ev.value)); break;
        case "accepted": n += lenTotal(10, 0); break;
        case "finished": n += lenTotal(14, executionFinishedCodec.len(ev.value)); break;
        case "attemptRestarted": n += lenTotal(13, attemptRestartedCodec.len(ev.value)); break;
      }
    }
    return n;
  },
  write(w, m: ExecutionEventInput) {
    w.str(1, m.jobId);
    w.u64(2, m.sequence);
    w.str(3, m.attemptId);
    w.u64(4, m.atMs);
    const ev = m.event;
    if (ev) {
      switch (ev.case) {
        case "output":
          w.hdr(12, outputCodec.len(ev.value));
          outputCodec.write(w, ev.value);
          break;
        case "assigned":
          w.hdr(11, assignedCodec.len(ev.value));
          assignedCodec.write(w, ev.value);
          break;
        case "accepted":
          w.hdr(10, 0);
          break;
        case "finished":
          w.hdr(14, executionFinishedCodec.len(ev.value));
          executionFinishedCodec.write(w, ev.value);
          break;
        case "attemptRestarted":
          w.hdr(13, attemptRestartedCodec.len(ev.value));
          attemptRestartedCodec.write(w, ev.value);
          break;
      }
    }
  },
  read(r, end, out: ExecutionEvent = { jobId: "", sequence: 0n, attemptId: "", atMs: 0n }) {
    while (r.pos < end) {
      r.readTag(end);
      switch (r.fn) {
        case 1: r.want(WIRE_LEN); out.jobId = r.readString(end); break;
        case 2: r.want(WIRE_VARINT); out.sequence = r.readUint64(end); break;
        case 3: r.want(WIRE_LEN); out.attemptId = r.readString(end); break;
        case 4: r.want(WIRE_VARINT); out.atMs = r.readUint64(end); break;
        case 10:
          r.want(WIRE_LEN);
          out.event = {
            case: "accepted",
            value: emptyCodecObj.read(r, r.readSubEnd(end), out.event?.case === "accepted" ? out.event.value : undefined),
          };
          break;
        case 11:
          r.want(WIRE_LEN);
          out.event = {
            case: "assigned",
            value: assignedCodec.read(r, r.readSubEnd(end), out.event?.case === "assigned" ? out.event.value : undefined),
          };
          break;
        case 12:
          r.want(WIRE_LEN);
          out.event = {
            case: "output",
            value: outputCodec.read(r, r.readSubEnd(end), out.event?.case === "output" ? out.event.value : undefined),
          };
          break;
        case 13:
          r.want(WIRE_LEN);
          out.event = {
            case: "attemptRestarted",
            value: attemptRestartedCodec.read(r, r.readSubEnd(end), out.event?.case === "attemptRestarted" ? out.event.value : undefined),
          };
          break;
        case 14:
          r.want(WIRE_LEN);
          out.event = {
            case: "finished",
            value: executionFinishedCodec.read(r, r.readSubEnd(end), out.event?.case === "finished" ? out.event.value : undefined),
          };
          break;
        default:
          r.skipField(r.wt, end);
      }
    }
    return out;
  },
};

const attemptOpenCodec: Codec = {
  len: (m: AttemptOpenInput) =>
    strLen(1, m.leaseId) + u64Len(2, m.generation) + u64Len(3, m.resumeAfterSequence),
  write(w, m: AttemptOpenInput) {
    w.str(1, m.leaseId);
    w.u64(2, m.generation);
    w.u64(3, m.resumeAfterSequence);
  },
  read(r, end, out: AttemptOpen = { leaseId: "", generation: 0n, resumeAfterSequence: 0n }) {
    while (r.pos < end) {
      r.readTag(end);
      switch (r.fn) {
        case 1: r.want(WIRE_LEN); out.leaseId = r.readString(end); break;
        case 2: r.want(WIRE_VARINT); out.generation = r.readUint64(end); break;
        case 3: r.want(WIRE_VARINT); out.resumeAfterSequence = r.readUint64(end); break;
        default: r.skipField(r.wt, end);
      }
    }
    return out;
  },
};

const finishCodec: Codec = {
  len(m: FinishInput): number {
    let n = 0;
    const o = m.outcome;
    if (o) {
      if (o.case === "succeeded") n += lenTotal(1, 0);
      else if (o.case === "failed") n += lenTotal(2, errorInfoCodec.len(o.value));
      else if (o.case === "cancelled") n += lenTotal(3, 0);
    }
    if (m.handlerDurationMs !== undefined) {
      n += tagLen(4) + varint64Len(norm64(m.handlerDurationMs));
    }
    return n;
  },
  write(w, m: FinishInput) {
    const o = m.outcome;
    if (o) {
      if (o.case === "succeeded") w.hdr(1, 0);
      else if (o.case === "failed") {
        w.hdr(2, errorInfoCodec.len(o.value));
        errorInfoCodec.write(w, o.value);
      } else if (o.case === "cancelled") w.hdr(3, 0);
    }
    if (m.handlerDurationMs !== undefined) {
      w.tag(4, WIRE_VARINT);
      w.v64(norm64(m.handlerDurationMs));
    }
  },
  read(r, end, out: Finish = {}) {
    while (r.pos < end) {
      r.readTag(end);
      switch (r.fn) {
        case 1:
          r.want(WIRE_LEN);
          out.outcome = {
            case: "succeeded",
            value: emptyCodecObj.read(r, r.readSubEnd(end), out.outcome?.case === "succeeded" ? out.outcome.value : undefined),
          };
          break;
        case 2:
          r.want(WIRE_LEN);
          out.outcome = {
            case: "failed",
            value: errorInfoCodec.read(r, r.readSubEnd(end), out.outcome?.case === "failed" ? out.outcome.value : undefined),
          };
          break;
        case 3:
          r.want(WIRE_LEN);
          out.outcome = {
            case: "cancelled",
            value: emptyCodecObj.read(r, r.readSubEnd(end), out.outcome?.case === "cancelled" ? out.outcome.value : undefined),
          };
          break;
        case 4:
          r.want(WIRE_VARINT);
          out.handlerDurationMs = r.readUint64(end);
          break;
        default:
          r.skipField(r.wt, end);
      }
    }
    return out;
  },
};

const attemptUpCodec: Codec = {
  len(m: AttemptUpInput): number {
    const v = m.msg;
    if (!v) return 0;
    switch (v.case) {
      case "frame": return lenTotal(2, frameCodec.len(v.value));
      case "result": return lenTotal(3, resultChunkCodec.len(v.value));
      case "open": return lenTotal(1, attemptOpenCodec.len(v.value));
      case "finish": return lenTotal(5, finishCodec.len(v.value));
    }
  },
  write(w, m: AttemptUpInput) {
    const v = m.msg;
    if (!v) return;
    switch (v.case) {
      case "frame":
        w.hdr(2, frameCodec.len(v.value));
        frameCodec.write(w, v.value);
        break;
      case "result":
        w.hdr(3, resultChunkCodec.len(v.value));
        resultChunkCodec.write(w, v.value);
        break;
      case "open":
        w.hdr(1, attemptOpenCodec.len(v.value));
        attemptOpenCodec.write(w, v.value);
        break;
      case "finish":
        w.hdr(5, finishCodec.len(v.value));
        finishCodec.write(w, v.value);
        break;
    }
  },
  read(r, end, out: AttemptUp = {}) {
    while (r.pos < end) {
      r.readTag(end);
      switch (r.fn) {
        case 2:
          r.want(WIRE_LEN);
          out.msg = {
            case: "frame",
            value: frameCodec.read(r, r.readSubEnd(end), out.msg?.case === "frame" ? out.msg.value : undefined),
          };
          break;
        case 3:
          r.want(WIRE_LEN);
          out.msg = {
            case: "result",
            value: resultChunkCodec.read(r, r.readSubEnd(end), out.msg?.case === "result" ? out.msg.value : undefined),
          };
          break;
        case 1:
          r.want(WIRE_LEN);
          out.msg = {
            case: "open",
            value: attemptOpenCodec.read(r, r.readSubEnd(end), out.msg?.case === "open" ? out.msg.value : undefined),
          };
          break;
        case 5:
          r.want(WIRE_LEN);
          out.msg = {
            case: "finish",
            value: finishCodec.read(r, r.readSubEnd(end), out.msg?.case === "finish" ? out.msg.value : undefined),
          };
          break;
        default:
          r.skipField(r.wt, end);
      }
    }
    return out;
  },
};

const attemptOpenedCodec: Codec = {
  len: (m: AttemptOpenedInput) => u64Len(1, m.resumeFromSequence) + boolLen(2, m.terminalReplay),
  write(w, m: AttemptOpenedInput) {
    w.u64(1, m.resumeFromSequence);
    w.bool(2, m.terminalReplay);
  },
  read(r, end, out: AttemptOpened = { resumeFromSequence: 0n, terminalReplay: false }) {
    while (r.pos < end) {
      r.readTag(end);
      switch (r.fn) {
        case 1: r.want(WIRE_VARINT); out.resumeFromSequence = r.readUint64(end); break;
        case 2: r.want(WIRE_VARINT); out.terminalReplay = r.readBool(end); break;
        default: r.skipField(r.wt, end);
      }
    }
    return out;
  },
};

const attemptFinishedCodec: Codec = {
  len: (m: AttemptFinishedInput) => enumLen(1, m.status) + strLen(2, m.reason),
  write(w, m: AttemptFinishedInput) {
    w.enm(1, m.status);
    w.str(2, m.reason);
  },
  read(r, end, out: AttemptFinished = { status: 0, reason: "" }) {
    while (r.pos < end) {
      r.readTag(end);
      switch (r.fn) {
        case 1: r.want(WIRE_VARINT); out.status = r.readEnum(end); break;
        case 2: r.want(WIRE_LEN); out.reason = r.readString(end); break;
        default: r.skipField(r.wt, end);
      }
    }
    return out;
  },
};

const attemptDownCodec: Codec = {
  len(m: AttemptDownInput): number {
    const v = m.msg;
    if (!v) return 0;
    switch (v.case) {
      case "ack": return lenTotal(3, ackCodec.len(v.value));
      case "input": return lenTotal(2, inputDataCodec.len(v.value));
      case "opened": return lenTotal(1, attemptOpenedCodec.len(v.value));
      case "finished": return lenTotal(4, attemptFinishedCodec.len(v.value));
    }
  },
  write(w, m: AttemptDownInput) {
    const v = m.msg;
    if (!v) return;
    switch (v.case) {
      case "ack":
        w.hdr(3, ackCodec.len(v.value));
        ackCodec.write(w, v.value);
        break;
      case "input":
        w.hdr(2, inputDataCodec.len(v.value));
        inputDataCodec.write(w, v.value);
        break;
      case "opened":
        w.hdr(1, attemptOpenedCodec.len(v.value));
        attemptOpenedCodec.write(w, v.value);
        break;
      case "finished":
        w.hdr(4, attemptFinishedCodec.len(v.value));
        attemptFinishedCodec.write(w, v.value);
        break;
    }
  },
  read(r, end, out: AttemptDown = {}) {
    while (r.pos < end) {
      r.readTag(end);
      switch (r.fn) {
        case 3:
          r.want(WIRE_LEN);
          out.msg = {
            case: "ack",
            value: ackCodec.read(r, r.readSubEnd(end), out.msg?.case === "ack" ? out.msg.value : undefined),
          };
          break;
        case 2:
          r.want(WIRE_LEN);
          out.msg = {
            case: "input",
            value: inputDataCodec.read(r, r.readSubEnd(end), out.msg?.case === "input" ? out.msg.value : undefined),
          };
          break;
        case 1:
          r.want(WIRE_LEN);
          out.msg = {
            case: "opened",
            value: attemptOpenedCodec.read(r, r.readSubEnd(end), out.msg?.case === "opened" ? out.msg.value : undefined),
          };
          break;
        case 4:
          r.want(WIRE_LEN);
          out.msg = {
            case: "finished",
            value: attemptFinishedCodec.read(r, r.readSubEnd(end), out.msg?.case === "finished" ? out.msg.value : undefined),
          };
          break;
        default:
          r.skipField(r.wt, end);
      }
    }
    return out;
  },
};

// ---------- Message schemas (field number order, matching execution.proto) ----------

const S_Empty = def([], emptyCodecObj);
const S_Object = def([], objectCodec);
const S_Demand = def([[1, "interface", STR], [2, "requiredAttributes", SMAP], [3, "preferredAttributes", SMAP], [4, "requiredCapacity", IMAP], [5, "affinityKey", STR]]);
const S_ExecutionPolicy = def([[1, "durability", ENUM], [2, "priority", U32], [3, "queueTimeoutMs", U64], [4, "executionTimeoutMs", U64], [5, "totalTimeoutMs", U64], [6, "spilloverAfterMs", U64], [7, "maxAttempts", U32], [8, "fairnessKey", STR], [9, "concurrencyKey", STR]]);
const S_JobSpec = def([[1, "demand", MSG, S_Demand], [2, "inputs", RMSG, S_Object], [3, "params", BYTES], [4, "policy", MSG, S_ExecutionPolicy], [5, "metadata", SMAP]]);
const S_ErrorInfo = def([], errorInfoCodec);
const S_Job = def([[1, "jobId", STR], [2, "state", ENUM], [3, "durability", ENUM], [4, "interface", STR], [5, "attemptCount", U32], [6, "currentAttemptId", STR], [7, "currentWorkerId", STR], [8, "createdAtMs", U64], [9, "updatedAtMs", U64], [10, "error", MSG, S_ErrorInfo], [11, "outputs", RMSG, S_Object], [12, "metadata", SMAP], [13, "specHash", STR], [14, "namespace", STR]]);
const S_SubmitRequest = def([[1, "jobId", STR], [2, "conflictPolicy", ENUM], [3, "spec", MSG, S_JobSpec]]);
const S_SubmitResponse = def([[1, "job", MSG, S_Job], [2, "created", BOOL]]);
const S_InvokeStart = def([[1, "spec", MSG, S_JobSpec], [2, "streamingInput", BOOL]]);
const S_InputChunk = def([[1, "data", BYTES], [2, "metadata", SMAP]]);
const S_InvokeRequest = def([[0, "msg", ONEOF, [[1, "start", S_InvokeStart], [2, "input", S_InputChunk], [3, "inputEnd", S_Empty]]]]);
const S_Assigned = def([], assignedCodec);
const S_Output = def([], outputCodec);
const S_AttemptRestarted = def([], attemptRestartedCodec);
const S_ExecutionFinished = def([], executionFinishedCodec);
const S_ExecutionEvent = def([], executionEventCodec);
const S_WatchRequest = def([[1, "jobId", STR], [2, "afterSequence", U64]]);
const S_GetJobRequest = def([[1, "jobId", STR], [2, "includeOutputs", BOOL]]);
const S_CancelRequest = def([[1, "jobId", STR], [2, "reason", STR]]);
const S_ListJobsRequest = def([[1, "states", RENUM], [2, "limit", U32], [3, "pageToken", STR]]);
const S_ListJobsResponse = def([[1, "jobs", RMSG, S_Job], [2, "nextPageToken", STR]]);
const S_Hello = def([[1, "workerId", STR], [2, "resumeSessionId", STR], [3, "interfaces", RSTR], [4, "attributes", SMAP], [5, "totalCapacity", IMAP], [6, "maxConcurrency", U32], [7, "sdkVersion", STR], [8, "heldLeaseIds", RSTR], [9, "metadata", SMAP], [10, "pool", STR]]);
const S_Credit = def([], creditCodec);
const S_Load = def([], loadCodec);
const S_Heartbeat = def([], heartbeatCodec);
const S_AssignReply = def([], assignReplyCodec);
const S_CancelAck = def([], cancelAckCodec);
const S_Draining = def([], drainingCodec);
const workerMessageCodec: Codec = {
  len(m: WorkerMessageInput): number {
    const v = m.msg;
    if (!v) return 0;
    switch (v.case) {
      case "heartbeat": return lenTotal(3, heartbeatCodec.len(v.value));
      case "credit": return lenTotal(2, creditCodec.len(v.value));
      case "assignReply": return lenTotal(4, assignReplyCodec.len(v.value));
      case "cancelAck": return lenTotal(5, cancelAckCodec.len(v.value));
      case "draining": return lenTotal(6, drainingCodec.len(v.value));
      case "hello": return lenTotal(1, lenMsg(S_Hello, v.value, null));
    }
  },
  write(w, m: WorkerMessageInput) {
    const v = m.msg;
    if (!v) return;
    switch (v.case) {
      case "heartbeat":
        w.hdr(3, heartbeatCodec.len(v.value));
        heartbeatCodec.write(w, v.value);
        break;
      case "credit":
        w.hdr(2, creditCodec.len(v.value));
        creditCodec.write(w, v.value);
        break;
      case "assignReply":
        w.hdr(4, assignReplyCodec.len(v.value));
        assignReplyCodec.write(w, v.value);
        break;
      case "cancelAck":
        w.hdr(5, cancelAckCodec.len(v.value));
        cancelAckCodec.write(w, v.value);
        break;
      case "draining":
        w.hdr(6, drainingCodec.len(v.value));
        drainingCodec.write(w, v.value);
        break;
      case "hello":
        w.hdr(1, lenMsg(S_Hello, v.value, null));
        writeMsg(w, S_Hello, v.value);
        break;
    }
  },
  read(r, end, out: WorkerMessage = {}) {
    while (r.pos < end) {
      r.readTag(end);
      switch (r.fn) {
        case 3:
          r.want(WIRE_LEN);
          out.msg = {
            case: "heartbeat",
            value: heartbeatCodec.read(r, r.readSubEnd(end), out.msg?.case === "heartbeat" ? out.msg.value : undefined),
          };
          break;
        case 2:
          r.want(WIRE_LEN);
          out.msg = {
            case: "credit",
            value: creditCodec.read(r, r.readSubEnd(end), out.msg?.case === "credit" ? out.msg.value : undefined),
          };
          break;
        case 4:
          r.want(WIRE_LEN);
          out.msg = {
            case: "assignReply",
            value: assignReplyCodec.read(r, r.readSubEnd(end), out.msg?.case === "assignReply" ? out.msg.value : undefined),
          };
          break;
        case 5:
          r.want(WIRE_LEN);
          out.msg = {
            case: "cancelAck",
            value: cancelAckCodec.read(r, r.readSubEnd(end), out.msg?.case === "cancelAck" ? out.msg.value : undefined),
          };
          break;
        case 6:
          r.want(WIRE_LEN);
          out.msg = {
            case: "draining",
            value: drainingCodec.read(r, r.readSubEnd(end), out.msg?.case === "draining" ? out.msg.value : undefined),
          };
          break;
        case 1:
          r.want(WIRE_LEN);
          out.msg = {
            case: "hello",
            value: readMsg(S_Hello, r, r.readSubEnd(end), out.msg?.case === "hello" ? out.msg.value : undefined),
          };
          break;
        default:
          r.skipField(r.wt, end);
      }
    }
    return out;
  },
};
const S_WorkerMessage = def([], workerMessageCodec);
const S_Limits = def([], limitsCodec);
const S_Welcome = def([], welcomeCodec);
const S_Assign = def([], assignCodec);
const S_Cancel = def([], cancelCodec);
const S_ServerMessage = def([], serverMessageCodec);
const S_AttemptOpen = def([], attemptOpenCodec);
const S_Frame = def([], frameCodec);
const S_ResultChunk = def([], resultChunkCodec);
const S_Finish = def([], finishCodec);
const S_AttemptUp = def([], attemptUpCodec);
const S_AttemptOpened = def([], attemptOpenedCodec);
const S_InputData = def([], inputDataCodec);
const S_Ack = def([], ackCodec);
const S_AttemptFinished = def([], attemptFinishedCodec);
const S_AttemptDown = def([], attemptDownCodec);

// ---------- Public encode / decode API: [encodedLenX, encodeX, decodeX] ----------

export const [encodedLenObject, encodeObject, decodeObject] = codec<ProtoObjectInput, ProtoObject>(S_Object);
export const [encodedLenDemand, encodeDemand, decodeDemand] = codec<DemandInput, Demand>(S_Demand);
export const [encodedLenExecutionPolicy, encodeExecutionPolicy, decodeExecutionPolicy] = codec<ExecutionPolicyInput, ExecutionPolicy>(S_ExecutionPolicy);
export const [encodedLenJobSpec, encodeJobSpec, decodeJobSpec] = codec<JobSpecInput, JobSpec>(S_JobSpec);
export const [encodedLenErrorInfo, encodeErrorInfo, decodeErrorInfo] = codec<ErrorInfoInput, ErrorInfo>(S_ErrorInfo);
export const [encodedLenJob, encodeJob, decodeJob] = codec<JobInput, Job>(S_Job);
export const [encodedLenSubmitRequest, encodeSubmitRequest, decodeSubmitRequest] = codec<SubmitRequestInput, SubmitRequest>(S_SubmitRequest);
export const [encodedLenSubmitResponse, encodeSubmitResponse, decodeSubmitResponse] = codec<SubmitResponseInput, SubmitResponse>(S_SubmitResponse);
export const [encodedLenInvokeStart, encodeInvokeStart, decodeInvokeStart] = codec<InvokeStartInput, InvokeStart>(S_InvokeStart);
export const [encodedLenInputChunk, encodeInputChunk, decodeInputChunk] = codec<InputChunkInput, InputChunk>(S_InputChunk);
export const [encodedLenInputEnd, encodeInputEnd, decodeInputEnd] = emptyCodec<InputEnd>(S_Empty);
export const [encodedLenInvokeRequest, encodeInvokeRequest, decodeInvokeRequest] = codec<InvokeRequestInput, InvokeRequest>(S_InvokeRequest);
export const [encodedLenAccepted, encodeAccepted, decodeAccepted] = emptyCodec<Accepted>(S_Empty);
export const [encodedLenAssigned, encodeAssigned, decodeAssigned] = codec<AssignedInput, Assigned>(S_Assigned);
export const [encodedLenOutput, encodeOutput, decodeOutput] = codec<OutputInput, Output>(S_Output);
export const [encodedLenAttemptRestarted, encodeAttemptRestarted, decodeAttemptRestarted] = codec<AttemptRestartedInput, AttemptRestarted>(S_AttemptRestarted);
export const [encodedLenExecutionFinished, encodeExecutionFinished, decodeExecutionFinished] = codec<ExecutionFinishedInput, ExecutionFinished>(S_ExecutionFinished);
/** Encodes a `ServerMessage` with a single length pass (zero double-len of inner payload). */
function encodeServerMessageDirect(msg: ServerMessageInput, prefixByte: number): Uint8Array {
  const v = msg.msg;
  if (!v) {
    if (prefixByte < 0) return new Uint8Array(0);
    const out = new Uint8Array(1);
    out[0] = prefixByte;
    return out;
  }
  let fieldNo: number;
  let innerLen: number;
  switch (v.case) {
    case "assign": fieldNo = 2; innerLen = assignCodec.len(v.value); break;
    case "welcome": fieldNo = 1; innerLen = welcomeCodec.len(v.value); break;
    case "cancel": fieldNo = 3; innerLen = cancelCodec.len(v.value); break;
    case "fence": fieldNo = 4; innerLen = cancelCodec.len(v.value); break;
    case "drain": fieldNo = 5; innerLen = 0; break;
  }
  const total = (prefixByte >= 0 ? 1 : 0) + lenTotal(fieldNo, innerLen);
  const w = new ProtoWriter(total, null);
  if (prefixByte >= 0) w.writeByte(prefixByte);
  w.hdr(fieldNo, innerLen);
  switch (v.case) {
    case "assign": assignCodec.write(w, v.value); break;
    case "welcome": welcomeCodec.write(w, v.value); break;
    case "cancel": cancelCodec.write(w, v.value); break;
    case "fence": cancelCodec.write(w, v.value); break;
    case "drain": break;
  }
  return w.buf;
}

/** Encodes an `ExecutionEvent` with a single length pass (zero double-len of inner variant). */
function encodeExecutionEventDirect(m: ExecutionEventInput, prefixByte: number): Uint8Array {
  let n = strLen(1, m.jobId) + u64Len(2, m.sequence) + strLen(3, m.attemptId) + u64Len(4, m.atMs);
  const ev = m.event;
  let evFieldNo = 0;
  let evInnerLen = 0;
  if (ev) {
    switch (ev.case) {
      case "output": evFieldNo = 12; evInnerLen = outputCodec.len(ev.value); break;
      case "assigned": evFieldNo = 11; evInnerLen = assignedCodec.len(ev.value); break;
      case "accepted": evFieldNo = 10; evInnerLen = 0; break;
      case "finished": evFieldNo = 14; evInnerLen = executionFinishedCodec.len(ev.value); break;
      case "attemptRestarted": evFieldNo = 13; evInnerLen = attemptRestartedCodec.len(ev.value); break;
    }
    n += lenTotal(evFieldNo, evInnerLen);
  }
  if (n === 0 && prefixByte < 0) return new Uint8Array(0);
  const w = new ProtoWriter((prefixByte >= 0 ? 1 : 0) + n, null);
  if (prefixByte >= 0) w.writeByte(prefixByte);
  w.str(1, m.jobId);
  w.u64(2, m.sequence);
  w.str(3, m.attemptId);
  w.u64(4, m.atMs);
  if (ev) {
    w.hdr(evFieldNo, evInnerLen);
    switch (ev.case) {
      case "output": outputCodec.write(w, ev.value); break;
      case "assigned": assignedCodec.write(w, ev.value); break;
      case "accepted": break;
      case "finished": executionFinishedCodec.write(w, ev.value); break;
      case "attemptRestarted": attemptRestartedCodec.write(w, ev.value); break;
    }
  }
  return w.buf;
}

/** Encodes an `AttemptDown` with a single length pass (zero double-len of inner payload). */
function encodeAttemptDownDirect(msg: AttemptDownInput, prefixByte: number): Uint8Array {
  const v = msg.msg;
  if (!v) {
    if (prefixByte < 0) return new Uint8Array(0);
    const out = new Uint8Array(1);
    out[0] = prefixByte;
    return out;
  }
  let fieldNo: number;
  let innerLen: number;
  switch (v.case) {
    case "ack": fieldNo = 3; innerLen = ackCodec.len(v.value); break;
    case "input": fieldNo = 2; innerLen = inputDataCodec.len(v.value); break;
    case "opened": fieldNo = 1; innerLen = attemptOpenedCodec.len(v.value); break;
    case "finished": fieldNo = 4; innerLen = attemptFinishedCodec.len(v.value); break;
  }
  const w = new ProtoWriter((prefixByte >= 0 ? 1 : 0) + lenTotal(fieldNo, innerLen), null);
  if (prefixByte >= 0) w.writeByte(prefixByte);
  w.hdr(fieldNo, innerLen);
  switch (v.case) {
    case "ack": ackCodec.write(w, v.value); break;
    case "input": inputDataCodec.write(w, v.value); break;
    case "opened": attemptOpenedCodec.write(w, v.value); break;
    case "finished": attemptFinishedCodec.write(w, v.value); break;
  }
  return w.buf;
}

export const [encodedLenExecutionEvent, _encodeExecutionEventSchema, decodeExecutionEvent] = codec<ExecutionEventInput, ExecutionEvent>(S_ExecutionEvent);
export const encodeExecutionEvent = (event: ExecutionEventInput): Uint8Array => encodeExecutionEventDirect(event, -1);
void _encodeExecutionEventSchema;
export const [encodedLenWatchRequest, encodeWatchRequest, decodeWatchRequest] = codec<WatchRequestInput, WatchRequest>(S_WatchRequest);
export const [encodedLenGetJobRequest, encodeGetJobRequest, decodeGetJobRequest] = codec<GetJobRequestInput, GetJobRequest>(S_GetJobRequest);
export const [encodedLenCancelRequest, encodeCancelRequest, decodeCancelRequest] = codec<CancelRequestInput, CancelRequest>(S_CancelRequest);
export const [encodedLenListJobsRequest, encodeListJobsRequest, decodeListJobsRequest] = codec<ListJobsRequestInput, ListJobsRequest>(S_ListJobsRequest);
export const [encodedLenListJobsResponse, encodeListJobsResponse, decodeListJobsResponse] = codec<ListJobsResponseInput, ListJobsResponse>(S_ListJobsResponse);
export const [encodedLenHello, encodeHello, decodeHello] = codec<HelloInput, Hello>(S_Hello);
export const [encodedLenCredit, encodeCredit, decodeCredit] = codec<CreditInput, Credit>(S_Credit);
export const [encodedLenLoad, encodeLoad, decodeLoad] = codec<LoadInput, Load>(S_Load);
export const [encodedLenHeartbeat, encodeHeartbeat, decodeHeartbeat] = codec<HeartbeatInput, Heartbeat>(S_Heartbeat);
export const [encodedLenAssignReply, encodeAssignReply, decodeAssignReply] = codec<AssignReplyInput, AssignReply>(S_AssignReply);
export const [encodedLenCancelAck, encodeCancelAck, decodeCancelAck] = codec<CancelAckInput, CancelAck>(S_CancelAck);
export const [encodedLenDraining, encodeDraining, decodeDraining] = codec<DrainingInput, Draining>(S_Draining);
export const [encodedLenWorkerMessage, encodeWorkerMessage, decodeWorkerMessage] = codec<WorkerMessageInput, WorkerMessage>(S_WorkerMessage);
export const [encodedLenLimits, encodeLimits, decodeLimits] = codec<LimitsInput, Limits>(S_Limits);
export const [encodedLenWelcome, encodeWelcome, decodeWelcome] = codec<WelcomeInput, Welcome>(S_Welcome);
export const [encodedLenAssign, encodeAssign, decodeAssign] = codec<AssignInput, Assign>(S_Assign);
export const [encodedLenCancel, encodeCancel, decodeCancel] = codec<CancelInput, Cancel>(S_Cancel);
export const [encodedLenFence, encodeFence, decodeFence] = codec<FenceInput, Fence>(S_Cancel);
export const [encodedLenDrain, encodeDrain, decodeDrain] = emptyCodec<Drain>(S_Empty);
export const [encodedLenServerMessage, _encodeServerMessageSchema, decodeServerMessage] = codec<ServerMessageInput, ServerMessage>(S_ServerMessage);
export const encodeServerMessage = (msg: ServerMessageInput): Uint8Array => encodeServerMessageDirect(msg, -1);
void _encodeServerMessageSchema;
export const [encodedLenAttemptOpen, encodeAttemptOpen, decodeAttemptOpen] = codec<AttemptOpenInput, AttemptOpen>(S_AttemptOpen);
export const [encodedLenFrame, encodeFrame, decodeFrame] = codec<FrameInput, Frame>(S_Frame);
export const [encodedLenResultChunk, encodeResultChunk, decodeResultChunk] = codec<ResultChunkInput, ResultChunk>(S_ResultChunk);
export const [encodedLenSucceeded, encodeSucceeded, decodeSucceeded] = emptyCodec<Succeeded>(S_Empty);
export const [encodedLenCancelled, encodeCancelled, decodeCancelled] = emptyCodec<Cancelled>(S_Empty);
export const [encodedLenFinish, encodeFinish, decodeFinish] = codec<FinishInput, Finish>(S_Finish);
export const [encodedLenAttemptUp, encodeAttemptUp, decodeAttemptUp] = codec<AttemptUpInput, AttemptUp>(S_AttemptUp);
export const [encodedLenAttemptOpened, encodeAttemptOpened, decodeAttemptOpened] = codec<AttemptOpenedInput, AttemptOpened>(S_AttemptOpened);
export const [encodedLenInputData, encodeInputData, decodeInputData] = codec<InputDataInput, InputData>(S_InputData);
export const [encodedLenAck, encodeAck, decodeAck] = codec<AckInput, Ack>(S_Ack);
export const [encodedLenAttemptFinished, encodeAttemptFinished, decodeAttemptFinished] = codec<AttemptFinishedInput, AttemptFinished>(S_AttemptFinished);
export const [encodedLenAttemptDown, _encodeAttemptDownSchema, decodeAttemptDown] = codec<AttemptDownInput, AttemptDown>(S_AttemptDown);
export const encodeAttemptDown = (msg: AttemptDownInput): Uint8Array => encodeAttemptDownDirect(msg, -1);
void _encodeAttemptDownSchema;

// ---------- WebSocket Downstream Framing ----------

export type WsFrame =
  | { kind: "data"; payload: Uint8Array }
  | { kind: "error"; code: number; message: string };

/** Encodes a downstream WebSocket data frame: `0x00 || payload`. */
export function encodeWsDataFrame(payload: Uint8Array): Uint8Array {
  const out = new Uint8Array(1 + payload.byteLength);
  out[0] = 0x00;
  out.set(payload, 1);
  return out;
}

/** Encodes a downstream WebSocket error frame: `0x01 || u8(code) || utf8(message)`. */
export function encodeWsErrorFrame(code: number, message: string): Uint8Array {
  const msgBytes = utf8ByteLen(message);
  const out = new Uint8Array(2 + msgBytes);
  out[0] = 0x01;
  out[1] = code & 0xff;
  if (msgBytes > 0) utf8Encoder.encodeInto(message, out.subarray(2));
  return out;
}

/** Decodes a downstream WebSocket binary frame (`0x00 || proto` or `0x01 || code || utf8`). */
export function decodeWsFrame(frame: Uint8Array): WsFrame {
  if (frame.byteLength < 1) throw new ProtoDecodeError("empty WebSocket frame");
  const tag = frame[0]!;
  if (tag === 0x00) return { kind: "data", payload: frame.subarray(1) };
  if (tag === 0x01) {
    if (frame.byteLength < 2) throw new ProtoDecodeError("truncated WebSocket error frame");
    const code = frame[1]!;
    let message: string;
    try {
      message = fatalUtf8Decoder.decode(frame.subarray(2));
    } catch {
      throw new ProtoDecodeError("invalid UTF-8 in WebSocket error frame");
    }
    return { kind: "error", code, message };
  }
  throw new ProtoDecodeError(`unknown WebSocket frame tag 0x${tag.toString(16)}`);
}

/**
 * Exact byte length of a downstream `0x00 || ExecutionEvent` WebSocket frame,
 * including `ExecutionFinished` with segmented multi-MiB outputs.
 */
export function encodedLenExecutionEventWsFrame(event: ExecutionEventInput): number {
  return 1 + encodedLenExecutionEvent(event);
}

/**
 * Single-allocation pre-sized encoder for `0x00 || ExecutionEvent` WebSocket frames
 * (including `ExecutionEvent{Finished}` with multi-MiB or segmented `Uint8Array[]`
 * output payloads).
 */
export function encodeExecutionEventWsFrame(event: ExecutionEventInput): Uint8Array {
  return encodeExecutionEventDirect(event, 0x00);
}

/** Single-allocation pre-sized encoder for `0x00 || ServerMessage` WebSocket frames. */
export function encodeServerMessageWsFrame(msg: ServerMessageInput): Uint8Array {
  return encodeServerMessageDirect(msg, 0x00);
}

/** Single-allocation pre-sized encoder for `0x00 || AttemptDown` WebSocket frames. */
export function encodeAttemptDownWsFrame(msg: AttemptDownInput): Uint8Array {
  return encodeAttemptDownDirect(msg, 0x00);
}

const finishedFrameEvent = (input: ExecutionFinishedFrameInput): ExecutionEventInput => ({
  jobId: input.jobId,
  sequence: input.sequence,
  attemptId: input.attemptId,
  atMs: input.atMs,
  event: { case: "finished", value: { state: input.state, outputs: input.outputs, error: input.error } },
});

/** Exact byte length of `0x00 || ExecutionEvent{Finished}` frame. */
export function encodedLenExecutionFinishedWsFrame(input: ExecutionFinishedFrameInput): number {
  return encodedLenExecutionEventWsFrame(finishedFrameEvent(input));
}

/**
 * Single-allocation pre-sized encoder specifically for `ExecutionEvent{Finished}`
 * WebSocket frames (`0x00 || ExecutionEvent`). Supports both contiguous `Uint8Array`
 * and segmented `readonly Uint8Array[]` output inline payloads without intermediate
 * copies.
 */
export function encodeExecutionFinishedWsFrame(input: ExecutionFinishedFrameInput): Uint8Array {
  return encodeExecutionEventWsFrame(finishedFrameEvent(input));
}
