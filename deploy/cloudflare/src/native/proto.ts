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
export interface Finish { outcome?: FinishOutcome }
export interface FinishInput { outcome?: FinishOutcomeInput }

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
  const keys = globalThis.Object.keys(map);
  let total = 0;
  for (let i = 0; i < keys.length; i++) {
    const k = keys[i]!;
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
  private dv: DataView | null = null;

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
    this.dv ??= new DataView(this.buf.buffer, this.buf.byteOffset, this.buf.byteLength);
    this.dv.setFloat64(this.pos, value, true);
    this.pos += 8;
  }

  str(no: number, value: string | undefined): void {
    if (value) this.astr(no, value);
  }

  /** Always-emitted string (oneof / repeated members). */
  astr(no: number, value: string): void {
    this.hdr(no, utf8ByteLen(value));
    this.rawStr(value);
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
  private dv: DataView | null = null;

  constructor(buf: Uint8Array) {
    this.buf = buf;
  }

  readVarint64(end: number): bigint {
    const buf = this.buf;
    let lo = 0;
    let i = 0;
    for (;;) {
      if (this.pos >= end) throw new ProtoDecodeError("unexpected EOF while reading varint");
      const b = buf[this.pos++]!;
      lo += (b & 0x7f) * 2 ** (7 * i);
      if (b < 0x80) return BigInt(lo);
      if (++i === 7) break;
    }
    let result = BigInt(lo);
    for (; i < 10; i++) {
      if (this.pos >= end) throw new ProtoDecodeError("unexpected EOF while reading varint");
      const b = buf[this.pos++]!;
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
    if (this.pos + 8 > end) throw new ProtoDecodeError("unexpected EOF while reading 64-bit double");
    this.dv ??= new DataView(this.buf.buffer, this.buf.byteOffset, this.buf.byteLength);
    const val = this.dv.getFloat64(this.pos, true);
    this.pos += 8;
    return val;
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

/** Expected wire type per kind (index = kind). */
const WIRE_OF = [2, 2, 0, 0, 0, 0, 1, 2, 2, 2, 2, 2, 2, 2, 0, 2, 2];

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
    if (kind === MSG || kind === OPT32) continue;
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
      case U64: out[name] = r.readUint64(end); break;
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

// ---------- Message schemas (field number order, matching execution.proto) ----------

const S_Empty = def([]);
const S_Object = def([], objectCodec);
const S_Demand = def([[1, "interface", STR], [2, "requiredAttributes", SMAP], [3, "preferredAttributes", SMAP], [4, "requiredCapacity", IMAP], [5, "affinityKey", STR]]);
const S_ExecutionPolicy = def([[1, "durability", ENUM], [2, "priority", U32], [3, "queueTimeoutMs", U64], [4, "executionTimeoutMs", U64], [5, "totalTimeoutMs", U64], [6, "spilloverAfterMs", U64], [7, "maxAttempts", U32], [8, "fairnessKey", STR], [9, "concurrencyKey", STR]]);
const S_JobSpec = def([[1, "demand", MSG, S_Demand], [2, "inputs", RMSG, S_Object], [3, "params", BYTES], [4, "policy", MSG, S_ExecutionPolicy], [5, "metadata", SMAP]]);
const S_ErrorInfo = def([[1, "code", STR], [2, "message", STR], [3, "retryable", BOOL]]);
const S_Job = def([[1, "jobId", STR], [2, "state", ENUM], [3, "durability", ENUM], [4, "interface", STR], [5, "attemptCount", U32], [6, "currentAttemptId", STR], [7, "currentWorkerId", STR], [8, "createdAtMs", U64], [9, "updatedAtMs", U64], [10, "error", MSG, S_ErrorInfo], [11, "outputs", RMSG, S_Object], [12, "metadata", SMAP], [13, "specHash", STR], [14, "namespace", STR]]);
const S_SubmitRequest = def([[1, "jobId", STR], [2, "conflictPolicy", ENUM], [3, "spec", MSG, S_JobSpec]]);
const S_SubmitResponse = def([[1, "job", MSG, S_Job], [2, "created", BOOL]]);
const S_InvokeStart = def([[1, "spec", MSG, S_JobSpec], [2, "streamingInput", BOOL]]);
const S_InputChunk = def([[1, "data", BYTES], [2, "metadata", SMAP]]);
const S_InvokeRequest = def([[0, "msg", ONEOF, [[1, "start", S_InvokeStart], [2, "input", S_InputChunk], [3, "inputEnd", S_Empty]]]]);
const S_Assigned = def([[1, "workerId", STR], [2, "attemptNumber", U32]]);
const S_Output = def([[1, "data", BYTES], [2, "metadata", SMAP], [3, "progress", BOOL]]);
const S_AttemptRestarted = def([[1, "previousAttemptId", STR], [2, "reason", MSG, S_ErrorInfo]]);
const S_ExecutionFinished = def([[1, "state", ENUM], [2, "outputs", RMSG, S_Object], [3, "error", MSG, S_ErrorInfo]]);
const S_ExecutionEvent = def([[1, "jobId", STR], [2, "sequence", U64], [3, "attemptId", STR], [4, "atMs", U64], [0, "event", ONEOF, [[10, "accepted", S_Empty], [11, "assigned", S_Assigned], [12, "output", S_Output], [13, "attemptRestarted", S_AttemptRestarted], [14, "finished", S_ExecutionFinished]]]]);
const S_WatchRequest = def([[1, "jobId", STR], [2, "afterSequence", U64]]);
const S_GetJobRequest = def([[1, "jobId", STR], [2, "includeOutputs", BOOL]]);
const S_CancelRequest = def([[1, "jobId", STR], [2, "reason", STR]]);
const S_ListJobsRequest = def([[1, "states", RENUM], [2, "limit", U32], [3, "pageToken", STR]]);
const S_ListJobsResponse = def([[1, "jobs", RMSG, S_Job], [2, "nextPageToken", STR]]);
const S_Hello = def([[1, "workerId", STR], [2, "resumeSessionId", STR], [3, "interfaces", RSTR], [4, "attributes", SMAP], [5, "totalCapacity", IMAP], [6, "maxConcurrency", U32], [7, "sdkVersion", STR], [8, "heldLeaseIds", RSTR], [9, "metadata", SMAP], [10, "pool", STR]]);
const S_Credit = def([[1, "freeSlots", U32]]);
const S_Load = def([[1, "queued", U32], [2, "running", U32], [3, "utilization", DBL], [4, "custom", DMAP]]);
const S_Heartbeat = def([[1, "load", MSG, S_Load]]);
const S_AssignReply = def([[1, "leaseId", STR], [2, "accepted", BOOL], [3, "rejectReason", STR]]);
const S_CancelAck = def([[1, "leaseId", STR]]);
const S_Draining = def([[1, "preempting", BOOL]]);
const S_WorkerMessage = def([[0, "msg", ONEOF, [[1, "hello", S_Hello], [2, "credit", S_Credit], [3, "heartbeat", S_Heartbeat], [4, "assignReply", S_AssignReply], [5, "cancelAck", S_CancelAck], [6, "draining", S_Draining]]]]);
const S_Limits = def([[1, "maxControlMessageBytes", U32], [2, "maxChunkBytes", U32], [3, "attemptWindowBytes", U32], [4, "assignInlineLimit", U32]]);
const S_Welcome = def([[1, "sessionId", STR], [2, "generation", U64], [3, "heartbeatIntervalMs", U32], [4, "heartbeatTimeoutMs", U32], [5, "limits", MSG, S_Limits], [6, "resumedLeaseIds", RSTR]]);
const S_Assign = def([[1, "leaseId", STR], [2, "attemptId", STR], [3, "jobId", STR], [4, "attemptNumber", U32], [5, "generation", U64], [6, "interface", STR], [7, "params", BYTES], [8, "reservedCapacity", IMAP], [9, "metadata", SMAP], [10, "inlineInputs", RMSG, S_Object], [11, "inputsViaAttempt", BOOL], [12, "streamingInput", BOOL], [13, "executionDeadlineMs", U64], [14, "totalDeadlineMs", U64]]);
const S_Cancel = def([[1, "leaseId", STR], [2, "reason", STR]]);
const S_ServerMessage = def([[0, "msg", ONEOF, [[1, "welcome", S_Welcome], [2, "assign", S_Assign], [3, "cancel", S_Cancel], [4, "fence", S_Cancel], [5, "drain", S_Empty]]]]);
const S_AttemptOpen = def([[1, "leaseId", STR], [2, "generation", U64], [3, "resumeAfterSequence", U64]]);
const S_Frame = def([], frameCodec);
const S_ResultChunk = def([], resultChunkCodec);
const S_Finish = def([[0, "outcome", ONEOF, [[1, "succeeded", S_Empty], [2, "failed", S_ErrorInfo], [3, "cancelled", S_Empty]]]]);
const S_AttemptUp = def([[0, "msg", ONEOF, [[1, "open", S_AttemptOpen], [2, "frame", S_Frame], [3, "result", S_ResultChunk], [5, "finish", S_Finish]]]]);
const S_AttemptOpened = def([[1, "resumeFromSequence", U64], [2, "terminalReplay", BOOL]]);
const S_InputData = def([], inputDataCodec);
const S_Ack = def([], ackCodec);
const S_AttemptFinished = def([[1, "status", ENUM], [2, "reason", STR]]);
const S_AttemptDown = def([[0, "msg", ONEOF, [[1, "opened", S_AttemptOpened], [2, "input", S_InputData], [3, "ack", S_Ack], [4, "finished", S_AttemptFinished]]]]);

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
export const [encodedLenExecutionEvent, encodeExecutionEvent, decodeExecutionEvent] = codec<ExecutionEventInput, ExecutionEvent>(S_ExecutionEvent);
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
export const [encodedLenServerMessage, encodeServerMessage, decodeServerMessage] = codec<ServerMessageInput, ServerMessage>(S_ServerMessage);
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
export const [encodedLenAttemptDown, encodeAttemptDown, decodeAttemptDown] = codec<AttemptDownInput, AttemptDown>(S_AttemptDown);

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
 * output payloads). Computes exact length first (recording nested lengths once) and
 * writes directly into one `Uint8Array` with no intermediate concatenation.
 */
export function encodeExecutionEventWsFrame(event: ExecutionEventInput): Uint8Array {
  const L: number[] = [];
  const w = new ProtoWriter(1 + lenMsg(S_ExecutionEvent, event, L), L);
  w.writeByte(0x00);
  writeMsg(w, S_ExecutionEvent, event);
  return w.buf;
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
