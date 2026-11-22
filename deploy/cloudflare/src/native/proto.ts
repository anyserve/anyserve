/**
 * Hand-written zero-eval Protobuf v3 codec for `crates/anyserve-proto/protos/execution.proto`
 * (`package anyserve.v2`), plus AnyServe WebSocket binary frame helpers.
 *
 * Compatible with both Cloudflare `workerd` (strict CSP: no dynamic code generation)
 * and plain Node 24, with zero host-specific runtime imports.
 */

const WIRE_VARINT = 0;
const WIRE_64BIT = 1;
const WIRE_LEN = 2;
const WIRE_32BIT = 5;

const utf8Encoder = new TextEncoder();
const fatalUtf8Decoder = new TextDecoder("utf-8", {
  fatal: true,
  ignoreBOM: false,
});
const EMPTY_BYTES = new Uint8Array(0);

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

export type ObjectBody =
  | { case: "inline"; value: Uint8Array }
  | { case: "uri"; value: string };

export interface ProtoObject {
  body?: ObjectBody;
  metadata: Record<string, string>;
}

export type { ProtoObject as Object };

export interface ProtoObjectInput {
  body?: ObjectBody;
  metadata?: Record<string, string>;
}

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

export interface ErrorInfo {
  code: string;
  message: string;
  retryable: boolean;
}

export interface ErrorInfoInput {
  code?: string;
  message?: string;
  retryable?: boolean;
}

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

export interface SubmitRequest {
  jobId: string;
  conflictPolicy: ConflictPolicy;
  spec?: JobSpec;
}

export interface SubmitRequestInput {
  jobId?: string;
  conflictPolicy?: ConflictPolicy;
  spec?: JobSpecInput;
}

export interface SubmitResponse {
  job?: Job;
  created: boolean;
}

export interface SubmitResponseInput {
  job?: JobInput;
  created?: boolean;
}

export interface InvokeStart {
  spec?: JobSpec;
  streamingInput: boolean;
}

export interface InvokeStartInput {
  spec?: JobSpecInput;
  streamingInput?: boolean;
}

export interface InputChunk {
  data: Uint8Array;
  metadata: Record<string, string>;
}

export interface InputChunkInput {
  data?: Uint8Array;
  metadata?: Record<string, string>;
}

export interface InputEnd {}

export type InvokeRequestMsg =
  | { case: "start"; value: InvokeStart }
  | { case: "input"; value: InputChunk }
  | { case: "inputEnd"; value: InputEnd };

export type InvokeRequestMsgInput =
  | { case: "start"; value: InvokeStartInput }
  | { case: "input"; value: InputChunkInput }
  | { case: "inputEnd"; value?: InputEnd };

export interface InvokeRequest {
  msg?: InvokeRequestMsg;
}

export interface InvokeRequestInput {
  msg?: InvokeRequestMsgInput;
}

export interface Accepted {}

export interface Assigned {
  workerId: string;
  attemptNumber: number;
}

export interface AssignedInput {
  workerId?: string;
  attemptNumber?: number;
}

export interface Output {
  data: Uint8Array;
  metadata: Record<string, string>;
  progress: boolean;
}

export interface OutputInput {
  data?: Uint8Array;
  metadata?: Record<string, string>;
  progress?: boolean;
}

export interface AttemptRestarted {
  previousAttemptId: string;
  reason?: ErrorInfo;
}

export interface AttemptRestartedInput {
  previousAttemptId?: string;
  reason?: ErrorInfoInput;
}

export interface ExecutionFinished {
  state: JobState;
  outputs: ProtoObject[];
  error?: ErrorInfo;
}

/**
 * Allows segmented/chunked inline bytes (`readonly Uint8Array[]`) when pre-sizing
 * and writing large `ExecutionFinished` frames directly into one buffer without
 * concatenating multi-MiB objects first.
 */
export type SegmentedObjectBody =
  | { case: "inline"; value: Uint8Array | readonly Uint8Array[] }
  | { case: "uri"; value: string };

export interface SegmentedObjectInput {
  body?: SegmentedObjectBody;
  metadata?: Record<string, string>;
}

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

export interface WatchRequest {
  jobId: string;
  afterSequence: bigint;
}

export interface WatchRequestInput {
  jobId?: string;
  afterSequence?: bigint;
}

export interface GetJobRequest {
  jobId: string;
  includeOutputs: boolean;
}

export interface GetJobRequestInput {
  jobId?: string;
  includeOutputs?: boolean;
}

export interface CancelRequest {
  jobId: string;
  reason: string;
}

export interface CancelRequestInput {
  jobId?: string;
  reason?: string;
}

export interface ListJobsRequest {
  states: JobState[];
  limit: number;
  pageToken: string;
}

export interface ListJobsRequestInput {
  states?: readonly JobState[];
  limit?: number;
  pageToken?: string;
}

export interface ListJobsResponse {
  jobs: Job[];
  nextPageToken: string;
}

export interface ListJobsResponseInput {
  jobs?: readonly JobInput[];
  nextPageToken?: string;
}

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

export interface Credit {
  freeSlots: number;
}

export interface CreditInput {
  freeSlots?: number;
}

export interface Load {
  queued: number;
  running: number;
  utilization: number;
  custom: Record<string, number>;
}

export interface LoadInput {
  queued?: number;
  running?: number;
  utilization?: number;
  custom?: Record<string, number>;
}

export interface Heartbeat {
  load?: Load;
}

export interface HeartbeatInput {
  load?: LoadInput;
}

export interface AssignReply {
  leaseId: string;
  accepted: boolean;
  rejectReason: string;
}

export interface AssignReplyInput {
  leaseId?: string;
  accepted?: boolean;
  rejectReason?: string;
}

export interface CancelAck {
  leaseId: string;
}

export interface CancelAckInput {
  leaseId?: string;
}

export interface Draining {
  preempting: boolean;
}

export interface DrainingInput {
  preempting?: boolean;
}

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

export interface WorkerMessage {
  msg?: WorkerMessageVariant;
}

export interface WorkerMessageInput {
  msg?: WorkerMessageVariantInput;
}

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

export interface Cancel {
  leaseId: string;
  reason: string;
}

export interface CancelInput {
  leaseId?: string;
  reason?: string;
}

export interface Fence {
  leaseId: string;
  reason: string;
}

export interface FenceInput {
  leaseId?: string;
  reason?: string;
}

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

export interface ServerMessage {
  msg?: ServerMessageVariant;
}

export interface ServerMessageInput {
  msg?: ServerMessageVariantInput;
}

export interface AttemptOpen {
  leaseId: string;
  generation: bigint;
  resumeAfterSequence: bigint;
}

export interface AttemptOpenInput {
  leaseId?: string;
  generation?: bigint;
  resumeAfterSequence?: bigint;
}

export interface Frame {
  sequence: bigint;
  data: Uint8Array;
  metadata: Record<string, string>;
  progress: boolean;
}

export interface FrameInput {
  sequence?: bigint;
  data?: Uint8Array;
  metadata?: Record<string, string>;
  progress?: boolean;
}

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

export interface Finish {
  outcome?: FinishOutcome;
}

export interface FinishInput {
  outcome?: FinishOutcomeInput;
}

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

export interface AttemptUp {
  msg?: AttemptUpVariant;
}

export interface AttemptUpInput {
  msg?: AttemptUpVariantInput;
}

export interface AttemptOpened {
  resumeFromSequence: bigint;
  terminalReplay: boolean;
}

export interface AttemptOpenedInput {
  resumeFromSequence?: bigint;
  terminalReplay?: boolean;
}

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

export interface Ack {
  upToSequence: bigint;
  resultBytes: bigint;
}

export interface AckInput {
  upToSequence?: bigint;
  resultBytes?: bigint;
}

export interface AttemptFinished {
  status: AttemptFinishedStatus;
  reason: string;
}

export interface AttemptFinishedInput {
  status?: AttemptFinishedStatus;
  reason?: string;
}

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

export interface AttemptDown {
  msg?: AttemptDownVariant;
}

export interface AttemptDownInput {
  msg?: AttemptDownVariantInput;
}

// ---------- Wire Primitives ----------

function utf8ByteLen(str: string): number {
  let bytes = 0;
  const len = str.length;
  for (let i = 0; i < len; i++) {
    const code = str.charCodeAt(i);
    if (code < 0x80) {
      bytes += 1;
    } else if (code < 0x800) {
      bytes += 2;
    } else if (
      code >= 0xd800 &&
      code <= 0xdbff &&
      i + 1 < len &&
      (str.charCodeAt(i + 1) & 0xfc00) === 0xdc00
    ) {
      bytes += 4;
      i++;
    } else {
      bytes += 3;
    }
  }
  return bytes;
}

function varint32Len(value: number): number {
  const u = value >>> 0;
  if (u < 0x80) return 1;
  if (u < 0x4000) return 2;
  if (u < 0x200000) return 3;
  if (u < 0x10000000) return 4;
  return 5;
}

function varint64Len(value: bigint): number {
  let u = BigInt.asUintN(64, value);
  let n = 1;
  while (u >= 0x80n) {
    u >>= 7n;
    n++;
  }
  return n;
}

function enumVarintLen(value: number): number {
  const i = value | 0;
  return i < 0 ? 10 : varint32Len(i);
}

function tagLen(fieldNo: number): number {
  return varint32Len(fieldNo << 3);
}

function lenDelimitedTotal(fieldNo: number, innerLen: number): number {
  return tagLen(fieldNo) + varint32Len(innerLen) + innerLen;
}

function stringFieldLen(fieldNo: number, value: string | undefined): number {
  if (!value) return 0;
  const n = utf8ByteLen(value);
  return lenDelimitedTotal(fieldNo, n);
}

function bytesFieldLen(fieldNo: number, value: Uint8Array | undefined): number {
  if (!value || value.byteLength === 0) return 0;
  return lenDelimitedTotal(fieldNo, value.byteLength);
}

function segmentedBytesTotalLen(value: Uint8Array | readonly Uint8Array[]): number {
  if (value instanceof Uint8Array) {
    return value.byteLength;
  }
  let sum = 0;
  for (let i = 0; i < value.length; i++) {
    sum += value[i]!.byteLength;
  }
  return sum;
}

function uint32FieldLen(fieldNo: number, value: number | undefined): number {
  const u = (value ?? 0) >>> 0;
  if (u === 0) return 0;
  return tagLen(fieldNo) + varint32Len(u);
}

function uint64FieldLen(fieldNo: number, value: bigint | undefined): number {
  const u = BigInt.asUintN(64, value ?? 0n);
  if (u === 0n) return 0;
  return tagLen(fieldNo) + varint64Len(u);
}

function boolFieldLen(fieldNo: number, value: boolean | undefined): number {
  if (!value) return 0;
  return tagLen(fieldNo) + 1;
}

function enumFieldLen(fieldNo: number, value: number | undefined): number {
  const v = (value ?? 0) | 0;
  if (v === 0) return 0;
  return tagLen(fieldNo) + enumVarintLen(v);
}

function doubleFieldLen(fieldNo: number, value: number | undefined): number {
  if (value === undefined || value === 0) return 0;
  return tagLen(fieldNo) + 8;
}

function compareBytesLex(a: Uint8Array, b: Uint8Array): number {
  const min = a.byteLength < b.byteLength ? a.byteLength : b.byteLength;
  for (let i = 0; i < min; i++) {
    const diff = a[i]! - b[i]!;
    if (diff !== 0) return diff;
  }
  return a.byteLength - b.byteLength;
}

function sortedMapKeys(record: Record<string, unknown> | undefined): string[] {
  if (!record) return [];
  const keys = globalThis.Object.keys(record);
  if (keys.length <= 1) return keys;
  // Fast ASCII check; fall back to exact UTF-8 byte sort if any key has non-ASCII code units.
  let allAscii = true;
  outer: for (let i = 0; i < keys.length; i++) {
    const k = keys[i]!;
    for (let j = 0; j < k.length; j++) {
      if (k.charCodeAt(j) >= 0x80) {
        allAscii = false;
        break outer;
      }
    }
  }
  if (allAscii) {
    keys.sort();
    return keys;
  }
  const encoded = keys.map((k) => ({ k, b: utf8Encoder.encode(k) }));
  encoded.sort((x, y) => compareBytesLex(x.b, y.b));
  return encoded.map((e) => e.k);
}

function stringMapFieldLen(
  fieldNo: number,
  map: Record<string, string> | undefined,
): number {
  if (!map) return 0;
  const keys = globalThis.Object.keys(map);
  let total = 0;
  for (let i = 0; i < keys.length; i++) {
    const k = keys[i]!;
    const v = map[k]!;
    const entryLen = stringFieldLen(1, k) + stringFieldLen(2, v);
    total += lenDelimitedTotal(fieldNo, entryLen);
  }
  return total;
}

function int64MapFieldLen(
  fieldNo: number,
  map: Record<string, bigint> | undefined,
): number {
  if (!map) return 0;
  const keys = globalThis.Object.keys(map);
  let total = 0;
  for (let i = 0; i < keys.length; i++) {
    const k = keys[i]!;
    const v = map[k]!;
    const vLen = v === 0n ? 0 : 1 + varint64Len(v);
    const entryLen = stringFieldLen(1, k) + vLen;
    total += lenDelimitedTotal(fieldNo, entryLen);
  }
  return total;
}

function doubleMapFieldLen(
  fieldNo: number,
  map: Record<string, number> | undefined,
): number {
  if (!map) return 0;
  const keys = globalThis.Object.keys(map);
  let total = 0;
  for (let i = 0; i < keys.length; i++) {
    const k = keys[i]!;
    const v = map[k]!;
    const vLen = v === 0 ? 0 : 1 + 8;
    const entryLen = stringFieldLen(1, k) + vLen;
    total += lenDelimitedTotal(fieldNo, entryLen);
  }
  return total;
}

function repeatedStringFieldLen(
  fieldNo: number,
  items: readonly string[] | undefined,
): number {
  if (!items || items.length === 0) return 0;
  let total = 0;
  for (let i = 0; i < items.length; i++) {
    const n = utf8ByteLen(items[i]!);
    total += lenDelimitedTotal(fieldNo, n);
  }
  return total;
}

function setMapProp<V>(target: Record<string, V>, key: string, value: V): void {
  if (key === "__proto__") {
    globalThis.Object.defineProperty(target, key, {
      value,
      enumerable: true,
      writable: true,
      configurable: true,
    });
  } else {
    target[key] = value;
  }
}

class ProtoWriter {
  readonly buf: Uint8Array;
  pos = 0;
  private dv: DataView | null = null;

  constructor(size: number) {
    this.buf = new Uint8Array(size);
  }

  private view(): DataView {
    if (!this.dv) {
      this.dv = new DataView(
        this.buf.buffer,
        this.buf.byteOffset,
        this.buf.byteLength,
      );
    }
    return this.dv;
  }

  writeByte(b: number): void {
    this.buf[this.pos++] = b & 0xff;
  }

  writeVarint32(value: number): void {
    let u = value >>> 0;
    while (u >= 0x80) {
      this.buf[this.pos++] = (u & 0x7f) | 0x80;
      u >>>= 7;
    }
    this.buf[this.pos++] = u;
  }

  writeVarint64(value: bigint): void {
    let u = BigInt.asUintN(64, value);
    while (u >= 0x80n) {
      this.buf[this.pos++] = Number(u & 0x7fn) | 0x80;
      u >>= 7n;
    }
    this.buf[this.pos++] = Number(u);
  }

  writeEnumVal(value: number): void {
    const v = value | 0;
    if (v < 0) {
      this.writeVarint64(BigInt(v));
    } else {
      this.writeVarint32(v);
    }
  }

  writeTag(fieldNo: number, wireType: number): void {
    this.writeVarint32((fieldNo << 3) | wireType);
  }

  writeDoubleVal(value: number): void {
    this.view().setFloat64(this.pos, value, true);
    this.pos += 8;
  }

  writeRawBytes(bytes: Uint8Array): void {
    if (bytes.byteLength > 0) {
      this.buf.set(bytes, this.pos);
      this.pos += bytes.byteLength;
    }
  }

  writeRawString(str: string): void {
    if (str.length === 0) return;
    const res = utf8Encoder.encodeInto(str, this.buf.subarray(this.pos));
    this.pos += res.written;
  }

  writeUint32Field(fieldNo: number, value: number | undefined): void {
    const u = (value ?? 0) >>> 0;
    if (u === 0) return;
    this.writeTag(fieldNo, WIRE_VARINT);
    this.writeVarint32(u);
  }

  writeUint64Field(fieldNo: number, value: bigint | undefined): void {
    const u = BigInt.asUintN(64, value ?? 0n);
    if (u === 0n) return;
    this.writeTag(fieldNo, WIRE_VARINT);
    this.writeVarint64(u);
  }

  writeBoolField(fieldNo: number, value: boolean | undefined): void {
    if (!value) return;
    this.writeTag(fieldNo, WIRE_VARINT);
    this.buf[this.pos++] = 1;
  }

  writeEnumField(fieldNo: number, value: number | undefined): void {
    const v = (value ?? 0) | 0;
    if (v === 0) return;
    this.writeTag(fieldNo, WIRE_VARINT);
    this.writeEnumVal(v);
  }

  writeDoubleField(fieldNo: number, value: number | undefined): void {
    if (value === undefined || value === 0) return;
    this.writeTag(fieldNo, WIRE_64BIT);
    this.writeDoubleVal(value);
  }

  writeStringField(fieldNo: number, value: string | undefined): void {
    if (!value) return;
    const n = utf8ByteLen(value);
    this.writeTag(fieldNo, WIRE_LEN);
    this.writeVarint32(n);
    this.writeRawString(value);
  }

  writeBytesField(fieldNo: number, value: Uint8Array | undefined): void {
    if (!value || value.byteLength === 0) return;
    this.writeTag(fieldNo, WIRE_LEN);
    this.writeVarint32(value.byteLength);
    this.writeRawBytes(value);
  }

  writeAlwaysStringField(fieldNo: number, value: string): void {
    const n = utf8ByteLen(value);
    this.writeTag(fieldNo, WIRE_LEN);
    this.writeVarint32(n);
    this.writeRawString(value);
  }

  writeAlwaysBytesField(fieldNo: number, value: Uint8Array): void {
    this.writeTag(fieldNo, WIRE_LEN);
    this.writeVarint32(value.byteLength);
    this.writeRawBytes(value);
  }

  writeAlwaysSegmentedBytesField(
    fieldNo: number,
    value: Uint8Array | readonly Uint8Array[],
  ): void {
    const total = segmentedBytesTotalLen(value);
    this.writeTag(fieldNo, WIRE_LEN);
    this.writeVarint32(total);
    if (value instanceof Uint8Array) {
      this.writeRawBytes(value);
    } else {
      for (let i = 0; i < value.length; i++) {
        this.writeRawBytes(value[i]!);
      }
    }
  }

  writeRepeatedStringField(
    fieldNo: number,
    items: readonly string[] | undefined,
  ): void {
    if (!items || items.length === 0) return;
    for (let i = 0; i < items.length; i++) {
      this.writeAlwaysStringField(fieldNo, items[i]!);
    }
  }

  writeStringMapField(
    fieldNo: number,
    map: Record<string, string> | undefined,
  ): void {
    const keys = sortedMapKeys(map);
    for (let i = 0; i < keys.length; i++) {
      const k = keys[i]!;
      const v = map![k]!;
      const entryLen = stringFieldLen(1, k) + stringFieldLen(2, v);
      this.writeTag(fieldNo, WIRE_LEN);
      this.writeVarint32(entryLen);
      this.writeStringField(1, k);
      this.writeStringField(2, v);
    }
  }

  writeInt64MapField(
    fieldNo: number,
    map: Record<string, bigint> | undefined,
  ): void {
    const keys = sortedMapKeys(map);
    for (let i = 0; i < keys.length; i++) {
      const k = keys[i]!;
      const v = map![k]!;
      const vLen = v === 0n ? 0 : 1 + varint64Len(v);
      const entryLen = stringFieldLen(1, k) + vLen;
      this.writeTag(fieldNo, WIRE_LEN);
      this.writeVarint32(entryLen);
      this.writeStringField(1, k);
      if (v !== 0n) {
        this.writeTag(2, WIRE_VARINT);
        this.writeVarint64(v);
      }
    }
  }

  writeDoubleMapField(
    fieldNo: number,
    map: Record<string, number> | undefined,
  ): void {
    const keys = sortedMapKeys(map);
    for (let i = 0; i < keys.length; i++) {
      const k = keys[i]!;
      const v = map![k]!;
      const vLen = v === 0 ? 0 : 1 + 8;
      const entryLen = stringFieldLen(1, k) + vLen;
      this.writeTag(fieldNo, WIRE_LEN);
      this.writeVarint32(entryLen);
      this.writeStringField(1, k);
      if (v !== 0) {
        this.writeTag(2, WIRE_64BIT);
        this.writeDoubleVal(v);
      }
    }
  }
}

class ProtoReader {
  readonly buf: Uint8Array;
  pos = 0;
  private dv: DataView | null = null;

  constructor(buf: Uint8Array) {
    this.buf = buf;
  }

  private view(): DataView {
    if (!this.dv) {
      this.dv = new DataView(
        this.buf.buffer,
        this.buf.byteOffset,
        this.buf.byteLength,
      );
    }
    return this.dv;
  }

  readVarint64(end: number): bigint {
    let result = 0n;
    for (let i = 0; i < 10; i++) {
      if (this.pos >= end) {
        throw new ProtoDecodeError("unexpected EOF while reading varint");
      }
      const b = this.buf[this.pos++]!;
      if (i === 9 && b > 0x01) {
        throw new ProtoDecodeError("varint exceeds 64-bit integer range");
      }
      result |= BigInt(b & 0x7f) << BigInt(i * 7);
      if ((b & 0x80) === 0) {
        return result;
      }
    }
    throw new ProtoDecodeError("varint exceeds 10 bytes");
  }

  readTag(end: number): [fieldNo: number, wireType: number] {
    const raw = this.readVarint64(end);
    if (raw > 0xffffffffn) {
      throw new ProtoDecodeError("field tag exceeds 32 bits");
    }
    const tag = Number(raw) >>> 0;
    const fieldNo = tag >>> 3;
    const wireType = tag & 0x07;
    if (fieldNo === 0) {
      throw new ProtoDecodeError("invalid protobuf field number 0");
    }
    if (
      wireType !== WIRE_VARINT &&
      wireType !== WIRE_64BIT &&
      wireType !== WIRE_LEN &&
      wireType !== WIRE_32BIT
    ) {
      throw new ProtoDecodeError(`invalid protobuf wire type ${wireType}`);
    }
    return [fieldNo, wireType];
  }

  expectWire(actual: number, expected: number): void {
    if (actual !== expected) {
      throw new ProtoDecodeError(
        `unexpected wire type ${actual} (expected ${expected})`,
      );
    }
  }

  readSubEnd(end: number): number {
    const len64 = this.readVarint64(end);
    const remaining = end - this.pos;
    if (len64 > BigInt(remaining)) {
      throw new ProtoDecodeError("length-delimited field exceeds buffer bounds");
    }
    return this.pos + Number(len64);
  }

  readUint32(end: number): number {
    const raw = this.readVarint64(end);
    return Number(raw & 0xffffffffn) >>> 0;
  }

  readUint64(end: number): bigint {
    return this.readVarint64(end);
  }

  readInt64(end: number): bigint {
    return BigInt.asIntN(64, this.readVarint64(end));
  }

  readEnum(end: number): number {
    return Number(BigInt.asIntN(32, this.readVarint64(end)));
  }

  readBool(end: number): boolean {
    return this.readVarint64(end) !== 0n;
  }

  readDouble(end: number): number {
    if (this.pos + 8 > end) {
      throw new ProtoDecodeError("unexpected EOF while reading 64-bit double");
    }
    const val = this.view().getFloat64(this.pos, true);
    this.pos += 8;
    return val;
  }

  readBytes(end: number): Uint8Array {
    const subEnd = this.readSubEnd(end);
    if (subEnd === this.pos) {
      return EMPTY_BYTES;
    }
    const out = this.buf.slice(this.pos, subEnd);
    this.pos = subEnd;
    return out;
  }

  readString(end: number): string {
    const subEnd = this.readSubEnd(end);
    if (subEnd === this.pos) {
      return "";
    }
    const sub = this.buf.subarray(this.pos, subEnd);
    this.pos = subEnd;
    try {
      return fatalUtf8Decoder.decode(sub);
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
        if (this.pos + 8 > end) {
          throw new ProtoDecodeError("unexpected EOF skipping 64-bit field");
        }
        this.pos += 8;
        break;
      case WIRE_LEN: {
        const subEnd = this.readSubEnd(end);
        this.pos = subEnd;
        break;
      }
      case WIRE_32BIT:
        if (this.pos + 4 > end) {
          throw new ProtoDecodeError("unexpected EOF skipping 32-bit field");
        }
        this.pos += 4;
        break;
      default:
        throw new ProtoDecodeError(`unsupported wire type ${wireType}`);
    }
  }

  readStringMapEntry(end: number, target: Record<string, string>): void {
    const subEnd = this.readSubEnd(end);
    let key = "";
    let val = "";
    while (this.pos < subEnd) {
      const [fn, wt] = this.readTag(subEnd);
      if (fn === 1) {
        this.expectWire(wt, WIRE_LEN);
        key = this.readString(subEnd);
      } else if (fn === 2) {
        this.expectWire(wt, WIRE_LEN);
        val = this.readString(subEnd);
      } else {
        this.skipField(wt, subEnd);
      }
    }
    setMapProp(target, key, val);
  }

  readInt64MapEntry(end: number, target: Record<string, bigint>): void {
    const subEnd = this.readSubEnd(end);
    let key = "";
    let val = 0n;
    while (this.pos < subEnd) {
      const [fn, wt] = this.readTag(subEnd);
      if (fn === 1) {
        this.expectWire(wt, WIRE_LEN);
        key = this.readString(subEnd);
      } else if (fn === 2) {
        this.expectWire(wt, WIRE_VARINT);
        val = this.readInt64(subEnd);
      } else {
        this.skipField(wt, subEnd);
      }
    }
    setMapProp(target, key, val);
  }

  readDoubleMapEntry(end: number, target: Record<string, number>): void {
    const subEnd = this.readSubEnd(end);
    let key = "";
    let val = 0;
    while (this.pos < subEnd) {
      const [fn, wt] = this.readTag(subEnd);
      if (fn === 1) {
        this.expectWire(wt, WIRE_LEN);
        key = this.readString(subEnd);
      } else if (fn === 2) {
        this.expectWire(wt, WIRE_64BIT);
        val = this.readDouble(subEnd);
      } else {
        this.skipField(wt, subEnd);
      }
    }
    setMapProp(target, key, val);
  }
}

function encodeWith<T>(
  msg: T,
  lenFn: (m: T) => number,
  writeFn: (w: ProtoWriter, m: T) => void,
): Uint8Array {
  const len = lenFn(msg);
  if (len === 0) return new Uint8Array(0);
  const w = new ProtoWriter(len);
  writeFn(w, msg);
  return w.buf;
}

function decodeWith<T>(
  buf: Uint8Array,
  readFn: (r: ProtoReader, end: number) => T,
): T {
  const r = new ProtoReader(buf);
  return readFn(r, buf.byteLength);
}

// ---------- Shared Messages ----------

// 1. Object
export function encodedLenObject(msg: ProtoObjectInput): number {
  let len = 0;
  if (msg.body) {
    if (msg.body.case === "inline") {
      len += lenDelimitedTotal(1, msg.body.value.byteLength);
    } else {
      len += lenDelimitedTotal(2, utf8ByteLen(msg.body.value));
    }
  }
  len += stringMapFieldLen(3, msg.metadata);
  return len;
}

function encodedLenSegmentedObject(msg: SegmentedObjectInput): number {
  let len = 0;
  if (msg.body) {
    if (msg.body.case === "inline") {
      len += lenDelimitedTotal(1, segmentedBytesTotalLen(msg.body.value));
    } else {
      len += lenDelimitedTotal(2, utf8ByteLen(msg.body.value));
    }
  }
  len += stringMapFieldLen(3, msg.metadata);
  return len;
}

function writeObject(w: ProtoWriter, msg: ProtoObjectInput): void {
  if (msg.body) {
    if (msg.body.case === "inline") {
      w.writeAlwaysBytesField(1, msg.body.value);
    } else {
      w.writeAlwaysStringField(2, msg.body.value);
    }
  }
  w.writeStringMapField(3, msg.metadata);
}

function writeSegmentedObject(w: ProtoWriter, msg: SegmentedObjectInput): void {
  if (msg.body) {
    if (msg.body.case === "inline") {
      w.writeAlwaysSegmentedBytesField(1, msg.body.value);
    } else {
      w.writeAlwaysStringField(2, msg.body.value);
    }
  }
  w.writeStringMapField(3, msg.metadata);
}

function readObjectInto(
  r: ProtoReader,
  end: number,
  out: ProtoObject = { metadata: {} },
): ProtoObject {
  while (r.pos < end) {
    const [fn, wt] = r.readTag(end);
    switch (fn) {
      case 1:
        r.expectWire(wt, WIRE_LEN);
        out.body = { case: "inline", value: r.readBytes(end) };
        break;
      case 2:
        r.expectWire(wt, WIRE_LEN);
        out.body = { case: "uri", value: r.readString(end) };
        break;
      case 3:
        r.expectWire(wt, WIRE_LEN);
        r.readStringMapEntry(end, out.metadata);
        break;
      default:
        r.skipField(wt, end);
    }
  }
  return out;
}

export function encodeObject(msg: ProtoObjectInput): Uint8Array {
  return encodeWith(msg, encodedLenObject, writeObject);
}

export function decodeObject(buf: Uint8Array): ProtoObject {
  return decodeWith(buf, (r, end) => readObjectInto(r, end));
}

// 2. Demand
export function encodedLenDemand(msg: DemandInput): number {
  return (
    stringFieldLen(1, msg.interface) +
    stringMapFieldLen(2, msg.requiredAttributes) +
    stringMapFieldLen(3, msg.preferredAttributes) +
    int64MapFieldLen(4, msg.requiredCapacity) +
    stringFieldLen(5, msg.affinityKey)
  );
}

function writeDemand(w: ProtoWriter, msg: DemandInput): void {
  w.writeStringField(1, msg.interface);
  w.writeStringMapField(2, msg.requiredAttributes);
  w.writeStringMapField(3, msg.preferredAttributes);
  w.writeInt64MapField(4, msg.requiredCapacity);
  w.writeStringField(5, msg.affinityKey);
}

function readDemandInto(
  r: ProtoReader,
  end: number,
  out: Demand = {
    interface: "",
    requiredAttributes: {},
    preferredAttributes: {},
    requiredCapacity: {},
    affinityKey: "",
  },
): Demand {
  while (r.pos < end) {
    const [fn, wt] = r.readTag(end);
    switch (fn) {
      case 1:
        r.expectWire(wt, WIRE_LEN);
        out.interface = r.readString(end);
        break;
      case 2:
        r.expectWire(wt, WIRE_LEN);
        r.readStringMapEntry(end, out.requiredAttributes);
        break;
      case 3:
        r.expectWire(wt, WIRE_LEN);
        r.readStringMapEntry(end, out.preferredAttributes);
        break;
      case 4:
        r.expectWire(wt, WIRE_LEN);
        r.readInt64MapEntry(end, out.requiredCapacity);
        break;
      case 5:
        r.expectWire(wt, WIRE_LEN);
        out.affinityKey = r.readString(end);
        break;
      default:
        r.skipField(wt, end);
    }
  }
  return out;
}

export function encodeDemand(msg: DemandInput): Uint8Array {
  return encodeWith(msg, encodedLenDemand, writeDemand);
}

export function decodeDemand(buf: Uint8Array): Demand {
  return decodeWith(buf, (r, end) => readDemandInto(r, end));
}

// 3. ExecutionPolicy
export function encodedLenExecutionPolicy(msg: ExecutionPolicyInput): number {
  return (
    enumFieldLen(1, msg.durability) +
    uint32FieldLen(2, msg.priority) +
    uint64FieldLen(3, msg.queueTimeoutMs) +
    uint64FieldLen(4, msg.executionTimeoutMs) +
    uint64FieldLen(5, msg.totalTimeoutMs) +
    uint64FieldLen(6, msg.spilloverAfterMs) +
    uint32FieldLen(7, msg.maxAttempts) +
    stringFieldLen(8, msg.fairnessKey) +
    stringFieldLen(9, msg.concurrencyKey)
  );
}

function writeExecutionPolicy(w: ProtoWriter, msg: ExecutionPolicyInput): void {
  w.writeEnumField(1, msg.durability);
  w.writeUint32Field(2, msg.priority);
  w.writeUint64Field(3, msg.queueTimeoutMs);
  w.writeUint64Field(4, msg.executionTimeoutMs);
  w.writeUint64Field(5, msg.totalTimeoutMs);
  w.writeUint64Field(6, msg.spilloverAfterMs);
  w.writeUint32Field(7, msg.maxAttempts);
  w.writeStringField(8, msg.fairnessKey);
  w.writeStringField(9, msg.concurrencyKey);
}

function readExecutionPolicyInto(
  r: ProtoReader,
  end: number,
  out: ExecutionPolicy = {
    durability: Durability.DURABILITY_UNSPECIFIED,
    priority: 0,
    queueTimeoutMs: 0n,
    executionTimeoutMs: 0n,
    totalTimeoutMs: 0n,
    spilloverAfterMs: 0n,
    maxAttempts: 0,
    fairnessKey: "",
    concurrencyKey: "",
  },
): ExecutionPolicy {
  while (r.pos < end) {
    const [fn, wt] = r.readTag(end);
    switch (fn) {
      case 1:
        r.expectWire(wt, WIRE_VARINT);
        out.durability = r.readEnum(end) as Durability;
        break;
      case 2:
        r.expectWire(wt, WIRE_VARINT);
        out.priority = r.readUint32(end);
        break;
      case 3:
        r.expectWire(wt, WIRE_VARINT);
        out.queueTimeoutMs = r.readUint64(end);
        break;
      case 4:
        r.expectWire(wt, WIRE_VARINT);
        out.executionTimeoutMs = r.readUint64(end);
        break;
      case 5:
        r.expectWire(wt, WIRE_VARINT);
        out.totalTimeoutMs = r.readUint64(end);
        break;
      case 6:
        r.expectWire(wt, WIRE_VARINT);
        out.spilloverAfterMs = r.readUint64(end);
        break;
      case 7:
        r.expectWire(wt, WIRE_VARINT);
        out.maxAttempts = r.readUint32(end);
        break;
      case 8:
        r.expectWire(wt, WIRE_LEN);
        out.fairnessKey = r.readString(end);
        break;
      case 9:
        r.expectWire(wt, WIRE_LEN);
        out.concurrencyKey = r.readString(end);
        break;
      default:
        r.skipField(wt, end);
    }
  }
  return out;
}

export function encodeExecutionPolicy(msg: ExecutionPolicyInput): Uint8Array {
  return encodeWith(msg, encodedLenExecutionPolicy, writeExecutionPolicy);
}

export function decodeExecutionPolicy(buf: Uint8Array): ExecutionPolicy {
  return decodeWith(buf, (r, end) => readExecutionPolicyInto(r, end));
}

// 4. JobSpec
export function encodedLenJobSpec(msg: JobSpecInput): number {
  let len = 0;
  if (msg.demand !== undefined) {
    len += lenDelimitedTotal(1, encodedLenDemand(msg.demand));
  }
  if (msg.inputs) {
    for (let i = 0; i < msg.inputs.length; i++) {
      len += lenDelimitedTotal(2, encodedLenObject(msg.inputs[i]!));
    }
  }
  len += bytesFieldLen(3, msg.params);
  if (msg.policy !== undefined) {
    len += lenDelimitedTotal(4, encodedLenExecutionPolicy(msg.policy));
  }
  len += stringMapFieldLen(5, msg.metadata);
  return len;
}

function writeJobSpec(w: ProtoWriter, msg: JobSpecInput): void {
  if (msg.demand !== undefined) {
    const subLen = encodedLenDemand(msg.demand);
    w.writeTag(1, WIRE_LEN);
    w.writeVarint32(subLen);
    writeDemand(w, msg.demand);
  }
  if (msg.inputs) {
    for (let i = 0; i < msg.inputs.length; i++) {
      const item = msg.inputs[i]!;
      const subLen = encodedLenObject(item);
      w.writeTag(2, WIRE_LEN);
      w.writeVarint32(subLen);
      writeObject(w, item);
    }
  }
  w.writeBytesField(3, msg.params);
  if (msg.policy !== undefined) {
    const subLen = encodedLenExecutionPolicy(msg.policy);
    w.writeTag(4, WIRE_LEN);
    w.writeVarint32(subLen);
    writeExecutionPolicy(w, msg.policy);
  }
  w.writeStringMapField(5, msg.metadata);
}

function readJobSpecInto(
  r: ProtoReader,
  end: number,
  out: JobSpec = { inputs: [], params: EMPTY_BYTES, metadata: {} },
): JobSpec {
  while (r.pos < end) {
    const [fn, wt] = r.readTag(end);
    switch (fn) {
      case 1:
        r.expectWire(wt, WIRE_LEN);
        out.demand = readDemandInto(r, r.readSubEnd(end), out.demand);
        break;
      case 2:
        r.expectWire(wt, WIRE_LEN);
        out.inputs.push(readObjectInto(r, r.readSubEnd(end)));
        break;
      case 3:
        r.expectWire(wt, WIRE_LEN);
        out.params = r.readBytes(end);
        break;
      case 4:
        r.expectWire(wt, WIRE_LEN);
        out.policy = readExecutionPolicyInto(r, r.readSubEnd(end), out.policy);
        break;
      case 5:
        r.expectWire(wt, WIRE_LEN);
        r.readStringMapEntry(end, out.metadata);
        break;
      default:
        r.skipField(wt, end);
    }
  }
  return out;
}

export function encodeJobSpec(msg: JobSpecInput): Uint8Array {
  return encodeWith(msg, encodedLenJobSpec, writeJobSpec);
}

export function decodeJobSpec(buf: Uint8Array): JobSpec {
  return decodeWith(buf, (r, end) => readJobSpecInto(r, end));
}

// 5. ErrorInfo
export function encodedLenErrorInfo(msg: ErrorInfoInput): number {
  return (
    stringFieldLen(1, msg.code) +
    stringFieldLen(2, msg.message) +
    boolFieldLen(3, msg.retryable)
  );
}

function writeErrorInfo(w: ProtoWriter, msg: ErrorInfoInput): void {
  w.writeStringField(1, msg.code);
  w.writeStringField(2, msg.message);
  w.writeBoolField(3, msg.retryable);
}

function readErrorInfoInto(
  r: ProtoReader,
  end: number,
  out: ErrorInfo = { code: "", message: "", retryable: false },
): ErrorInfo {
  while (r.pos < end) {
    const [fn, wt] = r.readTag(end);
    switch (fn) {
      case 1:
        r.expectWire(wt, WIRE_LEN);
        out.code = r.readString(end);
        break;
      case 2:
        r.expectWire(wt, WIRE_LEN);
        out.message = r.readString(end);
        break;
      case 3:
        r.expectWire(wt, WIRE_VARINT);
        out.retryable = r.readBool(end);
        break;
      default:
        r.skipField(wt, end);
    }
  }
  return out;
}

export function encodeErrorInfo(msg: ErrorInfoInput): Uint8Array {
  return encodeWith(msg, encodedLenErrorInfo, writeErrorInfo);
}

export function decodeErrorInfo(buf: Uint8Array): ErrorInfo {
  return decodeWith(buf, (r, end) => readErrorInfoInto(r, end));
}

// 6. Job
export function encodedLenJob(msg: JobInput): number {
  let len =
    stringFieldLen(1, msg.jobId) +
    enumFieldLen(2, msg.state) +
    enumFieldLen(3, msg.durability) +
    stringFieldLen(4, msg.interface) +
    uint32FieldLen(5, msg.attemptCount) +
    stringFieldLen(6, msg.currentAttemptId) +
    stringFieldLen(7, msg.currentWorkerId) +
    uint64FieldLen(8, msg.createdAtMs) +
    uint64FieldLen(9, msg.updatedAtMs);
  if (msg.error !== undefined) {
    len += lenDelimitedTotal(10, encodedLenErrorInfo(msg.error));
  }
  if (msg.outputs) {
    for (let i = 0; i < msg.outputs.length; i++) {
      len += lenDelimitedTotal(11, encodedLenObject(msg.outputs[i]!));
    }
  }
  len +=
    stringMapFieldLen(12, msg.metadata) +
    stringFieldLen(13, msg.specHash) +
    stringFieldLen(14, msg.namespace);
  return len;
}

function writeJob(w: ProtoWriter, msg: JobInput): void {
  w.writeStringField(1, msg.jobId);
  w.writeEnumField(2, msg.state);
  w.writeEnumField(3, msg.durability);
  w.writeStringField(4, msg.interface);
  w.writeUint32Field(5, msg.attemptCount);
  w.writeStringField(6, msg.currentAttemptId);
  w.writeStringField(7, msg.currentWorkerId);
  w.writeUint64Field(8, msg.createdAtMs);
  w.writeUint64Field(9, msg.updatedAtMs);
  if (msg.error !== undefined) {
    const subLen = encodedLenErrorInfo(msg.error);
    w.writeTag(10, WIRE_LEN);
    w.writeVarint32(subLen);
    writeErrorInfo(w, msg.error);
  }
  if (msg.outputs) {
    for (let i = 0; i < msg.outputs.length; i++) {
      const item = msg.outputs[i]!;
      const subLen = encodedLenObject(item);
      w.writeTag(11, WIRE_LEN);
      w.writeVarint32(subLen);
      writeObject(w, item);
    }
  }
  w.writeStringMapField(12, msg.metadata);
  w.writeStringField(13, msg.specHash);
  w.writeStringField(14, msg.namespace);
}

function readJobInto(
  r: ProtoReader,
  end: number,
  out: Job = {
    jobId: "",
    state: JobState.JOB_STATE_UNSPECIFIED,
    durability: Durability.DURABILITY_UNSPECIFIED,
    interface: "",
    attemptCount: 0,
    currentAttemptId: "",
    currentWorkerId: "",
    createdAtMs: 0n,
    updatedAtMs: 0n,
    outputs: [],
    metadata: {},
    specHash: "",
    namespace: "",
  },
): Job {
  while (r.pos < end) {
    const [fn, wt] = r.readTag(end);
    switch (fn) {
      case 1:
        r.expectWire(wt, WIRE_LEN);
        out.jobId = r.readString(end);
        break;
      case 2:
        r.expectWire(wt, WIRE_VARINT);
        out.state = r.readEnum(end) as JobState;
        break;
      case 3:
        r.expectWire(wt, WIRE_VARINT);
        out.durability = r.readEnum(end) as Durability;
        break;
      case 4:
        r.expectWire(wt, WIRE_LEN);
        out.interface = r.readString(end);
        break;
      case 5:
        r.expectWire(wt, WIRE_VARINT);
        out.attemptCount = r.readUint32(end);
        break;
      case 6:
        r.expectWire(wt, WIRE_LEN);
        out.currentAttemptId = r.readString(end);
        break;
      case 7:
        r.expectWire(wt, WIRE_LEN);
        out.currentWorkerId = r.readString(end);
        break;
      case 8:
        r.expectWire(wt, WIRE_VARINT);
        out.createdAtMs = r.readUint64(end);
        break;
      case 9:
        r.expectWire(wt, WIRE_VARINT);
        out.updatedAtMs = r.readUint64(end);
        break;
      case 10:
        r.expectWire(wt, WIRE_LEN);
        out.error = readErrorInfoInto(r, r.readSubEnd(end), out.error);
        break;
      case 11:
        r.expectWire(wt, WIRE_LEN);
        out.outputs.push(readObjectInto(r, r.readSubEnd(end)));
        break;
      case 12:
        r.expectWire(wt, WIRE_LEN);
        r.readStringMapEntry(end, out.metadata);
        break;
      case 13:
        r.expectWire(wt, WIRE_LEN);
        out.specHash = r.readString(end);
        break;
      case 14:
        r.expectWire(wt, WIRE_LEN);
        out.namespace = r.readString(end);
        break;
      default:
        r.skipField(wt, end);
    }
  }
  return out;
}

export function encodeJob(msg: JobInput): Uint8Array {
  return encodeWith(msg, encodedLenJob, writeJob);
}

export function decodeJob(buf: Uint8Array): Job {
  return decodeWith(buf, (r, end) => readJobInto(r, end));
}

// ---------- Client Messages ----------

// 7. SubmitRequest
export function encodedLenSubmitRequest(msg: SubmitRequestInput): number {
  let len = stringFieldLen(1, msg.jobId) + enumFieldLen(2, msg.conflictPolicy);
  if (msg.spec !== undefined) {
    len += lenDelimitedTotal(3, encodedLenJobSpec(msg.spec));
  }
  return len;
}

function writeSubmitRequest(w: ProtoWriter, msg: SubmitRequestInput): void {
  w.writeStringField(1, msg.jobId);
  w.writeEnumField(2, msg.conflictPolicy);
  if (msg.spec !== undefined) {
    const subLen = encodedLenJobSpec(msg.spec);
    w.writeTag(3, WIRE_LEN);
    w.writeVarint32(subLen);
    writeJobSpec(w, msg.spec);
  }
}

function readSubmitRequestInto(
  r: ProtoReader,
  end: number,
  out: SubmitRequest = {
    jobId: "",
    conflictPolicy: ConflictPolicy.CONFLICT_POLICY_UNSPECIFIED,
  },
): SubmitRequest {
  while (r.pos < end) {
    const [fn, wt] = r.readTag(end);
    switch (fn) {
      case 1:
        r.expectWire(wt, WIRE_LEN);
        out.jobId = r.readString(end);
        break;
      case 2:
        r.expectWire(wt, WIRE_VARINT);
        out.conflictPolicy = r.readEnum(end) as ConflictPolicy;
        break;
      case 3:
        r.expectWire(wt, WIRE_LEN);
        out.spec = readJobSpecInto(r, r.readSubEnd(end), out.spec);
        break;
      default:
        r.skipField(wt, end);
    }
  }
  return out;
}

export function encodeSubmitRequest(msg: SubmitRequestInput): Uint8Array {
  return encodeWith(msg, encodedLenSubmitRequest, writeSubmitRequest);
}

export function decodeSubmitRequest(buf: Uint8Array): SubmitRequest {
  return decodeWith(buf, (r, end) => readSubmitRequestInto(r, end));
}

// 8. SubmitResponse
export function encodedLenSubmitResponse(msg: SubmitResponseInput): number {
  let len = 0;
  if (msg.job !== undefined) {
    len += lenDelimitedTotal(1, encodedLenJob(msg.job));
  }
  len += boolFieldLen(2, msg.created);
  return len;
}

function writeSubmitResponse(w: ProtoWriter, msg: SubmitResponseInput): void {
  if (msg.job !== undefined) {
    const subLen = encodedLenJob(msg.job);
    w.writeTag(1, WIRE_LEN);
    w.writeVarint32(subLen);
    writeJob(w, msg.job);
  }
  w.writeBoolField(2, msg.created);
}

function readSubmitResponseInto(
  r: ProtoReader,
  end: number,
  out: SubmitResponse = { created: false },
): SubmitResponse {
  while (r.pos < end) {
    const [fn, wt] = r.readTag(end);
    switch (fn) {
      case 1:
        r.expectWire(wt, WIRE_LEN);
        out.job = readJobInto(r, r.readSubEnd(end), out.job);
        break;
      case 2:
        r.expectWire(wt, WIRE_VARINT);
        out.created = r.readBool(end);
        break;
      default:
        r.skipField(wt, end);
    }
  }
  return out;
}

export function encodeSubmitResponse(msg: SubmitResponseInput): Uint8Array {
  return encodeWith(msg, encodedLenSubmitResponse, writeSubmitResponse);
}

export function decodeSubmitResponse(buf: Uint8Array): SubmitResponse {
  return decodeWith(buf, (r, end) => readSubmitResponseInto(r, end));
}

// 9. InvokeStart
export function encodedLenInvokeStart(msg: InvokeStartInput): number {
  let len = 0;
  if (msg.spec !== undefined) {
    len += lenDelimitedTotal(1, encodedLenJobSpec(msg.spec));
  }
  len += boolFieldLen(2, msg.streamingInput);
  return len;
}

function writeInvokeStart(w: ProtoWriter, msg: InvokeStartInput): void {
  if (msg.spec !== undefined) {
    const subLen = encodedLenJobSpec(msg.spec);
    w.writeTag(1, WIRE_LEN);
    w.writeVarint32(subLen);
    writeJobSpec(w, msg.spec);
  }
  w.writeBoolField(2, msg.streamingInput);
}

function readInvokeStartInto(
  r: ProtoReader,
  end: number,
  out: InvokeStart = { streamingInput: false },
): InvokeStart {
  while (r.pos < end) {
    const [fn, wt] = r.readTag(end);
    switch (fn) {
      case 1:
        r.expectWire(wt, WIRE_LEN);
        out.spec = readJobSpecInto(r, r.readSubEnd(end), out.spec);
        break;
      case 2:
        r.expectWire(wt, WIRE_VARINT);
        out.streamingInput = r.readBool(end);
        break;
      default:
        r.skipField(wt, end);
    }
  }
  return out;
}

export function encodeInvokeStart(msg: InvokeStartInput): Uint8Array {
  return encodeWith(msg, encodedLenInvokeStart, writeInvokeStart);
}

export function decodeInvokeStart(buf: Uint8Array): InvokeStart {
  return decodeWith(buf, (r, end) => readInvokeStartInto(r, end));
}

// 10. InputChunk
export function encodedLenInputChunk(msg: InputChunkInput): number {
  return bytesFieldLen(1, msg.data) + stringMapFieldLen(2, msg.metadata);
}

function writeInputChunk(w: ProtoWriter, msg: InputChunkInput): void {
  w.writeBytesField(1, msg.data);
  w.writeStringMapField(2, msg.metadata);
}

function readInputChunkInto(
  r: ProtoReader,
  end: number,
  out: InputChunk = { data: EMPTY_BYTES, metadata: {} },
): InputChunk {
  while (r.pos < end) {
    const [fn, wt] = r.readTag(end);
    switch (fn) {
      case 1:
        r.expectWire(wt, WIRE_LEN);
        out.data = r.readBytes(end);
        break;
      case 2:
        r.expectWire(wt, WIRE_LEN);
        r.readStringMapEntry(end, out.metadata);
        break;
      default:
        r.skipField(wt, end);
    }
  }
  return out;
}

export function encodeInputChunk(msg: InputChunkInput): Uint8Array {
  return encodeWith(msg, encodedLenInputChunk, writeInputChunk);
}

export function decodeInputChunk(buf: Uint8Array): InputChunk {
  return decodeWith(buf, (r, end) => readInputChunkInto(r, end));
}

// 11. InputEnd
export function encodedLenInputEnd(_msg?: InputEnd): number {
  return 0;
}

function readEmptyInto<T extends object>(
  r: ProtoReader,
  end: number,
  out: T,
): T {
  while (r.pos < end) {
    const [, wt] = r.readTag(end);
    r.skipField(wt, end);
  }
  return out;
}

export function encodeInputEnd(_msg?: InputEnd): Uint8Array {
  return new Uint8Array(0);
}

export function decodeInputEnd(buf: Uint8Array): InputEnd {
  return decodeWith(buf, (r, end) => readEmptyInto(r, end, {}));
}

// 12. InvokeRequest
export function encodedLenInvokeRequest(msg: InvokeRequestInput): number {
  if (!msg.msg) return 0;
  switch (msg.msg.case) {
    case "start":
      return lenDelimitedTotal(1, encodedLenInvokeStart(msg.msg.value));
    case "input":
      return lenDelimitedTotal(2, encodedLenInputChunk(msg.msg.value));
    case "inputEnd":
      return lenDelimitedTotal(3, 0);
  }
}

function writeInvokeRequest(w: ProtoWriter, msg: InvokeRequestInput): void {
  if (!msg.msg) return;
  switch (msg.msg.case) {
    case "start": {
      const subLen = encodedLenInvokeStart(msg.msg.value);
      w.writeTag(1, WIRE_LEN);
      w.writeVarint32(subLen);
      writeInvokeStart(w, msg.msg.value);
      break;
    }
    case "input": {
      const subLen = encodedLenInputChunk(msg.msg.value);
      w.writeTag(2, WIRE_LEN);
      w.writeVarint32(subLen);
      writeInputChunk(w, msg.msg.value);
      break;
    }
    case "inputEnd":
      w.writeTag(3, WIRE_LEN);
      w.writeVarint32(0);
      break;
  }
}

function readInvokeRequestInto(
  r: ProtoReader,
  end: number,
  out: InvokeRequest = {},
): InvokeRequest {
  while (r.pos < end) {
    const [fn, wt] = r.readTag(end);
    switch (fn) {
      case 1:
        r.expectWire(wt, WIRE_LEN);
        out.msg = {
          case: "start",
          value: readInvokeStartInto(
            r,
            r.readSubEnd(end),
            out.msg?.case === "start" ? out.msg.value : undefined,
          ),
        };
        break;
      case 2:
        r.expectWire(wt, WIRE_LEN);
        out.msg = {
          case: "input",
          value: readInputChunkInto(
            r,
            r.readSubEnd(end),
            out.msg?.case === "input" ? out.msg.value : undefined,
          ),
        };
        break;
      case 3:
        r.expectWire(wt, WIRE_LEN);
        out.msg = {
          case: "inputEnd",
          value: readEmptyInto(r, r.readSubEnd(end), {}),
        };
        break;
      default:
        r.skipField(wt, end);
    }
  }
  return out;
}

export function encodeInvokeRequest(msg: InvokeRequestInput): Uint8Array {
  return encodeWith(msg, encodedLenInvokeRequest, writeInvokeRequest);
}

export function decodeInvokeRequest(buf: Uint8Array): InvokeRequest {
  return decodeWith(buf, (r, end) => readInvokeRequestInto(r, end));
}

// 13. Accepted
export function encodedLenAccepted(_msg?: Accepted): number {
  return 0;
}

export function encodeAccepted(_msg?: Accepted): Uint8Array {
  return new Uint8Array(0);
}

export function decodeAccepted(buf: Uint8Array): Accepted {
  return decodeWith(buf, (r, end) => readEmptyInto(r, end, {}));
}

// 14. Assigned
export function encodedLenAssigned(msg: AssignedInput): number {
  return stringFieldLen(1, msg.workerId) + uint32FieldLen(2, msg.attemptNumber);
}

function writeAssigned(w: ProtoWriter, msg: AssignedInput): void {
  w.writeStringField(1, msg.workerId);
  w.writeUint32Field(2, msg.attemptNumber);
}

function readAssignedInto(
  r: ProtoReader,
  end: number,
  out: Assigned = { workerId: "", attemptNumber: 0 },
): Assigned {
  while (r.pos < end) {
    const [fn, wt] = r.readTag(end);
    switch (fn) {
      case 1:
        r.expectWire(wt, WIRE_LEN);
        out.workerId = r.readString(end);
        break;
      case 2:
        r.expectWire(wt, WIRE_VARINT);
        out.attemptNumber = r.readUint32(end);
        break;
      default:
        r.skipField(wt, end);
    }
  }
  return out;
}

export function encodeAssigned(msg: AssignedInput): Uint8Array {
  return encodeWith(msg, encodedLenAssigned, writeAssigned);
}

export function decodeAssigned(buf: Uint8Array): Assigned {
  return decodeWith(buf, (r, end) => readAssignedInto(r, end));
}

// 15. Output
export function encodedLenOutput(msg: OutputInput): number {
  return (
    bytesFieldLen(1, msg.data) +
    stringMapFieldLen(2, msg.metadata) +
    boolFieldLen(3, msg.progress)
  );
}

function writeOutput(w: ProtoWriter, msg: OutputInput): void {
  w.writeBytesField(1, msg.data);
  w.writeStringMapField(2, msg.metadata);
  w.writeBoolField(3, msg.progress);
}

function readOutputInto(
  r: ProtoReader,
  end: number,
  out: Output = { data: EMPTY_BYTES, metadata: {}, progress: false },
): Output {
  while (r.pos < end) {
    const [fn, wt] = r.readTag(end);
    switch (fn) {
      case 1:
        r.expectWire(wt, WIRE_LEN);
        out.data = r.readBytes(end);
        break;
      case 2:
        r.expectWire(wt, WIRE_LEN);
        r.readStringMapEntry(end, out.metadata);
        break;
      case 3:
        r.expectWire(wt, WIRE_VARINT);
        out.progress = r.readBool(end);
        break;
      default:
        r.skipField(wt, end);
    }
  }
  return out;
}

export function encodeOutput(msg: OutputInput): Uint8Array {
  return encodeWith(msg, encodedLenOutput, writeOutput);
}

export function decodeOutput(buf: Uint8Array): Output {
  return decodeWith(buf, (r, end) => readOutputInto(r, end));
}

// 16. AttemptRestarted
export function encodedLenAttemptRestarted(msg: AttemptRestartedInput): number {
  let len = stringFieldLen(1, msg.previousAttemptId);
  if (msg.reason !== undefined) {
    len += lenDelimitedTotal(2, encodedLenErrorInfo(msg.reason));
  }
  return len;
}

function writeAttemptRestarted(
  w: ProtoWriter,
  msg: AttemptRestartedInput,
): void {
  w.writeStringField(1, msg.previousAttemptId);
  if (msg.reason !== undefined) {
    const subLen = encodedLenErrorInfo(msg.reason);
    w.writeTag(2, WIRE_LEN);
    w.writeVarint32(subLen);
    writeErrorInfo(w, msg.reason);
  }
}

function readAttemptRestartedInto(
  r: ProtoReader,
  end: number,
  out: AttemptRestarted = { previousAttemptId: "" },
): AttemptRestarted {
  while (r.pos < end) {
    const [fn, wt] = r.readTag(end);
    switch (fn) {
      case 1:
        r.expectWire(wt, WIRE_LEN);
        out.previousAttemptId = r.readString(end);
        break;
      case 2:
        r.expectWire(wt, WIRE_LEN);
        out.reason = readErrorInfoInto(r, r.readSubEnd(end), out.reason);
        break;
      default:
        r.skipField(wt, end);
    }
  }
  return out;
}

export function encodeAttemptRestarted(msg: AttemptRestartedInput): Uint8Array {
  return encodeWith(msg, encodedLenAttemptRestarted, writeAttemptRestarted);
}

export function decodeAttemptRestarted(buf: Uint8Array): AttemptRestarted {
  return decodeWith(buf, (r, end) => readAttemptRestartedInto(r, end));
}

// 17. ExecutionFinished
export function encodedLenExecutionFinished(
  msg: ExecutionFinishedInput,
): number {
  let len = enumFieldLen(1, msg.state);
  if (msg.outputs) {
    for (let i = 0; i < msg.outputs.length; i++) {
      len += lenDelimitedTotal(2, encodedLenSegmentedObject(msg.outputs[i]!));
    }
  }
  if (msg.error !== undefined) {
    len += lenDelimitedTotal(3, encodedLenErrorInfo(msg.error));
  }
  return len;
}

function writeExecutionFinished(
  w: ProtoWriter,
  msg: ExecutionFinishedInput,
): void {
  w.writeEnumField(1, msg.state);
  if (msg.outputs) {
    for (let i = 0; i < msg.outputs.length; i++) {
      const item = msg.outputs[i]!;
      const subLen = encodedLenSegmentedObject(item);
      w.writeTag(2, WIRE_LEN);
      w.writeVarint32(subLen);
      writeSegmentedObject(w, item);
    }
  }
  if (msg.error !== undefined) {
    const subLen = encodedLenErrorInfo(msg.error);
    w.writeTag(3, WIRE_LEN);
    w.writeVarint32(subLen);
    writeErrorInfo(w, msg.error);
  }
}

function readExecutionFinishedInto(
  r: ProtoReader,
  end: number,
  out: ExecutionFinished = {
    state: JobState.JOB_STATE_UNSPECIFIED,
    outputs: [],
  },
): ExecutionFinished {
  while (r.pos < end) {
    const [fn, wt] = r.readTag(end);
    switch (fn) {
      case 1:
        r.expectWire(wt, WIRE_VARINT);
        out.state = r.readEnum(end) as JobState;
        break;
      case 2:
        r.expectWire(wt, WIRE_LEN);
        out.outputs.push(readObjectInto(r, r.readSubEnd(end)));
        break;
      case 3:
        r.expectWire(wt, WIRE_LEN);
        out.error = readErrorInfoInto(r, r.readSubEnd(end), out.error);
        break;
      default:
        r.skipField(wt, end);
    }
  }
  return out;
}

export function encodeExecutionFinished(
  msg: ExecutionFinishedInput,
): Uint8Array {
  return encodeWith(msg, encodedLenExecutionFinished, writeExecutionFinished);
}

export function decodeExecutionFinished(buf: Uint8Array): ExecutionFinished {
  return decodeWith(buf, (r, end) => readExecutionFinishedInto(r, end));
}

// 18. ExecutionEvent
export function encodedLenExecutionEvent(msg: ExecutionEventInput): number {
  let len =
    stringFieldLen(1, msg.jobId) +
    uint64FieldLen(2, msg.sequence) +
    stringFieldLen(3, msg.attemptId) +
    uint64FieldLen(4, msg.atMs);
  if (msg.event) {
    switch (msg.event.case) {
      case "accepted":
        len += lenDelimitedTotal(10, 0);
        break;
      case "assigned":
        len += lenDelimitedTotal(11, encodedLenAssigned(msg.event.value));
        break;
      case "output":
        len += lenDelimitedTotal(12, encodedLenOutput(msg.event.value));
        break;
      case "attemptRestarted":
        len += lenDelimitedTotal(
          13,
          encodedLenAttemptRestarted(msg.event.value),
        );
        break;
      case "finished":
        len += lenDelimitedTotal(
          14,
          encodedLenExecutionFinished(msg.event.value),
        );
        break;
    }
  }
  return len;
}

function writeExecutionEvent(w: ProtoWriter, msg: ExecutionEventInput): void {
  w.writeStringField(1, msg.jobId);
  w.writeUint64Field(2, msg.sequence);
  w.writeStringField(3, msg.attemptId);
  w.writeUint64Field(4, msg.atMs);
  if (msg.event) {
    switch (msg.event.case) {
      case "accepted":
        w.writeTag(10, WIRE_LEN);
        w.writeVarint32(0);
        break;
      case "assigned": {
        const subLen = encodedLenAssigned(msg.event.value);
        w.writeTag(11, WIRE_LEN);
        w.writeVarint32(subLen);
        writeAssigned(w, msg.event.value);
        break;
      }
      case "output": {
        const subLen = encodedLenOutput(msg.event.value);
        w.writeTag(12, WIRE_LEN);
        w.writeVarint32(subLen);
        writeOutput(w, msg.event.value);
        break;
      }
      case "attemptRestarted": {
        const subLen = encodedLenAttemptRestarted(msg.event.value);
        w.writeTag(13, WIRE_LEN);
        w.writeVarint32(subLen);
        writeAttemptRestarted(w, msg.event.value);
        break;
      }
      case "finished": {
        const subLen = encodedLenExecutionFinished(msg.event.value);
        w.writeTag(14, WIRE_LEN);
        w.writeVarint32(subLen);
        writeExecutionFinished(w, msg.event.value);
        break;
      }
    }
  }
}

function readExecutionEventInto(
  r: ProtoReader,
  end: number,
  out: ExecutionEvent = { jobId: "", sequence: 0n, attemptId: "", atMs: 0n },
): ExecutionEvent {
  while (r.pos < end) {
    const [fn, wt] = r.readTag(end);
    switch (fn) {
      case 1:
        r.expectWire(wt, WIRE_LEN);
        out.jobId = r.readString(end);
        break;
      case 2:
        r.expectWire(wt, WIRE_VARINT);
        out.sequence = r.readUint64(end);
        break;
      case 3:
        r.expectWire(wt, WIRE_LEN);
        out.attemptId = r.readString(end);
        break;
      case 4:
        r.expectWire(wt, WIRE_VARINT);
        out.atMs = r.readUint64(end);
        break;
      case 10:
        r.expectWire(wt, WIRE_LEN);
        out.event = {
          case: "accepted",
          value: readEmptyInto(r, r.readSubEnd(end), {}),
        };
        break;
      case 11:
        r.expectWire(wt, WIRE_LEN);
        out.event = {
          case: "assigned",
          value: readAssignedInto(
            r,
            r.readSubEnd(end),
            out.event?.case === "assigned" ? out.event.value : undefined,
          ),
        };
        break;
      case 12:
        r.expectWire(wt, WIRE_LEN);
        out.event = {
          case: "output",
          value: readOutputInto(
            r,
            r.readSubEnd(end),
            out.event?.case === "output" ? out.event.value : undefined,
          ),
        };
        break;
      case 13:
        r.expectWire(wt, WIRE_LEN);
        out.event = {
          case: "attemptRestarted",
          value: readAttemptRestartedInto(
            r,
            r.readSubEnd(end),
            out.event?.case === "attemptRestarted"
              ? out.event.value
              : undefined,
          ),
        };
        break;
      case 14:
        r.expectWire(wt, WIRE_LEN);
        out.event = {
          case: "finished",
          value: readExecutionFinishedInto(
            r,
            r.readSubEnd(end),
            out.event?.case === "finished" ? out.event.value : undefined,
          ),
        };
        break;
      default:
        r.skipField(wt, end);
    }
  }
  return out;
}

export function encodeExecutionEvent(msg: ExecutionEventInput): Uint8Array {
  return encodeWith(msg, encodedLenExecutionEvent, writeExecutionEvent);
}

export function decodeExecutionEvent(buf: Uint8Array): ExecutionEvent {
  return decodeWith(buf, (r, end) => readExecutionEventInto(r, end));
}

// 19. WatchRequest
export function encodedLenWatchRequest(msg: WatchRequestInput): number {
  return stringFieldLen(1, msg.jobId) + uint64FieldLen(2, msg.afterSequence);
}

function writeWatchRequest(w: ProtoWriter, msg: WatchRequestInput): void {
  w.writeStringField(1, msg.jobId);
  w.writeUint64Field(2, msg.afterSequence);
}

function readWatchRequestInto(
  r: ProtoReader,
  end: number,
  out: WatchRequest = { jobId: "", afterSequence: 0n },
): WatchRequest {
  while (r.pos < end) {
    const [fn, wt] = r.readTag(end);
    switch (fn) {
      case 1:
        r.expectWire(wt, WIRE_LEN);
        out.jobId = r.readString(end);
        break;
      case 2:
        r.expectWire(wt, WIRE_VARINT);
        out.afterSequence = r.readUint64(end);
        break;
      default:
        r.skipField(wt, end);
    }
  }
  return out;
}

export function encodeWatchRequest(msg: WatchRequestInput): Uint8Array {
  return encodeWith(msg, encodedLenWatchRequest, writeWatchRequest);
}

export function decodeWatchRequest(buf: Uint8Array): WatchRequest {
  return decodeWith(buf, (r, end) => readWatchRequestInto(r, end));
}

// 20. GetJobRequest
export function encodedLenGetJobRequest(msg: GetJobRequestInput): number {
  return stringFieldLen(1, msg.jobId) + boolFieldLen(2, msg.includeOutputs);
}

function writeGetJobRequest(w: ProtoWriter, msg: GetJobRequestInput): void {
  w.writeStringField(1, msg.jobId);
  w.writeBoolField(2, msg.includeOutputs);
}

function readGetJobRequestInto(
  r: ProtoReader,
  end: number,
  out: GetJobRequest = { jobId: "", includeOutputs: false },
): GetJobRequest {
  while (r.pos < end) {
    const [fn, wt] = r.readTag(end);
    switch (fn) {
      case 1:
        r.expectWire(wt, WIRE_LEN);
        out.jobId = r.readString(end);
        break;
      case 2:
        r.expectWire(wt, WIRE_VARINT);
        out.includeOutputs = r.readBool(end);
        break;
      default:
        r.skipField(wt, end);
    }
  }
  return out;
}

export function encodeGetJobRequest(msg: GetJobRequestInput): Uint8Array {
  return encodeWith(msg, encodedLenGetJobRequest, writeGetJobRequest);
}

export function decodeGetJobRequest(buf: Uint8Array): GetJobRequest {
  return decodeWith(buf, (r, end) => readGetJobRequestInto(r, end));
}

// 21. CancelRequest
export function encodedLenCancelRequest(msg: CancelRequestInput): number {
  return stringFieldLen(1, msg.jobId) + stringFieldLen(2, msg.reason);
}

function writeCancelRequest(w: ProtoWriter, msg: CancelRequestInput): void {
  w.writeStringField(1, msg.jobId);
  w.writeStringField(2, msg.reason);
}

function readCancelRequestInto(
  r: ProtoReader,
  end: number,
  out: CancelRequest = { jobId: "", reason: "" },
): CancelRequest {
  while (r.pos < end) {
    const [fn, wt] = r.readTag(end);
    switch (fn) {
      case 1:
        r.expectWire(wt, WIRE_LEN);
        out.jobId = r.readString(end);
        break;
      case 2:
        r.expectWire(wt, WIRE_LEN);
        out.reason = r.readString(end);
        break;
      default:
        r.skipField(wt, end);
    }
  }
  return out;
}

export function encodeCancelRequest(msg: CancelRequestInput): Uint8Array {
  return encodeWith(msg, encodedLenCancelRequest, writeCancelRequest);
}

export function decodeCancelRequest(buf: Uint8Array): CancelRequest {
  return decodeWith(buf, (r, end) => readCancelRequestInto(r, end));
}

// 22. ListJobsRequest (repeated JobState states = 1 is packed in proto3!)
export function encodedLenListJobsRequest(msg: ListJobsRequestInput): number {
  let len = 0;
  if (msg.states && msg.states.length > 0) {
    let packedLen = 0;
    for (let i = 0; i < msg.states.length; i++) {
      packedLen += enumVarintLen(msg.states[i]!);
    }
    len += lenDelimitedTotal(1, packedLen);
  }
  len += uint32FieldLen(2, msg.limit) + stringFieldLen(3, msg.pageToken);
  return len;
}

function writeListJobsRequest(w: ProtoWriter, msg: ListJobsRequestInput): void {
  if (msg.states && msg.states.length > 0) {
    let packedLen = 0;
    for (let i = 0; i < msg.states.length; i++) {
      packedLen += enumVarintLen(msg.states[i]!);
    }
    w.writeTag(1, WIRE_LEN);
    w.writeVarint32(packedLen);
    for (let i = 0; i < msg.states.length; i++) {
      w.writeEnumVal(msg.states[i]!);
    }
  }
  w.writeUint32Field(2, msg.limit);
  w.writeStringField(3, msg.pageToken);
}

function readListJobsRequestInto(
  r: ProtoReader,
  end: number,
  out: ListJobsRequest = { states: [], limit: 0, pageToken: "" },
): ListJobsRequest {
  while (r.pos < end) {
    const [fn, wt] = r.readTag(end);
    switch (fn) {
      case 1:
        if (wt === WIRE_LEN) {
          const subEnd = r.readSubEnd(end);
          while (r.pos < subEnd) {
            out.states.push(r.readEnum(subEnd) as JobState);
          }
        } else if (wt === WIRE_VARINT) {
          out.states.push(r.readEnum(end) as JobState);
        } else {
          r.expectWire(wt, WIRE_LEN);
        }
        break;
      case 2:
        r.expectWire(wt, WIRE_VARINT);
        out.limit = r.readUint32(end);
        break;
      case 3:
        r.expectWire(wt, WIRE_LEN);
        out.pageToken = r.readString(end);
        break;
      default:
        r.skipField(wt, end);
    }
  }
  return out;
}

export function encodeListJobsRequest(msg: ListJobsRequestInput): Uint8Array {
  return encodeWith(msg, encodedLenListJobsRequest, writeListJobsRequest);
}

export function decodeListJobsRequest(buf: Uint8Array): ListJobsRequest {
  return decodeWith(buf, (r, end) => readListJobsRequestInto(r, end));
}

// 23. ListJobsResponse
export function encodedLenListJobsResponse(msg: ListJobsResponseInput): number {
  let len = 0;
  if (msg.jobs) {
    for (let i = 0; i < msg.jobs.length; i++) {
      len += lenDelimitedTotal(1, encodedLenJob(msg.jobs[i]!));
    }
  }
  len += stringFieldLen(2, msg.nextPageToken);
  return len;
}

function writeListJobsResponse(
  w: ProtoWriter,
  msg: ListJobsResponseInput,
): void {
  if (msg.jobs) {
    for (let i = 0; i < msg.jobs.length; i++) {
      const item = msg.jobs[i]!;
      const subLen = encodedLenJob(item);
      w.writeTag(1, WIRE_LEN);
      w.writeVarint32(subLen);
      writeJob(w, item);
    }
  }
  w.writeStringField(2, msg.nextPageToken);
}

function readListJobsResponseInto(
  r: ProtoReader,
  end: number,
  out: ListJobsResponse = { jobs: [], nextPageToken: "" },
): ListJobsResponse {
  while (r.pos < end) {
    const [fn, wt] = r.readTag(end);
    switch (fn) {
      case 1:
        r.expectWire(wt, WIRE_LEN);
        out.jobs.push(readJobInto(r, r.readSubEnd(end)));
        break;
      case 2:
        r.expectWire(wt, WIRE_LEN);
        out.nextPageToken = r.readString(end);
        break;
      default:
        r.skipField(wt, end);
    }
  }
  return out;
}

export function encodeListJobsResponse(msg: ListJobsResponseInput): Uint8Array {
  return encodeWith(msg, encodedLenListJobsResponse, writeListJobsResponse);
}

export function decodeListJobsResponse(buf: Uint8Array): ListJobsResponse {
  return decodeWith(buf, (r, end) => readListJobsResponseInto(r, end));
}

// ---------- Worker Control Messages ----------

// 24. Hello
export function encodedLenHello(msg: HelloInput): number {
  return (
    stringFieldLen(1, msg.workerId) +
    stringFieldLen(2, msg.resumeSessionId) +
    repeatedStringFieldLen(3, msg.interfaces) +
    stringMapFieldLen(4, msg.attributes) +
    int64MapFieldLen(5, msg.totalCapacity) +
    uint32FieldLen(6, msg.maxConcurrency) +
    stringFieldLen(7, msg.sdkVersion) +
    repeatedStringFieldLen(8, msg.heldLeaseIds) +
    stringMapFieldLen(9, msg.metadata) +
    stringFieldLen(10, msg.pool)
  );
}

function writeHello(w: ProtoWriter, msg: HelloInput): void {
  w.writeStringField(1, msg.workerId);
  w.writeStringField(2, msg.resumeSessionId);
  w.writeRepeatedStringField(3, msg.interfaces);
  w.writeStringMapField(4, msg.attributes);
  w.writeInt64MapField(5, msg.totalCapacity);
  w.writeUint32Field(6, msg.maxConcurrency);
  w.writeStringField(7, msg.sdkVersion);
  w.writeRepeatedStringField(8, msg.heldLeaseIds);
  w.writeStringMapField(9, msg.metadata);
  w.writeStringField(10, msg.pool);
}

function readHelloInto(
  r: ProtoReader,
  end: number,
  out: Hello = {
    workerId: "",
    resumeSessionId: "",
    interfaces: [],
    attributes: {},
    totalCapacity: {},
    maxConcurrency: 0,
    sdkVersion: "",
    heldLeaseIds: [],
    metadata: {},
    pool: "",
  },
): Hello {
  while (r.pos < end) {
    const [fn, wt] = r.readTag(end);
    switch (fn) {
      case 1:
        r.expectWire(wt, WIRE_LEN);
        out.workerId = r.readString(end);
        break;
      case 2:
        r.expectWire(wt, WIRE_LEN);
        out.resumeSessionId = r.readString(end);
        break;
      case 3:
        r.expectWire(wt, WIRE_LEN);
        out.interfaces.push(r.readString(end));
        break;
      case 4:
        r.expectWire(wt, WIRE_LEN);
        r.readStringMapEntry(end, out.attributes);
        break;
      case 5:
        r.expectWire(wt, WIRE_LEN);
        r.readInt64MapEntry(end, out.totalCapacity);
        break;
      case 6:
        r.expectWire(wt, WIRE_VARINT);
        out.maxConcurrency = r.readUint32(end);
        break;
      case 7:
        r.expectWire(wt, WIRE_LEN);
        out.sdkVersion = r.readString(end);
        break;
      case 8:
        r.expectWire(wt, WIRE_LEN);
        out.heldLeaseIds.push(r.readString(end));
        break;
      case 9:
        r.expectWire(wt, WIRE_LEN);
        r.readStringMapEntry(end, out.metadata);
        break;
      case 10:
        r.expectWire(wt, WIRE_LEN);
        out.pool = r.readString(end);
        break;
      default:
        r.skipField(wt, end);
    }
  }
  return out;
}

export function encodeHello(msg: HelloInput): Uint8Array {
  return encodeWith(msg, encodedLenHello, writeHello);
}

export function decodeHello(buf: Uint8Array): Hello {
  return decodeWith(buf, (r, end) => readHelloInto(r, end));
}

// 25. Credit
export function encodedLenCredit(msg: CreditInput): number {
  return uint32FieldLen(1, msg.freeSlots);
}

function writeCredit(w: ProtoWriter, msg: CreditInput): void {
  w.writeUint32Field(1, msg.freeSlots);
}

function readCreditInto(
  r: ProtoReader,
  end: number,
  out: Credit = { freeSlots: 0 },
): Credit {
  while (r.pos < end) {
    const [fn, wt] = r.readTag(end);
    switch (fn) {
      case 1:
        r.expectWire(wt, WIRE_VARINT);
        out.freeSlots = r.readUint32(end);
        break;
      default:
        r.skipField(wt, end);
    }
  }
  return out;
}

export function encodeCredit(msg: CreditInput): Uint8Array {
  return encodeWith(msg, encodedLenCredit, writeCredit);
}

export function decodeCredit(buf: Uint8Array): Credit {
  return decodeWith(buf, (r, end) => readCreditInto(r, end));
}

// 26. Load
export function encodedLenLoad(msg: LoadInput): number {
  return (
    uint32FieldLen(1, msg.queued) +
    uint32FieldLen(2, msg.running) +
    doubleFieldLen(3, msg.utilization) +
    doubleMapFieldLen(4, msg.custom)
  );
}

function writeLoad(w: ProtoWriter, msg: LoadInput): void {
  w.writeUint32Field(1, msg.queued);
  w.writeUint32Field(2, msg.running);
  w.writeDoubleField(3, msg.utilization);
  w.writeDoubleMapField(4, msg.custom);
}

function readLoadInto(
  r: ProtoReader,
  end: number,
  out: Load = { queued: 0, running: 0, utilization: 0, custom: {} },
): Load {
  while (r.pos < end) {
    const [fn, wt] = r.readTag(end);
    switch (fn) {
      case 1:
        r.expectWire(wt, WIRE_VARINT);
        out.queued = r.readUint32(end);
        break;
      case 2:
        r.expectWire(wt, WIRE_VARINT);
        out.running = r.readUint32(end);
        break;
      case 3:
        r.expectWire(wt, WIRE_64BIT);
        out.utilization = r.readDouble(end);
        break;
      case 4:
        r.expectWire(wt, WIRE_LEN);
        r.readDoubleMapEntry(end, out.custom);
        break;
      default:
        r.skipField(wt, end);
    }
  }
  return out;
}

export function encodeLoad(msg: LoadInput): Uint8Array {
  return encodeWith(msg, encodedLenLoad, writeLoad);
}

export function decodeLoad(buf: Uint8Array): Load {
  return decodeWith(buf, (r, end) => readLoadInto(r, end));
}

// 27. Heartbeat
export function encodedLenHeartbeat(msg: HeartbeatInput): number {
  if (msg.load === undefined) return 0;
  return lenDelimitedTotal(1, encodedLenLoad(msg.load));
}

function writeHeartbeat(w: ProtoWriter, msg: HeartbeatInput): void {
  if (msg.load !== undefined) {
    const subLen = encodedLenLoad(msg.load);
    w.writeTag(1, WIRE_LEN);
    w.writeVarint32(subLen);
    writeLoad(w, msg.load);
  }
}

function readHeartbeatInto(
  r: ProtoReader,
  end: number,
  out: Heartbeat = {},
): Heartbeat {
  while (r.pos < end) {
    const [fn, wt] = r.readTag(end);
    switch (fn) {
      case 1:
        r.expectWire(wt, WIRE_LEN);
        out.load = readLoadInto(r, r.readSubEnd(end), out.load);
        break;
      default:
        r.skipField(wt, end);
    }
  }
  return out;
}

export function encodeHeartbeat(msg: HeartbeatInput): Uint8Array {
  return encodeWith(msg, encodedLenHeartbeat, writeHeartbeat);
}

export function decodeHeartbeat(buf: Uint8Array): Heartbeat {
  return decodeWith(buf, (r, end) => readHeartbeatInto(r, end));
}

// 28. AssignReply
export function encodedLenAssignReply(msg: AssignReplyInput): number {
  return (
    stringFieldLen(1, msg.leaseId) +
    boolFieldLen(2, msg.accepted) +
    stringFieldLen(3, msg.rejectReason)
  );
}

function writeAssignReply(w: ProtoWriter, msg: AssignReplyInput): void {
  w.writeStringField(1, msg.leaseId);
  w.writeBoolField(2, msg.accepted);
  w.writeStringField(3, msg.rejectReason);
}

function readAssignReplyInto(
  r: ProtoReader,
  end: number,
  out: AssignReply = { leaseId: "", accepted: false, rejectReason: "" },
): AssignReply {
  while (r.pos < end) {
    const [fn, wt] = r.readTag(end);
    switch (fn) {
      case 1:
        r.expectWire(wt, WIRE_LEN);
        out.leaseId = r.readString(end);
        break;
      case 2:
        r.expectWire(wt, WIRE_VARINT);
        out.accepted = r.readBool(end);
        break;
      case 3:
        r.expectWire(wt, WIRE_LEN);
        out.rejectReason = r.readString(end);
        break;
      default:
        r.skipField(wt, end);
    }
  }
  return out;
}

export function encodeAssignReply(msg: AssignReplyInput): Uint8Array {
  return encodeWith(msg, encodedLenAssignReply, writeAssignReply);
}

export function decodeAssignReply(buf: Uint8Array): AssignReply {
  return decodeWith(buf, (r, end) => readAssignReplyInto(r, end));
}

// 29. CancelAck
export function encodedLenCancelAck(msg: CancelAckInput): number {
  return stringFieldLen(1, msg.leaseId);
}

function writeCancelAck(w: ProtoWriter, msg: CancelAckInput): void {
  w.writeStringField(1, msg.leaseId);
}

function readCancelAckInto(
  r: ProtoReader,
  end: number,
  out: CancelAck = { leaseId: "" },
): CancelAck {
  while (r.pos < end) {
    const [fn, wt] = r.readTag(end);
    switch (fn) {
      case 1:
        r.expectWire(wt, WIRE_LEN);
        out.leaseId = r.readString(end);
        break;
      default:
        r.skipField(wt, end);
    }
  }
  return out;
}

export function encodeCancelAck(msg: CancelAckInput): Uint8Array {
  return encodeWith(msg, encodedLenCancelAck, writeCancelAck);
}

export function decodeCancelAck(buf: Uint8Array): CancelAck {
  return decodeWith(buf, (r, end) => readCancelAckInto(r, end));
}

// 30. Draining
export function encodedLenDraining(msg: DrainingInput): number {
  return boolFieldLen(1, msg.preempting);
}

function writeDraining(w: ProtoWriter, msg: DrainingInput): void {
  w.writeBoolField(1, msg.preempting);
}

function readDrainingInto(
  r: ProtoReader,
  end: number,
  out: Draining = { preempting: false },
): Draining {
  while (r.pos < end) {
    const [fn, wt] = r.readTag(end);
    switch (fn) {
      case 1:
        r.expectWire(wt, WIRE_VARINT);
        out.preempting = r.readBool(end);
        break;
      default:
        r.skipField(wt, end);
    }
  }
  return out;
}

export function encodeDraining(msg: DrainingInput): Uint8Array {
  return encodeWith(msg, encodedLenDraining, writeDraining);
}

export function decodeDraining(buf: Uint8Array): Draining {
  return decodeWith(buf, (r, end) => readDrainingInto(r, end));
}

// 31. WorkerMessage
export function encodedLenWorkerMessage(msg: WorkerMessageInput): number {
  if (!msg.msg) return 0;
  switch (msg.msg.case) {
    case "hello":
      return lenDelimitedTotal(1, encodedLenHello(msg.msg.value));
    case "credit":
      return lenDelimitedTotal(2, encodedLenCredit(msg.msg.value));
    case "heartbeat":
      return lenDelimitedTotal(3, encodedLenHeartbeat(msg.msg.value));
    case "assignReply":
      return lenDelimitedTotal(4, encodedLenAssignReply(msg.msg.value));
    case "cancelAck":
      return lenDelimitedTotal(5, encodedLenCancelAck(msg.msg.value));
    case "draining":
      return lenDelimitedTotal(6, encodedLenDraining(msg.msg.value));
  }
}

function writeWorkerMessage(w: ProtoWriter, msg: WorkerMessageInput): void {
  if (!msg.msg) return;
  switch (msg.msg.case) {
    case "hello": {
      const subLen = encodedLenHello(msg.msg.value);
      w.writeTag(1, WIRE_LEN);
      w.writeVarint32(subLen);
      writeHello(w, msg.msg.value);
      break;
    }
    case "credit": {
      const subLen = encodedLenCredit(msg.msg.value);
      w.writeTag(2, WIRE_LEN);
      w.writeVarint32(subLen);
      writeCredit(w, msg.msg.value);
      break;
    }
    case "heartbeat": {
      const subLen = encodedLenHeartbeat(msg.msg.value);
      w.writeTag(3, WIRE_LEN);
      w.writeVarint32(subLen);
      writeHeartbeat(w, msg.msg.value);
      break;
    }
    case "assignReply": {
      const subLen = encodedLenAssignReply(msg.msg.value);
      w.writeTag(4, WIRE_LEN);
      w.writeVarint32(subLen);
      writeAssignReply(w, msg.msg.value);
      break;
    }
    case "cancelAck": {
      const subLen = encodedLenCancelAck(msg.msg.value);
      w.writeTag(5, WIRE_LEN);
      w.writeVarint32(subLen);
      writeCancelAck(w, msg.msg.value);
      break;
    }
    case "draining": {
      const subLen = encodedLenDraining(msg.msg.value);
      w.writeTag(6, WIRE_LEN);
      w.writeVarint32(subLen);
      writeDraining(w, msg.msg.value);
      break;
    }
  }
}

function readWorkerMessageInto(
  r: ProtoReader,
  end: number,
  out: WorkerMessage = {},
): WorkerMessage {
  while (r.pos < end) {
    const [fn, wt] = r.readTag(end);
    switch (fn) {
      case 1:
        r.expectWire(wt, WIRE_LEN);
        out.msg = {
          case: "hello",
          value: readHelloInto(
            r,
            r.readSubEnd(end),
            out.msg?.case === "hello" ? out.msg.value : undefined,
          ),
        };
        break;
      case 2:
        r.expectWire(wt, WIRE_LEN);
        out.msg = {
          case: "credit",
          value: readCreditInto(
            r,
            r.readSubEnd(end),
            out.msg?.case === "credit" ? out.msg.value : undefined,
          ),
        };
        break;
      case 3:
        r.expectWire(wt, WIRE_LEN);
        out.msg = {
          case: "heartbeat",
          value: readHeartbeatInto(
            r,
            r.readSubEnd(end),
            out.msg?.case === "heartbeat" ? out.msg.value : undefined,
          ),
        };
        break;
      case 4:
        r.expectWire(wt, WIRE_LEN);
        out.msg = {
          case: "assignReply",
          value: readAssignReplyInto(
            r,
            r.readSubEnd(end),
            out.msg?.case === "assignReply" ? out.msg.value : undefined,
          ),
        };
        break;
      case 5:
        r.expectWire(wt, WIRE_LEN);
        out.msg = {
          case: "cancelAck",
          value: readCancelAckInto(
            r,
            r.readSubEnd(end),
            out.msg?.case === "cancelAck" ? out.msg.value : undefined,
          ),
        };
        break;
      case 6:
        r.expectWire(wt, WIRE_LEN);
        out.msg = {
          case: "draining",
          value: readDrainingInto(
            r,
            r.readSubEnd(end),
            out.msg?.case === "draining" ? out.msg.value : undefined,
          ),
        };
        break;
      default:
        r.skipField(wt, end);
    }
  }
  return out;
}

export function encodeWorkerMessage(msg: WorkerMessageInput): Uint8Array {
  return encodeWith(msg, encodedLenWorkerMessage, writeWorkerMessage);
}

export function decodeWorkerMessage(buf: Uint8Array): WorkerMessage {
  return decodeWith(buf, (r, end) => readWorkerMessageInto(r, end));
}

// 32. Limits
export function encodedLenLimits(msg: LimitsInput): number {
  return (
    uint32FieldLen(1, msg.maxControlMessageBytes) +
    uint32FieldLen(2, msg.maxChunkBytes) +
    uint32FieldLen(3, msg.attemptWindowBytes) +
    uint32FieldLen(4, msg.assignInlineLimit)
  );
}

function writeLimits(w: ProtoWriter, msg: LimitsInput): void {
  w.writeUint32Field(1, msg.maxControlMessageBytes);
  w.writeUint32Field(2, msg.maxChunkBytes);
  w.writeUint32Field(3, msg.attemptWindowBytes);
  w.writeUint32Field(4, msg.assignInlineLimit);
}

function readLimitsInto(
  r: ProtoReader,
  end: number,
  out: Limits = {
    maxControlMessageBytes: 0,
    maxChunkBytes: 0,
    attemptWindowBytes: 0,
    assignInlineLimit: 0,
  },
): Limits {
  while (r.pos < end) {
    const [fn, wt] = r.readTag(end);
    switch (fn) {
      case 1:
        r.expectWire(wt, WIRE_VARINT);
        out.maxControlMessageBytes = r.readUint32(end);
        break;
      case 2:
        r.expectWire(wt, WIRE_VARINT);
        out.maxChunkBytes = r.readUint32(end);
        break;
      case 3:
        r.expectWire(wt, WIRE_VARINT);
        out.attemptWindowBytes = r.readUint32(end);
        break;
      case 4:
        r.expectWire(wt, WIRE_VARINT);
        out.assignInlineLimit = r.readUint32(end);
        break;
      default:
        r.skipField(wt, end);
    }
  }
  return out;
}

export function encodeLimits(msg: LimitsInput): Uint8Array {
  return encodeWith(msg, encodedLenLimits, writeLimits);
}

export function decodeLimits(buf: Uint8Array): Limits {
  return decodeWith(buf, (r, end) => readLimitsInto(r, end));
}

// 33. Welcome
export function encodedLenWelcome(msg: WelcomeInput): number {
  let len =
    stringFieldLen(1, msg.sessionId) +
    uint64FieldLen(2, msg.generation) +
    uint32FieldLen(3, msg.heartbeatIntervalMs) +
    uint32FieldLen(4, msg.heartbeatTimeoutMs);
  if (msg.limits !== undefined) {
    len += lenDelimitedTotal(5, encodedLenLimits(msg.limits));
  }
  len += repeatedStringFieldLen(6, msg.resumedLeaseIds);
  return len;
}

function writeWelcome(w: ProtoWriter, msg: WelcomeInput): void {
  w.writeStringField(1, msg.sessionId);
  w.writeUint64Field(2, msg.generation);
  w.writeUint32Field(3, msg.heartbeatIntervalMs);
  w.writeUint32Field(4, msg.heartbeatTimeoutMs);
  if (msg.limits !== undefined) {
    const subLen = encodedLenLimits(msg.limits);
    w.writeTag(5, WIRE_LEN);
    w.writeVarint32(subLen);
    writeLimits(w, msg.limits);
  }
  w.writeRepeatedStringField(6, msg.resumedLeaseIds);
}

function readWelcomeInto(
  r: ProtoReader,
  end: number,
  out: Welcome = {
    sessionId: "",
    generation: 0n,
    heartbeatIntervalMs: 0,
    heartbeatTimeoutMs: 0,
    resumedLeaseIds: [],
  },
): Welcome {
  while (r.pos < end) {
    const [fn, wt] = r.readTag(end);
    switch (fn) {
      case 1:
        r.expectWire(wt, WIRE_LEN);
        out.sessionId = r.readString(end);
        break;
      case 2:
        r.expectWire(wt, WIRE_VARINT);
        out.generation = r.readUint64(end);
        break;
      case 3:
        r.expectWire(wt, WIRE_VARINT);
        out.heartbeatIntervalMs = r.readUint32(end);
        break;
      case 4:
        r.expectWire(wt, WIRE_VARINT);
        out.heartbeatTimeoutMs = r.readUint32(end);
        break;
      case 5:
        r.expectWire(wt, WIRE_LEN);
        out.limits = readLimitsInto(r, r.readSubEnd(end), out.limits);
        break;
      case 6:
        r.expectWire(wt, WIRE_LEN);
        out.resumedLeaseIds.push(r.readString(end));
        break;
      default:
        r.skipField(wt, end);
    }
  }
  return out;
}

export function encodeWelcome(msg: WelcomeInput): Uint8Array {
  return encodeWith(msg, encodedLenWelcome, writeWelcome);
}

export function decodeWelcome(buf: Uint8Array): Welcome {
  return decodeWith(buf, (r, end) => readWelcomeInto(r, end));
}

// 34. Assign
export function encodedLenAssign(msg: AssignInput): number {
  let len =
    stringFieldLen(1, msg.leaseId) +
    stringFieldLen(2, msg.attemptId) +
    stringFieldLen(3, msg.jobId) +
    uint32FieldLen(4, msg.attemptNumber) +
    uint64FieldLen(5, msg.generation) +
    stringFieldLen(6, msg.interface) +
    bytesFieldLen(7, msg.params) +
    int64MapFieldLen(8, msg.reservedCapacity) +
    stringMapFieldLen(9, msg.metadata);
  if (msg.inlineInputs) {
    for (let i = 0; i < msg.inlineInputs.length; i++) {
      len += lenDelimitedTotal(10, encodedLenObject(msg.inlineInputs[i]!));
    }
  }
  len +=
    boolFieldLen(11, msg.inputsViaAttempt) +
    boolFieldLen(12, msg.streamingInput) +
    uint64FieldLen(13, msg.executionDeadlineMs) +
    uint64FieldLen(14, msg.totalDeadlineMs);
  return len;
}

function writeAssign(w: ProtoWriter, msg: AssignInput): void {
  w.writeStringField(1, msg.leaseId);
  w.writeStringField(2, msg.attemptId);
  w.writeStringField(3, msg.jobId);
  w.writeUint32Field(4, msg.attemptNumber);
  w.writeUint64Field(5, msg.generation);
  w.writeStringField(6, msg.interface);
  w.writeBytesField(7, msg.params);
  w.writeInt64MapField(8, msg.reservedCapacity);
  w.writeStringMapField(9, msg.metadata);
  if (msg.inlineInputs) {
    for (let i = 0; i < msg.inlineInputs.length; i++) {
      const item = msg.inlineInputs[i]!;
      const subLen = encodedLenObject(item);
      w.writeTag(10, WIRE_LEN);
      w.writeVarint32(subLen);
      writeObject(w, item);
    }
  }
  w.writeBoolField(11, msg.inputsViaAttempt);
  w.writeBoolField(12, msg.streamingInput);
  w.writeUint64Field(13, msg.executionDeadlineMs);
  w.writeUint64Field(14, msg.totalDeadlineMs);
}

function readAssignInto(
  r: ProtoReader,
  end: number,
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
): Assign {
  while (r.pos < end) {
    const [fn, wt] = r.readTag(end);
    switch (fn) {
      case 1:
        r.expectWire(wt, WIRE_LEN);
        out.leaseId = r.readString(end);
        break;
      case 2:
        r.expectWire(wt, WIRE_LEN);
        out.attemptId = r.readString(end);
        break;
      case 3:
        r.expectWire(wt, WIRE_LEN);
        out.jobId = r.readString(end);
        break;
      case 4:
        r.expectWire(wt, WIRE_VARINT);
        out.attemptNumber = r.readUint32(end);
        break;
      case 5:
        r.expectWire(wt, WIRE_VARINT);
        out.generation = r.readUint64(end);
        break;
      case 6:
        r.expectWire(wt, WIRE_LEN);
        out.interface = r.readString(end);
        break;
      case 7:
        r.expectWire(wt, WIRE_LEN);
        out.params = r.readBytes(end);
        break;
      case 8:
        r.expectWire(wt, WIRE_LEN);
        r.readInt64MapEntry(end, out.reservedCapacity);
        break;
      case 9:
        r.expectWire(wt, WIRE_LEN);
        r.readStringMapEntry(end, out.metadata);
        break;
      case 10:
        r.expectWire(wt, WIRE_LEN);
        out.inlineInputs.push(readObjectInto(r, r.readSubEnd(end)));
        break;
      case 11:
        r.expectWire(wt, WIRE_VARINT);
        out.inputsViaAttempt = r.readBool(end);
        break;
      case 12:
        r.expectWire(wt, WIRE_VARINT);
        out.streamingInput = r.readBool(end);
        break;
      case 13:
        r.expectWire(wt, WIRE_VARINT);
        out.executionDeadlineMs = r.readUint64(end);
        break;
      case 14:
        r.expectWire(wt, WIRE_VARINT);
        out.totalDeadlineMs = r.readUint64(end);
        break;
      default:
        r.skipField(wt, end);
    }
  }
  return out;
}

export function encodeAssign(msg: AssignInput): Uint8Array {
  return encodeWith(msg, encodedLenAssign, writeAssign);
}

export function decodeAssign(buf: Uint8Array): Assign {
  return decodeWith(buf, (r, end) => readAssignInto(r, end));
}

// 35. Cancel
export function encodedLenCancel(msg: CancelInput): number {
  return stringFieldLen(1, msg.leaseId) + stringFieldLen(2, msg.reason);
}

function writeCancel(w: ProtoWriter, msg: CancelInput): void {
  w.writeStringField(1, msg.leaseId);
  w.writeStringField(2, msg.reason);
}

function readCancelInto(
  r: ProtoReader,
  end: number,
  out: Cancel = { leaseId: "", reason: "" },
): Cancel {
  while (r.pos < end) {
    const [fn, wt] = r.readTag(end);
    switch (fn) {
      case 1:
        r.expectWire(wt, WIRE_LEN);
        out.leaseId = r.readString(end);
        break;
      case 2:
        r.expectWire(wt, WIRE_LEN);
        out.reason = r.readString(end);
        break;
      default:
        r.skipField(wt, end);
    }
  }
  return out;
}

export function encodeCancel(msg: CancelInput): Uint8Array {
  return encodeWith(msg, encodedLenCancel, writeCancel);
}

export function decodeCancel(buf: Uint8Array): Cancel {
  return decodeWith(buf, (r, end) => readCancelInto(r, end));
}

// 36. Fence
export function encodedLenFence(msg: FenceInput): number {
  return stringFieldLen(1, msg.leaseId) + stringFieldLen(2, msg.reason);
}

function writeFence(w: ProtoWriter, msg: FenceInput): void {
  w.writeStringField(1, msg.leaseId);
  w.writeStringField(2, msg.reason);
}

function readFenceInto(
  r: ProtoReader,
  end: number,
  out: Fence = { leaseId: "", reason: "" },
): Fence {
  while (r.pos < end) {
    const [fn, wt] = r.readTag(end);
    switch (fn) {
      case 1:
        r.expectWire(wt, WIRE_LEN);
        out.leaseId = r.readString(end);
        break;
      case 2:
        r.expectWire(wt, WIRE_LEN);
        out.reason = r.readString(end);
        break;
      default:
        r.skipField(wt, end);
    }
  }
  return out;
}

export function encodeFence(msg: FenceInput): Uint8Array {
  return encodeWith(msg, encodedLenFence, writeFence);
}

export function decodeFence(buf: Uint8Array): Fence {
  return decodeWith(buf, (r, end) => readFenceInto(r, end));
}

// 37. Drain
export function encodedLenDrain(_msg?: Drain): number {
  return 0;
}

export function encodeDrain(_msg?: Drain): Uint8Array {
  return new Uint8Array(0);
}

export function decodeDrain(buf: Uint8Array): Drain {
  return decodeWith(buf, (r, end) => readEmptyInto(r, end, {}));
}

// 38. ServerMessage
export function encodedLenServerMessage(msg: ServerMessageInput): number {
  if (!msg.msg) return 0;
  switch (msg.msg.case) {
    case "welcome":
      return lenDelimitedTotal(1, encodedLenWelcome(msg.msg.value));
    case "assign":
      return lenDelimitedTotal(2, encodedLenAssign(msg.msg.value));
    case "cancel":
      return lenDelimitedTotal(3, encodedLenCancel(msg.msg.value));
    case "fence":
      return lenDelimitedTotal(4, encodedLenFence(msg.msg.value));
    case "drain":
      return lenDelimitedTotal(5, 0);
  }
}

function writeServerMessage(w: ProtoWriter, msg: ServerMessageInput): void {
  if (!msg.msg) return;
  switch (msg.msg.case) {
    case "welcome": {
      const subLen = encodedLenWelcome(msg.msg.value);
      w.writeTag(1, WIRE_LEN);
      w.writeVarint32(subLen);
      writeWelcome(w, msg.msg.value);
      break;
    }
    case "assign": {
      const subLen = encodedLenAssign(msg.msg.value);
      w.writeTag(2, WIRE_LEN);
      w.writeVarint32(subLen);
      writeAssign(w, msg.msg.value);
      break;
    }
    case "cancel": {
      const subLen = encodedLenCancel(msg.msg.value);
      w.writeTag(3, WIRE_LEN);
      w.writeVarint32(subLen);
      writeCancel(w, msg.msg.value);
      break;
    }
    case "fence": {
      const subLen = encodedLenFence(msg.msg.value);
      w.writeTag(4, WIRE_LEN);
      w.writeVarint32(subLen);
      writeFence(w, msg.msg.value);
      break;
    }
    case "drain":
      w.writeTag(5, WIRE_LEN);
      w.writeVarint32(0);
      break;
  }
}

function readServerMessageInto(
  r: ProtoReader,
  end: number,
  out: ServerMessage = {},
): ServerMessage {
  while (r.pos < end) {
    const [fn, wt] = r.readTag(end);
    switch (fn) {
      case 1:
        r.expectWire(wt, WIRE_LEN);
        out.msg = {
          case: "welcome",
          value: readWelcomeInto(
            r,
            r.readSubEnd(end),
            out.msg?.case === "welcome" ? out.msg.value : undefined,
          ),
        };
        break;
      case 2:
        r.expectWire(wt, WIRE_LEN);
        out.msg = {
          case: "assign",
          value: readAssignInto(
            r,
            r.readSubEnd(end),
            out.msg?.case === "assign" ? out.msg.value : undefined,
          ),
        };
        break;
      case 3:
        r.expectWire(wt, WIRE_LEN);
        out.msg = {
          case: "cancel",
          value: readCancelInto(
            r,
            r.readSubEnd(end),
            out.msg?.case === "cancel" ? out.msg.value : undefined,
          ),
        };
        break;
      case 4:
        r.expectWire(wt, WIRE_LEN);
        out.msg = {
          case: "fence",
          value: readFenceInto(
            r,
            r.readSubEnd(end),
            out.msg?.case === "fence" ? out.msg.value : undefined,
          ),
        };
        break;
      case 5:
        r.expectWire(wt, WIRE_LEN);
        out.msg = {
          case: "drain",
          value: readEmptyInto(r, r.readSubEnd(end), {}),
        };
        break;
      default:
        r.skipField(wt, end);
    }
  }
  return out;
}

export function encodeServerMessage(msg: ServerMessageInput): Uint8Array {
  return encodeWith(msg, encodedLenServerMessage, writeServerMessage);
}

export function decodeServerMessage(buf: Uint8Array): ServerMessage {
  return decodeWith(buf, (r, end) => readServerMessageInto(r, end));
}

// ---------- Worker Data / Attempt Messages ----------

// 39. AttemptOpen
export function encodedLenAttemptOpen(msg: AttemptOpenInput): number {
  return (
    stringFieldLen(1, msg.leaseId) +
    uint64FieldLen(2, msg.generation) +
    uint64FieldLen(3, msg.resumeAfterSequence)
  );
}

function writeAttemptOpen(w: ProtoWriter, msg: AttemptOpenInput): void {
  w.writeStringField(1, msg.leaseId);
  w.writeUint64Field(2, msg.generation);
  w.writeUint64Field(3, msg.resumeAfterSequence);
}

function readAttemptOpenInto(
  r: ProtoReader,
  end: number,
  out: AttemptOpen = { leaseId: "", generation: 0n, resumeAfterSequence: 0n },
): AttemptOpen {
  while (r.pos < end) {
    const [fn, wt] = r.readTag(end);
    switch (fn) {
      case 1:
        r.expectWire(wt, WIRE_LEN);
        out.leaseId = r.readString(end);
        break;
      case 2:
        r.expectWire(wt, WIRE_VARINT);
        out.generation = r.readUint64(end);
        break;
      case 3:
        r.expectWire(wt, WIRE_VARINT);
        out.resumeAfterSequence = r.readUint64(end);
        break;
      default:
        r.skipField(wt, end);
    }
  }
  return out;
}

export function encodeAttemptOpen(msg: AttemptOpenInput): Uint8Array {
  return encodeWith(msg, encodedLenAttemptOpen, writeAttemptOpen);
}

export function decodeAttemptOpen(buf: Uint8Array): AttemptOpen {
  return decodeWith(buf, (r, end) => readAttemptOpenInto(r, end));
}

// 40. Frame
export function encodedLenFrame(msg: FrameInput): number {
  return (
    uint64FieldLen(1, msg.sequence) +
    bytesFieldLen(2, msg.data) +
    stringMapFieldLen(3, msg.metadata) +
    boolFieldLen(4, msg.progress)
  );
}

function writeFrame(w: ProtoWriter, msg: FrameInput): void {
  w.writeUint64Field(1, msg.sequence);
  w.writeBytesField(2, msg.data);
  w.writeStringMapField(3, msg.metadata);
  w.writeBoolField(4, msg.progress);
}

function readFrameInto(
  r: ProtoReader,
  end: number,
  out: Frame = {
    sequence: 0n,
    data: EMPTY_BYTES,
    metadata: {},
    progress: false,
  },
): Frame {
  while (r.pos < end) {
    const [fn, wt] = r.readTag(end);
    switch (fn) {
      case 1:
        r.expectWire(wt, WIRE_VARINT);
        out.sequence = r.readUint64(end);
        break;
      case 2:
        r.expectWire(wt, WIRE_LEN);
        out.data = r.readBytes(end);
        break;
      case 3:
        r.expectWire(wt, WIRE_LEN);
        r.readStringMapEntry(end, out.metadata);
        break;
      case 4:
        r.expectWire(wt, WIRE_VARINT);
        out.progress = r.readBool(end);
        break;
      default:
        r.skipField(wt, end);
    }
  }
  return out;
}

export function encodeFrame(msg: FrameInput): Uint8Array {
  return encodeWith(msg, encodedLenFrame, writeFrame);
}

export function decodeFrame(buf: Uint8Array): Frame {
  return decodeWith(buf, (r, end) => readFrameInto(r, end));
}

// 41. ResultChunk
export function encodedLenResultChunk(msg: ResultChunkInput): number {
  return (
    uint32FieldLen(1, msg.index) +
    bytesFieldLen(2, msg.data) +
    boolFieldLen(3, msg.last) +
    stringMapFieldLen(4, msg.metadata) +
    stringFieldLen(5, msg.uri)
  );
}

function writeResultChunk(w: ProtoWriter, msg: ResultChunkInput): void {
  w.writeUint32Field(1, msg.index);
  w.writeBytesField(2, msg.data);
  w.writeBoolField(3, msg.last);
  w.writeStringMapField(4, msg.metadata);
  w.writeStringField(5, msg.uri);
}

function readResultChunkInto(
  r: ProtoReader,
  end: number,
  out: ResultChunk = {
    index: 0,
    data: EMPTY_BYTES,
    last: false,
    metadata: {},
    uri: "",
  },
): ResultChunk {
  while (r.pos < end) {
    const [fn, wt] = r.readTag(end);
    switch (fn) {
      case 1:
        r.expectWire(wt, WIRE_VARINT);
        out.index = r.readUint32(end);
        break;
      case 2:
        r.expectWire(wt, WIRE_LEN);
        out.data = r.readBytes(end);
        break;
      case 3:
        r.expectWire(wt, WIRE_VARINT);
        out.last = r.readBool(end);
        break;
      case 4:
        r.expectWire(wt, WIRE_LEN);
        r.readStringMapEntry(end, out.metadata);
        break;
      case 5:
        r.expectWire(wt, WIRE_LEN);
        out.uri = r.readString(end);
        break;
      default:
        r.skipField(wt, end);
    }
  }
  return out;
}

export function encodeResultChunk(msg: ResultChunkInput): Uint8Array {
  return encodeWith(msg, encodedLenResultChunk, writeResultChunk);
}

export function decodeResultChunk(buf: Uint8Array): ResultChunk {
  return decodeWith(buf, (r, end) => readResultChunkInto(r, end));
}

// 42. Succeeded & 43. Cancelled
export function encodedLenSucceeded(_msg?: Succeeded): number {
  return 0;
}

export function encodeSucceeded(_msg?: Succeeded): Uint8Array {
  return new Uint8Array(0);
}

export function decodeSucceeded(buf: Uint8Array): Succeeded {
  return decodeWith(buf, (r, end) => readEmptyInto(r, end, {}));
}

export function encodedLenCancelled(_msg?: Cancelled): number {
  return 0;
}

export function encodeCancelled(_msg?: Cancelled): Uint8Array {
  return new Uint8Array(0);
}

export function decodeCancelled(buf: Uint8Array): Cancelled {
  return decodeWith(buf, (r, end) => readEmptyInto(r, end, {}));
}

// 44. Finish
export function encodedLenFinish(msg: FinishInput): number {
  if (!msg.outcome) return 0;
  switch (msg.outcome.case) {
    case "succeeded":
      return lenDelimitedTotal(1, 0);
    case "failed":
      return lenDelimitedTotal(2, encodedLenErrorInfo(msg.outcome.value));
    case "cancelled":
      return lenDelimitedTotal(3, 0);
  }
}

function writeFinish(w: ProtoWriter, msg: FinishInput): void {
  if (!msg.outcome) return;
  switch (msg.outcome.case) {
    case "succeeded":
      w.writeTag(1, WIRE_LEN);
      w.writeVarint32(0);
      break;
    case "failed": {
      const subLen = encodedLenErrorInfo(msg.outcome.value);
      w.writeTag(2, WIRE_LEN);
      w.writeVarint32(subLen);
      writeErrorInfo(w, msg.outcome.value);
      break;
    }
    case "cancelled":
      w.writeTag(3, WIRE_LEN);
      w.writeVarint32(0);
      break;
  }
}

function readFinishInto(
  r: ProtoReader,
  end: number,
  out: Finish = {},
): Finish {
  while (r.pos < end) {
    const [fn, wt] = r.readTag(end);
    switch (fn) {
      case 1:
        r.expectWire(wt, WIRE_LEN);
        out.outcome = {
          case: "succeeded",
          value: readEmptyInto(r, r.readSubEnd(end), {}),
        };
        break;
      case 2:
        r.expectWire(wt, WIRE_LEN);
        out.outcome = {
          case: "failed",
          value: readErrorInfoInto(
            r,
            r.readSubEnd(end),
            out.outcome?.case === "failed" ? out.outcome.value : undefined,
          ),
        };
        break;
      case 3:
        r.expectWire(wt, WIRE_LEN);
        out.outcome = {
          case: "cancelled",
          value: readEmptyInto(r, r.readSubEnd(end), {}),
        };
        break;
      default:
        r.skipField(wt, end);
    }
  }
  return out;
}

export function encodeFinish(msg: FinishInput): Uint8Array {
  return encodeWith(msg, encodedLenFinish, writeFinish);
}

export function decodeFinish(buf: Uint8Array): Finish {
  return decodeWith(buf, (r, end) => readFinishInto(r, end));
}

// 45. AttemptUp (open=1, frame=2, result=3, finish=5)
export function encodedLenAttemptUp(msg: AttemptUpInput): number {
  if (!msg.msg) return 0;
  switch (msg.msg.case) {
    case "open":
      return lenDelimitedTotal(1, encodedLenAttemptOpen(msg.msg.value));
    case "frame":
      return lenDelimitedTotal(2, encodedLenFrame(msg.msg.value));
    case "result":
      return lenDelimitedTotal(3, encodedLenResultChunk(msg.msg.value));
    case "finish":
      return lenDelimitedTotal(5, encodedLenFinish(msg.msg.value));
  }
}

function writeAttemptUp(w: ProtoWriter, msg: AttemptUpInput): void {
  if (!msg.msg) return;
  switch (msg.msg.case) {
    case "open": {
      const subLen = encodedLenAttemptOpen(msg.msg.value);
      w.writeTag(1, WIRE_LEN);
      w.writeVarint32(subLen);
      writeAttemptOpen(w, msg.msg.value);
      break;
    }
    case "frame": {
      const subLen = encodedLenFrame(msg.msg.value);
      w.writeTag(2, WIRE_LEN);
      w.writeVarint32(subLen);
      writeFrame(w, msg.msg.value);
      break;
    }
    case "result": {
      const subLen = encodedLenResultChunk(msg.msg.value);
      w.writeTag(3, WIRE_LEN);
      w.writeVarint32(subLen);
      writeResultChunk(w, msg.msg.value);
      break;
    }
    case "finish": {
      const subLen = encodedLenFinish(msg.msg.value);
      w.writeTag(5, WIRE_LEN);
      w.writeVarint32(subLen);
      writeFinish(w, msg.msg.value);
      break;
    }
  }
}

function readAttemptUpInto(
  r: ProtoReader,
  end: number,
  out: AttemptUp = {},
): AttemptUp {
  while (r.pos < end) {
    const [fn, wt] = r.readTag(end);
    switch (fn) {
      case 1:
        r.expectWire(wt, WIRE_LEN);
        out.msg = {
          case: "open",
          value: readAttemptOpenInto(
            r,
            r.readSubEnd(end),
            out.msg?.case === "open" ? out.msg.value : undefined,
          ),
        };
        break;
      case 2:
        r.expectWire(wt, WIRE_LEN);
        out.msg = {
          case: "frame",
          value: readFrameInto(
            r,
            r.readSubEnd(end),
            out.msg?.case === "frame" ? out.msg.value : undefined,
          ),
        };
        break;
      case 3:
        r.expectWire(wt, WIRE_LEN);
        out.msg = {
          case: "result",
          value: readResultChunkInto(
            r,
            r.readSubEnd(end),
            out.msg?.case === "result" ? out.msg.value : undefined,
          ),
        };
        break;
      case 5:
        r.expectWire(wt, WIRE_LEN);
        out.msg = {
          case: "finish",
          value: readFinishInto(
            r,
            r.readSubEnd(end),
            out.msg?.case === "finish" ? out.msg.value : undefined,
          ),
        };
        break;
      default:
        r.skipField(wt, end);
    }
  }
  return out;
}

export function encodeAttemptUp(msg: AttemptUpInput): Uint8Array {
  return encodeWith(msg, encodedLenAttemptUp, writeAttemptUp);
}

export function decodeAttemptUp(buf: Uint8Array): AttemptUp {
  return decodeWith(buf, (r, end) => readAttemptUpInto(r, end));
}

// 46. AttemptOpened
export function encodedLenAttemptOpened(msg: AttemptOpenedInput): number {
  return (
    uint64FieldLen(1, msg.resumeFromSequence) +
    boolFieldLen(2, msg.terminalReplay)
  );
}

function writeAttemptOpened(w: ProtoWriter, msg: AttemptOpenedInput): void {
  w.writeUint64Field(1, msg.resumeFromSequence);
  w.writeBoolField(2, msg.terminalReplay);
}

function readAttemptOpenedInto(
  r: ProtoReader,
  end: number,
  out: AttemptOpened = { resumeFromSequence: 0n, terminalReplay: false },
): AttemptOpened {
  while (r.pos < end) {
    const [fn, wt] = r.readTag(end);
    switch (fn) {
      case 1:
        r.expectWire(wt, WIRE_VARINT);
        out.resumeFromSequence = r.readUint64(end);
        break;
      case 2:
        r.expectWire(wt, WIRE_VARINT);
        out.terminalReplay = r.readBool(end);
        break;
      default:
        r.skipField(wt, end);
    }
  }
  return out;
}

export function encodeAttemptOpened(msg: AttemptOpenedInput): Uint8Array {
  return encodeWith(msg, encodedLenAttemptOpened, writeAttemptOpened);
}

export function decodeAttemptOpened(buf: Uint8Array): AttemptOpened {
  return decodeWith(buf, (r, end) => readAttemptOpenedInto(r, end));
}

// 47. InputData (`optional uint32 object_index = 5`)
export function encodedLenInputData(msg: InputDataInput): number {
  let len =
    uint64FieldLen(1, msg.sequence) +
    bytesFieldLen(2, msg.data) +
    boolFieldLen(3, msg.last) +
    stringMapFieldLen(4, msg.metadata);
  if (msg.objectIndex !== undefined) {
    len += tagLen(5) + varint32Len(msg.objectIndex >>> 0);
  }
  len +=
    stringFieldLen(6, msg.uri) +
    boolFieldLen(7, msg.parameterChunk) +
    boolFieldLen(8, msg.objectEnd);
  return len;
}

function writeInputData(w: ProtoWriter, msg: InputDataInput): void {
  w.writeUint64Field(1, msg.sequence);
  w.writeBytesField(2, msg.data);
  w.writeBoolField(3, msg.last);
  w.writeStringMapField(4, msg.metadata);
  if (msg.objectIndex !== undefined) {
    w.writeTag(5, WIRE_VARINT);
    w.writeVarint32(msg.objectIndex >>> 0);
  }
  w.writeStringField(6, msg.uri);
  w.writeBoolField(7, msg.parameterChunk);
  w.writeBoolField(8, msg.objectEnd);
}

function readInputDataInto(
  r: ProtoReader,
  end: number,
  out: InputData = {
    sequence: 0n,
    data: EMPTY_BYTES,
    last: false,
    metadata: {},
    uri: "",
    parameterChunk: false,
    objectEnd: false,
  },
): InputData {
  while (r.pos < end) {
    const [fn, wt] = r.readTag(end);
    switch (fn) {
      case 1:
        r.expectWire(wt, WIRE_VARINT);
        out.sequence = r.readUint64(end);
        break;
      case 2:
        r.expectWire(wt, WIRE_LEN);
        out.data = r.readBytes(end);
        break;
      case 3:
        r.expectWire(wt, WIRE_VARINT);
        out.last = r.readBool(end);
        break;
      case 4:
        r.expectWire(wt, WIRE_LEN);
        r.readStringMapEntry(end, out.metadata);
        break;
      case 5:
        r.expectWire(wt, WIRE_VARINT);
        out.objectIndex = r.readUint32(end);
        break;
      case 6:
        r.expectWire(wt, WIRE_LEN);
        out.uri = r.readString(end);
        break;
      case 7:
        r.expectWire(wt, WIRE_VARINT);
        out.parameterChunk = r.readBool(end);
        break;
      case 8:
        r.expectWire(wt, WIRE_VARINT);
        out.objectEnd = r.readBool(end);
        break;
      default:
        r.skipField(wt, end);
    }
  }
  return out;
}

export function encodeInputData(msg: InputDataInput): Uint8Array {
  return encodeWith(msg, encodedLenInputData, writeInputData);
}

export function decodeInputData(buf: Uint8Array): InputData {
  return decodeWith(buf, (r, end) => readInputDataInto(r, end));
}

// 48. Ack
export function encodedLenAck(msg: AckInput): number {
  return uint64FieldLen(1, msg.upToSequence) + uint64FieldLen(2, msg.resultBytes);
}

function writeAck(w: ProtoWriter, msg: AckInput): void {
  w.writeUint64Field(1, msg.upToSequence);
  w.writeUint64Field(2, msg.resultBytes);
}

function readAckInto(
  r: ProtoReader,
  end: number,
  out: Ack = { upToSequence: 0n, resultBytes: 0n },
): Ack {
  while (r.pos < end) {
    const [fn, wt] = r.readTag(end);
    switch (fn) {
      case 1:
        r.expectWire(wt, WIRE_VARINT);
        out.upToSequence = r.readUint64(end);
        break;
      case 2:
        r.expectWire(wt, WIRE_VARINT);
        out.resultBytes = r.readUint64(end);
        break;
      default:
        r.skipField(wt, end);
    }
  }
  return out;
}

export function encodeAck(msg: AckInput): Uint8Array {
  return encodeWith(msg, encodedLenAck, writeAck);
}

export function decodeAck(buf: Uint8Array): Ack {
  return decodeWith(buf, (r, end) => readAckInto(r, end));
}

// 49. AttemptFinished
export function encodedLenAttemptFinished(msg: AttemptFinishedInput): number {
  return enumFieldLen(1, msg.status) + stringFieldLen(2, msg.reason);
}

function writeAttemptFinished(w: ProtoWriter, msg: AttemptFinishedInput): void {
  w.writeEnumField(1, msg.status);
  w.writeStringField(2, msg.reason);
}

function readAttemptFinishedInto(
  r: ProtoReader,
  end: number,
  out: AttemptFinished = {
    status: AttemptFinishedStatus.STATUS_UNSPECIFIED,
    reason: "",
  },
): AttemptFinished {
  while (r.pos < end) {
    const [fn, wt] = r.readTag(end);
    switch (fn) {
      case 1:
        r.expectWire(wt, WIRE_VARINT);
        out.status = r.readEnum(end) as AttemptFinishedStatus;
        break;
      case 2:
        r.expectWire(wt, WIRE_LEN);
        out.reason = r.readString(end);
        break;
      default:
        r.skipField(wt, end);
    }
  }
  return out;
}

export function encodeAttemptFinished(msg: AttemptFinishedInput): Uint8Array {
  return encodeWith(msg, encodedLenAttemptFinished, writeAttemptFinished);
}

export function decodeAttemptFinished(buf: Uint8Array): AttemptFinished {
  return decodeWith(buf, (r, end) => readAttemptFinishedInto(r, end));
}

// 50. AttemptDown
export function encodedLenAttemptDown(msg: AttemptDownInput): number {
  if (!msg.msg) return 0;
  switch (msg.msg.case) {
    case "opened":
      return lenDelimitedTotal(1, encodedLenAttemptOpened(msg.msg.value));
    case "input":
      return lenDelimitedTotal(2, encodedLenInputData(msg.msg.value));
    case "ack":
      return lenDelimitedTotal(3, encodedLenAck(msg.msg.value));
    case "finished":
      return lenDelimitedTotal(4, encodedLenAttemptFinished(msg.msg.value));
  }
}

function writeAttemptDown(w: ProtoWriter, msg: AttemptDownInput): void {
  if (!msg.msg) return;
  switch (msg.msg.case) {
    case "opened": {
      const subLen = encodedLenAttemptOpened(msg.msg.value);
      w.writeTag(1, WIRE_LEN);
      w.writeVarint32(subLen);
      writeAttemptOpened(w, msg.msg.value);
      break;
    }
    case "input": {
      const subLen = encodedLenInputData(msg.msg.value);
      w.writeTag(2, WIRE_LEN);
      w.writeVarint32(subLen);
      writeInputData(w, msg.msg.value);
      break;
    }
    case "ack": {
      const subLen = encodedLenAck(msg.msg.value);
      w.writeTag(3, WIRE_LEN);
      w.writeVarint32(subLen);
      writeAck(w, msg.msg.value);
      break;
    }
    case "finished": {
      const subLen = encodedLenAttemptFinished(msg.msg.value);
      w.writeTag(4, WIRE_LEN);
      w.writeVarint32(subLen);
      writeAttemptFinished(w, msg.msg.value);
      break;
    }
  }
}

function readAttemptDownInto(
  r: ProtoReader,
  end: number,
  out: AttemptDown = {},
): AttemptDown {
  while (r.pos < end) {
    const [fn, wt] = r.readTag(end);
    switch (fn) {
      case 1:
        r.expectWire(wt, WIRE_LEN);
        out.msg = {
          case: "opened",
          value: readAttemptOpenedInto(
            r,
            r.readSubEnd(end),
            out.msg?.case === "opened" ? out.msg.value : undefined,
          ),
        };
        break;
      case 2:
        r.expectWire(wt, WIRE_LEN);
        out.msg = {
          case: "input",
          value: readInputDataInto(
            r,
            r.readSubEnd(end),
            out.msg?.case === "input" ? out.msg.value : undefined,
          ),
        };
        break;
      case 3:
        r.expectWire(wt, WIRE_LEN);
        out.msg = {
          case: "ack",
          value: readAckInto(
            r,
            r.readSubEnd(end),
            out.msg?.case === "ack" ? out.msg.value : undefined,
          ),
        };
        break;
      case 4:
        r.expectWire(wt, WIRE_LEN);
        out.msg = {
          case: "finished",
          value: readAttemptFinishedInto(
            r,
            r.readSubEnd(end),
            out.msg?.case === "finished" ? out.msg.value : undefined,
          ),
        };
        break;
      default:
        r.skipField(wt, end);
    }
  }
  return out;
}

export function encodeAttemptDown(msg: AttemptDownInput): Uint8Array {
  return encodeWith(msg, encodedLenAttemptDown, writeAttemptDown);
}

export function decodeAttemptDown(buf: Uint8Array): AttemptDown {
  return decodeWith(buf, (r, end) => readAttemptDownInto(r, end));
}

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
  if (msgBytes > 0) {
    utf8Encoder.encodeInto(message, out.subarray(2));
  }
  return out;
}

/** Decodes a downstream WebSocket binary frame (`0x00 || proto` or `0x01 || code || utf8`). */
export function decodeWsFrame(frame: Uint8Array): WsFrame {
  if (frame.byteLength < 1) {
    throw new ProtoDecodeError("empty WebSocket frame");
  }
  const tag = frame[0]!;
  if (tag === 0x00) {
    return { kind: "data", payload: frame.subarray(1) };
  }
  if (tag === 0x01) {
    if (frame.byteLength < 2) {
      throw new ProtoDecodeError("truncated WebSocket error frame");
    }
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
export function encodedLenExecutionEventWsFrame(
  event: ExecutionEventInput,
): number {
  return 1 + encodedLenExecutionEvent(event);
}

/**
 * Single-allocation pre-sized encoder for `0x00 || ExecutionEvent` WebSocket frames
 * (including `ExecutionEvent{Finished}` with multi-MiB or segmented `Uint8Array[]`
 * output payloads). Computes exact length first and writes directly into one
 * `Uint8Array` with no intermediate concatenation.
 */
export function encodeExecutionEventWsFrame(
  event: ExecutionEventInput,
): Uint8Array {
  const protoLen = encodedLenExecutionEvent(event);
  const w = new ProtoWriter(1 + protoLen);
  w.writeByte(0x00);
  writeExecutionEvent(w, event);
  return w.buf;
}

/** Exact byte length of `0x00 || ExecutionEvent{Finished}` frame. */
export function encodedLenExecutionFinishedWsFrame(
  input: ExecutionFinishedFrameInput,
): number {
  return encodedLenExecutionEventWsFrame({
    jobId: input.jobId,
    sequence: input.sequence,
    attemptId: input.attemptId,
    atMs: input.atMs,
    event: {
      case: "finished",
      value: {
        state: input.state,
        outputs: input.outputs,
        error: input.error,
      },
    },
  });
}

/**
 * Single-allocation pre-sized encoder specifically for `ExecutionEvent{Finished}`
 * WebSocket frames (`0x00 || ExecutionEvent`). Supports both contiguous `Uint8Array`
 * and segmented `readonly Uint8Array[]` output inline payloads without intermediate
 * copies.
 */
export function encodeExecutionFinishedWsFrame(
  input: ExecutionFinishedFrameInput,
): Uint8Array {
  return encodeExecutionEventWsFrame({
    jobId: input.jobId,
    sequence: input.sequence,
    attemptId: input.attemptId,
    atMs: input.atMs,
    event: {
      case: "finished",
      value: {
        state: input.state,
        outputs: input.outputs,
        error: input.error,
      },
    },
  });
}
