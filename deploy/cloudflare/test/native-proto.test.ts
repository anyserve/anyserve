import { inspect } from "node:util";
import { readFileSync } from "node:fs";
import { resolve, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { createHash } from "node:crypto";
import { describe, expect, it } from "vitest";

import {
  AttemptFinishedStatus,
  ConflictPolicy,
  Durability,
  JobState,
  ProtoDecodeError,
  decodeAccepted,
  decodeAck,
  decodeAssign,
  decodeAssignReply,
  decodeAssigned,
  decodeAttemptDown,
  decodeAttemptFinished,
  decodeAttemptOpen,
  decodeAttemptOpened,
  decodeAttemptRestarted,
  decodeAttemptUp,
  decodeCancel,
  decodeCancelAck,
  decodeCancelRequest,
  decodeCancelled,
  decodeCredit,
  decodeDemand,
  decodeDrain,
  decodeDraining,
  decodeErrorInfo,
  decodeExecutionEvent,
  decodeExecutionFinished,
  decodeExecutionPolicy,
  decodeFence,
  decodeFinish,
  decodeFrame,
  decodeGetJobRequest,
  decodeHeartbeat,
  decodeHello,
  decodeInputChunk,
  decodeInputData,
  decodeInputEnd,
  decodeInvokeRequest,
  decodeInvokeStart,
  decodeJob,
  decodeJobSpec,
  decodeLimits,
  decodeListJobsRequest,
  decodeListJobsResponse,
  decodeLoad,
  decodeObject,
  decodeOutput,
  decodeResultChunk,
  decodeServerMessage,
  decodeSubmitRequest,
  decodeSubmitResponse,
  decodeSucceeded,
  decodeWatchRequest,
  decodeWelcome,
  decodeWorkerMessage,
  decodeWsFrame,
  encodeAccepted,
  encodeAck,
  encodeAssign,
  encodeAssignReply,
  encodeAssigned,
  encodeAttemptDown,
  encodeAttemptFinished,
  encodeAttemptOpen,
  encodeAttemptOpened,
  encodeAttemptRestarted,
  encodeAttemptUp,
  encodeCancel,
  encodeCancelAck,
  encodeCancelRequest,
  encodeCancelled,
  encodeCredit,
  encodeDemand,
  encodeDrain,
  encodeDraining,
  encodeErrorInfo,
  encodeExecutionEvent,
  encodeExecutionEventWsFrame,
  encodeExecutionFinished,
  encodeExecutionFinishedWsFrame,
  encodeExecutionPolicy,
  encodeFence,
  encodeFinish,
  encodeFrame,
  encodeGetJobRequest,
  encodeHeartbeat,
  encodeHello,
  encodeInputChunk,
  encodeInputData,
  encodeInputEnd,
  encodeInvokeRequest,
  encodeInvokeStart,
  encodeJob,
  encodeJobSpec,
  encodeLimits,
  encodeListJobsRequest,
  encodeListJobsResponse,
  encodeLoad,
  encodeObject,
  encodeOutput,
  encodeResultChunk,
  encodeServerMessage,
  encodeSubmitRequest,
  encodeSubmitResponse,
  encodeSucceeded,
  encodeWatchRequest,
  encodeWelcome,
  encodeWorkerMessage,
  encodeWsDataFrame,
  encodeWsErrorFrame,
  encodedLenExecutionEventWsFrame,
  encodedLenExecutionFinishedWsFrame,
  encodedLenInputData,
  encodedLenSubmitRequest,
} from "../src/native/proto";

import {
  DEFAULT_NAMESPACE,
  IDENTITY_EXCLUDED_METADATA_KEYS,
  IncrementalSha256,
  OBJECT_OVERHEAD,
  OBJECT_SHA256_METADATA_KEY,
  OBJECT_SIZE_METADATA_KEY,
  SCOPED_PREFIX,
  bytesToHex,
  compareUtf8,
  createAsyncSha256Sink,
  decodeScopedId,
  encodeScopedId,
  outcomeDigest,
  outcomeDigestStream,
  resultObjectAccountedBytes,
  sha256Hex,
  sha256HexStream,
  sortMapByUtf8Bytes,
  specHash,
  specHashStream,
} from "../src/native/identity";

import {
  AuthPolicy,
  GRPC_CODE_HEADER,
  GrpcCode,
  GrpcStatusError,
  NAMESPACE_HEADER,
  Role,
  ScopedToken,
  constantTimeEqual,
  constantTimeEqualBytes,
  defaultPrincipal,
  grpcErrorResponse,
  httpStatusForGrpcCode,
  validateNamespace,
} from "../src/native/auth";

const __dirname = dirname(fileURLToPath(import.meta.url));

function fromHex(hex: string): Uint8Array {
  if (hex.length % 2 !== 0) throw new Error(`odd hex length: ${hex.length}`);
  const out = new Uint8Array(hex.length / 2);
  for (let i = 0; i < out.byteLength; i++) {
    out[i] = Number.parseInt(hex.slice(i * 2, i * 2 + 2), 16);
  }
  return out;
}

function concatBytes(...parts: Uint8Array[]): Uint8Array {
  const total = parts.reduce((acc, p) => acc + p.byteLength, 0);
  const out = new Uint8Array(total);
  let offset = 0;
  for (const p of parts) {
    out.set(p, offset);
    offset += p.byteLength;
  }
  return out;
}

const enc = new TextEncoder();

describe("native/proto.ts — zero-eval Protobuf v3 codec & WS framing", () => {
  it("contains no eval, Function constructor, cloudflare:workers, or node:* imports across native proto/identity/auth", () => {
    for (const rel of [
      "../src/native/proto.ts",
      "../src/native/identity.ts",
      "../src/native/auth.ts",
    ]) {
      const src = readFileSync(resolve(__dirname, rel), "utf8");
      expect(src).not.toMatch(/\beval\s*\(/);
      expect(src).not.toMatch(/\bnew\s+Function\s*\(/);
      expect(src).not.toMatch(/cloudflare:workers/);
      expect(src).not.toMatch(/from\s+["']node:/);
    }
  });

  it("matches Rust prost golden hex for InputData.objectIndex (0 vs positive vs undefined)", () => {
    // Derived from Rust `anyserve_proto::v2::InputData::encode_to_vec`:
    // 1) object_index = Some(0) => emits `28 00`!
    const idx0Bytes = encodeInputData({
      sequence: 1n,
      objectIndex: 0,
      objectEnd: true,
    });
    expect(bytesToHex(idx0Bytes)).toBe("080128004001");
    expect(encodedLenInputData({ sequence: 1n, objectIndex: 0, objectEnd: true })).toBe(
      idx0Bytes.byteLength,
    );
    const decodedIdx0 = decodeInputData(idx0Bytes);
    expect(decodedIdx0.objectIndex).toBe(0);
    expect(decodedIdx0.sequence).toBe(1n);
    expect(decodedIdx0.objectEnd).toBe(true);

    // 2) object_index = Some(7)
    const idx7Bytes = encodeInputData({
      sequence: 42n,
      data: enc.encode("chunk"),
      metadata: { k: "v" },
      objectIndex: 7,
      objectEnd: true,
    });
    expect(bytesToHex(idx7Bytes)).toBe(
      "082a12056368756e6b22060a016b12017628074001",
    );
    expect(decodeInputData(idx7Bytes)).toEqual({
      sequence: 42n,
      data: enc.encode("chunk"),
      last: false,
      metadata: { k: "v" },
      objectIndex: 7,
      uri: "",
      parameterChunk: false,
      objectEnd: true,
    });

    // 3) object_index = undefined (parameter chunk) => field 5 omitted!
    const noneBytes = encodeInputData({
      sequence: 2n,
      data: enc.encode("{}"),
      parameterChunk: true,
      objectEnd: true,
    });
    expect(bytesToHex(noneBytes)).toBe("080212027b7d38014001");
    const decodedNone = decodeInputData(noneBytes);
    expect(decodedNone.objectIndex).toBeUndefined();
    expect(decodedNone.parameterChunk).toBe(true);

    // 4) trailer with sequence > 2^53 and objectIndex undefined
    const trailerBytes = encodeInputData({
      sequence: 9007199254740995n,
      last: true,
    });
    expect(bytesToHex(trailerBytes)).toBe("0883808080808080101801");
    const decodedTrailer = decodeInputData(trailerBytes);
    expect(decodedTrailer.sequence).toBe(9007199254740995n);
    expect(decodedTrailer.last).toBe(true);
    expect(decodedTrailer.objectIndex).toBeUndefined();
  });

  it("matches Rust prost golden hex for >2^53 uint64 and negative/extreme 10-byte int64 values", () => {
    // Ack { up_to_sequence: 2^53 + 1, result_bytes: u64::MAX }
    const ackBytes = encodeAck({
      upToSequence: 9007199254740993n,
      resultBytes: 18446744073709551615n,
    });
    expect(bytesToHex(ackBytes)).toBe(
      "08818080808080801010ffffffffffffffffff01",
    );
    expect(decodeAck(ackBytes)).toEqual({
      upToSequence: 9007199254740993n,
      resultBytes: 18446744073709551615n,
    });

    // Demand with map<string, int64>: -1, i64::MIN, i64::MAX
    const dNeg1 = encodeDemand({
      interface: "gpu.train",
      requiredCapacity: { delta: -1n },
      affinityKey: "aff-1",
    });
    expect(bytesToHex(dNeg1)).toBe(
      "0a096770752e747261696e22120a0564656c746110ffffffffffffffffff012a056166662d31",
    );
    expect(decodeDemand(dNeg1).requiredCapacity).toEqual({ delta: -1n });

    const dMin = encodeDemand({
      interface: "gpu.train",
      requiredCapacity: { min: -9223372036854775808n },
    });
    expect(bytesToHex(dMin)).toBe(
      "0a096770752e747261696e22100a036d696e1080808080808080808001",
    );
    expect(decodeDemand(dMin).requiredCapacity).toEqual({
      min: -9223372036854775808n,
    });

    const dMax = encodeDemand({
      interface: "gpu.train",
      requiredCapacity: { max: 9223372036854775807n },
    });
    expect(bytesToHex(dMax)).toBe(
      "0a096770752e747261696e220f0a036d617810ffffffffffffffff7f",
    );
    expect(decodeDemand(dMax).requiredCapacity).toEqual({
      max: 9223372036854775807n,
    });

    // Also test -(2^53 + 1) and 0n inside map<string, int64>
    const round = decodeDemand(
      encodeDemand({
        requiredCapacity: {
          belowSafeInt: -9007199254740993n,
          zeroVal: 0n,
        },
      }),
    );
    expect(round.requiredCapacity).toEqual({
      belowSafeInt: -9007199254740993n,
      zeroVal: 0n,
    });

    // Truncates >u32::MAX varints to low 32 bits identically to prost (`prost_over_u32_ok: 5`)
    const overU32 = fromHex("088580808010");
    expect(decodeCredit(overU32).freeSlots).toBe(5);
  });

  it("matches Rust prost golden hex for IEEE-754 LE doubles in Load.utilization and Load.custom", () => {
    const loadBytes = encodeLoad({
      queued: 3,
      running: 5,
      utilization: 0.75,
      custom: { temp_c: -42.5 },
    });
    expect(bytesToHex(loadBytes)).toBe(
      "0803100519000000000000e83f22110a0674656d705f631100000000004045c0",
    );
    expect(decodeLoad(loadBytes)).toEqual({
      queued: 3,
      running: 5,
      utilization: 0.75,
      custom: { temp_c: -42.5 },
    });

    // Default 0.0 in map value omits field 2 inside MapEntry
    const zeroMapBytes = encodeLoad({
      utilization: 0,
      custom: { idle: 0 },
    });
    expect(bytesToHex(zeroMapBytes)).toBe("22060a0469646c65");
    expect(decodeLoad(zeroMapBytes)).toEqual({
      queued: 0,
      running: 0,
      utilization: 0,
      custom: { idle: 0 },
    });

    // Special IEEE-754 values (PI, Infinity, -Infinity, NaN, subnormals)
    const special = decodeLoad(
      encodeLoad({
        utilization: Math.PI,
        custom: {
          inf: Number.POSITIVE_INFINITY,
          ninf: Number.NEGATIVE_INFINITY,
          nan: Number.NaN,
          tiny: 1e-308,
        },
      }),
    );
    expect(special.utilization).toBe(Math.PI);
    expect(special.custom.inf).toBe(Number.POSITIVE_INFINITY);
    expect(special.custom.ninf).toBe(Number.NEGATIVE_INFINITY);
    expect(Number.isNaN(special.custom.nan)).toBe(true);
    expect(special.custom.tiny).toBe(1e-308);
  });

  it("matches Rust prost golden hex for AttemptUp oneofs (1=open, 2=frame, 3=result, 5=finish) and ExecutionEvent / ServerMessage / ListJobsRequest", () => {
    const upOpen = encodeAttemptUp({
      msg: {
        case: "open",
        value: {
          leaseId: "lease-1",
          generation: (1n << 54n) + 7n,
          resumeAfterSequence: 12n,
        },
      },
    });
    expect(bytesToHex(upOpen)).toBe(
      "0a140a076c656173652d31108780808080808020180c",
    );
    expect(decodeAttemptUp(upOpen)).toEqual({
      msg: {
        case: "open",
        value: {
          leaseId: "lease-1",
          generation: (1n << 54n) + 7n,
          resumeAfterSequence: 12n,
        },
      },
    });

    const upFrame = encodeAttemptUp({
      msg: {
        case: "frame",
        value: {
          sequence: 99n,
          data: new Uint8Array([0xde, 0xad, 0xbe, 0xef]),
          metadata: { m: "1" },
          progress: true,
        },
      },
    });
    expect(bytesToHex(upFrame)).toBe(
      "121208631204deadbeef1a060a016d1201312001",
    );

    const upResult = encodeAttemptUp({
      msg: {
        case: "result",
        value: {
          index: 0,
          last: true,
          uri: "r2://bucket/key",
        },
      },
    });
    expect(bytesToHex(upResult)).toBe(
      "1a1318012a0f72323a2f2f6275636b65742f6b6579",
    );

    // Field 5 (`0x2a` = (5 << 3) | 2) for finish!
    const upFinishOk = encodeAttemptUp({
      msg: { case: "finish", value: { outcome: { case: "succeeded" } } },
    });
    expect(bytesToHex(upFinishOk)).toBe("2a020a00");
    expect(decodeAttemptUp(upFinishOk)).toEqual({
      msg: {
        case: "finish",
        value: { outcome: { case: "succeeded", value: {} } },
      },
    });

    const upFinishFail = encodeAttemptUp({
      msg: {
        case: "finish",
        value: {
          outcome: {
            case: "failed",
            value: { code: "boom", message: "bad", retryable: true },
          },
        },
      },
    });
    expect(bytesToHex(upFinishFail)).toBe(
      "2a0f120d0a04626f6f6d12036261641801",
    );

    const upFinishCancel = encodeAttemptUp({
      msg: { case: "finish", value: { outcome: { case: "cancelled" } } },
    });
    expect(bytesToHex(upFinishCancel)).toBe("2a021a00");

    // ExecutionEvent Accepted (field 10 => 0x52 0x00) and Finished (field 14 => 0x72)
    const evAccepted = encodeExecutionEvent({
      jobId: "job-1",
      sequence: (3n << 32n) + 1n,
      atMs: 1_700_000_000_123n,
      event: { case: "accepted" },
    });
    expect(bytesToHex(evAccepted)).toBe(
      "0a056a6f622d3110818080803020fbd095ffbc315200",
    );
    expect(decodeExecutionEvent(evAccepted)).toEqual({
      jobId: "job-1",
      sequence: (3n << 32n) + 1n,
      attemptId: "",
      atMs: 1_700_000_000_123n,
      event: { case: "accepted", value: {} },
    });

    const evFinished = encodeExecutionEvent({
      jobId: "job-1",
      sequence: (3n << 32n) + 4n,
      attemptId: "att-1",
      atMs: 1_700_000_000_999n,
      event: {
        case: "finished",
        value: {
          state: JobState.SUCCEEDED,
          outputs: [
            { body: { case: "inline", value: new Uint8Array(0) } },
            {
              body: { case: "inline", value: enc.encode("result-payload") },
              metadata: { ct: "application/octet-stream" },
            },
            { body: { case: "uri", value: "s3://b/k" } },
          ],
        },
      },
    });
    expect(bytesToHex(evFinished)).toBe(
      "0a056a6f622d311084808080301a056174742d3120e7d795ffbc317244080312020a0012300a0e726573756c742d7061796c6f61641a1e0a02637412186170706c69636174696f6e2f6f637465742d73747265616d120a120873333a2f2f622f6b",
    );
    const decodedFinished = decodeExecutionEvent(evFinished);
    expect(decodedFinished.event?.case).toBe("finished");
    if (decodedFinished.event?.case === "finished") {
      expect(decodedFinished.event.value.outputs[0]).toEqual({
        body: { case: "inline", value: new Uint8Array(0) },
        metadata: {},
      });
      expect(decodedFinished.event.value.outputs[2]).toEqual({
        body: { case: "uri", value: "s3://b/k" },
        metadata: {},
      });
    }

    // ListJobsRequest packed enum field + unpacked compatibility
    const listPacked = encodeListJobsRequest({
      states: [
        JobState.QUEUED,
        JobState.RUNNING,
        JobState.SUCCEEDED,
        JobState.FAILED,
        JobState.CANCELLED,
      ],
      limit: 50,
      pageToken: "tok-next",
    });
    expect(bytesToHex(listPacked)).toBe(
      "0a05010203040510321a08746f6b2d6e657874",
    );
    expect(decodeListJobsRequest(listPacked)).toEqual({
      states: [
        JobState.QUEUED,
        JobState.RUNNING,
        JobState.SUCCEEDED,
        JobState.FAILED,
        JobState.CANCELLED,
      ],
      limit: 50,
      pageToken: "tok-next",
    });
    // Unpacked wire type 0 tags (`08 01 08 03`) also decode properly
    expect(decodeListJobsRequest(fromHex("08010803100a")).states).toEqual([
      JobState.QUEUED,
      JobState.SUCCEEDED,
    ]);

    // ServerMessage Welcome + Drain + AttemptDown Fenced
    const srvWelcome = encodeServerMessage({
      msg: {
        case: "welcome",
        value: {
          sessionId: "sess-42",
          generation: 9007199254740999n,
          heartbeatIntervalMs: 30_000,
          heartbeatTimeoutMs: 90_000,
          limits: {
            maxControlMessageBytes: 262_144,
            maxChunkBytes: 65_536,
            attemptWindowBytes: 4_194_304,
            assignInlineLimit: 65_536,
          },
          resumedLeaseIds: ["l1", "", "l2"],
        },
      },
    });
    expect(bytesToHex(srvWelcome)).toBe(
      "0a370a07736573732d343210878080808080801018b0ea012090bf052a11088080101080800418808080022080800432026c31320032026c32",
    );
    expect(decodeServerMessage(srvWelcome)).toEqual({
      msg: {
        case: "welcome",
        value: {
          sessionId: "sess-42",
          generation: 9007199254740999n,
          heartbeatIntervalMs: 30_000,
          heartbeatTimeoutMs: 90_000,
          limits: {
            maxControlMessageBytes: 262_144,
            maxChunkBytes: 65_536,
            attemptWindowBytes: 4_194_304,
            assignInlineLimit: 65_536,
          },
          resumedLeaseIds: ["l1", "", "l2"],
        },
      },
    });

    expect(bytesToHex(encodeServerMessage({ msg: { case: "drain" } }))).toBe(
      "2a00",
    );
    expect(
      bytesToHex(
        encodeAttemptDown({
          msg: {
            case: "finished",
            value: {
              status: AttemptFinishedStatus.FENCED,
              reason: "stale_generation",
            },
          },
        }),
      ),
    ).toBe("2214080212107374616c655f67656e65726174696f6e");
  });

  it("round-trips every remaining message type in execution.proto cleanly", () => {
    const policy = {
      durability: Durability.DURABLE,
      priority: 3,
      queueTimeoutMs: 5000n,
      executionTimeoutMs: 60000n,
      totalTimeoutMs: 180000n,
      spilloverAfterMs: 2500n,
      maxAttempts: 4,
      fairnessKey: "fk",
      concurrencyKey: "ck",
    };
    expect(decodeExecutionPolicy(encodeExecutionPolicy(policy))).toEqual(policy);

    const spec = {
      demand: {
        interface: "infer",
        requiredAttributes: { gpu: "h100" },
        preferredAttributes: { zone: "a" },
        requiredCapacity: { vram: 80000n, neg: -5n },
        affinityKey: "aff",
      },
      inputs: [
        { body: { case: "inline" as const, value: new Uint8Array([1, 2, 3]) }, metadata: { a: "b" } },
        { body: { case: "uri" as const, value: "" }, metadata: {} },
      ],
      params: new Uint8Array([9, 8]),
      policy,
      metadata: { tenant: "t1", ["__proto__"]: "safe" },
    };
    const subReq = {
      jobId: "j-100",
      conflictPolicy: ConflictPolicy.FAIL,
      spec,
    };
    const subReqBytes = encodeSubmitRequest(subReq);
    expect(encodedLenSubmitRequest(subReq)).toBe(subReqBytes.byteLength);
    const decodedSubReq = decodeSubmitRequest(subReqBytes);
    expect(decodedSubReq.jobId).toBe("j-100");
    expect(decodedSubReq.conflictPolicy).toBe(ConflictPolicy.FAIL);
    expect(decodedSubReq.spec?.metadata["__proto__"]).toBe("safe");
    expect(({} as Record<string, unknown>).safe).toBeUndefined();

    const job = {
      jobId: "j-100",
      state: JobState.FAILED,
      durability: Durability.EPHEMERAL,
      interface: "infer",
      attemptCount: 2,
      currentAttemptId: "a-2",
      currentWorkerId: "w-9",
      createdAtMs: 1000n,
      updatedAtMs: 2000n,
      error: { code: "err", message: "boom", retryable: false },
      outputs: [{ body: { case: "inline" as const, value: enc.encode("out") }, metadata: {} }],
      metadata: { m: "v" },
      specHash: "abcd",
      namespace: "team-a",
    };
    expect(decodeJob(encodeJob(job))).toEqual(job);
    expect(decodeSubmitResponse(encodeSubmitResponse({ job, created: true }))).toEqual({
      job,
      created: true,
    });
    expect(
      decodeListJobsResponse(
        encodeListJobsResponse({ jobs: [job], nextPageToken: "p2" }),
      ),
    ).toEqual({ jobs: [job], nextPageToken: "p2" });

    expect(decodeJobSpec(encodeJobSpec(spec)).demand?.interface).toBe("infer");
    expect(decodeErrorInfo(encodeErrorInfo(job.error))).toEqual(job.error);
    expect(decodeObject(encodeObject({}))).toEqual({ metadata: {} });

    // Invoke stream messages
    expect(
      decodeInvokeStart(encodeInvokeStart({ spec, streamingInput: true }))
        .streamingInput,
    ).toBe(true);
    expect(
      decodeInputChunk(
        encodeInputChunk({ data: new Uint8Array([7]), metadata: { c: "1" } }),
      ),
    ).toEqual({ data: new Uint8Array([7]), metadata: { c: "1" } });
    expect(decodeInputEnd(encodeInputEnd())).toEqual({});
    expect(
      decodeInvokeRequest(
        encodeInvokeRequest({
          msg: { case: "start", value: { streamingInput: true } },
        }),
      ),
    ).toEqual({
      msg: { case: "start", value: { streamingInput: true } },
    });
    expect(
      decodeInvokeRequest(
        encodeInvokeRequest({
          msg: { case: "input", value: { data: new Uint8Array([42]) } },
        }),
      ),
    ).toEqual({
      msg: {
        case: "input",
        value: { data: new Uint8Array([42]), metadata: {} },
      },
    });
    expect(
      decodeInvokeRequest(encodeInvokeRequest({ msg: { case: "inputEnd" } })),
    ).toEqual({ msg: { case: "inputEnd", value: {} } });

    // Event messages
    expect(decodeAccepted(encodeAccepted())).toEqual({});
    expect(
      decodeAssigned(encodeAssigned({ workerId: "w1", attemptNumber: 3 })),
    ).toEqual({ workerId: "w1", attemptNumber: 3 });
    expect(
      decodeOutput(
        encodeOutput({
          data: new Uint8Array([1]),
          metadata: { p: "1" },
          progress: true,
        }),
      ),
    ).toEqual({
      data: new Uint8Array([1]),
      metadata: { p: "1" },
      progress: true,
    });
    expect(
      decodeAttemptRestarted(
        encodeAttemptRestarted({
          previousAttemptId: "a1",
          reason: { code: "worker_lost", message: "gone", retryable: true },
        }),
      ),
    ).toEqual({
      previousAttemptId: "a1",
      reason: { code: "worker_lost", message: "gone", retryable: true },
    });
    expect(
      decodeExecutionFinished(
        encodeExecutionFinished({
          state: JobState.CANCELLED,
          error: { code: "cancelled", message: "by user", retryable: false },
        }),
      ),
    ).toEqual({
      state: JobState.CANCELLED,
      outputs: [],
      error: { code: "cancelled", message: "by user", retryable: false },
    });
    expect(
      decodeExecutionEvent(
        encodeExecutionEvent({
          jobId: "j",
          sequence: 2n,
          attemptId: "a1",
          atMs: 5n,
          event: {
            case: "assigned",
            value: { workerId: "w1", attemptNumber: 1 },
          },
        }),
      ).event,
    ).toEqual({
      case: "assigned",
      value: { workerId: "w1", attemptNumber: 1 },
    });
    expect(
      decodeExecutionEvent(
        encodeExecutionEvent({
          jobId: "j",
          sequence: 3n,
          attemptId: "a1",
          atMs: 6n,
          event: {
            case: "output",
            value: { data: new Uint8Array([5]), progress: false },
          },
        }),
      ).event,
    ).toEqual({
      case: "output",
      value: { data: new Uint8Array([5]), metadata: {}, progress: false },
    });
    expect(
      decodeExecutionEvent(
        encodeExecutionEvent({
          jobId: "j",
          sequence: 4n,
          attemptId: "a2",
          atMs: 7n,
          event: {
            case: "attemptRestarted",
            value: {
              previousAttemptId: "a1",
              reason: { code: "c", message: "m", retryable: true },
            },
          },
        }),
      ).event?.case,
    ).toBe("attemptRestarted");

    // Watch / GetJob / Cancel
    expect(
      decodeWatchRequest(
        encodeWatchRequest({ jobId: "j1", afterSequence: 9007199254740993n }),
      ),
    ).toEqual({ jobId: "j1", afterSequence: 9007199254740993n });
    expect(
      decodeGetJobRequest(
        encodeGetJobRequest({ jobId: "j1", includeOutputs: true }),
      ),
    ).toEqual({ jobId: "j1", includeOutputs: true });
    expect(
      decodeCancelRequest(
        encodeCancelRequest({ jobId: "j1", reason: "stop" }),
      ),
    ).toEqual({ jobId: "j1", reason: "stop" });

    // Worker control messages
    const hello = {
      workerId: "w-1",
      resumeSessionId: "s-0",
      interfaces: ["llm", "embed"],
      attributes: { region: "eu" },
      totalCapacity: { vram: 24000n },
      maxConcurrency: 8,
      sdkVersion: "0.2.0",
      heldLeaseIds: ["lease-a"],
      metadata: { host: "node-1" },
      pool: "spot",
    };
    expect(decodeHello(encodeHello(hello))).toEqual(hello);
    expect(
      decodeWorkerMessage(
        encodeWorkerMessage({ msg: { case: "hello", value: hello } }),
      ),
    ).toEqual({ msg: { case: "hello", value: hello } });
    expect(
      decodeWorkerMessage(
        encodeWorkerMessage({ msg: { case: "credit", value: { freeSlots: 4 } } }),
      ),
    ).toEqual({ msg: { case: "credit", value: { freeSlots: 4 } } });
    expect(
      decodeWorkerMessage(
        encodeWorkerMessage({
          msg: {
            case: "heartbeat",
            value: { load: { queued: 1, running: 2, utilization: 0.5, custom: { gpu: 0.9 } } },
          },
        }),
      ),
    ).toEqual({
      msg: {
        case: "heartbeat",
        value: { load: { queued: 1, running: 2, utilization: 0.5, custom: { gpu: 0.9 } } },
      },
    });
    expect(decodeHeartbeat(encodeHeartbeat({}))).toEqual({});
    const assignReply = { leaseId: "l-1", accepted: false, rejectReason: "busy" };
    expect(decodeAssignReply(encodeAssignReply(assignReply))).toEqual(assignReply);
    expect(
      decodeWorkerMessage(
        encodeWorkerMessage({ msg: { case: "assignReply", value: assignReply } }),
      ).msg?.case,
    ).toBe("assignReply");
    expect(decodeCancelAck(encodeCancelAck({ leaseId: "l-1" }))).toEqual({
      leaseId: "l-1",
    });
    expect(
      decodeWorkerMessage(
        encodeWorkerMessage({ msg: { case: "cancelAck", value: { leaseId: "l-1" } } }),
      ).msg?.case,
    ).toBe("cancelAck");
    expect(decodeDraining(encodeDraining({ preempting: true }))).toEqual({
      preempting: true,
    });
    expect(
      decodeWorkerMessage(
        encodeWorkerMessage({ msg: { case: "draining", value: { preempting: true } } }),
      ).msg?.case,
    ).toBe("draining");

    // Server messages
    expect(decodeLimits(encodeLimits({ maxChunkBytes: 65536 }))).toEqual({
      maxControlMessageBytes: 0,
      maxChunkBytes: 65536,
      attemptWindowBytes: 0,
      assignInlineLimit: 0,
    });
    expect(decodeWelcome(encodeWelcome({ sessionId: "s" })).sessionId).toBe("s");
    const assign = {
      leaseId: "l1",
      attemptId: "a1",
      jobId: "j1",
      attemptNumber: 1,
      generation: 7n,
      interface: "llm",
      params: new Uint8Array([1, 2]),
      reservedCapacity: { gpu: 1n },
      metadata: { k: "v" },
      inlineInputs: [{ body: { case: "inline" as const, value: new Uint8Array([3]) }, metadata: {} }],
      inputsViaAttempt: true,
      streamingInput: true,
      executionDeadlineMs: 9007199254740993n,
      totalDeadlineMs: 9007199254740999n,
    };
    expect(decodeAssign(encodeAssign(assign))).toEqual(assign);
    expect(
      decodeServerMessage(
        encodeServerMessage({ msg: { case: "assign", value: assign } }),
      ),
    ).toEqual({ msg: { case: "assign", value: assign } });
    expect(
      decodeServerMessage(
        encodeServerMessage({
          msg: { case: "cancel", value: { leaseId: "l1", reason: "timeout" } },
        }),
      ),
    ).toEqual({
      msg: { case: "cancel", value: { leaseId: "l1", reason: "timeout" } },
    });
    expect(decodeCancel(encodeCancel({ leaseId: "l1", reason: "r" }))).toEqual({
      leaseId: "l1",
      reason: "r",
    });
    expect(
      decodeServerMessage(
        encodeServerMessage({
          msg: { case: "fence", value: { leaseId: "l1", reason: "fenced" } },
        }),
      ),
    ).toEqual({
      msg: { case: "fence", value: { leaseId: "l1", reason: "fenced" } },
    });
    expect(decodeFence(encodeFence({ leaseId: "l1", reason: "f" }))).toEqual({
      leaseId: "l1",
      reason: "f",
    });
    expect(decodeDrain(encodeDrain())).toEqual({});

    // Attempt messages
    expect(
      decodeAttemptOpen(
        encodeAttemptOpen({
          leaseId: "l1",
          generation: 3n,
          resumeAfterSequence: 10n,
        }),
      ),
    ).toEqual({ leaseId: "l1", generation: 3n, resumeAfterSequence: 10n });
    expect(
      decodeFrame(
        encodeFrame({ sequence: 5n, data: new Uint8Array([9]), progress: true }),
      ).sequence,
    ).toBe(5n);
    expect(
      decodeResultChunk(
        encodeResultChunk({ index: 2, data: new Uint8Array([8]), last: true }),
      ).index,
    ).toBe(2);
    expect(decodeSucceeded(encodeSucceeded())).toEqual({});
    expect(decodeCancelled(encodeCancelled())).toEqual({});
    expect(
      decodeFinish(encodeFinish({ outcome: { case: "succeeded" } })),
    ).toEqual({ outcome: { case: "succeeded", value: {} } });
    expect(
      decodeAttemptOpened(
        encodeAttemptOpened({ resumeFromSequence: 4n, terminalReplay: true }),
      ),
    ).toEqual({ resumeFromSequence: 4n, terminalReplay: true });
    expect(
      decodeAttemptFinished(
        encodeAttemptFinished({
          status: AttemptFinishedStatus.COMMITTED,
          reason: "ok",
        }),
      ),
    ).toEqual({ status: AttemptFinishedStatus.COMMITTED, reason: "ok" });
    expect(
      decodeAttemptDown(
        encodeAttemptDown({
          msg: {
            case: "opened",
            value: { resumeFromSequence: 1n, terminalReplay: false },
          },
        }),
      ).msg?.case,
    ).toBe("opened");
    expect(
      decodeAttemptDown(
        encodeAttemptDown({
          msg: { case: "input", value: { sequence: 1n, objectIndex: 0 } },
        }),
      ),
    ).toEqual({
      msg: {
        case: "input",
        value: {
          sequence: 1n,
          data: new Uint8Array(0),
          last: false,
          metadata: {},
          objectIndex: 0,
          uri: "",
          parameterChunk: false,
          objectEnd: false,
        },
      },
    });
    expect(
      decodeAttemptDown(
        encodeAttemptDown({
          msg: { case: "ack", value: { upToSequence: 9n, resultBytes: 128n } },
        }),
      ).msg?.case,
    ).toBe("ack");
  });

  it("skips unknown fields of wire types 0, 1, 2, and 5, including field 4 gap on AttemptUp", () => {
    // Unknown fields:
    // field 20, wire type 0 (varint): tag (20<<3)|0 = 160 -> a0 01, val = 9999 -> 8f 4e
    // field 21, wire type 1 (64-bit): tag (21<<3)|1 = 169 -> a9 01, 8 bytes
    // field 22, wire type 2 (len-delimited): tag (22<<3)|2 = 178 -> b2 01, len 3, "xyz"
    // field 23, wire type 5 (32-bit): tag (23<<3)|5 = 189 -> bd 01, 4 bytes
    // AttemptUp field 4 (unused gap in AttemptUp oneof!), wire type 2: tag (4<<3)|2 = 0x22, len 2, aa bb
    const unknownBlock = fromHex(
      "a0018f4e" +
        "a9010102030405060708" +
        "b2010378797a" +
        "bd01deadbeef" +
        "2202aabb",
    );
    const realFinish = encodeAttemptUp({
      msg: { case: "finish", value: { outcome: { case: "succeeded" } } },
    });
    const combined = concatBytes(unknownBlock, realFinish, unknownBlock);
    expect(decodeAttemptUp(combined)).toEqual({
      msg: {
        case: "finish",
        value: { outcome: { case: "succeeded", value: {} } },
      },
    });

    // Oneof "last-wins" when two arms appear sequentially in wire stream
    const twoOneofs = concatBytes(
      encodeObject({ body: { case: "inline", value: new Uint8Array([1, 2]) } }),
      encodeObject({ body: { case: "uri", value: "r2://final" } }),
    );
    expect(decodeObject(twoOneofs)).toEqual({
      body: { case: "uri", value: "r2://final" },
      metadata: {},
    });
  });

  it("throws ProtoDecodeError on truncation, bad wire types, >10-byte varints, wrong field wire types, and invalid UTF-8", () => {
    const malformedCases: Array<[string, Uint8Array, (b: Uint8Array) => unknown]> = [
      // Truncated varints
      ["truncated tag varint", fromHex("80"), decodeWatchRequest],
      ["truncated value varint", fromHex("108080"), decodeWatchRequest],
      // >10-byte varint (10th byte has continuation bit 0x80)
      [
        "11-byte varint",
        fromHex("108080808080808080808001"),
        decodeWatchRequest,
      ],
      // 10th byte > 0x01 (>64-bit integer overflow)
      [
        "overflow 10th byte 0x02",
        fromHex("1080808080808080808002"),
        decodeWatchRequest,
      ],
      // Field number 0
      ["field number 0 varint", fromHex("0001"), decodeWatchRequest],
      ["field number 0 len", fromHex("0200"), decodeWatchRequest],
      // Bad wire types 3, 4, 6, 7
      ["wire type 3 (start group)", fromHex("0b"), decodeWatchRequest],
      ["wire type 4 (end group)", fromHex("0c"), decodeWatchRequest],
      ["wire type 6", fromHex("0e"), decodeWatchRequest],
      ["wire type 7", fromHex("0f"), decodeWatchRequest],
      // Known field with wrong wire type
      ["string field with varint wire type", fromHex("0801"), decodeWatchRequest],
      ["uint64 field with len wire type", fromHex("1200"), decodeWatchRequest],
      ["double field with varint wire type", fromHex("1801"), decodeLoad],
      // Truncated length-delimited field & huge varint length
      ["truncated string payload", fromHex("0a056162"), decodeWatchRequest],
      [
        "huge u64 length overflow",
        fromHex("0affffffffffffffffff01"),
        decodeWatchRequest,
      ],
      // Truncated fixed64 / fixed32
      ["truncated double (7 bytes)", fromHex("1901020304050607"), decodeLoad],
      [
        "truncated unknown 64-bit",
        fromHex("f901010203"),
        decodeWatchRequest,
      ],
      [
        "truncated unknown 32-bit",
        fromHex("fd010102"),
        decodeWatchRequest,
      ],
      // Invalid UTF-8 sequences (fatal TextDecoder)
      ["invalid UTF-8 0xff", fromHex("0a01ff"), decodeWatchRequest],
      [
        "invalid UTF-8 lone continuation",
        fromHex("0a0180"),
        decodeWatchRequest,
      ],
      [
        "invalid UTF-8 overlong slash",
        fromHex("0a02c0af"),
        decodeWatchRequest,
      ],
      [
        "invalid UTF-8 lone surrogate U+D800",
        fromHex("0a03eda080"),
        decodeWatchRequest,
      ],
      [
        "invalid UTF-8 truncated 4-byte emoji",
        fromHex("0a03f09fa6"),
        decodeWatchRequest,
      ],
    ];

    for (const [label, buf, fn] of malformedCases) {
      expect(() => fn(buf), label).toThrow(ProtoDecodeError);
    }
  });

  it("encodes/decodes WS frames and single-allocation pre-sized ExecutionEvent{Finished} frames over segmented multi-MiB outputs", () => {
    const payload = encodeCredit({ freeSlots: 16 });
    const dataFrame = encodeWsDataFrame(payload);
    expect(dataFrame[0]).toBe(0x00);
    expect(decodeWsFrame(dataFrame)).toEqual({ kind: "data", payload });

    const errFrame = encodeWsErrorFrame(GrpcCode.OutOfRange, "cursor expired: 🦀");
    expect(errFrame[0]).toBe(0x01);
    expect(errFrame[1]).toBe(11);
    expect(decodeWsFrame(errFrame)).toEqual({
      kind: "error",
      code: 11,
      message: "cursor expired: 🦀",
    });

    expect(() => decodeWsFrame(new Uint8Array(0))).toThrow(ProtoDecodeError);
    expect(() => decodeWsFrame(new Uint8Array([0x01]))).toThrow(ProtoDecodeError);
    expect(() => decodeWsFrame(new Uint8Array([0x02, 0x00]))).toThrow(
      ProtoDecodeError,
    );
    expect(() => decodeWsFrame(new Uint8Array([0x01, 0x03, 0xff]))).toThrow(
      ProtoDecodeError,
    );

    // Multi-MiB segmented ExecutionFinished WS frame (3 x 1.2 MiB segmented into 512 KiB chunks)
    const segA1 = new Uint8Array(512 * 1024).fill(0x11);
    const segA2 = new Uint8Array(512 * 1024).fill(0x22);
    const segA3 = new Uint8Array(200 * 1024).fill(0x33);
    const segB1 = new Uint8Array(600 * 1024).fill(0x44);
    const segB2 = new Uint8Array(600 * 1024).fill(0x55);

    const frameInput = {
      jobId: "big-finished-job",
      sequence: (5n << 32n) + 9n,
      attemptId: "attempt-77",
      atMs: 1_700_123_456_789n,
      state: JobState.SUCCEEDED,
      outputs: [
        {
          body: { case: "inline" as const, value: [segA1, segA2, segA3] },
          metadata: { idx: "0" },
        },
        {
          body: { case: "inline" as const, value: [segB1, segB2] },
          metadata: { idx: "1" },
        },
        {
          body: { case: "uri" as const, value: "s3://bucket/external" },
          metadata: { idx: "2" },
        },
      ],
    };

    const expectedLen = encodedLenExecutionFinishedWsFrame(frameInput);
    const wsFrame = encodeExecutionFinishedWsFrame(frameInput);
    expect(wsFrame.byteLength).toBe(expectedLen);
    expect(wsFrame.buffer.byteLength).toBe(expectedLen); // exact single-buffer sizing, no excess backing ArrayBuffer!
    expect(wsFrame[0]).toBe(0x00);

    // Compare against reference `encodeWsDataFrame(encodeExecutionEvent(...))` with concatenated buffers
    const refFrame = encodeWsDataFrame(
      encodeExecutionEvent({
        jobId: frameInput.jobId,
        sequence: frameInput.sequence,
        attemptId: frameInput.attemptId,
        atMs: frameInput.atMs,
        event: {
          case: "finished",
          value: {
            state: JobState.SUCCEEDED,
            outputs: [
              {
                body: { case: "inline", value: concatBytes(segA1, segA2, segA3) },
                metadata: { idx: "0" },
              },
              {
                body: { case: "inline", value: concatBytes(segB1, segB2) },
                metadata: { idx: "1" },
              },
              {
                body: { case: "uri", value: "s3://bucket/external" },
                metadata: { idx: "2" },
              },
            ],
          },
        },
      }),
    );
    expect(wsFrame.byteLength).toBe(refFrame.byteLength);
    expect(Buffer.compare(wsFrame, refFrame)).toBe(0);

    // Also check encodeExecutionEventWsFrame directly
    const genericEvFrame = encodeExecutionEventWsFrame({
      jobId: "j",
      sequence: 1n,
      event: { case: "accepted" },
    });
    expect(genericEvFrame.byteLength).toBe(
      encodedLenExecutionEventWsFrame({
        jobId: "j",
        sequence: 1n,
        event: { case: "accepted" },
      }),
    );
    expect(decodeExecutionEvent(genericEvFrame.subarray(1)).event?.case).toBe(
      "accepted",
    );
  });
});

describe("native/identity.ts — byte-exact parity with crates/anyserve-core/src/runtime/identity.rs", () => {
  it("exports matching constants and resultObjectAccountedBytes", () => {
    expect(DEFAULT_NAMESPACE).toBe("default");
    expect(SCOPED_PREFIX).toBe("@ns:");
    expect(OBJECT_OVERHEAD).toBe(64);
    expect(IDENTITY_EXCLUDED_METADATA_KEYS).toEqual([
      "traceparent",
      "tracestate",
      "baggage",
      "x-request-id",
    ]);
    expect(OBJECT_SHA256_METADATA_KEY).toBe("anyserve.sha256");
    expect(OBJECT_SIZE_METADATA_KEY).toBe("anyserve.size_bytes");
    expect(resultObjectAccountedBytes({})).toBe(64n);
    expect(
      resultObjectAccountedBytes({
        metadata: { a: "bc", "🦀": "x" }, // 1+2 + 4+1 = 8 UTF-8 bytes
        uri: "s3://b/k", // 8 bytes
        dataByteLength: 100,
      }),
    ).toBe(64n + 8n + 8n + 100n);
  });

  it("enforces bijective encodeScopedId / decodeScopedId including ('default', '@ns:team:j') != ('team', 'j')", () => {
    expect(encodeScopedId("default", "job-1")).toBe("job-1");
    expect(decodeScopedId("job-1")).toEqual(["default", "job-1"]);

    const escapedDefault = encodeScopedId("default", "@ns:team:j");
    const realTeam = encodeScopedId("team", "j");
    expect(escapedDefault).toBe("@ns:default:@ns:team:j");
    expect(realTeam).toBe("@ns:team:j");
    expect(escapedDefault).not.toBe(realTeam);

    expect(decodeScopedId(escapedDefault)).toEqual(["default", "@ns:team:j"]);
    expect(decodeScopedId(realTeam)).toEqual(["team", "j"]);

    const pairs: Array<[string, string]> = [
      ["default", "plain"],
      ["default", "@ns:weird:id"],
      ["default", "@ns:default:@ns:team:j"],
      ["default", "@ns:nocolon"],
      ["robot-a", "job-1"],
      ["robot-b", "job-1"],
      ["tenant.prod_1-a", "part1:part2:part3"],
      ["team", ""],
    ];
    for (const [ns, raw] of pairs) {
      const encoded = encodeScopedId(ns, raw);
      expect(decodeScopedId(encoded)).toEqual([ns, raw]);
    }
    // Malformed bare "@ns:nocolon" without a colon falls back to default namespace
    expect(decodeScopedId("@ns:nocolon")).toEqual(["default", "@ns:nocolon"]);
  });

  it("sorts map keys in UTF-8 BYTE order rather than JS UTF-16 code-unit order (astral vs BMP U+E000..U+FFFF)", () => {
    const keys = ["🦀", "￥", "𐀀", "\uE000", "é", "z"];
    // Prove JS UTF-16 `.sort()` gets this WRONG:
    const utf16Order = [...keys].sort();
    expect(utf16Order).toEqual(["z", "é", "𐀀", "🦀", "\uE000", "￥"]);

    // Whereas UTF-8 byte order (Rust `BTreeMap<String, _>`) puts 3-byte BMP E000/FFE5 BEFORE 4-byte astral 10000/1F980:
    const utf8Order = [...keys].sort(compareUtf8);
    expect(utf8Order).toEqual(["z", "é", "\uE000", "￥", "𐀀", "🦀"]);
    expect(
      sortMapByUtf8Bytes({
        "🦀": 1,
        "￥": 2,
        "𐀀": 3,
        "\uE000": 4,
        é: 5,
        z: 6,
      }).map((e) => e.key),
    ).toEqual(["z", "é", "\uE000", "￥", "𐀀", "🦀"]);

    // Exact Rust `identity.rs::spec_hash` golden SHA-256 on astral + BMP high keys!
    const astralSpec = {
      demand: {
        interface: "unicode.order",
        requiredAttributes: {
          "🦀": "crab",
          "￥": "yen",
          "𐀀": "linear-b",
          "\uE000": "pua",
          é: "acute",
          z: "ascii",
        },
        requiredCapacity: {
          "🦀-cap": 10n,
          "￥-cap": 20n,
        },
      },
      metadata: {
        "🦀-meta": "1",
        "￥-meta": "2",
        traceparent: "ignored",
      },
    };
    expect(specHash(astralSpec, Durability.DURABLE)).toBe(
      "f6bd6769d48e50d309947ecb29196896a421d3e8bd0a2df1773afbe38d231e32",
    );

    // Reversed insertion order produces the exact same digest
    const reversedSpec = {
      demand: {
        interface: "unicode.order",
        requiredAttributes: {
          z: "ascii",
          é: "acute",
          "\uE000": "pua",
          "𐀀": "linear-b",
          "￥": "yen",
          "🦀": "crab",
        },
        requiredCapacity: {
          "￥-cap": 20n,
          "🦀-cap": 10n,
        },
      },
      metadata: {
        "￥-meta": "2",
        "🦀-meta": "1",
      },
    };
    expect(specHash(reversedSpec, "durable")).toBe(
      "f6bd6769d48e50d309947ecb29196896a421d3e8bd0a2df1773afbe38d231e32",
    );
  });

  it("matches Rust identity.rs golden SHA-256 literals for specHash, scheduling-hint exclusion, and URI digest rules", () => {
    const baseSpec = {
      demand: {
        interface: "vision.segment",
        requiredAttributes: { arch: "arm64", accel: "cuda" },
        preferredAttributes: { zone: "us-east" },
        requiredCapacity: { vram_mb: 16384n, cpu_milli: 4000n },
        affinityKey: "affinity-hint-1",
      },
      inputs: [
        {
          body: { case: "inline" as const, value: enc.encode("hello-world") },
          metadata: { mime: "text/plain" },
        },
        {
          body: { case: "inline" as const, value: new Uint8Array(0) },
          metadata: {},
        },
      ],
      params: enc.encode('{"top_k":5}'),
      policy: {
        durability: Durability.DURABLE,
        priority: 2,
        queueTimeoutMs: 5_000n,
        executionTimeoutMs: 30_000n,
        totalTimeoutMs: 120_000n,
        spilloverAfterMs: 1_500n,
        maxAttempts: 4,
        fairnessKey: "other-fairness",
        concurrencyKey: "other-concurrency",
      },
      metadata: {
        tenant: "acme",
        traceparent: "different-trace",
        tracestate: "different-state",
        baggage: "different-baggage",
        "x-request-id": "different-req",
      },
    };

    expect(specHash(baseSpec, Durability.DURABLE)).toBe(
      "b1c7c08dd5bb779fc14582b95ac897faa84a806a8924531445cff3c85fe20f9a",
    );
    expect(specHash(baseSpec, Durability.EPHEMERAL)).toBe(
      "302ea114a0bac9e9ab600c6af7df9ef995d4eb1685e096871b9d5e52846785a5",
    );

    // URI with anyserve.sha256 + anyserve.size_bytes ignores transient query string
    const uriShaSizeA = {
      demand: { interface: "uri.test" },
      inputs: [
        {
          body: {
            case: "uri" as const,
            value: "https://r2.example.com/obj?sig=transient1",
          },
          metadata: {
            "anyserve.sha256":
              "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            "anyserve.size_bytes": "987654",
            "￥": "yen",
            "🦀": "crab",
          },
        },
      ],
    };
    const uriShaSizeB = {
      demand: { interface: "uri.test" },
      inputs: [
        {
          body: {
            case: "uri" as const,
            value: "https://completely-different-host.example/path?X-Amz-Signature=999",
          },
          metadata: uriShaSizeA.inputs[0]!.metadata,
        },
      ],
    };
    expect(specHash(uriShaSizeA, Durability.DURABLE)).toBe(
      "a68057a3c8ade1d8727deb2bcc9f41551c69bc8ce4ed83bfdb979edd3bd0c3f4",
    );
    expect(specHash(uriShaSizeB, Durability.DURABLE)).toBe(
      "a68057a3c8ade1d8727deb2bcc9f41551c69bc8ce4ed83bfdb979edd3bd0c3f4",
    );

    // URI with anyserve.sha256 and NO anyserve.size_bytes
    const uriShaNoSize = {
      demand: { interface: "uri.test" },
      inputs: [
        {
          body: {
            case: "uri" as const,
            value: "https://r2.example.com/obj?sig=any",
          },
          metadata: {
            "anyserve.sha256":
              "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
            mime: "image/png",
          },
        },
      ],
    };
    expect(specHash(uriShaNoSize, Durability.DURABLE)).toBe(
      "37859800146800c22fc35e0b2f1031ea83787af1f680e608140cad8b58fb5d0e",
    );

    // URI with empty anyserve.sha256 ("") falls back to hashing the full URI + metadata
    const uriEmptySha = {
      demand: { interface: "uri.test" },
      inputs: [
        {
          body: { case: "uri" as const, value: "s3://bucket/fixed-key" },
          metadata: {
            "anyserve.sha256": "",
            "anyserve.size_bytes": "42",
          },
        },
      ],
    };
    expect(specHash(uriEmptySha, Durability.DURABLE)).toBe(
      "a6a43e618935f16a9fe633bf9e1feb5cb1aa363873bee0198df1a4d41b889b3b",
    );
  });

  it("matches Rust identity.rs golden SHA-256 literals for outcomeDigest and streaming chunked objects (Node fallback + DigestStream)", async () => {
    expect(outcomeDigest([], null)).toBe(
      "d2daed68e729d68edce00908ed2c0490773dbf65698b9112c5b3ef63f3c51f8d",
    );

    const mixedOutputs = [
      {
        body: { case: "inline" as const, value: enc.encode("out-0") },
        metadata: { "🦀": "v1", "￥": "v2" },
      },
      {
        body: {
          case: "uri" as const,
          value: "https://cdn.example/x?token=other-transient",
        },
        metadata: {
          "anyserve.sha256": "cafebabe",
          "anyserve.size_bytes": "4096",
        },
      },
    ];
    expect(outcomeDigest(mixedOutputs)).toBe(
      "c6b21c63f5ae406ee1c17e96732e6a90c71b1c1ff8c0982db9da4dfde2572e30",
    );

    expect(
      outcomeDigest(mixedOutputs, {
        code: "worker_failed",
        message: "OOM on device 0",
        retryable: false,
      }),
    ).toBe("166099f8aef0f044e9a882a82a9d08d1d6c0a90f717e2ce42ccf8f2f01c85506");

    expect(
      outcomeDigest([], {
        code: "execution_timeout",
        message: "exceeded 30s",
        retryable: true,
      }),
    ).toBe("3b457ef361afa443e6d047cebdc922d7d8ec6b5b96a26e5d2aa680de19f0ab9c");

    // 200 KiB deterministic payload split into irregular odd chunk sizes (1, 7, 63, 64, 65, 4099, ...)
    const totalLen = 200 * 1024;
    const full = new Uint8Array(totalLen);
    for (let i = 0; i < totalLen; i++) {
      full[i] = (Math.imul(i, 31) + 17) & 0xff;
    }
    // Verify IncrementalSha256 against Node's OpenSSL crypto.createHash across 64-byte block boundaries
    const nodeSha = createHash("sha256").update(full).digest("hex");
    expect(sha256Hex(full)).toBe(nodeSha);

    const irregularSizes = [1, 7, 55, 1, 63, 64, 65, 127, 128, 129, 4099, 16384];
    const chunks: Uint8Array[] = [];
    let offset = 0;
    let stepIdx = 0;
    while (offset < totalLen) {
      const sz = Math.min(
        irregularSizes[stepIdx % irregularSizes.length]!,
        totalLen - offset,
      );
      chunks.push(full.subarray(offset, offset + sz));
      offset += sz;
      stepIdx++;
    }
    const inc = new IncrementalSha256();
    for (const c of chunks) inc.update(c);
    expect(inc.digestHex()).toBe(nodeSha);
    expect(await sha256HexStream(chunks)).toBe(nodeSha);

    const EXPECTED_RUST_200KIB_OUTCOME =
      "215c13c5f75f4f22b025787baa71b91974bc573ab39a90c586bd821b2c9e1545";

    // Sync chunked array
    expect(
      outcomeDigest([
        {
          kind: "inline_chunks",
          byteLength: totalLen,
          chunks,
          metadata: { part: "full" },
        },
      ]),
    ).toBe(EXPECTED_RUST_200KIB_OUTCOME);

    // Async streaming via Node incremental fallback (ReadableStream)
    const readable = new ReadableStream<Uint8Array>({
      start(controller) {
        for (const c of chunks) controller.enqueue(c);
        controller.close();
      },
    });
    const fallbackSink = createAsyncSha256Sink();
    expect(fallbackSink.usesDigestStream).toBe(false);
    expect(
      await outcomeDigestStream([
        {
          kind: "inline_stream",
          byteLength: totalLen,
          chunks: readable,
          metadata: { part: "full" },
        },
      ]),
    ).toBe(EXPECTED_RUST_200KIB_OUTCOME);

    // Simulate Cloudflare workerd's `crypto.DigestStream` on globalThis.crypto!
    const cryptoObj = globalThis.crypto as unknown as Record<string, unknown>;
    const prevDigestStream = cryptoObj.DigestStream;
    class MockWorkerdDigestStream extends WritableStream<BufferSource> {
      readonly digest: Promise<ArrayBuffer>;
      constructor(algo: string) {
        expect(algo).toBe("SHA-256");
        const hasher = createHash("sha256");
        let resolveDigest!: (buf: ArrayBuffer) => void;
        const p = new Promise<ArrayBuffer>((res) => {
          resolveDigest = res;
        });
        super({
          write(chunk) {
            const view = ArrayBuffer.isView(chunk)
              ? new Uint8Array(chunk.buffer, chunk.byteOffset, chunk.byteLength)
              : new Uint8Array(chunk);
            hasher.update(view);
          },
          close() {
            const d = hasher.digest();
            resolveDigest(
              d.buffer.slice(d.byteOffset, d.byteOffset + d.byteLength),
            );
          },
        });
        this.digest = p;
      }
    }
    try {
      cryptoObj.DigestStream = MockWorkerdDigestStream;
      const dsSink = createAsyncSha256Sink();
      expect(dsSink.usesDigestStream).toBe(true);

      async function* asyncGen() {
        for (const c of chunks) yield c;
      }
      expect(
        await outcomeDigestStream([
          {
            kind: "inline_stream",
            byteLength: totalLen,
            chunks: asyncGen(),
            metadata: { part: "full" },
          },
        ]),
      ).toBe(EXPECTED_RUST_200KIB_OUTCOME);

      // Also verify specHashStream via DigestStream matches specHash
      const streamSpecHash = await specHashStream(
        {
          demand: { interface: "stream.job" },
          inputs: [
            {
              kind: "inline_stream",
              byteLength: totalLen,
              chunks,
              metadata: { "🦀": "1", "￥": "2" },
            },
          ],
          params: {
            byteLength: 4,
            chunks: [new Uint8Array([1, 2]), new Uint8Array([3, 4])],
          },
        },
        Durability.DURABLE,
      );
      const syncEquivalent = specHash(
        {
          demand: { interface: "stream.job" },
          inputs: [
            {
              body: { case: "inline", value: full },
              metadata: { "🦀": "1", "￥": "2" },
            },
          ],
          params: new Uint8Array([1, 2, 3, 4]),
        },
        Durability.DURABLE,
      );
      expect(streamSpecHash).toBe(syncEquivalent);
    } finally {
      if (prevDigestStream === undefined) {
        delete cryptoObj.DigestStream;
      } else {
        cryptoObj.DigestStream = prevDigestStream;
      }
    }

    // Mismatched declared byteLength throws
    await expect(
      outcomeDigestStream([
        {
          kind: "inline_stream",
          byteLength: totalLen + 1,
          chunks,
        },
      ]),
    ).rejects.toThrow(/length mismatch/);
  });
});

describe("native/auth.ts — parity with auth.rs + http_ws_service.rs, matrix & redaction", () => {
  it("maps every GrpcCode to HTTP status and formats grpcErrorResponse with decimal x-anyserve-grpc-code", async () => {
    const expectedStatuses: Array<[GrpcCode, number]> = [
      [GrpcCode.InvalidArgument, 400],
      [GrpcCode.OutOfRange, 400],
      [GrpcCode.Unauthenticated, 401],
      [GrpcCode.PermissionDenied, 403],
      [GrpcCode.NotFound, 404],
      [GrpcCode.AlreadyExists, 409],
      [GrpcCode.Aborted, 409],
      [GrpcCode.FailedPrecondition, 412],
      [GrpcCode.ResourceExhausted, 429],
      [GrpcCode.Unimplemented, 501],
      [GrpcCode.Unavailable, 503],
      [GrpcCode.DeadlineExceeded, 504],
      [GrpcCode.Internal, 500],
      [GrpcCode.Unknown, 500],
      [GrpcCode.Cancelled, 500],
      [GrpcCode.DataLoss, 500],
    ];
    for (const [code, http] of expectedStatuses) {
      expect(httpStatusForGrpcCode(code)).toBe(http);
    }

    const err = GrpcStatusError.unauthenticated("missing or invalid bearer token");
    const resp = grpcErrorResponse(err, { "access-control-allow-origin": "*" });
    expect(resp.status).toBe(401);
    expect(resp.headers.get(GRPC_CODE_HEADER)).toBe("16");
    expect(resp.headers.get("content-type")).toBe("text/plain; charset=utf-8");
    expect(resp.headers.get("access-control-allow-origin")).toBe("*");
    expect(await resp.text()).toBe("missing or invalid bearer token");

    const resp2 = grpcErrorResponse(GrpcCode.ResourceExhausted, "daily row quota reached");
    expect(resp2.status).toBe(429);
    expect(resp2.headers.get(GRPC_CODE_HEADER)).toBe("8");
    expect(await resp2.text()).toBe("daily row quota reached");
  });

  it("validates namespaces (1..=64 ASCII [a-zA-Z0-9._-]) and rejects invalid ones with 400/3", () => {
    expect(validateNamespace("default")).toBe("default");
    expect(validateNamespace("A")).toBe("A");
    expect(validateNamespace("tenant.prod_01-eu")).toBe("tenant.prod_01-eu");
    const max64 = "a".repeat(64);
    expect(validateNamespace(max64)).toBe(max64);

    for (const bad of [
      "",
      "a".repeat(65),
      "bad/ns",
      "team:sub",
      "with space",
      "ns@1",
      "🦀",
      "line\nbreak",
    ]) {
      try {
        validateNamespace(bad);
        expect.fail(`expected ${JSON.stringify(bad)} to fail`);
      } catch (e) {
        expect(e).toBeInstanceOf(GrpcStatusError);
        const ge = e as GrpcStatusError;
        expect(ge.code).toBe(GrpcCode.InvalidArgument);
        expect(ge.httpStatus).toBe(400);
      }
    }
  });

  it("performs constant-time comparison without early exit across equal, unequal, prefix, and empty inputs", () => {
    expect(constantTimeEqual("", "")).toBe(true);
    expect(constantTimeEqual("tok-abc-123", "tok-abc-123")).toBe(true);
    expect(constantTimeEqual("tok-abc-123", "tok-abc-124")).toBe(false);
    expect(constantTimeEqual("tok-abc", "tok-abc-123")).toBe(false);
    expect(constantTimeEqual("tok-abc-123", "tok-abc")).toBe(false);
    expect(constantTimeEqual("🦀-secret", "🦀-secret")).toBe(true);
    expect(constantTimeEqual("🦀-secret", "￥-secret")).toBe(false);
    expect(
      constantTimeEqualBytes(new Uint8Array([0, 1, 2]), new Uint8Array([0, 1])),
    ).toBe(false);
  });

  it("supports anonymous mode when no tokens are configured, honouring x-anyserve-namespace and 400/3 on bad namespaces", () => {
    const anon = AuthPolicy.fromEnv({
      ANYSERVE_CLIENT_TOKENS: "  ,  ",
      ANYSERVE_WORKER_TOKENS: "",
      ANYSERVE_SCOPED_TOKENS: "   ",
    });
    expect(anon.isEnabled()).toBe(false);
    expect(defaultPrincipal()).toEqual({
      namespace: "default",
      role: Role.Client,
      subject: "anonymous",
      forcedPool: null,
      isOperator: true,
    });

    expect(anon.verifyClientHeaders(new Headers())).toEqual({
      namespace: "default",
      role: Role.Client,
      subject: "anonymous",
      forcedPool: null,
      isOperator: true,
    });

    expect(
      anon.verifyClientHeaders(
        new Headers({ [NAMESPACE_HEADER]: "  edge-robot-7  " }),
      ),
    ).toEqual({
      namespace: "edge-robot-7",
      role: Role.Client,
      subject: "anonymous",
      forcedPool: null,
      isOperator: true,
    });

    expect(
      anon.verifyWorkerHeaders({ [NAMESPACE_HEADER]: "edge-team" }),
    ).toEqual({
      namespace: "edge-team",
      role: Role.Worker,
      subject: "anonymous",
      forcedPool: null,
      isOperator: false,
    });

    expect(
      anon.authorizeOperatorHeaders({ [NAMESPACE_HEADER]: "ops-ns" }),
    ).toEqual({
      namespace: "ops-ns",
      role: Role.Client,
      subject: "anonymous",
      forcedPool: null,
      isOperator: true,
    });

    // Bad x-anyserve-namespace in anonymous mode => 400 / GrpcCode.InvalidArgument (3)
    for (const fn of [
      () => anon.verifyClientHeaders({ [NAMESPACE_HEADER]: "bad/ns" }),
      () => anon.verifyWorkerHeaders({ [NAMESPACE_HEADER]: "bad/ns" }),
      () => anon.authorizeOperatorHeaders({ [NAMESPACE_HEADER]: "bad/ns" }),
    ]) {
      try {
        fn();
        expect.fail("expected 400/3");
      } catch (e) {
        const ge = e as GrpcStatusError;
        expect(ge.code).toBe(GrpcCode.InvalidArgument);
        expect(ge.httpStatus).toBe(400);
      }
    }
  });

  it("enforces full auth / role / namespace / forcedPool / operator matrix (401/16, 403/7)", () => {
    const policy = AuthPolicy.fromEnv({
      ANYSERVE_CLIENT_TOKENS: " op-client-1 , op-client-2 ",
      ANYSERVE_WORKER_TOKENS: " global-worker-1 ",
      ANYSERVE_SCOPED_TOKENS: JSON.stringify([
        {
          token: " scoped-client-a ",
          role: "client",
          namespace: "tenant-a",
          subject: "svc-a",
        },
        {
          token: "scoped-client-default",
          role: "CLIENT",
          namespace: "default",
        },
        {
          token: "scoped-worker-factory",
          role: "worker",
          namespace: "factory-a",
          subject: "jetson-1",
          forced_pool: "jetson-local",
        },
      ]),
    });
    expect(policy.isEnabled()).toBe(true);

    // 1. Operator client token: accepted for client + operator, rejected for worker (403/7), ignores spoofed namespace header
    const opHeaders = new Headers({
      Authorization: "Bearer op-client-1",
      [NAMESPACE_HEADER]: "spoofed-ns",
    });
    expect(policy.verifyClientHeaders(opHeaders)).toEqual({
      namespace: "default",
      role: Role.Client,
      subject: "client-token",
      forcedPool: null,
      isOperator: true,
    });
    expect(policy.authorizeOperatorHeaders(opHeaders)).toEqual({
      namespace: "default",
      role: Role.Client,
      subject: "client-token",
      forcedPool: null,
      isOperator: true,
    });
    expect(() => policy.verifyWorkerHeaders(opHeaders)).toThrowError(
      expect.objectContaining({
        code: GrpcCode.PermissionDenied,
        httpStatus: 403,
      }),
    );

    // 2. Global worker token (lowercase `bearer ` prefix): accepted for worker, 403/7 for client & operator
    const workerHeaders = { authorization: "bearer global-worker-1" };
    expect(policy.verifyWorkerHeaders(workerHeaders)).toEqual({
      namespace: "default",
      role: Role.Worker,
      subject: "worker-token",
      forcedPool: null,
      isOperator: false,
    });
    expect(() => policy.verifyClientHeaders(workerHeaders)).toThrowError(
      expect.objectContaining({
        code: GrpcCode.PermissionDenied,
        httpStatus: 403,
      }),
    );
    expect(() => policy.authorizeOperatorHeaders(workerHeaders)).toThrowError(
      expect.objectContaining({
        code: GrpcCode.PermissionDenied,
        httpStatus: 403,
      }),
    );

    // 3. Scoped client token: derives tenant-a, ignores spoofed header, isOperator=false => 403/7 on operator API!
    const scopedClientHeaders = [
      ["Authorization", "Bearer scoped-client-a"],
      [NAMESPACE_HEADER, "spoofed"],
    ] as const;
    expect(policy.verifyClientHeaders(scopedClientHeaders)).toEqual({
      namespace: "tenant-a",
      role: Role.Client,
      subject: "svc-a",
      forcedPool: null,
      isOperator: false,
    });
    expect(() =>
      policy.authorizeOperatorHeaders(scopedClientHeaders),
    ).toThrowError(
      expect.objectContaining({
        code: GrpcCode.PermissionDenied,
        httpStatus: 403,
      }),
    );
    expect(policy.authorizeConsoleHeaders(scopedClientHeaders)).toEqual({
      namespace: "tenant-a",
      role: Role.Client,
      subject: "svc-a",
      forcedPool: null,
      isOperator: false,
    });

    // Even a scoped client token in "default" namespace is NOT an operator!
    const scopedDefaultHeaders = {
      Authorization: "Bearer scoped-client-default",
    };
    expect(policy.verifyClientHeaders(scopedDefaultHeaders)).toEqual({
      namespace: "default",
      role: Role.Client,
      subject: "client-token",
      forcedPool: null,
      isOperator: false,
    });
    expect(() =>
      policy.authorizeOperatorHeaders(scopedDefaultHeaders),
    ).toThrowError(
      expect.objectContaining({
        code: GrpcCode.PermissionDenied,
        httpStatus: 403,
      }),
    );

    // 4. Scoped worker token: derives factory-a + forcedPool jetson-local, ignores spoofed namespace header
    const scopedWorkerHeaders = new Headers({
      authorization: "Bearer scoped-worker-factory",
      [NAMESPACE_HEADER]: "spoofed",
    });
    expect(policy.verifyWorkerHeaders(scopedWorkerHeaders)).toEqual({
      namespace: "factory-a",
      role: Role.Worker,
      subject: "jetson-1",
      forcedPool: "jetson-local",
      isOperator: false,
    });
    expect(() =>
      policy.verifyClientHeaders(scopedWorkerHeaders),
    ).toThrowError(
      expect.objectContaining({
        code: GrpcCode.PermissionDenied,
        httpStatus: 403,
      }),
    );
    expect(() =>
      policy.authorizeOperatorHeaders(scopedWorkerHeaders),
    ).toThrowError(
      expect.objectContaining({
        code: GrpcCode.PermissionDenied,
        httpStatus: 403,
      }),
    );
    expect(() =>
      policy.authorizeConsoleHeaders(scopedWorkerHeaders),
    ).toThrowError(
      expect.objectContaining({
        code: GrpcCode.PermissionDenied,
        httpStatus: 403,
      }),
    );

    // 5. Missing, malformed, or unknown tokens => 401 / GrpcCode.Unauthenticated (16)
    for (const badAuth of [
      new Headers(),
      new Headers({ Authorization: "Basic dXNlcjpwYXNz" }),
      new Headers({ Authorization: "Bearer " }),
      new Headers({ Authorization: "Bearer wrong-token" }),
    ]) {
      for (const call of [
        () => policy.verifyClientHeaders(badAuth),
        () => policy.verifyWorkerHeaders(badAuth),
        () => policy.authorizeOperatorHeaders(badAuth),
      ]) {
        expect(call).toThrowError(
          expect.objectContaining({
            code: GrpcCode.Unauthenticated,
            httpStatus: 401,
          }),
        );
      }
    }

    // Invalid ANYSERVE_SCOPED_TOKENS configs fail loudly at load time
    expect(() =>
      AuthPolicy.fromEnv({ ANYSERVE_SCOPED_TOKENS: "{not-json" }),
    ).toThrow();
    expect(() =>
      AuthPolicy.fromEnv({
        ANYSERVE_SCOPED_TOKENS: JSON.stringify([
          { token: "t", role: "admin", namespace: "default" },
        ]),
      }),
    ).toThrow(/role/);
    expect(() =>
      AuthPolicy.fromEnv({
        ANYSERVE_SCOPED_TOKENS: JSON.stringify([
          { token: "t", role: "client", namespace: "bad/ns" },
        ]),
      }),
    ).toThrow(/namespace/);
  });

  it("redacts all secret tokens in toString, toJSON, JSON.stringify, Object.keys, and util.inspect", () => {
    const secretClient = "ultra-secret-client-tok-991";
    const secretWorker = "ultra-secret-worker-tok-992";
    const secretScoped = "ultra-secret-scoped-tok-993";

    const scoped = new ScopedToken({
      token: secretScoped,
      role: Role.Worker,
      namespace: "factory-a",
      subject: "jetson-1",
      forcedPool: "jetson-local",
    });

    const policy = new AuthPolicy([secretClient], [secretWorker]).withScopedTokens([
      scoped,
    ]);

    for (const repr of [
      scoped.toString(),
      JSON.stringify(scoped),
      inspect(scoped),
      Object.keys(scoped).join(","),
      policy.toString(),
      JSON.stringify(policy),
      inspect(policy),
      Object.keys(policy).join(","),
    ]) {
      expect(repr).not.toContain(secretClient);
      expect(repr).not.toContain(secretWorker);
      expect(repr).not.toContain(secretScoped);
    }

    expect(scoped.toJSON()).toEqual({
      token: "[redacted]",
      role: "worker",
      namespace: "factory-a",
      subject: "jetson-1",
      forcedPool: "jetson-local",
    });
    expect(policy.toJSON()).toEqual({
      clientTokenCount: 1,
      workerTokenCount: 1,
      scopedTokens: [
        {
          token: "[redacted]",
          role: "worker",
          namespace: "factory-a",
          subject: "jetson-1",
          forcedPool: "jetson-local",
        },
      ],
    });
    expect(policy.toString()).toContain("client_token_count: 1");
    expect(policy.toString()).toContain("worker_token_count: 1");
    expect(policy.toString()).toContain("[redacted]");
  });
});

import * as pbAll from "../src/native/proto";
import { compareUtf8 as cmpUtf8, resultObjectAccountedBytes as accountedBytes } from "../src/native/identity";

describe("native/proto.ts — schema-driven codec invariants (added)", () => {
  const te = new TextEncoder();
  const hex = (b: Uint8Array) => Buffer.from(b).toString("hex");
  const sampleStrings = [
    "", "a", "abc", "h\u00e9llo", "\u65e5\u672c\u8a9e", "z\u{1F600}", "\u{10000}", "\ue000", "\ud800", "\udc00", "x\ud800y", "tail\ud83d",
    "\u07ff", "\u0800", "\uffff", "a".repeat(300), "\u00e9".repeat(70),
  ];

  it("exports zero-allocation utf8ByteLen matching TextEncoder exactly", () => {
    for (const s of sampleStrings) {
      expect(pbAll.utf8ByteLen(s)).toBe(te.encode(s).byteLength);
    }
    let seed = 7;
    const next = () => (seed = (seed * 1103515245 + 12345) & 0x7fffffff);
    for (let i = 0; i < 500; i++) {
      let str = "";
      for (let j = next() % 12; j > 0; j--) str += String.fromCharCode(next() % 0xffff);
      expect(pbAll.utf8ByteLen(str)).toBe(te.encode(str).byteLength);
    }
    expect(accountedBytes({ metadata: { k: "\u00e9", "\u65e5": "v" }, uri: "z\u{1F600}", dataByteLength: 5 })).toBe(
      BigInt(OBJECT_OVERHEAD + 1 + 2 + 3 + 1 + 5 + 5),
    );
  });

  it("compareUtf8 keeps unsigned UTF-8 byte order across ASCII, multi-byte, prefix and surrogate cases", () => {
    const byBytes = (a: string, b: string) => {
      const x = te.encode(a);
      const y = te.encode(b);
      const n = Math.min(x.length, y.length);
      for (let i = 0; i < n; i++) if (x[i] !== y[i]) return x[i]! - y[i]!;
      return x.length - y.length;
    };
    for (const a of sampleStrings) {
      for (const b of sampleStrings) {
        expect(Math.sign(cmpUtf8(a, b))).toBe(Math.sign(byBytes(a, b)));
      }
    }
    expect(Math.sign(cmpUtf8("ab", "abc"))).toBe(-1);
    expect(Math.sign(cmpUtf8("b", "a\u00e9"))).toBe(1);
    expect(Math.sign(cmpUtf8("\ue000", "\u{10000}"))).toBe(-1);
  });

  it("map entries are emitted in UTF-8 byte order regardless of insertion order (non-ASCII keys, 0/1 entries)", () => {
    const keys = ["\ue000", "\u{10000}", "b", "a", "\u00e9", "Z", "\u65e5"];
    const reference = keys.slice().sort((a, b) => Math.sign(cmpUtf8(a, b)));
    const build = (order: string[]) => {
      const metadata: Record<string, string> = {};
      for (const k of order) metadata[k] = `v-${k}`;
      return encodeInputChunk({ data: new Uint8Array([1]), metadata });
    };
    const expected = hex(build(reference));
    for (let r = 0; r < 8; r++) {
      const shuffled = keys.slice();
      for (let i = shuffled.length - 1; i > 0; i--) {
        const j = (i * 7 + r * 3) % (i + 1);
        [shuffled[i], shuffled[j]] = [shuffled[j]!, shuffled[i]!];
      }
      expect(hex(build(shuffled))).toBe(expected);
    }
    expect(hex(encodeInputChunk({ metadata: {} }))).toBe("");
    expect(hex(encodeInputChunk({ metadata: { k: "v" } }))).toBe("12060a016b120176");
    expect(decodeInputChunk(build(reference)).metadata).toEqual(Object.fromEntries(reference.map((k) => [k, `v-${k}`])));
  });

  it("encodedLen* equals encoded byte length for nested, oneof and segmented messages", () => {
    const job = {
      jobId: "j-\u00e9", state: JobState.SUCCEEDED, durability: Durability.DURABLE, interface: "echo", attemptCount: 3,
      createdAtMs: 1_700_000_000_000n, updatedAtMs: 2n ** 63n, error: { code: "c", message: "m", retryable: true },
      outputs: [{ body: { case: "inline" as const, value: new Uint8Array(200) }, metadata: { a: "b", "\u00e9": "x" } }, { body: { case: "uri" as const, value: "r2://x" } }],
      metadata: { z: "1", a: "2" }, specHash: "h", namespace: "ns",
    };
    expect(pbAll.encodedLenJob(job)).toBe(pbAll.encodeJob(job).byteLength);
    const ev = {
      jobId: "j", sequence: 9n, attemptId: "a", atMs: 5n,
      event: { case: "finished" as const, value: { state: JobState.SUCCEEDED, outputs: [{ body: { case: "inline" as const, value: [new Uint8Array(5).fill(1), new Uint8Array(0), new Uint8Array(7).fill(2)] }, metadata: { m: "n" } }] } },
    };
    const frame = pbAll.encodeExecutionEventWsFrame(ev);
    expect(frame.byteLength).toBe(1 + pbAll.encodedLenExecutionEvent(ev));
    expect(frame[0]).toBe(0);
    expect(hex(frame.subarray(1))).toBe(hex(pbAll.encodeExecutionEvent(ev)));
    const dec = pbAll.decodeExecutionEvent(frame.subarray(1));
    expect(dec.event?.case === "finished" && dec.event.value.outputs[0]!.body).toEqual({
      case: "inline",
      value: new Uint8Array([1, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2, 2]),
    });
    for (const msg of [
      { msg: { case: "heartbeat" as const, value: { load: { queued: 1, running: 2, utilization: 0.5, custom: { b: 1, a: 2, z: 0 } } } } },
      { msg: { case: "draining" as const, value: { preempting: true } } },
      {},
    ]) {
      expect(pbAll.encodedLenWorkerMessage(msg)).toBe(pbAll.encodeWorkerMessage(msg).byteLength);
    }
  });

  it("keeps hot-message wire details: optional object_index, AttemptUp oneof numbers, u64/i64 extremes", () => {
    expect(hex(pbAll.encodeInputData({ objectIndex: 0 }))).toBe("2800");
    expect(hex(pbAll.encodeInputData({}))).toBe("");
    expect(pbAll.decodeInputData(pbAll.encodeInputData({ objectIndex: 0 })).objectIndex).toBe(0);
    expect(pbAll.decodeInputData(new Uint8Array(0)).objectIndex).toBeUndefined();
    const first = (m: Parameters<typeof pbAll.encodeAttemptUp>[0]) => pbAll.encodeAttemptUp(m)[0];
    expect(first({ msg: { case: "open", value: { leaseId: "l" } } })).toBe(0x0a);
    expect(first({ msg: { case: "frame", value: { sequence: 1n } } })).toBe(0x12);
    expect(first({ msg: { case: "result", value: { index: 1 } } })).toBe(0x1a);
    expect(first({ msg: { case: "finish", value: { outcome: { case: "cancelled" } } } })).toBe(0x2a);
    for (const v of [0n, 1n, 127n, 128n, 2n ** 32n, 2n ** 53n - 1n, 2n ** 53n, 2n ** 53n + 1n, 2n ** 63n, 2n ** 64n - 1n]) {
      expect(pbAll.decodeAck(pbAll.encodeAck({ upToSequence: v, resultBytes: v })).upToSequence).toBe(v);
      expect(pbAll.encodedLenAck({ upToSequence: v })).toBe(pbAll.encodeAck({ upToSequence: v }).byteLength);
    }
    const cap = { a: -(2n ** 63n), b: 2n ** 63n - 1n, c: -1n, d: 0n };
    expect(pbAll.decodeDemand(pbAll.encodeDemand({ requiredCapacity: cap })).requiredCapacity).toEqual(cap);
    const dm = { k: 0.1, nan: Number.NaN, neg: -2.5, zero: 0 };
    const back = pbAll.decodeLoad(pbAll.encodeLoad({ custom: dm })).custom;
    expect(back.k).toBe(0.1);
    expect(back.nan).toBeNaN();
    expect(back.neg).toBe(-2.5);
    expect(back.zero).toBe(0);
  });

  it("skips unknown fields, merges repeated sub-messages, and rejects malformed input with ProtoDecodeError", () => {
    const unknown = new Uint8Array([0xf8, 0x06, 0x01, 0xc1, 0x06, 1, 2, 3, 4, 5, 6, 7, 8, 0xca, 0x06, 0x02, 0xaa, 0xbb, 0xd5, 0x06, 1, 2, 3, 4]);
    const withUnknown = concatBytes(pbAll.encodeAck({ upToSequence: 5n }), unknown);
    expect(pbAll.decodeAck(withUnknown).upToSequence).toBe(5n);
    const twoDemands = concatBytes(
      pbAll.encodeJobSpec({ demand: { interface: "a" } }),
      pbAll.encodeJobSpec({ demand: { affinityKey: "b" } }),
    );
    expect(pbAll.decodeJobSpec(twoDemands).demand).toMatchObject({ interface: "a", affinityKey: "b" });
    const bad: Uint8Array[] = [
      new Uint8Array([0x08]), new Uint8Array([0x08, 0x80]), new Uint8Array([0x0b, 0x00]), new Uint8Array([0x00, 0x01]),
      new Uint8Array([0x12, 0x05, 0x01]), new Uint8Array([0x0a, 0x01, 0xff]), new Uint8Array([0x08, ...Array(9).fill(0xff), 0x02]),
      new Uint8Array([0x09, 1, 2]), new Uint8Array([0x80, 0x80, 0x80, 0x80, 0x80, 0x01]),
    ];
    for (const b of bad) {
      expect(() => pbAll.decodeFrame(b), hex(b)).toThrow(pbAll.ProtoDecodeError);
      expect(() => pbAll.decodeHello(b), hex(b)).toThrow(pbAll.ProtoDecodeError);
    }
    expect(Durability.DURABLE).toBe(2);
    expect(JobState.SUCCEEDED).toBe(3);
    expect(GrpcCode.ResourceExhausted).toBe(8);
  });

  it("AuthPolicy.withScopedTokens returns an independent policy that keeps client/worker tokens", () => {
    const base = new AuthPolicy(["c1"], ["w1"]);
    const next = base.withScopedTokens([{ token: "s1", role: Role.Client, namespace: "ns-a" }]);
    expect(base.toJSON().scopedTokens).toHaveLength(0);
    expect(next.toJSON()).toMatchObject({ clientTokenCount: 1, workerTokenCount: 1 });
    expect(next.principalForToken("s1")?.namespace).toBe("ns-a");
    expect(next.principalForToken("c1")?.isOperator).toBe(true);
    expect(base.principalForToken("s1")).toBeNull();
    expect(next).toBeInstanceOf(AuthPolicy);
  });
});
