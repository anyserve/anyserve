// Client Invoke / Watch sockets and unary Submit / GetJob / Cancel / ListJobs RPCs plus job admission.
// Methods are mixed into `NativeEngine.prototype` (`this: NativeEngine`).

import { GrpcStatusError } from "./auth";
import {
  EVENT_OVERHEAD_BYTES,
  MAX_IDENTIFIER_BYTES,
  MAX_METADATA_BYTES,
  MAX_OBJECTS,
  WS_READY_STATE_OPEN,
  writeSocketAttachment,
  type NativeWebSocket,
  type SocketAttachment,
} from "./host";
import { encodeScopedId, specHash } from "./identity";
import {
  decodeInlineAssignBundle,
  serializeSpecJson,
  splitU64,
  type DurabilityName,
  type JobStateName,
  type StoredObject,
  type StoredSpecMetadata,
} from "./store";
import { utf8ByteLen } from "./utf8";
import * as pb from "./proto";
import {
  INLINE_ASSIGN_CACHE_MAX_BYTES,
  metadataByteLen,
  toNumberClamped,
  protoStateToName,
  type JobMem,
  type WatcherMem,
} from "./engine-types";
import type { NativeEngine } from "./engine";

export const clientHandlers = {
  async handleInvokeSocketMessage(
    this: NativeEngine,
    ws: NativeWebSocket,
    att: SocketAttachment,
    bytes: Uint8Array,
    nowMs: number,
  ): Promise<void> {
    const req = pb.decodeInvokeRequest(bytes);
    if (att.handshakeDone === 0) {
      if (req.msg?.case !== "start") {
        throw GrpcStatusError.invalidArgument(
          "first invocation message must be Start",
        );
      }
      await this.handleInvokeStart(ws, att, req.msg.value, nowMs);
      return;
    }

    const job = this.jobs.get(att.id);
    if (!job || !job.streamingInput || job.inputEnded) {
      throw GrpcStatusError.invalidArgument("unexpected invocation input message");
    }

    if (req.msg?.case === "input") {
      const chunk = req.msg.value;
      if (chunk.data.byteLength > this.config.maxInlineObjectBytes) {
        throw GrpcStatusError.invalidArgument(
          `streaming input chunk of ${chunk.data.byteLength} bytes exceeds ${this.config.maxInlineObjectBytes}`,
        );
      }
      if (metadataByteLen(chunk.metadata) > MAX_METADATA_BYTES) {
        throw GrpcStatusError.invalidArgument("streaming input metadata is too large");
      }
      const nextTotal = job.totalInputBytes + chunk.data.byteLength;
      if (nextTotal > this.config.maxInlineTotalBytes) {
        throw GrpcStatusError.invalidArgument(
          `inputs and params total ${nextTotal} bytes, above ${this.config.maxInlineTotalBytes}`,
        );
      }
      if (this.totalInputBytesHeld + chunk.data.byteLength > this.config.maxPendingBytes) {
        throw GrpcStatusError.resourceExhausted("input budget is exhausted");
      }

      // Section 4.4: Persist streaming InputChunk BEFORE webSocketMessage returns!
      const ok = await this.store.appendStreamInputChunk(
        job.scopedJobId,
        chunk.data,
        chunk.metadata,
        nowMs,
        () => {
          const cur = this.jobs.get(job.scopedJobId);
          if (
            !cur ||
            cur.callerConnId !== att.connId ||
            cur.inputEnded ||
            (cur.state !== "queued" && cur.state !== "running")
          ) {
            return false;
          }
          cur.streamChunkCount += 1;
          cur.totalInputBytes += chunk.data.byteLength;
          this.totalInputBytesHeld += chunk.data.byteLength;
          this.store.exec(
            `UPDATE native_jobs
             SET stream_chunk_count = ?, total_input_bytes = ?
             WHERE scoped_job_id = ?`,
            cur.streamChunkCount,
            cur.totalInputBytes,
            cur.scopedJobId,
          );
          return true;
        },
      );
      if (!ok) {
        return;
      }
      if (job.currentLeaseId) {
        await this.pumpAttemptInputs(job.currentLeaseId);
      }
      return;
    }

    if (req.msg?.case === "inputEnd") {
      // Section 4.4: Persist InputEnd BEFORE webSocketMessage returns!
      this.store.transactionSync(() => {
        const cur = this.jobs.get(job.scopedJobId);
        if (!cur || cur.callerConnId !== att.connId) return;
        cur.inputEnded = true;
        this.store.exec(
          `UPDATE native_jobs SET input_ended = 1 WHERE scoped_job_id = ?`,
          cur.scopedJobId,
        );
      });
      if (job.currentLeaseId) {
        await this.pumpAttemptInputs(job.currentLeaseId);
      }
      return;
    }

    throw GrpcStatusError.invalidArgument("unexpected invocation input message");
  },

  async handleInvokeStart(
    this: NativeEngine,
    ws: NativeWebSocket,
    att: SocketAttachment,
    start: pb.InvokeStart,
    nowMs: number,
  ): Promise<void> {
    if (!start.spec) {
      throw GrpcStatusError.invalidArgument("spec is required");
    }
    const normalized = this.normalizeJobSpec(start.spec, "ephemeral");
    const usesLarge = this.specUsesMultiChunkOrR2(normalized.inputs, normalized.params, start.streamingInput);
    const estRetained = 4_096 + normalized.totalPayloadBytes;
    const shedReason = this.store.checkNewJobAdmission(nowMs, estRetained, usesLarge);
    if (shedReason !== null) {
      throw GrpcStatusError.resourceExhausted(shedReason);
    }

    this.checkQueueAndInputBudgets(att.namespace, normalized.spec.demand.interface, normalized.totalPayloadBytes);

    const rawJobId = crypto.randomUUID();
    const scopedJobId = encodeScopedId(att.namespace, rawJobId);
    const bundlePlan = await this.store.prepareObjectBundle(
      normalized.inputs,
      normalized.params,
      nowMs,
    );

    if (usesLarge || bundlePlan.stagedUploads.length > 0) {
      this.store.recordLargeJob(nowMs);
    }

    const notifyJobs = new Set<string>([scopedJobId]);
    let committed = false;
    this.store.transactionSync(() => {
      if (ws.readyState !== WS_READY_STATE_OPEN) {
        return;
      }
      if (!this.store.claimStagedUploadsInTxn(bundlePlan.stagedUploads)) {
        return;
      }
      writeSocketAttachment(ws, {
        ...att,
        id: scopedJobId,
        handshakeDone: 1,
        lastSeenAtMs: nowMs,
      });
      this.insertAcceptedJobInTxn({
        scopedJobId,
        rawJobId,
        namespace: att.namespace,
        durability: "ephemeral",
        specHash: "",
        spec: normalized.spec,
        maxAttempts: normalized.maxAttempts,
        totalInputBytes: normalized.totalPayloadBytes,
        retainedBytesEst: estRetained,
        callerConnId: att.connId,
        streamingInput: start.streamingInput,
        bundleRows: bundlePlan.rows,
        nowMs,
      });
      this.dispatchInTxn(nowMs, notifyJobs);
      committed = true;
    });

    if (!committed) {
      await this.store.compensateUnpublishedUploads(bundlePlan.stagedUploads, nowMs);
      throw GrpcStatusError.unavailable("failed to commit InvokeStart");
    }

    for (const jid of notifyJobs) {
      await this.notifyJobFollowers(jid);
    }
    await this.refreshAlarm(nowMs);
  },

  async handleWatchSocketMessage(
    this: NativeEngine,
    ws: NativeWebSocket,
    att: SocketAttachment,
    bytes: Uint8Array,
    nowMs: number,
  ): Promise<void> {
    if (att.handshakeDone === 1) {
      throw GrpcStatusError.invalidArgument("unexpected watch message");
    }
    const req = pb.decodeWatchRequest(bytes);
    const scopedJobId = encodeScopedId(att.namespace, req.jobId);
    const job = this.jobs.get(scopedJobId);
    if (!job) {
      throw GrpcStatusError.notFound(`job '${req.jobId}' does not exist`);
    }

    // Cursor check (section 4.3 & 4.4: after == 0 or after >= oldest - 1 is valid; older returns OutOfRange(11) with oldest!)
    if (req.afterSequence !== 0n && req.afterSequence < job.evictedThrough) {
      const oldest = job.evictedThrough + 1n;
      throw GrpcStatusError.outOfRange(
        `cursor expired; oldest available sequence is ${oldest}`,
      );
    }

    writeSocketAttachment(ws, {
      ...att,
      id: scopedJobId,
      handshakeDone: 1,
      lastSeenAtMs: nowMs,
    });

    const watcher: WatcherMem = {
      watchConnId: att.connId,
      scopedJobId,
      rawJobId: job.rawJobId,
      namespace: att.namespace,
      afterSeq: req.afterSequence,
      deliveredSeq: req.afterSequence,
      finished: false,
    };

    this.store.transactionSync(() => {
      this.watchers.set(att.connId, watcher);
      let set = this.watchersByJob.get(scopedJobId);
      if (!set) {
        set = new Set();
        this.watchersByJob.set(scopedJobId, set);
      }
      set.add(att.connId);
      const aSplit = splitU64(watcher.afterSeq);
      const dSplit = splitU64(watcher.deliveredSeq);
      this.store.exec(
        `INSERT INTO native_watchers (
          watch_conn_id, scoped_job_id, raw_job_id, namespace,
          after_seq_hi, after_seq_lo, delivered_seq_hi, delivered_seq_lo, finished
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0)`,
        att.connId,
        scopedJobId,
        job.rawJobId,
        att.namespace,
        aSplit.hi,
        aSplit.lo,
        dSplit.hi,
        dSplit.lo,
      );
    });

    await this.deliverWatcherEvents(att.connId);
  },

  async handleSubmitRpc(
    this: NativeEngine,
    namespace: string,
    req: pb.SubmitRequest,
    nowMs: number,
  ): Promise<pb.SubmitResponse> {
    if (!req.spec) {
      throw GrpcStatusError.invalidArgument("spec is required");
    }
    if (
      req.conflictPolicy !== pb.ConflictPolicy.CONFLICT_POLICY_UNSPECIFIED &&
      req.conflictPolicy !== pb.ConflictPolicy.FAIL &&
      req.conflictPolicy !== pb.ConflictPolicy.USE_EXISTING
    ) {
      throw GrpcStatusError.invalidArgument("unknown conflict policy");
    }
    const durability: DurabilityName =
      req.spec.policy?.durability === pb.Durability.EPHEMERAL
        ? "ephemeral"
        : "durable";
    const normalized = this.normalizeJobSpec(req.spec, durability);

    const requestedId = req.jobId.trim();
    if (requestedId && utf8ByteLen(requestedId) > MAX_IDENTIFIER_BYTES) {
      throw GrpcStatusError.invalidArgument("job_id is too long");
    }

    const computedHash =
      requestedId.length > 0 || durability === "durable"
        ? specHash(normalized.protoForHash, durability)
        : "";

    const rawJobId = requestedId.length > 0 ? requestedId : crypto.randomUUID();
    const scopedJobId = encodeScopedId(namespace, rawJobId);

    // Idempotency / conflict check before staging any R2 uploads!
    const existing = this.jobs.get(scopedJobId);
    if (existing) {
      this.checkSubmitConflict(rawJobId, existing.specHash, computedHash, req.conflictPolicy);
      return {
        job: await this.jobMemToProto(existing, false, nowMs),
        created: false,
      };
    }

    const usesLarge = this.specUsesMultiChunkOrR2(normalized.inputs, normalized.params, false);
    const estRetained = 4_096 + (usesLarge ? 2_048 : normalized.totalPayloadBytes);
    const shedReason = this.store.checkNewJobAdmission(nowMs, estRetained, usesLarge);
    if (shedReason !== null) {
      throw GrpcStatusError.resourceExhausted(shedReason);
    }

    this.checkQueueAndInputBudgets(
      namespace,
      normalized.spec.demand.interface,
      normalized.totalPayloadBytes,
    );

    const bundlePlan = await this.store.prepareObjectBundle(
      normalized.inputs,
      normalized.params,
      nowMs,
    );
    if (usesLarge || bundlePlan.stagedUploads.length > 0) {
      this.store.recordLargeJob(nowMs);
    }

    const notifyJobs = new Set<string>();
    let raceExisting: JobMem | null = null;
    let createdJob: JobMem | null = null;

    try {
      this.store.transactionSync(() => {
        const curExisting = this.jobs.get(scopedJobId);
        if (curExisting) {
          this.checkSubmitConflict(
            rawJobId,
            curExisting.specHash,
            computedHash,
            req.conflictPolicy,
          );
          raceExisting = curExisting;
          return;
        }
        if (!this.store.claimStagedUploadsInTxn(bundlePlan.stagedUploads)) {
          throw GrpcStatusError.unavailable("input staging expired; retry submission");
        }
        createdJob = this.insertAcceptedJobInTxn({
          scopedJobId,
          rawJobId,
          namespace,
          durability,
          specHash: computedHash,
          spec: normalized.spec,
          maxAttempts: normalized.maxAttempts,
          totalInputBytes: normalized.totalPayloadBytes,
          retainedBytesEst: estRetained,
          callerConnId: null,
          streamingInput: false,
          bundleRows: bundlePlan.rows,
          nowMs,
        });
        this.dispatchInTxn(nowMs, notifyJobs);
      });
    } catch (err) {
      await this.store.compensateUnpublishedUploads(bundlePlan.stagedUploads, nowMs);
      throw err;
    }

    if (raceExisting !== null) {
      await this.store.compensateUnpublishedUploads(bundlePlan.stagedUploads, nowMs);
      return {
        job: await this.jobMemToProto(raceExisting, false, nowMs),
        created: false,
      };
    }

    for (const jid of notifyJobs) {
      await this.notifyJobFollowers(jid);
    }
    await this.refreshAlarm(nowMs);

    return {
      job: await this.jobMemToProto(createdJob!, false, nowMs),
      created: true,
    };
  },

  checkSubmitConflict(
    this: NativeEngine,
    rawJobId: string,
    existingHash: string,
    incomingHash: string,
    policy: pb.ConflictPolicy,
  ): void {
    if (!existingHash) {
      throw GrpcStatusError.alreadyExists(
        `job '${rawJobId}' exists and was not created by an idempotent submission`,
      );
    }
    if (existingHash !== incomingHash) {
      throw GrpcStatusError.alreadyExists(
        `job '${rawJobId}' exists with a different specification`,
      );
    }
    if (policy === pb.ConflictPolicy.FAIL) {
      throw GrpcStatusError.alreadyExists(`job '${rawJobId}' already exists`);
    }
  },

  async handleGetJobRpc(
    this: NativeEngine,
    namespace: string,
    req: pb.GetJobRequest,
    nowMs: number,
  ): Promise<pb.Job> {
    const scopedJobId = encodeScopedId(namespace, req.jobId);
    const job = this.jobs.get(scopedJobId);
    if (!job) {
      throw GrpcStatusError.notFound(`job '${req.jobId}' does not exist`);
    }
    return await this.jobMemToProto(job, req.includeOutputs, nowMs);
  },

  async handleCancelRpc(
    this: NativeEngine,
    namespace: string,
    req: pb.CancelRequest,
    nowMs: number,
  ): Promise<pb.Job> {
    const scopedJobId = encodeScopedId(namespace, req.jobId);
    const job = this.jobs.get(scopedJobId);
    if (!job) {
      throw GrpcStatusError.notFound(`job '${req.jobId}' does not exist`);
    }
    if (
      job.state === "succeeded" ||
      job.state === "failed" ||
      job.state === "cancelled"
    ) {
      return await this.jobMemToProto(job, false, nowMs);
    }
    const reason = req.reason.trim() || "cancelled";
    const notifyJobs = new Set<string>([scopedJobId]);
    this.store.transactionSync(() => {
      this.cancelJobInTxn(scopedJobId, reason, nowMs);
      this.dispatchInTxn(nowMs, notifyJobs);
    });
    for (const jid of notifyJobs) {
      await this.notifyJobFollowers(jid);
    }
    await this.refreshAlarm(nowMs);
    return await this.jobMemToProto(job, false, nowMs);
  },

  handleListJobsRpc(
    this: NativeEngine,
    namespace: string,
    req: pb.ListJobsRequest,
    nowMs: number,
  ): pb.ListJobsResponse {
    const states: JobStateName[] = [];
    for (const s of req.states) {
      const name = protoStateToName(s);
      if (!name) {
        throw GrpcStatusError.invalidArgument("unknown job state");
      }
      if (!states.includes(name)) {
        states.push(name);
      }
    }
    let offset = 0;
    if (req.pageToken.trim().length > 0) {
      const parsed = Number(req.pageToken);
      if (!Number.isInteger(parsed) || parsed < 0) {
        throw GrpcStatusError.invalidArgument("invalid page_token");
      }
      offset = parsed;
    }
    const limit = req.limit === 0 ? 100 : req.limit;
    const res = this.queryJobsInternal({
      states,
      interfaceName: null,
      namespace,
      workerId: null,
      search: null,
      fromMs: null,
      toMs: null,
      limit,
      offset,
      histogramBuckets: 0,
    }, nowMs);

    return {
      jobs: res.matchedPage.map((j) => this.jobMemToProtoSyncWithoutOutputs(j)),
      nextPageToken: res.nextPageToken ?? "",
    };
  },

  normalizeJobSpec(
    this: NativeEngine,
    protoSpec: pb.JobSpec,
    durability: DurabilityName,
  ): {
    spec: StoredSpecMetadata;
    inputs: StoredObject[];
    params: Uint8Array;
    maxAttempts: number;
    totalPayloadBytes: number;
    protoForHash: pb.JobSpecInput;
  } {
    if (!protoSpec.demand) {
      throw GrpcStatusError.invalidArgument("demand is required");
    }
    const iface = protoSpec.demand.interface;
    if (!iface || iface.trim().length === 0) {
      throw GrpcStatusError.invalidArgument("demand.interface is required");
    }
    if (utf8ByteLen(iface) > MAX_IDENTIFIER_BYTES) {
      throw GrpcStatusError.invalidArgument(
        `demand.interface exceeds ${MAX_IDENTIFIER_BYTES} bytes`,
      );
    }
    if (protoSpec.inputs.length > MAX_OBJECTS) {
      throw GrpcStatusError.invalidArgument(
        `a job may carry at most ${MAX_OBJECTS} input objects`,
      );
    }

    let totalPayload = protoSpec.params.byteLength;
    const inputs: StoredObject[] = [];
    let inputMetaBytes = 0;

    for (const inp of protoSpec.inputs) {
      if (!inp.body) {
        throw GrpcStatusError.invalidArgument("object body is required");
      }
      let inlineSize = 0;
      if (inp.body.case === "uri") {
        if (inp.body.value.length === 0) {
          throw GrpcStatusError.invalidArgument("object URI must not be empty");
        }
        const uriBytes = utf8ByteLen(inp.body.value);
        if (uriBytes > MAX_METADATA_BYTES) {
          throw GrpcStatusError.invalidArgument(
            `object URI of ${uriBytes} bytes exceeds ${MAX_METADATA_BYTES} bytes`,
          );
        }
        inputs.push({
          kind: "uri",
          uri: inp.body.value,
          metadata: { ...inp.metadata },
        });
      } else {
        inlineSize = inp.body.value.byteLength;
        inputs.push({
          kind: "inline",
          data: inp.body.value,
          metadata: { ...inp.metadata },
        });
      }
      if (inlineSize > this.config.maxInlineObjectBytes) {
        throw GrpcStatusError.invalidArgument(
          `input object of ${inlineSize} bytes exceeds ${this.config.maxInlineObjectBytes} bytes`,
        );
      }
      totalPayload += inlineSize;
      inputMetaBytes += metadataByteLen(inp.metadata);
    }

    if (protoSpec.params.byteLength > this.config.maxInlineObjectBytes) {
      throw GrpcStatusError.invalidArgument(
        `params of ${protoSpec.params.byteLength} bytes exceeds ${this.config.maxInlineObjectBytes} bytes`,
      );
    }
    if (totalPayload > this.config.maxInlineTotalBytes) {
      throw GrpcStatusError.invalidArgument(
        `inputs and params total ${totalPayload} bytes, above ${this.config.maxInlineTotalBytes}`,
      );
    }

    let capKeyBytes = 0;
    for (const [k, v] of Object.entries(protoSpec.demand.requiredCapacity)) {
      if (v < 0n) {
        throw GrpcStatusError.invalidArgument(
          "required capacity cannot be negative",
        );
      }
      capKeyBytes += utf8ByteLen(k);
    }

    const totalMeta =
      utf8ByteLen(iface) +
      metadataByteLen(protoSpec.metadata) +
      metadataByteLen(protoSpec.demand.requiredAttributes) +
      metadataByteLen(protoSpec.demand.preferredAttributes) +
      capKeyBytes +
      inputMetaBytes;
    if (totalMeta > MAX_METADATA_BYTES) {
      throw GrpcStatusError.invalidArgument(
        `metadata and attributes total ${totalMeta} bytes, above ${MAX_METADATA_BYTES}`,
      );
    }

    const pol = protoSpec.policy;
    if (
      pol &&
      pol.durability !== pb.Durability.DURABILITY_UNSPECIFIED &&
      pol.durability !== pb.Durability.EPHEMERAL &&
      pol.durability !== pb.Durability.DURABLE
    ) {
      throw GrpcStatusError.invalidArgument("unknown durability");
    }
    const clampedPriority = Math.min(
      pol?.priority ?? 0,
      this.config.priorityLevels - 1,
    );
    const rawMaxAttempts =
      pol && pol.maxAttempts > 0 ? pol.maxAttempts : this.config.defaultMaxAttempts;
    const maxAttempts = Math.min(
      Math.max(1, rawMaxAttempts),
      this.config.maxAttemptsLimit,
    );
    const policyMaxAttemptsNormalized =
      pol && pol.maxAttempts > 0 ? pol.maxAttempts : null;

    const queueTimeoutMs =
      pol && pol.queueTimeoutMs > 0n ? toNumberClamped(pol.queueTimeoutMs) : null;
    const execTimeoutMs =
      pol && pol.executionTimeoutMs > 0n
        ? toNumberClamped(pol.executionTimeoutMs)
        : null;
    const totalTimeoutMs =
      pol && pol.totalTimeoutMs > 0n ? toNumberClamped(pol.totalTimeoutMs) : null;
    const spilloverAfterMs =
      pol && pol.spilloverAfterMs > 0n
        ? toNumberClamped(pol.spilloverAfterMs)
        : null;

    const storedSpec: StoredSpecMetadata = {
      demand: {
        interface: iface,
        required_attributes: { ...protoSpec.demand.requiredAttributes },
        preferred_attributes: { ...protoSpec.demand.preferredAttributes },
        required_capacity: { ...protoSpec.demand.requiredCapacity },
        affinity_key: protoSpec.demand.affinityKey,
      },
      policy: {
        durability,
        priority: clampedPriority,
        queue_timeout_ms: queueTimeoutMs,
        execution_timeout_ms: execTimeoutMs,
        total_timeout_ms: totalTimeoutMs,
        spillover_after_ms: spilloverAfterMs,
        max_attempts: policyMaxAttemptsNormalized,
        fairness_key: pol?.fairnessKey ?? "",
        concurrency_key: pol?.concurrencyKey ?? "",
      },
      metadata: { ...protoSpec.metadata },
      params_len: protoSpec.params.byteLength,
      input_count: inputs.length,
    };

    // Normalized proto spec for specHash (matchesRuntime::normalize before identity::spec_hash!)
    const protoForHash: pb.JobSpecInput = {
      demand: protoSpec.demand,
      inputs: protoSpec.inputs,
      params: protoSpec.params,
      metadata: protoSpec.metadata,
      policy: {
        durability:
          durability === "durable" ? pb.Durability.DURABLE : pb.Durability.EPHEMERAL,
        priority: clampedPriority,
        queueTimeoutMs: queueTimeoutMs !== null ? BigInt(queueTimeoutMs) : 0n,
        executionTimeoutMs: execTimeoutMs !== null ? BigInt(execTimeoutMs) : 0n,
        totalTimeoutMs: totalTimeoutMs !== null ? BigInt(totalTimeoutMs) : 0n,
        spilloverAfterMs: spilloverAfterMs !== null ? BigInt(spilloverAfterMs) : 0n,
        maxAttempts: policyMaxAttemptsNormalized ?? 0,
        fairnessKey: pol?.fairnessKey ?? "",
        concurrencyKey: pol?.concurrencyKey ?? "",
      },
    };

    return {
      spec: storedSpec,
      inputs,
      params: protoSpec.params,
      maxAttempts,
      totalPayloadBytes: totalPayload,
      protoForHash,
    };
  },

  checkQueueAndInputBudgets(
    this: NativeEngine,
    namespace: string,
    interfaceName: string,
    payloadBytes: number,
  ): void {
    let unfinished = 0;
    for (const j of this.jobs.values()) {
      if (j.state === "queued" || j.state === "running") {
        unfinished += 1;
      }
    }
    if (unfinished >= this.config.maxPendingJobs) {
      throw GrpcStatusError.resourceExhausted("unfinished job limit reached");
    }
    const maxQueue =
      this.config.interfaces[interfaceName]?.maxQueue ?? this.config.defaultMaxQueue;
    const curQueued =
      this.queues.get(namespace)?.get(interfaceName)?.totalLen ?? 0;
    if (curQueued >= maxQueue) {
      throw GrpcStatusError.resourceExhausted(
        `queue for interface '${interfaceName}' is full`,
      );
    }
    if (this.totalInputBytesHeld + payloadBytes > this.config.maxPendingBytes) {
      throw GrpcStatusError.resourceExhausted("input budget is exhausted");
    }
  },

  insertAcceptedJobInTxn(this: NativeEngine, params: {
    scopedJobId: string;
    rawJobId: string;
    namespace: string;
    durability: DurabilityName;
    specHash: string;
    spec: StoredSpecMetadata;
    maxAttempts: number;
    totalInputBytes: number;
    retainedBytesEst: number;
    callerConnId: number | null;
    streamingInput: boolean;
    bundleRows: readonly { seq: number; manifest: string; packBlob: Uint8Array | null }[];
    nowMs: number;
  }): JobMem {
    const baseSeq = this.generation << 32n;
    const acceptedSeq = baseSeq + 1n;
    const queueTimeoutMs =
      params.spec.policy.queue_timeout_ms ??
      this.config.interfaces[params.spec.demand.interface]?.queueTimeoutMs ??
      null;
    const queueDeadlineMs =
      queueTimeoutMs !== null ? params.nowMs + queueTimeoutMs : null;
    const totalDeadlineMs =
      params.spec.policy.total_timeout_ms !== null
        ? params.nowMs + params.spec.policy.total_timeout_ms
        : null;
    const hasPreferred =
      Object.keys(params.spec.demand.preferred_attributes).length > 0;
    const spilloverAtMs =
      hasPreferred && params.spec.policy.spillover_after_ms !== null
        ? params.nowMs + params.spec.policy.spillover_after_ms
        : null;

    this.queueSeq += 1n;
    const qSeq = this.queueSeq;

    const job: JobMem = {
      scopedJobId: params.scopedJobId,
      rawJobId: params.rawJobId,
      namespace: params.namespace,
      interfaceName: params.spec.demand.interface,
      state: "queued",
      durability: params.durability,
      specHash: params.specHash,
      spec: params.spec,
      priority: params.spec.policy.priority,
      fairnessKey: params.spec.policy.fairness_key,
      concurrencyKey: params.spec.policy.concurrency_key,
      affinityKey: params.spec.demand.affinity_key,
      maxAttempts: params.maxAttempts,
      attemptCount: 0,
      createdAtMs: params.nowMs,
      updatedAtMs: params.nowMs,
      queuedSinceMs: params.nowMs,
      notBeforeMs: 0,
      queueDeadlineMs,
      totalDeadlineMs,
      spilloverAtMs,
      callerConnId: params.callerConnId,
      streamingInput: params.streamingInput,
      inputEnded: !params.streamingInput,
      streamChunkCount: 0,
      totalInputBytes: params.totalInputBytes,
      retainedBytesEst: params.retainedBytesEst,
      cancelRequested: null,
      error: null,
      winningLeaseId: null,
      resultDigest: null,
      lastAttemptId: null,
      lastWorkerId: null,
      currentLeaseId: null,
      failedWorkers: new Set(),
      rejectedSessions: new Set(),
      lastSequence: acceptedSeq,
      evictedThrough: 0n,
      primaryConsumed: params.callerConnId !== null ? 0n : null,
      queueSeq: qSeq,
      eventLogBytes: EVENT_OVERHEAD_BYTES,
      eventLogCount: 1,
      stalledSinceMs: null,
      inlineAssign: null,
    };

    const lastSplit = splitU64(job.lastSequence);
    const primSplit =
      job.primaryConsumed !== null ? splitU64(job.primaryConsumed) : null;

    this.store.exec(
      `INSERT INTO native_jobs (
        scoped_job_id, raw_job_id, namespace, interface_name, state, durability,
        spec_hash, spec_json, priority, fairness_key, concurrency_key, affinity_key,
        max_attempts, attempt_count, created_at_ms, updated_at_ms, queued_since_ms,
        not_before_ms, queue_deadline_ms, total_deadline_ms, spillover_at_ms,
        caller_conn_id, streaming_input, input_ended, stream_chunk_count,
        total_input_bytes, retained_bytes_est, cancel_requested, error_json,
        winning_lease_id, result_digest, last_attempt_id, last_worker_id,
        failed_workers_json, rejected_sessions_json,
        last_seq_hi, last_seq_lo, evicted_seq_hi, evicted_seq_lo,
        primary_seq_hi, primary_seq_lo
      ) VALUES (
        ?, ?, ?, ?, 'queued', ?,
        ?, ?, ?, ?, ?, ?,
        ?, 0, ?, ?, ?,
        0, ?, ?, ?,
        ?, ?, ?, 0,
        ?, ?, NULL, NULL,
        NULL, NULL, NULL, NULL,
        '[]', '[]',
        ?, ?, 0, 0,
        ?, ?
      )`,
      job.scopedJobId,
      job.rawJobId,
      job.namespace,
      job.interfaceName,
      job.durability,
      job.specHash,
      serializeSpecJson(job.spec),
      job.priority,
      job.fairnessKey,
      job.concurrencyKey,
      job.affinityKey,
      job.maxAttempts,
      job.createdAtMs,
      job.updatedAtMs,
      job.queuedSinceMs,
      job.queueDeadlineMs,
      job.totalDeadlineMs,
      job.spilloverAtMs,
      job.callerConnId,
      job.streamingInput ? 1 : 0,
      job.inputEnded ? 1 : 0,
      job.totalInputBytes,
      job.retainedBytesEst,
      lastSplit.hi,
      lastSplit.lo,
      primSplit ? primSplit.hi : null,
      primSplit ? primSplit.lo : null,
    );

    if (params.bundleRows.length > 0 && (params.spec.input_count > 0 || params.spec.params_len > 0)) {
      this.store.writeBundleRowsInTxn(
        job.scopedJobId,
        "static_in",
        params.bundleRows,
      );
    }

    // Persist Accepted event (1 row)
    this.store.exec(
      `INSERT INTO native_job_events (
        scoped_job_id, seq_hi, seq_lo, attempt_id, at_ms, kind, meta_json, payload, size_bytes
      ) VALUES (?, ?, ?, NULL, ?, 'accepted', NULL, NULL, ?)`,
      job.scopedJobId,
      lastSplit.hi,
      lastSplit.lo,
      params.nowMs,
      EVENT_OVERHEAD_BYTES,
    );

    if (
      !job.streamingInput &&
      job.totalInputBytes <= this.config.assignInlineLimit &&
      params.bundleRows.length > 0 &&
      this.inlineAssignCacheBytes + job.totalInputBytes <= INLINE_ASSIGN_CACHE_MAX_BYTES
    ) {
      const decoded = decodeInlineAssignBundle(params.bundleRows);
      if (decoded !== null && decoded.bytes <= this.config.assignInlineLimit) {
        job.inlineAssign = decoded;
        this.inlineAssignCacheBytes += decoded.bytes;
      }
    }
    this.jobs.set(job.scopedJobId, job);
    this.store.reservedRetainedBytes += job.retainedBytesEst;
    this.totalInputBytesHeld += job.totalInputBytes;
    this.totalEventBytesHeld += EVENT_OVERHEAD_BYTES;

    this.getOrCreateQueue(job.namespace, job.interfaceName).insert(
      this.config.priorityLevels,
      job.priority,
      job.fairnessKey,
      { seq: qSeq, scopedJobId: job.scopedJobId },
    );
    this.committedBatches += 1;
    this.committedOps += 1;
    return job;
  },
};

export type ClientHandlers = typeof clientHandlers;
