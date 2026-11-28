import { DatabaseSync } from "node:sqlite";
import { describe, expect, it } from "vitest";
import {
  createNodeSqliteStorage,
  InMemoryR2Bucket,
  startNodeJournalGatewayServer,
} from "../src/node-dev-gateway";
import {
  decodeBase64,
  DEFAULT_R2_SPILL_THRESHOLD_BYTES,
  encodeBase64,
  JournalStorage,
  objectR2Key,
} from "../src/storage";

function createTestJournal(options?: {
  thresholdBytes?: number;
  startMs?: number;
  authSecret?: string;
}) {
  const db = new DatabaseSync(":memory:");
  const { storage: sqlStorage, stats } = createNodeSqliteStorage(db, {
    enforceCloudflareLimits: true,
  });
  const r2 = new InMemoryR2Bucket();
  let now = options?.startMs ?? 1_000_000;
  const journal = new JournalStorage(sqlStorage, r2, {
    r2SpillThresholdBytes:
      options?.thresholdBytes ?? DEFAULT_R2_SPILL_THRESHOLD_BYTES,
    nowMs: () => now,
    authSecret: options?.authSecret,
  });
  return {
    db,
    sqlStorage,
    stats,
    r2,
    journal,
    now: () => now,
    advanceMs: (delta: number) => {
      now += delta;
    },
    setNowMs: (value: number) => {
      now = value;
    },
  };
}

function filledBytes(length: number, seed: number): Uint8Array {
  const buf = new Uint8Array(length);
  for (let i = 0; i < length; i++) {
    buf[i] = (seed + i) & 0xff;
  }
  return buf;
}

describe("Cloudflare DO SQLite + R2 harness invariants", () => {
  it("throws immediately if transactionSync returns a Promise", () => {
    const { sqlStorage } = createTestJournal();
    expect(() =>
      sqlStorage.transactionSync(async () => {
        await Promise.resolve();
      }),
    ).toThrow(/Promise|thenable/i);
  });

  it("throws immediately if any SQL statement exceeds 100 bound parameters", () => {
    const { sqlStorage } = createTestJournal();
    const placeholders = Array.from({ length: 101 }, () => "?").join(", ");
    const bindings = Array.from({ length: 101 }, (_, i) => i);
    expect(() =>
      sqlStorage.sql.exec(`SELECT ${placeholders}`, ...bindings),
    ).toThrow(/101 bound parameters/);
  });

  it("throws immediately if any SQL statement exceeds 100 KB", () => {
    const { sqlStorage } = createTestJournal();
    const oversizedSql = `SELECT '${"x".repeat(101 * 1024)}'`;
    expect(() => sqlStorage.sql.exec(oversizedSql)).toThrow(/102400/);
  });

  it("roundtrips RFC 4648 base64 for empty, small, and >1 MiB buffers", () => {
    for (const len of [0, 1, 2, 3, 17, 4096, 1_200_000]) {
      const original = filledBytes(len, 73);
      const encoded = encodeBase64(original);
      const decoded = decodeBase64(encoded);
      expect(decoded).toEqual(original);
    }
  });
});

describe("Contract C endpoints and durability semantics", () => {
  it("handles lease acquire, contention, renew, relinquish, and stale-term fencing", async () => {
    const ctx = createTestJournal({ startMs: 10_000 });

    const acq1 = await ctx.journal.acquire({
      holder_id: "leader-a",
      lease_duration_ms: 5_000,
    });
    expect(acq1).toEqual({ acquired: true, term: 1, active: [], workers: [] });

    // Contending acquire while lease is active fails.
    const acqContend = await ctx.journal.acquire({
      holder_id: "leader-b",
      lease_duration_ms: 5_000,
    });
    expect(acqContend).toEqual({ acquired: false });

    // Renew with right holder & term succeeds; wrong holder or term fails.
    ctx.advanceMs(3_000);
    expect(
      ctx.journal.renew({
        holder_id: "leader-a",
        term: 1,
        lease_duration_ms: 5_000,
      }),
    ).toEqual({ ok: true });
    expect(
      ctx.journal.renew({
        holder_id: "leader-b",
        term: 1,
        lease_duration_ms: 5_000,
      }),
    ).toEqual({ ok: false });
    expect(
      ctx.journal.renew({
        holder_id: "leader-a",
        term: 99,
        lease_duration_ms: 5_000,
      }),
    ).toEqual({ ok: false });

    // Relinquish allows immediate takeover without waiting for TTL.
    expect(ctx.journal.relinquish({ holder_id: "leader-a", term: 1 })).toEqual({
      ok: true,
    });
    const acq2 = await ctx.journal.acquire({
      holder_id: "leader-b",
      lease_duration_ms: 5_000,
    });
    expect(acq2).toEqual({ acquired: true, term: 2, active: [], workers: [] });

    // Stale term=1 commit_batch & purge are fenced and mutate nothing.
    const fencedCommit = await ctx.journal.commitBatch({
      holder_id: "leader-a",
      term: 1,
      ops: [
        {
          type: "accept",
          job_id: "stale-job",
          namespace: "default",
          spec_hash: "h",
          spec_json: "{}",
          inputs: [],
          params_b64: "",
          max_attempts: 3,
          created_at_ms: 13_000,
        },
      ],
    });
    expect(fencedCommit.outcome).toBe("fenced");
    expect(ctx.journal.specHash({ job_id: "stale-job" })).toEqual({
      spec_hash: null,
    });

    const fencedPurge = await ctx.journal.purge({
      holder_id: "leader-a",
      term: 1,
      before_ms: 999_999,
      batch_limit: 500,
    });
    expect(fencedPurge).toEqual({ fenced: true, deleted: 0 });

    // Lease expiry also allows leader-c takeover.
    ctx.advanceMs(5_001);
    expect(
      ctx.journal.renew({
        holder_id: "leader-b",
        term: 2,
        lease_duration_ms: 5_000,
      }),
    ).toEqual({ ok: false });
    const acq3 = await ctx.journal.acquire({
      holder_id: "leader-c",
      lease_duration_ms: 5_000,
    });
    expect(acq3).toEqual({ acquired: true, term: 3, active: [], workers: [] });
  });

  it("commits > 150 ops in one batch without exceeding 100 SQL bound parameters", async () => {
    const ctx = createTestJournal({ startMs: 1_000 });
    const acq = await ctx.journal.acquire({
      holder_id: "leader-1",
      lease_duration_ms: 30_000,
    });
    expect(acq.acquired).toBe(true);

    const ops = [];
    for (let i = 0; i < 165; i++) {
      ops.push({
        type: "accept",
        job_id: `batch-job-${i.toString().padStart(3, "0")}`,
        namespace: i % 2 === 0 ? "default" : "tenant-x",
        spec_hash: `hash-${i}`,
        spec_json: JSON.stringify({
          demand: { interface: "echo" },
          policy: {},
          metadata: { idx: String(i) },
        }),
        inputs: [
          {
            uri: null,
            data_b64: encodeBase64(new Uint8Array([i & 0xff, 1, 2])),
            metadata: { kind: "inline" },
          },
          {
            uri: `s3://bucket/obj-${i}`,
            data_b64: null,
            metadata: { kind: "uri" },
          },
        ],
        params_b64: encodeBase64(new Uint8Array([9, 8, 7])),
        max_attempts: 3,
        created_at_ms: 1_000 + i,
      });
    }

    const res = await ctx.journal.commitBatch({
      holder_id: "leader-1",
      term: 1,
      ops,
    });
    expect(res).toEqual({ outcome: "committed" });
    expect(ctx.stats.maxBoundParams).toBeLessThanOrEqual(10);
    expect(ctx.stats.maxBoundParams).toBeLessThanOrEqual(100);

    const check = await ctx.journal.loadJob({
      job_id: "batch-job-164",
      include_inputs: true,
      include_outputs: false,
      max_output_bytes: null,
    });
    expect(check.job?.job_id).toBe("batch-job-164");
    expect(check.job?.inputs).toHaveLength(2);
    expect(check.job?.inputs[1]?.uri).toBe("s3://bucket/obj-164");
  });

  it("spills > 1 MiB payloads to R2 outside transactionSync and serves 4096-byte Range previews", async () => {
    const ctx = createTestJournal({ startMs: 5_000 });
    await ctx.journal.acquire({
      holder_id: "leader-1",
      lease_duration_ms: 30_000,
    });

    const bigInput = filledBytes(1_200_000, 11);
    const bigParams = filledBytes(1_100_000, 29);
    const bigOutput = filledBytes(1_500_000, 47);
    const smallOutput = filledBytes(8_192, 61);

    const jobId = "@ns:robot-a:job/with:slash";
    const commit1 = await ctx.journal.commitBatch({
      holder_id: "leader-1",
      term: 1,
      ops: [
        {
          type: "accept",
          job_id: jobId,
          namespace: "robot-a",
          spec_hash: "spec-hash-r2",
          spec_json: JSON.stringify({
            demand: { interface: "vision" },
            policy: {},
            metadata: {},
          }),
          inputs: [
            {
              uri: null,
              data_b64: encodeBase64(bigInput),
              metadata: { mime: "application/octet-stream" },
            },
            {
              uri: "s3://external/frame.png",
              data_b64: null,
              metadata: { mime: "image/png" },
            },
          ],
          params_b64: encodeBase64(bigParams),
          max_attempts: 3,
          created_at_ms: 5_100,
        },
        {
          type: "probe",
        },
      ],
    });
    expect(commit1).toEqual({ outcome: "committed" });

    // Confirm R2 keys exist and SQLite content BLOB is NULL for spilled rows.
    const inputR2Key = objectR2Key(jobId, "input", 0);
    const paramsR2Key = objectR2Key(jobId, "params", 0);
    expect(ctx.r2.has(inputR2Key)).toBe(true);
    expect(ctx.r2.has(paramsR2Key)).toBe(true);
    const sqliteSpillRows = ctx.db
      .prepare(
        "SELECT role, content, r2_key, size_bytes FROM journal_objects WHERE job_id = ? AND r2_key IS NOT NULL ORDER BY role",
      )
      .all(jobId) as Array<{
      role: string;
      content: unknown;
      r2_key: string;
      size_bytes: number;
    }>;
    expect(sqliteSpillRows).toEqual([
      {
        role: "input",
        content: null,
        r2_key: inputR2Key,
        size_bytes: 1_200_000,
      },
      {
        role: "params",
        content: null,
        r2_key: paramsR2Key,
        size_bytes: 1_100_000,
      },
    ]);

    // Finish job with one > 1 MiB R2 output, one <= 1 MiB inline output, and one URI output.
    const commit2 = await ctx.journal.commitBatch({
      holder_id: "leader-1",
      term: 1,
      ops: [
        {
          type: "terminal",
          job_id: jobId,
          state: "succeeded",
          outputs: [
            {
              uri: null,
              data_b64: encodeBase64(bigOutput),
              metadata: { part: "big" },
            },
            {
              uri: null,
              data_b64: encodeBase64(smallOutput),
              metadata: { part: "small" },
            },
            {
              uri: "r2://archive/out.bin",
              data_b64: null,
              metadata: { part: "uri" },
            },
          ],
          error_json: null,
          attempt_id: "att-1",
          worker_id: "worker-1",
          winning_lease_id: "lease-win-1",
          outcome_digest: "digest-abc",
          at_ms: 5_500,
        },
      ],
    });
    expect(commit2).toEqual({ outcome: "committed" });

    // Spec hash & receipt lookups.
    expect(ctx.journal.specHash({ job_id: jobId })).toEqual({
      spec_hash: "spec-hash-r2",
    });
    expect(ctx.journal.receipt({ lease_id: "lease-win-1" })).toEqual({
      receipt: { job_id: jobId, result_digest: "digest-abc" },
    });
    expect(ctx.journal.receipt({ lease_id: "missing-lease" })).toEqual({
      receipt: null,
    });

    // Budget enforcement in load_job (uses SQLite size_bytes even for R2-spilled outputs).
    const r2GetsBefore = ctx.r2.getCalls.length;
    const budgetFail = await ctx.journal.loadJob({
      job_id: jobId,
      include_inputs: false,
      include_outputs: true,
      max_output_bytes: 1_500_000 + 8_192 - 1,
    });
    expect(budgetFail).toEqual({
      job: null,
      budget_exceeded: true,
      required_bytes: 1_508_192,
    });
    expect(ctx.r2.getCalls.length).toBe(r2GetsBefore);

    // Preview read uses R2 Range { offset: 0, length: 4096 } and SQLite SUBSTR.
    ctx.r2.getCalls.length = 0;
    const preview = await ctx.journal.loadJobPreview({
      job_id: jobId,
      preview_bytes: 4096,
    });
    expect(preview.job?.job_id).toBe(jobId);
    expect(preview.job?.outputs).toEqual([]);
    expect(preview.input_previews).toEqual([
      {
        uri: null,
        head_b64: encodeBase64(bigInput.subarray(0, 4096)),
        size_bytes: 1_200_000,
        metadata: { mime: "application/octet-stream" },
      },
      {
        uri: "s3://external/frame.png",
        head_b64: null,
        size_bytes: null,
        metadata: { mime: "image/png" },
      },
    ]);
    expect(preview.params_size_bytes).toBe(1_100_000);
    expect(preview.params_head_b64).toBe(
      encodeBase64(bigParams.subarray(0, 4096)),
    );
    expect(preview.output_previews).toEqual([
      {
        uri: null,
        head_b64: encodeBase64(bigOutput.subarray(0, 4096)),
        size_bytes: 1_500_000,
        metadata: { part: "big" },
      },
      {
        uri: null,
        head_b64: encodeBase64(smallOutput.subarray(0, 4096)),
        size_bytes: 8_192,
        metadata: { part: "small" },
      },
      {
        uri: "r2://archive/out.bin",
        head_b64: null,
        size_bytes: null,
        metadata: { part: "uri" },
      },
    ]);
    const outputR2Key = objectR2Key(jobId, "output", 0);
    expect(ctx.r2.getCalls).toEqual([
      { key: inputR2Key, range: { offset: 0, length: 4096 } },
      { key: outputR2Key, range: { offset: 0, length: 4096 } },
      { key: paramsR2Key, range: { offset: 0, length: 4096 } },
    ]);

    // Full load_job materializes complete inputs, params, and outputs.
    const loaded = await ctx.journal.loadJob({
      job_id: jobId,
      include_inputs: true,
      include_outputs: true,
      max_output_bytes: 1_508_192,
    });
    expect(loaded.budget_exceeded).toBe(false);
    expect(loaded.required_bytes).toBe(1_508_192);
    expect(decodeBase64(loaded.job!.inputs[0]!.data_b64!)).toEqual(bigInput);
    expect(decodeBase64(loaded.job!.params_b64)).toEqual(bigParams);
    expect(decodeBase64(loaded.job!.outputs[0]!.data_b64!)).toEqual(bigOutput);
    expect(decodeBase64(loaded.job!.outputs[1]!.data_b64!)).toEqual(
      smallOutput,
    );
    expect(loaded.job!.outputs[2]!.uri).toBe("r2://archive/out.bin");
  });

  it("recovers queued jobs with full inputs, params, and failed_workers across failover", async () => {
    const ctx = createTestJournal({ thresholdBytes: 16, startMs: 1_000 });
    await ctx.journal.acquire({
      holder_id: "leader-1",
      lease_duration_ms: 5_000,
    });

    const inputA = filledBytes(64, 5);
    const paramsA = filledBytes(48, 9);

    await ctx.journal.commitBatch({
      holder_id: "leader-1",
      term: 1,
      ops: [
        {
          type: "accept",
          job_id: "job-b",
          namespace: "default",
          spec_hash: "h-b",
          spec_json: "{}",
          inputs: [
            { uri: null, data_b64: encodeBase64(inputA), metadata: { a: "1" } },
          ],
          params_b64: encodeBase64(paramsA),
          max_attempts: 4,
          created_at_ms: 1_100,
        },
        {
          type: "accept",
          job_id: "job-a",
          namespace: "default",
          spec_hash: "h-a",
          spec_json: "{}",
          inputs: [],
          params_b64: "",
          max_attempts: 3,
          created_at_ms: 1_100,
        },
      ],
    });

    // Two failed/interrupted attempts on different workers for job-b.
    await ctx.journal.commitBatch({
      holder_id: "leader-1",
      term: 1,
      ops: [
        {
          type: "attempt_start",
          job_id: "job-b",
          attempt_number: 1,
          at_ms: 1_200,
          attempt_id: "att-1",
          worker_id: "worker-alpha",
        },
      ],
    });
    await ctx.journal.commitBatch({
      holder_id: "leader-1",
      term: 1,
      ops: [
        {
          type: "attempt_start",
          job_id: "job-b",
          attempt_number: 2,
          at_ms: 1_300,
          attempt_id: null,
          worker_id: "worker-beta",
        },
      ],
    });

    // Simulate leader-1 crash: lease expires, leader-2 takes over.
    ctx.advanceMs(6_000);
    const acq2 = await ctx.journal.acquire({
      holder_id: "leader-2",
      lease_duration_ms: 10_000,
    });
    expect(acq2.acquired).toBe(true);
    if (!acq2.acquired) throw new Error("expected acquired");
    expect(acq2.term).toBe(2);
    expect(acq2.active.map((j) => j.job_id)).toEqual(["job-a", "job-b"]);

    const recoveredB = acq2.active[1]!;
    expect(recoveredB.attempt_count).toBe(2);
    expect(recoveredB.last_attempt_id).toBe("att-1"); // COALESCE kept att-1
    expect(recoveredB.last_worker_id).toBe("worker-beta");
    expect(recoveredB.failed_workers).toEqual(["worker-alpha", "worker-beta"]);
    expect(decodeBase64(recoveredB.inputs[0]!.data_b64!)).toEqual(inputA);
    expect(decodeBase64(recoveredB.params_b64)).toEqual(paramsA);
  });

  it("rolls back atomic batch on op failure and cleans up newly uploaded R2 spills", async () => {
    const ctx = createTestJournal({ thresholdBytes: 16, startMs: 1_000 });
    await ctx.journal.acquire({
      holder_id: "leader-1",
      lease_duration_ms: 10_000,
    });

    const spilled = filledBytes(64, 3);
    const res = await ctx.journal.commitBatch({
      holder_id: "leader-1",
      term: 1,
      ops: [
        {
          type: "accept",
          job_id: "rollback-new",
          namespace: "default",
          spec_hash: "h1",
          spec_json: "{}",
          inputs: [
            { uri: null, data_b64: encodeBase64(spilled), metadata: {} },
          ],
          params_b64: "",
          max_attempts: 2,
          created_at_ms: 1_000,
        },
        {
          // Fails because "missing-job" does not exist.
          type: "attempt_start",
          job_id: "missing-job",
          attempt_number: 1,
          at_ms: 1_001,
          attempt_id: "a1",
          worker_id: "w1",
        },
      ],
    });

    expect(res.outcome).toBe("rolled_back");
    expect(ctx.journal.specHash({ job_id: "rollback-new" })).toEqual({
      spec_hash: null,
    });
    expect(ctx.r2.size).toBe(0);
  });

  it("supports scan_jobs cursor pagination across namespaces and ties", async () => {
    const ctx = createTestJournal({ startMs: 1_000 });
    await ctx.journal.acquire({
      holder_id: "leader-1",
      lease_duration_ms: 10_000,
    });

    const fixtures = [
      { id: "j1", ns: "default", at: 100, state: "succeeded" as const },
      { id: "@ns:alpha:j2", ns: "alpha", at: 200, state: "failed" as const },
      { id: "@ns:beta:j3", ns: "beta", at: 200, state: "cancelled" as const },
      { id: "j4", ns: "default", at: 200, state: "succeeded" as const },
      { id: "j-queued", ns: "default", at: 250, state: "queued" as const },
      { id: "j5", ns: "default", at: 300, state: "succeeded" as const },
    ];

    const ops: Record<string, unknown>[] = [];
    for (const f of fixtures) {
      ops.push({
        type: "accept",
        job_id: f.id,
        namespace: f.ns,
        spec_hash: `h-${f.id}`,
        spec_json: "{}",
        inputs: [
          {
            uri: null,
            data_b64: encodeBase64(new Uint8Array([1, 2, 3])),
            metadata: {},
          },
        ],
        params_b64: encodeBase64(new Uint8Array([4, 5])),
        max_attempts: 3,
        created_at_ms: f.at,
      });
    }
    expect(
      (await ctx.journal.commitBatch({ holder_id: "leader-1", term: 1, ops }))
        .outcome,
    ).toBe("committed");

    const termOps = fixtures
      .filter((f) => f.state !== "queued")
      .map((f) => ({
        type: "terminal",
        job_id: f.id,
        state: f.state,
        outputs: [
          {
            uri: null,
            data_b64: encodeBase64(new Uint8Array([7, 8])),
            metadata: {},
          },
        ],
        error_json: f.state === "failed" ? '{"code":"err","message":"bad","retryable":false}' : null,
        attempt_id: null,
        worker_id: null,
        winning_lease_id: null,
        outcome_digest: null,
        at_ms: f.at + 10,
      }));
    expect(
      (
        await ctx.journal.commitBatch({
          holder_id: "leader-1",
          term: 1,
          ops: termOps,
        })
      ).outcome,
    ).toBe("committed");

    // Global scan in [100, 300) excludes j5 (at 300, exclusive) and j-queued.
    const page1 = ctx.journal.scanJobs({
      from_ms: 100,
      to_ms: 300,
      before: null,
      namespace: null,
      limit: 2,
    });
    expect(page1.jobs.map((j) => j.job_id)).toEqual(["j4", "@ns:beta:j3"]);
    expect(page1.jobs.every((j) => j.inputs.length === 0 && j.outputs.length === 0 && j.params_b64 === "")).toBe(true);

    const last1 = page1.jobs[1]!;
    const page2 = ctx.journal.scanJobs({
      from_ms: 100,
      to_ms: 300,
      before: { created_at_ms: last1.created_at_ms, job_id: last1.job_id },
      namespace: null,
      limit: 2,
    });
    expect(page2.jobs.map((j) => j.job_id)).toEqual(["@ns:alpha:j2", "j1"]);

    // Namespace-scoped scan returns only alpha jobs.
    const alphaOnly = ctx.journal.scanJobs({
      from_ms: 0,
      to_ms: 1000,
      before: null,
      namespace: "alpha",
      limit: 251,
    });
    expect(alphaOnly.jobs.map((j) => j.job_id)).toEqual(["@ns:alpha:j2"]);
  });

  it("purges expired terminal jobs, failed_workers, and chunks > 1000 R2 deletions", async () => {
    const ctx = createTestJournal({ thresholdBytes: 8, startMs: 1_000 });
    await ctx.journal.acquire({
      holder_id: "leader-1",
      lease_duration_ms: 30_000,
    });

    // Create 120 expired terminal jobs each with 10 R2 outputs (1,200 R2 keys total)
    // plus 1 newer terminal job and 1 queued job.
    const outputs = Array.from({ length: 10 }, () => ({
      uri: null,
      data_b64: encodeBase64(filledBytes(16, 1)),
      metadata: {},
    }));
    const acceptOps = [];
    const startOps = [];
    const termOps = [];
    for (let i = 0; i < 120; i++) {
      const id = `old-${i}`;
      acceptOps.push({
        type: "accept",
        job_id: id,
        namespace: "default",
        spec_hash: `h-${i}`,
        spec_json: "{}",
        inputs: [],
        params_b64: "",
        max_attempts: 3,
        created_at_ms: 100 + i,
      });
      startOps.push({
        type: "attempt_start",
        job_id: id,
        attempt_number: 1,
        at_ms: 200 + i,
        attempt_id: `att-${i}`,
        worker_id: `worker-${i}`,
      });
      termOps.push({
        type: "terminal",
        job_id: id,
        state: "succeeded",
        outputs,
        error_json: null,
        attempt_id: `att-${i}`,
        worker_id: `worker-${i}`,
        winning_lease_id: null,
        outcome_digest: null,
        at_ms: 500 + i,
      });
    }
    expect(
      (
        await ctx.journal.commitBatch({
          holder_id: "leader-1",
          term: 1,
          ops: acceptOps,
        })
      ).outcome,
    ).toBe("committed");
    expect(
      (
        await ctx.journal.commitBatch({
          holder_id: "leader-1",
          term: 1,
          ops: startOps,
        })
      ).outcome,
    ).toBe("committed");
    expect(
      (
        await ctx.journal.commitBatch({
          holder_id: "leader-1",
          term: 1,
          ops: termOps,
        })
      ).outcome,
    ).toBe("committed");

    // Add 1 newer terminal job and 1 old queued job that must NOT be purged.
    await ctx.journal.commitBatch({
      holder_id: "leader-1",
      term: 1,
      ops: [
        {
          type: "accept",
          job_id: "recent-term",
          namespace: "default",
          spec_hash: "hr",
          spec_json: "{}",
          inputs: [],
          params_b64: "",
          max_attempts: 3,
          created_at_ms: 100,
        },
        {
          type: "accept",
          job_id: "old-queued",
          namespace: "default",
          spec_hash: "hq",
          spec_json: "{}",
          inputs: [
            {
              uri: null,
              data_b64: encodeBase64(filledBytes(16, 2)),
              metadata: {},
            },
          ],
          params_b64: "",
          max_attempts: 3,
          created_at_ms: 100,
        },
      ],
    });
    await ctx.journal.commitBatch({
      holder_id: "leader-1",
      term: 1,
      ops: [
        {
          type: "terminal",
          job_id: "recent-term",
          state: "failed",
          outputs: [],
          error_json: null,
          attempt_id: null,
          worker_id: null,
          winning_lease_id: null,
          outcome_digest: null,
          at_ms: 2_000,
        },
      ],
    });

    expect(ctx.r2.size).toBe(1_201);

    const purged = await ctx.journal.purge({
      holder_id: "leader-1",
      term: 1,
      before_ms: 1_000,
      batch_limit: 500,
    });
    expect(purged).toEqual({ fenced: false, deleted: 120 });

    // 1,200 R2 keys deleted in chunks of <= 1000 (1000 + 200), keeping old-queued's 1 R2 key.
    expect(ctx.r2.deleteCalls.map((c) => c.length)).toEqual([1000, 200]);
    expect(ctx.r2.size).toBe(1);
    expect(ctx.r2.has(objectR2Key("old-queued", "input", 0))).toBe(true);

    const remainingJobs = ctx.db
      .prepare("SELECT job_id FROM journal_jobs ORDER BY job_id")
      .all() as Array<{ job_id: string }>;
    expect(remainingJobs.map((r) => r.job_id)).toEqual([
      "old-queued",
      "recent-term",
    ]);
    const remainingFailedWorkers = ctx.db
      .prepare("SELECT COUNT(*) AS cnt FROM journal_failed_workers")
      .get() as { cnt: number };
    expect(remainingFailedWorkers.cnt).toBe(0);
  });

  it("enforces optional bearer auth and serves all /v1/* endpoints over HTTP via node-dev-gateway", async () => {
    const gateway = await startNodeJournalGatewayServer({
      authSecret: "super-secret-token",
      r2SpillThresholdBytes: 32,
    });
    try {
      // Missing auth returns 401.
      const unauth = await fetch(`${gateway.url}/v1/acquire`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ holder_id: "h1", lease_duration_ms: 5000 }),
      });
      expect(unauth.status).toBe(401);

      // Non-POST returns 405.
      const wrongMethod = await fetch(`${gateway.url}/v1/acquire`, {
        method: "GET",
        headers: { authorization: "Bearer super-secret-token" },
      });
      expect(wrongMethod.status).toBe(405);

      const post = async <T>(path: string, body: unknown): Promise<T> => {
        const res = await fetch(`${gateway.url}${path}`, {
          method: "POST",
          headers: {
            "content-type": "application/json",
            authorization: "Bearer super-secret-token",
          },
          body: JSON.stringify(body),
        });
        expect(res.status).toBe(200);
        return (await res.json()) as T;
      };

      const acq = await post<{ acquired: boolean; term?: number }>(
        "/v1/acquire",
        {
          holder_id: "node-leader",
          lease_duration_ms: 10_000,
        },
      );
      expect(acq).toEqual({ acquired: true, term: 1, active: [], workers: [] });

      const payload = filledBytes(64, 99);
      expect(
        await post("/v1/commit_batch", {
          holder_id: "node-leader",
          term: 1,
          ops: [
            {
              type: "accept",
              job_id: "http-job-1",
              namespace: "default",
              spec_hash: "http-hash",
              spec_json: "{}",
              inputs: [
                { uri: null, data_b64: encodeBase64(payload), metadata: {} },
              ],
              params_b64: "",
              max_attempts: 2,
              created_at_ms: 500,
            },
          ],
        }),
      ).toEqual({ outcome: "committed" });

      expect(await post("/v1/spec_hash", { job_id: "http-job-1" })).toEqual({
        spec_hash: "http-hash",
      });
      expect(
        await post("/v1/relinquish", { holder_id: "node-leader", term: 1 }),
      ).toEqual({ ok: true });
    } finally {
      await gateway.close();
    }
  });

  it("persists, forgets, fences, and recovers worker registry records", async () => {
    const ctx = createTestJournal({ startMs: 1_000 });
    const acq1 = await ctx.journal.acquire({
      holder_id: "leader-1",
      lease_duration_ms: 5_000,
    });
    expect(acq1).toEqual({ acquired: true, term: 1, active: [], workers: [] });

    const recA = JSON.stringify({
      namespace: "default",
      worker_id: "jetson-orin-nx",
      pool: "edge",
      interfaces: ["vision.v1"],
      last_seen_ms: 1_500,
      offline_since_ms: 1_600,
      offline_reason: "disconnected",
    });
    const recB = JSON.stringify({
      namespace: "tenant-a",
      worker_id: "rk3588-01",
      pool: "npu",
      interfaces: ["vision.v1"],
      last_seen_ms: 1_800,
      offline_since_ms: 1_900,
      offline_reason: "worker session ended",
    });

    expect(
      await ctx.journal.commitBatch({
        holder_id: "leader-1",
        term: 1,
        ops: [
          {
            type: "worker_upsert",
            namespace: "default",
            worker_id: "jetson-orin-nx",
            record_json: recA,
            updated_at_ms: 1_600,
          },
          {
            type: "worker_upsert",
            namespace: "tenant-a",
            worker_id: "rk3588-01",
            record_json: recB,
            updated_at_ms: 1_900,
          },
        ],
      }),
    ).toEqual({ outcome: "committed" });

    // Stale term is fenced and cannot mutate workers.
    expect(
      await ctx.journal.commitBatch({
        holder_id: "leader-1",
        term: 99,
        ops: [
          {
            type: "worker_forget",
            namespace: "default",
            worker_id: "jetson-orin-nx",
          },
        ],
      }),
    ).toMatchObject({ outcome: "fenced" });

    // Forget tenant-a worker under valid term.
    expect(
      await ctx.journal.commitBatch({
        holder_id: "leader-1",
        term: 1,
        ops: [
          {
            type: "worker_forget",
            namespace: "tenant-a",
            worker_id: "rk3588-01",
          },
        ],
      }),
    ).toEqual({ outcome: "committed" });

    // Relinquish and re-acquire as leader-2 -> recovers only jetson-orin-nx.
    expect(
      ctx.journal.relinquish({ holder_id: "leader-1", term: 1 }),
    ).toEqual({ ok: true });
    const acq2 = await ctx.journal.acquire({
      holder_id: "leader-2",
      lease_duration_ms: 5_000,
    });
    expect(acq2).toEqual({
      acquired: true,
      term: 2,
      active: [],
      workers: [recA],
    });
  });
});
