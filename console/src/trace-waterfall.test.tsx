// @vitest-environment jsdom
import { cleanup, fireEvent, render, screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ExecutionWaterfall, buildExecutionWaterfall, parseW3cTrace } from "./trace-waterfall";
import type { JobDetailResponse } from "./types";

afterEach(() => {
  cleanup();
});

function makeDetail(overrides: Partial<JobDetailResponse> = {}): JobDetailResponse {
  return {
    job: {
      job_id: "job-wf-1",
      namespace: "default",
      state: "succeeded",
      durability: "durable",
      interface_name: "llm.chat.v1",
      source: null,
      created_at_ms: 1_000,
      updated_at_ms: 1_500,
      duration_ms: 500,
      current_attempt_id: "att-2",
      last_error: null,
      error_code: null,
      error_retryable: null,
      metadata: {
        traceparent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
        tracestate: "congo=t61rcWkgMzE",
        "x-request-id": "req-77",
      },
      demand: null,
      policy: null,
      params_size_bytes: 0,
      params_preview: null,
      params_truncated: false,
      output_preview: null,
      inputs: [],
      outputs: [],
    },
    attempts: [
      {
        attempt_id: "att-1",
        state: "restarted",
        worker_id: "worker-a",
        created_at_ms: 1_050,
        finished_at_ms: 1_150,
        duration_ms: 100,
        last_error: "temporary error",
      },
      {
        attempt_id: "att-2",
        state: "succeeded",
        worker_id: "worker-b",
        created_at_ms: 1_200,
        finished_at_ms: 1_500,
        duration_ms: 300,
        last_error: null,
      },
    ],
    events: [
      { sequence: 1, kind: "accepted", created_at_ms: 1_000, payload_size_bytes: 0 },
      { sequence: 2, kind: "assigned", created_at_ms: 1_050, payload_size_bytes: 0, attempt_id: "att-1" },
      { sequence: 3, kind: "progress", created_at_ms: 1_100, payload_size_bytes: 16, attempt_id: "att-1" },
      { sequence: 4, kind: "attempt_restarted", created_at_ms: 1_150, payload_size_bytes: 0 },
      { sequence: 5, kind: "assigned", created_at_ms: 1_200, payload_size_bytes: 0, attempt_id: "att-2" },
      { sequence: 6, kind: "output", created_at_ms: 1_320, payload_size_bytes: 128, attempt_id: "att-2" },
      { sequence: 7, kind: "finished", created_at_ms: 1_500, payload_size_bytes: 0, attempt_id: "att-2" },
    ],
    history_incomplete: false,
    latest_worker: null,
    ...overrides,
  };
}

describe("parseW3cTrace", () => {
  it("parses valid sampled and unsampled traceparent headers", () => {
    const parsed = parseW3cTrace({
      traceparent: " 00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01 ",
      tracestate: "vendor=ok",
      "x-request-id": "req-1",
    });
    expect(parsed).toEqual({
      traceparent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
      traceparentValid: true,
      version: "00",
      traceId: "4bf92f3577b34da6a3ce929d0e0e4736",
      parentSpanId: "00f067aa0ba902b7",
      traceFlags: "01",
      sampled: true,
      tracestate: "vendor=ok",
      baggage: null,
      requestId: "req-1",
    });

    const unsampled = parseW3cTrace({
      traceparent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00",
    });
    expect(unsampled?.sampled).toBe(false);
  });

  it("rejects invalid traceparent and supports x-request-id fallback", () => {
    expect(
      parseW3cTrace({
        traceparent: "00-00000000000000000000000000000000-00f067aa0ba902b7-01",
      }),
    ).toBeNull();
    expect(
      parseW3cTrace({
        traceparent: "ff-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
      }),
    ).toBeNull();
    expect(
      parseW3cTrace({
        traceparent: "00-4BF92F3577B34DA6A3CE929D0E0E4736-00f067aa0ba902b7-01",
        "x-request-id": "req-fallback",
      }),
    ).toMatchObject({
      traceparentValid: false,
      traceId: null,
      requestId: "req-fallback",
    });
  });
});

describe("buildExecutionWaterfall", () => {
  it("computes queue wait, requeue gaps, exec time, TTFO, and attempt markers", () => {
    const model = buildExecutionWaterfall(makeDetail())!;
    expect(model.totalMs).toBe(500);
    expect(model.queueMs).toBe(100); // 50 ms initial + 50 ms requeue gap
    expect(model.execMs).toBe(400); // 100 ms + 300 ms
    expect(model.ttfoMs).toBe(320); // 1320 - 1000
    expect(model.retries).toBe(1);
    expect(model.spans.map((s) => s.kind)).toEqual(["job", "queue_wait", "attempt", "requeue_wait", "attempt"]);

    const firstAttempt = model.spans[2]!;
    const secondAttempt = model.spans[4]!;
    expect(firstAttempt.markers).toHaveLength(1);
    expect(firstAttempt.markers[0]?.kind).toBe("progress");
    expect(secondAttempt.markers).toHaveLength(1);
    expect(secondAttempt.markers[0]?.kind).toBe("output");
    expect(secondAttempt.markers[0]?.isTtfo).toBe(true);
  });

  it("handles queued jobs with zero attempts", () => {
    const d = makeDetail({
      attempts: [],
      events: [{ sequence: 1, kind: "accepted", created_at_ms: 1_000, payload_size_bytes: 0 }],
    });
    d.job.state = "queued";
    d.job.updated_at_ms = 1_250;
    const model = buildExecutionWaterfall(d)!;
    expect(model.queueMs).toBe(250);
    expect(model.execMs).toBe(0);
    expect(model.anyserveOverheadMs).toBe(250);
    expect(model.overheadExact).toBe(false);
    expect(model.ttfoMs).toBeNull();
    expect(model.retries).toBe(0);
    expect(model.spans.map((s) => s.kind)).toEqual(["job", "queue_wait"]);
  });

  it("separates admit persist, queue wait, relay overhead, and worker handler time", () => {
    const d = makeDetail({
      attempts: [
        {
          attempt_id: "att-1",
          state: "succeeded",
          worker_id: "worker-ws",
          created_at_ms: 1_007,
          finished_at_ms: 1_341,
          duration_ms: 334,
          handler_duration_ms: 1,
          last_error: null,
        },
      ],
      events: [
        { sequence: 1, kind: "accepted", created_at_ms: 1_005, payload_size_bytes: 0 },
        { sequence: 2, kind: "assigned", created_at_ms: 1_007, payload_size_bytes: 0, attempt_id: "att-1" },
        { sequence: 3, kind: "finished", created_at_ms: 1_341, payload_size_bytes: 0, attempt_id: "att-1" },
      ],
    });
    d.job.created_at_ms = 1_000;
    d.job.updated_at_ms = 1_341;
    d.job.duration_ms = 341;

    const model = buildExecutionWaterfall(d)!;
    expect(model.admitMs).toBe(5);
    expect(model.queueMs).toBe(2);
    expect(model.handlerMs).toBe(1);
    expect(model.relayMs).toBe(333);
    expect(model.anyserveOverheadMs).toBe(340);
    expect(model.overheadExact).toBe(true);
    expect(model.spans.map((s) => s.kind)).toEqual(["job", "admit", "queue_wait", "attempt"]);

    render(<ExecutionWaterfall detail={d} />);
    expect(screen.getByText("+340 ms")).toBeTruthy();
    expect(screen.getByText("anyserve · admit")).toBeTruthy();
    expect(screen.getByText("handler 1 ms · relay 333 ms")).toBeTruthy();
  });
});

describe("ExecutionWaterfall component", () => {
  it("renders W3C trace banner, waterfall metrics, and fires trace/worker callbacks", () => {
    const onFilterTrace = vi.fn();
    const onOpenWorker = vi.fn();
    render(<ExecutionWaterfall detail={makeDetail()} onFilterTrace={onFilterTrace} onOpenWorker={onOpenWorker} />);

    const traceBtn = screen.getByText("4bf92f3577b34da6a3ce929d0e0e4736");
    expect(screen.getByText("sampled")).toBeTruthy();
    expect(screen.getByText("req req-77")).toBeTruthy();
    expect(screen.getByText("320 ms")).toBeTruthy(); // TTFO metric

    fireEvent.click(traceBtn);
    expect(onFilterTrace).toHaveBeenCalledWith("4bf92f3577b34da6a3ce929d0e0e4736");

    fireEvent.click(screen.getByText("worker-b"));
    expect(onOpenWorker).toHaveBeenCalledWith("worker-b");
  });
});
