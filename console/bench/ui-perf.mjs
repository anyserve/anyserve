#!/usr/bin/env node
/**
 * Measure live browser rendering and interaction latency of the built Console UI
 * against a realistic 250-job / 40-worker / 12-interface / 20-pool dataset.
 */
import { spawn } from "node:child_process";
import fs from "node:fs";
import http from "node:http";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const DIST_DIR = path.join(ROOT, "dist");
const CHROME_BIN = process.env.CHROME_BIN || ["/usr/bin/google-chrome", "/usr/bin/chromium", "/usr/bin/chromium-browser"].find((p) => fs.existsSync(p));

if (!fs.existsSync(path.join(DIST_DIR, "index.html"))) {
  console.error("console/dist not found; run `npm --prefix console run build` first.");
  process.exit(1);
}
if (!CHROME_BIN) {
  console.error("Chrome/Chromium binary not found; set CHROME_BIN.");
  process.exit(1);
}

const MIME = {
  ".html": "text/html; charset=utf-8",
  ".js": "text/javascript; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".svg": "image/svg+xml",
  ".wasm": "application/wasm",
  ".json": "application/json",
};

function makeDataset() {
  const now = Date.now();
  const interfaces = Array.from({ length: 12 }, (_, i) => ({
    name: i % 3 === 0 ? `openai/chat/model-${i}` : i % 3 === 1 ? `openai/embeddings/embed-${i}` : `triton/vision-${i}`,
    queued: i < 3 ? 2 : 0,
    running: i < 5 ? 3 : 0,
    oldest_queued_age_ms: i < 3 ? 450 + i * 120 : 0,
    sessions: 3,
    free_slots: 9,
    policy: {
      max_queue: 256,
      queue_timeout_ms: 30_000,
      max_in_flight_per_key: 8,
      pool_tiers: [
        { pool: "", spillover_after_ms: 0, reconnect_grace_ms: 2_000 },
        { pool: "gpu-spillover", spillover_after_ms: 200, reconnect_grace_ms: 5_000 },
      ],
    },
  }));

  const pools = Array.from({ length: 20 }, (_, i) => ({
    namespace: i % 4 === 0 ? "tenant-a" : "default",
    interface: interfaces[i % interfaces.length].name,
    pool: i % 2 === 0 ? "" : "gpu-spillover",
    sessions: 2,
    free_slots: 6,
    active_leases: i % 3,
    desired_additional_slots: i === 1 ? 3 : 0,
  }));

  const workers = Array.from({ length: 40 }, (_, i) => ({
    worker_id: `worker-node-${String(i).padStart(2, "0")}`,
    session_id: `sess-${String(i).padStart(4, "0")}-abcdef`,
    namespace: i % 4 === 0 ? "tenant-a" : "default",
    pool: i % 2 === 0 ? "" : "gpu-spillover",
    healthy: i < 36,
    connected: i < 38,
    draining: i >= 36 && i < 38,
    interfaces: [interfaces[i % interfaces.length].name, interfaces[(i + 1) % interfaces.length].name],
    attributes: { site: i % 2 === 0 ? "edge" : "cloud", arch: "cuda" },
    total_capacity: { gpu: 1, slot: 8 },
    max_concurrency: 8,
    active_leases: i % 5,
    free_slots: 8 - (i % 5),
    utilization: (i % 5) / 8,
    connected_at_ms: now - 300_000,
    last_seen_at_ms: now - i * 100,
    load: { queued: 0, running: i % 5, utilization: (i % 5) / 8, custom: { vram_gb: 24 } },
  }));

  const largeJson = JSON.stringify({
    object: "list",
    model: "bge-m3",
    data: [{ object: "embedding", index: 0, embedding: Array.from({ length: 300 }, (_, k) => Number((Math.sin(k) * 0.1).toFixed(4))) }],
  });

  const jobs = Array.from({ length: 250 }, (_, i) => ({
    job_id: `job-${String(i).padStart(4, "0")}-8e43-4c80-b746`,
    namespace: i % 4 === 0 ? "tenant-a" : "default",
    state: i < 8 ? "running" : i < 12 ? "queued" : i % 15 === 0 ? "failed" : "succeeded",
    durability: i % 2 === 0 ? "durable" : "ephemeral",
    interface_name: interfaces[i % interfaces.length].name,
    source: i % 2 === 0 ? "openai-client" : "triton-client",
    created_at_ms: now - i * 1000,
    updated_at_ms: now - i * 950,
    duration_ms: i < 12 ? null : 18 + (i % 40),
    current_attempt_id: i < 12 ? `att-${i}` : null,
    latest_worker_id: i >= 8 && i < 12 ? null : workers[i % workers.length].worker_id,
    attempt_count: i >= 8 && i < 12 ? 0 : 1,
    last_error: i % 15 === 0 ? "HTTP 503: upstream busy" : null,
    error_code: i % 15 === 0 ? "worker_failed" : null,
    error_retryable: i % 15 === 0 ? true : null,
    metadata: { request_id: `req-${i}` },
  }));

  const detailMap = new Map(
    jobs.map((summary, i) => {
      const isLarge = i === 1 || i % 3 === 0;
      return [
        summary.job_id,
        {
          job: {
            ...summary,
            demand: {
              interface: summary.interface_name,
              required_attributes: {},
              preferred_attributes: { site: "edge" },
              required_capacity: { slot: 1 },
              affinity_key: i % 5 === 0 ? "session-chat-42" : "",
            },
            policy: {
              priority: 1,
              max_attempts: 3,
              durability: summary.durability === "durable" ? "Durable" : "Ephemeral",
              queue_timeout: { secs: 30, nanos: 0 },
              execution_timeout: { secs: 60, nanos: 0 },
              total_timeout: null,
              spillover_after: { secs: 0, nanos: 200_000_000 },
              fairness_key: "tenant-rag",
              concurrency_key: "",
            },
            params_size_bytes: isLarge ? 2048 : 112,
            params_preview: isLarge ? largeJson.slice(0, 4096) : '{"input":"hello world","model":"bge-m3"}',
            params_truncated: false,
            output_preview: {
              size_bytes: isLarge ? 16_777_216 : 128,
              content_type: "application/json",
              preview: isLarge ? largeJson.slice(0, 4096) : '{"ok":true,"tokens":12}',
              truncated: isLarge,
            },
            inputs: [],
            outputs: [],
          },
          attempts: summary.latest_worker_id
            ? [{ attempt_id: `att-${i}`, state: summary.state === "running" ? "assigned" : summary.state, worker_id: summary.latest_worker_id, created_at_ms: summary.created_at_ms, finished_at_ms: summary.duration_ms ? summary.updated_at_ms : null, duration_ms: summary.duration_ms, last_error: summary.last_error }]
            : [],
          events: [
            { sequence: 1, kind: "accepted", created_at_ms: summary.created_at_ms, payload_size_bytes: 0 },
            ...(summary.latest_worker_id ? [{ sequence: 2, kind: "assigned", created_at_ms: summary.created_at_ms + 2, payload_size_bytes: 0 }] : []),
          ],
          history_incomplete: false,
          latest_worker: summary.latest_worker_id ? workers[i % workers.length] : null,
        },
      ];
    }),
  );

  return {
    overview: {
      runtime: { code: "memory_plus_sqlite", label: "Memory + SQLite journal", journal: "sqlite" },
      health: { live: true, ready: true, journal: { status: "healthy", message: null } },
      control_plane: {
        grpc_listen: "127.0.0.1:50052", grpc_tls_enabled: false, console_listen: "127.0.0.1:3001", auth_enabled: false,
        generation: 1, retained_jobs: jobs.length, sessions: workers.length, max_sessions: 4096,
        priority_levels: 4, default_max_attempts: 3, heartbeat_interval_ms: 5000, heartbeat_timeout_ms: 15000, reconnect_grace_ms: 15000,
      },
      budgets: { input_bytes: 4096, max_pending_bytes: 2_147_483_648, result_bytes: 16_384, max_result_bytes: 2_147_483_648, event_bytes: 8192, max_event_bytes: 1_073_741_824 },
      journal_stats: { pending_ops: 0, committed_batches: 500, committed_ops: 1200, last_batch_size: 4, last_commit_micros: 420 },
      queue: { queued: 4, running: 8, active: 12 },
      history: { succeeded: 222, failed: 16, cancelled: 0, total: 238 },
      workers: { total: 40, healthy: 36, unavailable: 4, draining: 2, active_leases: 28, total_capacity: { gpu: 40, slot: 320 }, last_seen_at_ms: now },
      interfaces,
      pools,
      refreshed_at_ms: now,
    },
    workers: { workers, total: 40, healthy: 36, draining: 2, disconnected: 2 },
    jobs,
    detailMap,
  };
}

function startServer(data) {
  const server = http.createServer((req, res) => {
    const url = new URL(req.url, "http://127.0.0.1");
    if (url.pathname === "/api/overview") {
      data.overview.refreshed_at_ms = Date.now();
      res.writeHead(200, { "Content-Type": "application/json" });
      return res.end(JSON.stringify(data.overview));
    }
    if (url.pathname === "/api/workers") {
      res.writeHead(200, { "Content-Type": "application/json" });
      return res.end(JSON.stringify(data.workers));
    }
    if (url.pathname === "/api/jobs") {
      const states = (url.searchParams.get("states") || "").split(",").filter(Boolean);
      const iface = url.searchParams.get("interface");
      const ns = url.searchParams.get("namespace");
      const limit = Number(url.searchParams.get("limit") || 100);
      const filtered = data.jobs.filter(
        (j) =>
          (states.length === 0 || states.includes(j.state)) &&
          (!iface || j.interface_name === iface) &&
          (!ns || j.namespace === ns),
      );
      res.writeHead(200, { "Content-Type": "application/json" });
      return res.end(JSON.stringify({ jobs: filtered.slice(0, limit), next_page_token: null }));
    }
    if (url.pathname.startsWith("/api/jobs/")) {
      const id = decodeURIComponent(url.pathname.slice("/api/jobs/".length));
      const detail = data.detailMap.get(id);
      res.writeHead(detail ? 200 : 404, { "Content-Type": "application/json" });
      return res.end(JSON.stringify(detail ?? { error: "not found" }));
    }

    let filePath = path.join(DIST_DIR, url.pathname === "/" ? "index.html" : url.pathname);
    if (!fs.existsSync(filePath) || fs.statSync(filePath).isDirectory()) {
      filePath = path.join(DIST_DIR, "index.html");
    }
    const ext = path.extname(filePath);
    res.writeHead(200, { "Content-Type": MIME[ext] || "application/octet-stream" });
    fs.createReadStream(filePath).pipe(res);
  });

  return new Promise((resolve) => {
    server.listen(0, "127.0.0.1", () => {
      const { port } = server.address();
      resolve({ server, url: `http://127.0.0.1:${port}` });
    });
  });
}

class CdpClient {
  constructor(wsUrl) {
    this.ws = new WebSocket(wsUrl);
    this.nextId = 1;
    this.pending = new Map();
    this.ready = new Promise((resolve, reject) => {
      this.ws.addEventListener("open", resolve);
      this.ws.addEventListener("error", reject);
    });
    this.ws.addEventListener("message", (event) => {
      const msg = JSON.parse(event.data);
      if (msg.id && this.pending.has(msg.id)) {
        const { resolve, reject } = this.pending.get(msg.id);
        this.pending.delete(msg.id);
        if (msg.error) reject(new Error(msg.error.message));
        else resolve(msg.result);
      }
    });
  }
  async send(method, params = {}) {
    await this.ready;
    const id = this.nextId++;
    return new Promise((resolve, reject) => {
      this.pending.set(id, { resolve, reject });
      this.ws.send(JSON.stringify({ id, method, params }));
    });
  }
  async eval(expression) {
    const out = await this.send("Runtime.evaluate", { expression, awaitPromise: true, returnByValue: true });
    if (out.exceptionDetails) throw new Error(out.exceptionDetails.text || "Evaluation failed");
    return out.result.value;
  }
  close() { this.ws.close(); }
}

async function launchChrome(url) {
  const userDataDir = fs.mkdtempSync(path.join(os.tmpdir(), "anyserve-ui-bench-"));
  const proc = spawn(
    CHROME_BIN,
    [
      "--headless=new",
      "--no-sandbox",
      "--disable-gpu",
      "--remote-debugging-port=0",
      "--window-size=1440,900",
      `--user-data-dir=${userDataDir}`,
      url,
    ],
    { stdio: ["ignore", "ignore", "pipe"] },
  );

  const wsUrl = await new Promise((resolve, reject) => {
    const timeout = setTimeout(() => reject(new Error("Timed out waiting for Chrome DevTools WS")), 10_000);
    proc.stderr.on("data", (chunk) => {
      const match = String(chunk).match(/DevTools listening on (ws:\/\/[^\s]+)/);
      if (match) {
        clearTimeout(timeout);
        resolve(match[1]);
      }
    });
    proc.on("exit", (code) => {
      clearTimeout(timeout);
      reject(new Error(`Chrome exited early with code ${code}`));
    });
  });

  const targets = await (await fetch(wsUrl.replace("ws://", "http://").replace(/\/devtools\/.*/, "/json/list"))).json();
  const pageTarget = targets.find((t) => t.type === "page");
  const cdp = new CdpClient(pageTarget.webSocketDebuggerUrl);
  await cdp.send("Runtime.enable");
  await cdp.send("Performance.enable");
  return { proc, cdp, userDataDir };
}

async function waitForSelector(cdp, selector) {
  const deadline = Date.now() + 8_000;
  while (Date.now() < deadline) {
    if (await cdp.eval(`Boolean(document.querySelector(${JSON.stringify(selector)}))`)) return;
    await new Promise((r) => setTimeout(r, 25));
  }
  throw new Error(`Timed out waiting for selector: ${selector}`);
}

async function measureFrame(cdp, fnExpr) {
  return cdp.eval(`(async () => {
    const t0 = performance.now();
    ${fnExpr}
    await new Promise((r) => requestAnimationFrame(() => requestAnimationFrame(r)));
    return Number((performance.now() - t0).toFixed(2));
  })()`);
}

async function main() {
  const dataset = makeDataset();
  const { server, url } = await startServer(dataset);
  let chrome;
  try {
    chrome = await launchChrome(url);
    const { cdp } = chrome;
    await waitForSelector(cdp, ".overview-page");

    const loadMs = await cdp.eval(`Number(performance.getEntriesByType("navigation")[0]?.domContentLoadedEventEnd.toFixed(2) || 0)`);
    const overviewToJobsMs = await measureFrame(cdp, `document.querySelectorAll(".sidebar__item")[1].click();`);
    await waitForSelector(cdp, ".table tbody tr");
    const smallJobMs = await measureFrame(cdp, `document.querySelectorAll(".table tbody tr")[0].click();`);
    const largeJobMs = await measureFrame(cdp, `document.querySelectorAll(".table tbody tr")[1].click();`);
    await waitForSelector(cdp, ".payload-bar");
    const collapsedDomNodes = await cdp.eval(`document.querySelectorAll("*").length`);
    const expandLargeMs = await measureFrame(cdp, `document.querySelector(".payload-bar button")?.click();`);
    const expandedDomNodes = await cdp.eval(`document.querySelectorAll("*").length`);
    const searchFilterMs = await measureFrame(cdp, `
      const input = document.querySelector(".search-box__input");
      const setter = Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, "value").set;
      setter.call(input, "job-005");
      input.dispatchEvent(new Event("input", { bubbles: true }));
    `);
    const clearSearchMs = await measureFrame(cdp, `
      const input = document.querySelector(".search-box__input");
      const setter = Object.getOwnPropertyDescriptor(HTMLInputElement.prototype, "value").set;
      setter.call(input, "");
      input.dispatchEvent(new Event("input", { bubbles: true }));
    `);
    const jobsToInterfacesMs = await measureFrame(cdp, `document.querySelectorAll(".sidebar__item")[2].click();`);
    const interfacesToPoolsMs = await measureFrame(cdp, `document.querySelectorAll(".sidebar__item")[3].click();`);
    const poolsToWorkersMs = await measureFrame(cdp, `document.querySelectorAll(".sidebar__item")[4].click();`);
    await waitForSelector(cdp, ".occupancy-cell");
    const openConnectModalMs = await measureFrame(cdp, `
      Array.from(document.querySelectorAll("button")).find((b) => b.textContent.includes("Connect Worker")).click();
    `);
    const horizontalOverflow = await cdp.eval(`document.documentElement.scrollWidth > document.documentElement.clientWidth`);

    console.log(JSON.stringify({
      dataset: { jobs: 250, workers: 40, interfaces: 12, pools: 20, large_payload_bytes: 16_777_216 },
      timings_ms: {
        dom_content_loaded: loadMs,
        tab_overview_to_jobs: overviewToJobsMs,
        select_small_job: smallJobMs,
        select_large_job_collapsed: largeJobMs,
        expand_large_payload_preview: expandLargeMs,
        search_250_jobs: searchFilterMs,
        clear_search_250_jobs: clearSearchMs,
        tab_jobs_to_interfaces: jobsToInterfacesMs,
        tab_interfaces_to_pools: interfacesToPoolsMs,
        tab_pools_to_workers: poolsToWorkersMs,
        open_connect_worker_modal: openConnectModalMs,
      },
      dom_nodes: { collapsed_large_job: collapsedDomNodes, expanded_large_job: expandedDomNodes },
      horizontal_overflow: horizontalOverflow,
    }, null, 2));
  } finally {
    if (chrome) {
      chrome.cdp.close();
      chrome.proc.kill("SIGKILL");
      fs.rmSync(chrome.userDataDir, { recursive: true, force: true });
    }
    server.close();
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
