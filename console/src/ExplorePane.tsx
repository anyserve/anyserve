import { useEffect, useMemo, useRef, useState } from "react";
import { AuthenticationError, getAnalyticsJobs, getJob } from "./api";
import { estimateJobJsonBytes, MAX_DATA_BYTES, MAX_RESULT_ROWS, QUERY_PRESETS, resultCsv, type QueryResult } from "./analytics-data";
import type { AnalyticsEngine } from "./analytics-engine";
import { asErrorMessage, formatTimestamp, toDatetimeLocal } from "./format";
import { JobInspector } from "./pane-inspector";
import { PanelHeader } from "./pane-tables";
import type { AnalyticsSource, JobDetailResponse } from "./types";

interface Dataset { count: number; bytes: number; limited: boolean; from: number; to: number; source: AnalyticsSource; }

interface ExplorePaneProps {
  journalEnabled: boolean | null;
  onAuthenticationError: (error: unknown) => void;
  lockedNamespace?: string | null;
  availableNamespaces?: string[];
  onOpenWorker?: (workerId: string) => void;
  onOpenInterface?: (interfaceName: string) => void;
  onFilterNamespace?: (namespace: string) => void;
  onOpenJobInJobs?: (jobId: string, namespace?: string) => void;
}

export default function ExplorePane({
  journalEnabled,
  onAuthenticationError,
  lockedNamespace,
  availableNamespaces = [],
  onOpenWorker,
  onOpenInterface,
  onFilterNamespace,
  onOpenJobInJobs,
}: ExplorePaneProps) {
  const [chosenSource, setSource] = useState<AnalyticsSource | null>(null);
  const source = chosenSource ?? (journalEnabled ? "archive" : "retained");
  const [namespaceFilter, setNamespaceFilter] = useState(lockedNamespace ?? "all");
  const effectiveNamespace = lockedNamespace ?? (namespaceFilter === "all" ? undefined : namespaceFilter);
  const nsChoices = useMemo(() => {
    const set = new Set<string>(["default", ...availableNamespaces]);
    if (lockedNamespace) set.add(lockedNamespace);
    return Array.from(set).sort();
  }, [availableNamespaces, lockedNamespace]);
  const [windowMinutes, setWindowMinutes] = useState("15");
  const [from, setFrom] = useState(() => toDatetimeLocal(Date.now() - 3600_000));
  const [to, setTo] = useState(() => toDatetimeLocal(Date.now()));
  const [rowLimit, setRowLimit] = useState(10_000);
  const [sql, setSql] = useState<string>(QUERY_PRESETS[0].sql);
  const [dataset, setDataset] = useState<Dataset | null>(null);
  const [result, setResult] = useState<QueryResult | null>(null);
  const [busy, setBusy] = useState(false);
  const [status, setStatus] = useState("Choose a data source and time window, then load data.");
  const [error, setError] = useState<string | null>(null);
  const [filter, setFilter] = useState("");
  const [page, setPage] = useState(0);
  const [selectedJob, setSelectedJob] = useState<{ jobId: string; namespace?: string } | null>(null);
  const [detail, setDetail] = useState<JobDetailResponse | null>(null);
  const engine = useRef<AnalyticsEngine | null>(null);
  const operation = useRef<AbortController | null>(null);
  const authError = useRef(onAuthenticationError);
  authError.current = onAuthenticationError;

  useEffect(() => () => {
    operation.current?.abort(); operation.current = null;
    engine.current?.dispose(); engine.current = null;
  }, []);

  useEffect(() => {
    setDetail(null);
    if (!selectedJob) return;
    const c = new AbortController();
    void getJob(selectedJob.jobId, c.signal, selectedJob.namespace)
      .then((v) => { if (!c.signal.aborted) setDetail(v); })
      .catch((e) => { if (!c.signal.aborted) e instanceof AuthenticationError ? authError.current(e) : setError(asErrorMessage(e)); });
    return () => c.abort();
  }, [selectedJob]);

  function fail(err: unknown, c: AbortController) {
    if (operation.current !== c) return;
    if (err instanceof AuthenticationError) authError.current(err);
    else setError(c.signal.aborted ? "Cancelled. Load data to resume." : asErrorMessage(err));
    if (engine.current?.closed) { setDataset(null); setResult(null); setStatus("Query session ended. Load data to resume."); }
  }

  function display(next: QueryResult) { setResult(next); setFilter(""); setPage(0); setSelectedJob(null); }

  async function load() {
    const end = windowMinutes === "custom" ? new Date(to).getTime() : Date.now();
    const start = windowMinutes === "custom" ? new Date(from).getTime() : end - Number(windowMinutes) * 60_000;
    if (!Number.isFinite(start) || !Number.isFinite(end) || start < 0 || start >= end || end - start > 31 * 86400_000) {
      setError("Choose a valid time range of at most 31 days."); return;
    }
    const c = new AbortController();
    operation.current?.abort(); operation.current = c;
    engine.current?.dispose(); engine.current = null;
    setBusy(true); setError(null); setDataset(null); setResult(null); setSelectedJob(null);
    setStatus("Preparing query engine…");
    let database: AnalyticsEngine | null = null;
    let loaded = false;
    try {
      const { AnalyticsEngine } = await import("./analytics-engine");
      c.signal.throwIfAborted();
      const db = await AnalyticsEngine.create(c.signal);
      database = db; engine.current = db;
      const next: Dataset = { count: 0, bytes: 0, limited: false, from: start, to: end, source };
      let cursor: string | null = null;
      const cursors = new Set<string>();
      do {
        setStatus(`Loading metadata… ${next.count.toLocaleString()} jobs`);
        const res = await getAnalyticsJobs(
          source,
          start,
          end,
          Math.min(250, rowLimit - next.count),
          cursor,
          c.signal,
          effectiveNamespace,
        );
        c.signal.throwIfAborted();
        const accepted = [];
        for (const job of res.jobs) {
          const bytes = estimateJobJsonBytes(job);
          if (next.bytes + bytes > MAX_DATA_BYTES || next.count === rowLimit) { next.limited = true; break; }
          accepted.push(job); next.bytes += bytes; next.count++;
        }
        if (accepted.length) await db.append(accepted, c.signal);
        cursor = res.next_cursor;
        if (cursor && cursors.has(cursor)) throw new Error("The server returned a repeated cursor. Reload the data.");
        if (cursor) cursors.add(cursor);
        next.limited ||= cursor !== null && next.count === rowLimit;
      } while (cursor && !next.limited);
      if (operation.current !== c) return;
      loaded = true; setDataset(next);
      setStatus(`Loaded ${next.count.toLocaleString()} jobs · ${(next.bytes / 1048576).toFixed(1)} MiB`);
      const nextResult = await db.query(sql, c.signal);
      if (operation.current === c) display(nextResult);
    } catch (err) {
      if (!loaded) {
        database?.dispose();
        if (operation.current === c) { if (engine.current === database) engine.current = null; setStatus("Data load failed. Retry or choose a narrower range."); }
      }
      fail(err, c);
    } finally { if (operation.current === c) { operation.current = null; setBusy(false); } }
  }

  async function run() {
    if (!engine.current || engine.current.closed || busy) return;
    const c = new AbortController(); operation.current = c;
    setBusy(true); setError(null); setSelectedJob(null); setResult(null);
    try { const nextResult = await engine.current.query(sql, c.signal); if (operation.current === c) display(nextResult); }
    catch (err) { fail(err, c); }
    finally { if (operation.current === c) { operation.current = null; setBusy(false); } }
  }

  function cancel() {
    operation.current?.abort(); operation.current = null;
    engine.current?.dispose(); engine.current = null;
    setBusy(false); setError(null); setDataset(null); setResult(null); setStatus("Cancelled. Load data to resume.");
  }

  const filtered = useMemo(() => {
    const rows = result?.rows ?? [];
    if (!filter) return rows;
    const needle = filter.toLowerCase();
    return rows.filter((r) => r.some((cell) => cell !== null && cell.toLowerCase().includes(needle)));
  }, [result, filter]);
  const pageCount = Math.max(1, Math.ceil(filtered.length / 50));
  const jobCol = result?.columns.indexOf("job_id") ?? -1;
  const nsCol = result?.columns.indexOf("namespace") ?? -1;
  const ifaceCol = result?.columns.indexOf("interface_name") ?? -1;
  const workerCol = result?.columns.indexOf("latest_worker_id") ?? -1;

  function exportCsv() {
    if (!result) return;
    const url = URL.createObjectURL(new Blob([resultCsv({ ...result, rows: filtered })], { type: "text/csv;charset=utf-8" }));
    const link = document.createElement("a");
    link.href = url; link.download = "anyserve-query.csv"; link.click();
    setTimeout(() => URL.revokeObjectURL(url), 1_000);
  }

  return (
    <div className={`content-grid${selectedJob ? "" : " content-grid--single"}`}>
      <section className="panel explore">
        <PanelHeader title="Explore" meta="DuckDB-WASM SQL" />
        <div className="explore__body">
          <form className="explore__source" onSubmit={(e) => { e.preventDefault(); void load(); }}>
            <label>Data source
              <select aria-label="Data source" value={source} onChange={(e) => setSource(e.target.value as AnalyticsSource)} disabled={busy}>
                <option value="archive" disabled={!journalEnabled}>Durable history</option>
                <option value="retained">Current retained jobs</option>
              </select>
            </label>
            <label>Created within
              <select value={windowMinutes} onChange={(e) => setWindowMinutes(e.target.value)} disabled={busy}>
                <option value="15">Last 15 minutes</option><option value="60">Last hour</option>
                <option value="1440">Last 24 hours</option><option value="custom">Custom range</option>
              </select>
            </label>
            {windowMinutes === "custom" ? (
              <>
                <label>From<input type="datetime-local" step="1" value={from} onChange={(e) => setFrom(e.target.value)} disabled={busy} required /></label>
                <label>Until<input type="datetime-local" step="1" value={to} onChange={(e) => setTo(e.target.value)} disabled={busy} required /></label>
              </>
            ) : null}
            <label>Maximum jobs
              <select value={rowLimit} onChange={(e) => setRowLimit(Number(e.target.value))} disabled={busy}>
                <option value={1_000}>1,000</option><option value={10_000}>10,000</option><option value={100_000}>100,000</option>
              </select>
            </label>
            <label>Namespace
              <select
                aria-label="Namespace"
                value={lockedNamespace ?? namespaceFilter}
                onChange={(e) => setNamespaceFilter(e.target.value)}
                disabled={busy || Boolean(lockedNamespace)}
              >
                {lockedNamespace ? null : <option value="all">All namespaces</option>}
                {nsChoices.map((ns) => (
                  <option key={ns} value={ns}>{ns}</option>
                ))}
              </select>
            </label>
            <button className="button button--primary" type="submit" disabled={busy || journalEnabled === null}>Load data</button>
            {busy ? <button className="button" type="button" onClick={cancel}>Cancel</button> : null}
          </form>

          <p className="explore__hint">
            {source === "archive" ? "Completed durable jobs, including jobs no longer retained in memory." : "Active and recent jobs still held in memory, including ephemeral jobs."}{" "}
            Payloads are excluded. Loads are manual and capped at 32 MiB.
          </p>
          <p className="explore__status" role="status">{status}</p>
          {dataset ? <p className="explore__hint">{dataset.source === "archive" ? "Durable history" : "Current retained jobs"}: {formatTimestamp(dataset.from)} – {formatTimestamp(dataset.to)}</p> : null}
          {dataset?.limited ? <p className="inline-notice">This dataset is partial: the row or byte limit was reached. Queries cover only these newest loaded jobs. Narrow the time range for complete statistics.</p> : null}

          <div className="explore__query-heading">
            <label htmlFor="query-sql">SQL query</label>
            <select aria-label="Query template" defaultValue="" disabled={busy} onChange={(e) => { const p = QUERY_PRESETS.find((x) => x.name === e.target.value); if (p) setSql(p.sql); e.target.value = ""; }}>
              <option value="" disabled>Choose a query…</option>
              {QUERY_PRESETS.map((p) => <option key={p.name}>{p.name}</option>)}
            </select>
          </div>
          <textarea id="query-sql" spellCheck={false} value={sql} disabled={busy} onChange={(e) => setSql(e.target.value)} onKeyDown={(e) => { if ((e.ctrlKey || e.metaKey) && e.key === "Enter") { e.preventDefault(); void run(); } }} />
          <div className="explore__actions">
            <button className="button button--primary" disabled={busy || !dataset} onClick={() => void run()}>Run query</button>
            <span>SELECT / WITH · Ctrl/⌘ + Enter · 15-second limit</span>
          </div>

          <details className="explore__schema">
            <summary>Available tables and columns</summary>
            <p><code>job_id, state, interface_name, namespace, source, durability, created_at_ms, updated_at_ms, duration_ms, current_attempt_id, latest_worker_id, attempt_count, last_error, metadata</code></p>
            <p>Timestamps are Unix milliseconds. Include <code>job_id</code> in results to open task details.</p>
            <p>Query tags in <code>job_metadata (job_id, namespace, key, value)</code>: <code>SELECT jobs.* FROM jobs JOIN job_metadata USING (job_id, namespace) WHERE key = &apos;tag&apos; AND value = &apos;value&apos;</code></p>
          </details>

          {error ? <p className="explore__error" role="alert">{error}</p> : null}

          {result ? (
            <>
              <div className="explore__results-heading">
                <h2>Results <span>{result.rows.length.toLocaleString()} rows · {result.elapsedMs.toFixed(1)} ms</span></h2>
                <label className="explore__find">Find in results<input type="search" value={filter} onChange={(e) => { setFilter(e.target.value); setPage(0); }} /></label>
                <button className="button" onClick={exportCsv} disabled={!filtered.length || busy}>Export CSV</button>
              </div>
              {result.truncated || result.clippedCells ? (
                <p className="inline-notice">
                  Result limited to {MAX_RESULT_ROWS.toLocaleString()} rows / 4 MiB. {result.clippedCells ? `${result.clippedCells} large cells shortened. ` : ""}Use filters or aggregation to narrow the query.
                </p>
              ) : null}
              <div className="table-wrap explore__table">
                <table className="table mono">
                  <thead><tr>{jobCol >= 0 ? <th>Task</th> : null}{result.columns.map((c, i) => <th key={i}>{c}</th>)}</tr></thead>
                  <tbody>
                    {filtered.slice(page * 50, (page + 1) * 50).map((row, idx) => {
                      const rowNs = nsCol >= 0 ? (row[nsCol] ?? effectiveNamespace) : effectiveNamespace;
                      const isRowSelected =
                        jobCol >= 0 &&
                        selectedJob !== null &&
                        row[jobCol] === selectedJob.jobId &&
                        (rowNs ?? "default") === (selectedJob.namespace ?? "default");
                      return (
                      <tr key={idx} className={isRowSelected ? "is-selected" : ""}>
                        {jobCol >= 0 ? (
                          <td>
                            {row[jobCol] ? <button className="button" onClick={() => setSelectedJob({ jobId: row[jobCol]!, namespace: rowNs })}>Inspect</button> : null}
                          </td>
                        ) : null}
                        {row.map((val, col) => {
                          if (val === null) return <td key={col}><span className="explore__null">NULL</span></td>;
                          if (col === ifaceCol && onOpenInterface) {
                            return (
                              <td key={col} title={val}>
                                <button type="button" className="cell-link mono" onClick={() => onOpenInterface(val)}>{val}</button>
                              </td>
                            );
                          }
                          if (col === workerCol && onOpenWorker) {
                            return (
                              <td key={col} title={val}>
                                <button type="button" className="cell-link mono" onClick={() => onOpenWorker(val)}>{val}</button>
                              </td>
                            );
                          }
                          return <td key={col} title={val}>{val}</td>;
                        })}
                      </tr>
                      );
                    })}
                  </tbody>
                </table>
                {!filtered.length ? <p className="empty-inline" style={{ padding: 12 }}>No matching rows.</p> : null}
              </div>
              <div className="explore__pagination">
                <span>{filtered.length.toLocaleString()} matching rows · Page {page + 1} of {pageCount}</span>
                <button className="button" disabled={page === 0} onClick={() => setPage((v) => v - 1)}>Previous</button>
                <button className="button" disabled={page + 1 >= pageCount} onClick={() => setPage((v) => v + 1)}>Next</button>
              </div>
            </>
          ) : null}
        </div>
      </section>
      {selectedJob ? (
        <div className="explore__inspector">
          <div className="explore__inspector-bar">
            {onOpenJobInJobs ? (
              <button type="button" className="button" onClick={() => onOpenJobInJobs(selectedJob.jobId, selectedJob.namespace)}>
                Open in Jobs
              </button>
            ) : null}
            <button type="button" className="button explore__close" onClick={() => setSelectedJob(null)}>Close details</button>
          </div>
          <JobInspector
            detail={detail}
            onOpenWorker={onOpenWorker}
            onOpenInterface={onOpenInterface}
            onFilterNamespace={onFilterNamespace}
          />
        </div>
      ) : null}
    </div>
  );
}
