import { useEffect, useMemo, useRef, useState } from "react";
import { AuthenticationError, getAnalyticsJobs, getJob } from "./api";
import { MAX_DATA_BYTES, MAX_RESULT_ROWS, QUERY_PRESETS, resultCsv, type QueryResult } from "./analytics-data";
import type { AnalyticsEngine } from "./analytics-engine";
import { asErrorMessage, formatTimestamp } from "./format";
import { JobInspector } from "./panes";
import type { AnalyticsSource, JobDetailResponse } from "./types";

interface Dataset {
  count: number;
  bytes: number;
  limited: boolean;
  from: number;
  to: number;
  source: AnalyticsSource;
}

function localDate(value: number): string {
  const date = new Date(value);
  return new Date(value - date.getTimezoneOffset() * 60_000).toISOString().slice(0, 16);
}

export default function ExplorePane({ journalEnabled, distributed = false, onAuthenticationError }: {
  journalEnabled: boolean | null;
  distributed?: boolean;
  onAuthenticationError: (error: unknown) => void;
}) {
  const [chosenSource, setSource] = useState<AnalyticsSource | null>(null);
  const source = chosenSource ?? (journalEnabled ? "archive" : "retained");
  const [windowMinutes, setWindowMinutes] = useState("15");
  const [from, setFrom] = useState(() => localDate(Date.now() - 3600_000));
  const [to, setTo] = useState(() => localDate(Date.now()));
  const [rowLimit, setRowLimit] = useState(10_000);
  const [sql, setSql] = useState<string>(QUERY_PRESETS[0].sql);
  const [dataset, setDataset] = useState<Dataset | null>(null);
  const [result, setResult] = useState<QueryResult | null>(null);
  const [busy, setBusy] = useState(false);
  const [status, setStatus] = useState("Choose a data source and time window, then load data.");
  const [error, setError] = useState<string | null>(null);
  const [filter, setFilter] = useState("");
  const [page, setPage] = useState(0);
  const [selectedJob, setSelectedJob] = useState<string | null>(null);
  const [detail, setDetail] = useState<JobDetailResponse | null>(null);
  const engine = useRef<AnalyticsEngine | null>(null);
  const operation = useRef<AbortController | null>(null);
  const authError = useRef(onAuthenticationError);
  authError.current = onAuthenticationError;

  useEffect(() => () => {
    const current = operation.current;
    operation.current = null;
    current?.abort();
    engine.current?.dispose();
    engine.current = null;
  }, []);

  useEffect(() => {
    setDetail(null);
    if (!selectedJob) return;
    const controller = new AbortController();
    void getJob(selectedJob, controller.signal).then(value => {
      if (!controller.signal.aborted) setDetail(value);
    }).catch(error => {
      if (!controller.signal.aborted) {
        if (error instanceof AuthenticationError) authError.current(error);
        else setError(asErrorMessage(error));
      }
    });
    return () => controller.abort();
  }, [selectedJob]);

  function fail(error: unknown, controller: AbortController) {
    if (operation.current !== controller) return;
    if (error instanceof AuthenticationError) authError.current(error);
    else setError(controller.signal.aborted ? "Cancelled. Load data to resume." : asErrorMessage(error));
    if (engine.current?.closed) {
      setDataset(null); setResult(null); setStatus("Query session ended. Load data to resume.");
    }
  }

  function display(next: QueryResult) {
    setResult(next); setFilter(""); setPage(0); setSelectedJob(null);
  }

  async function load() {
    const end = windowMinutes === "custom" ? new Date(to).getTime() : Date.now();
    const start = windowMinutes === "custom" ? new Date(from).getTime() : end - Number(windowMinutes) * 60_000;
    if (!Number.isFinite(start) || !Number.isFinite(end) || start < 0 || start >= end || end - start > 31 * 86400_000) {
      setError("Choose a valid time range of at most 31 days."); return;
    }
    const controller = new AbortController();
    operation.current?.abort(); operation.current = controller;
    engine.current?.dispose(); engine.current = null;
    setBusy(true); setError(null); setDataset(null); setResult(null); setSelectedJob(null);
    setStatus("Preparing query engine…");
    let database: AnalyticsEngine | null = null;
    let loaded = false;
    try {
      const { AnalyticsEngine } = await import("./analytics-engine");
      controller.signal.throwIfAborted();
      const db = await AnalyticsEngine.create(controller.signal);
      database = db;
      engine.current = db;
      const next: Dataset = { count: 0, bytes: 0, limited: false, from: start, to: end, source };
      const encoder = new TextEncoder();
      let cursor: string | null = null;
      const cursors = new Set<string>();
      do {
        setStatus(`Loading metadata… ${next.count.toLocaleString()} jobs`);
        const response = await getAnalyticsJobs(source, start, end, Math.min(250, rowLimit - next.count), cursor, controller.signal);
        controller.signal.throwIfAborted();
        const accepted = [];
        for (const job of response.jobs) {
          const bytes = encoder.encode(JSON.stringify(job)).byteLength;
          if (next.bytes + bytes > MAX_DATA_BYTES || next.count === rowLimit) { next.limited = true; break; }
          accepted.push(job); next.bytes += bytes; next.count++;
        }
        if (accepted.length) await db.append(accepted, controller.signal);
        cursor = response.next_cursor;
        if (cursor && cursors.has(cursor)) throw new Error("The server returned a repeated cursor. Reload the data.");
        if (cursor) cursors.add(cursor);
        next.limited ||= cursor !== null && next.count === rowLimit;
      } while (cursor && !next.limited);
      if (operation.current !== controller) return;
      loaded = true;
      setDataset(next);
      setStatus(`Loaded ${next.count.toLocaleString()} jobs · ${(next.bytes / 1048576).toFixed(1)} MiB`);
      const result = await db.query(sql, controller.signal);
      if (operation.current === controller) display(result);
    } catch (error) {
      if (!loaded) {
        database?.dispose();
        if (operation.current === controller) {
          if (engine.current === database) engine.current = null;
          setStatus("Data load failed. Retry or choose a narrower range.");
        }
      }
      fail(error, controller);
    }
    finally { if (operation.current === controller) { operation.current = null; setBusy(false); } }
  }

  async function run() {
    if (!engine.current || engine.current.closed || busy) return;
    const controller = new AbortController(); operation.current = controller;
    setBusy(true); setError(null); setSelectedJob(null); setResult(null);
    try {
      const result = await engine.current.query(sql, controller.signal);
      if (operation.current === controller) display(result);
    }
    catch (error) { fail(error, controller); }
    finally { if (operation.current === controller) { operation.current = null; setBusy(false); } }
  }

  function cancel() {
    const controller = operation.current;
    operation.current = null;
    controller?.abort();
    engine.current?.dispose();
    engine.current = null;
    setBusy(false); setError(null); setDataset(null); setResult(null);
    setStatus("Cancelled. Load data to resume.");
  }

  const filtered = useMemo(() => !filter ? result?.rows ?? [] : result?.rows.filter(row => row.some(cell =>
    cell?.toLowerCase().includes(filter.toLowerCase()))) ?? [], [result, filter]);
  const pageCount = Math.max(1, Math.ceil(filtered.length / 50));
  const jobColumn = result?.columns.indexOf("job_id") ?? -1;

  function exportCsv() {
    if (!result) return;
    const url = URL.createObjectURL(new Blob([resultCsv({ ...result, rows: filtered })], { type: "text/csv;charset=utf-8" }));
    const link = document.createElement("a"); link.href = url; link.download = "anyserve-query.csv"; link.click();
    setTimeout(() => URL.revokeObjectURL(url), 1_000);
  }

  return <div className={`content-grid${selectedJob ? "" : " content-grid--single"}`}>
    <section className="panel explore">
      <div className="panel__header"><h1>Explore</h1><span>Query task metadata</span></div>
      <div className="explore__body">
        <form className="explore__source" onSubmit={event => { event.preventDefault(); void load(); }}>
          <label>Data source<select aria-label="Data source" value={source} onChange={event => setSource(event.target.value as AnalyticsSource)} disabled={busy}>
            <option value="archive" disabled={!journalEnabled}>Durable history</option>
            <option value="retained">Current retained jobs</option>
          </select></label>
          <label>Created within<select value={windowMinutes} onChange={event => setWindowMinutes(event.target.value)} disabled={busy}>
            <option value="15">Last 15 minutes</option><option value="60">Last hour</option>
            <option value="1440">Last 24 hours</option><option value="custom">Custom range</option>
          </select></label>
          {windowMinutes === "custom" ? <>
            <label>From<input type="datetime-local" value={from} onChange={event => setFrom(event.target.value)} disabled={busy} required /></label>
            <label>Until<input type="datetime-local" value={to} onChange={event => setTo(event.target.value)} disabled={busy} required /></label>
          </> : null}
          <label>Maximum jobs<select value={rowLimit} onChange={event => setRowLimit(Number(event.target.value))} disabled={busy}>
            <option value={1_000}>1,000</option><option value={10_000}>10,000</option><option value={100_000}>100,000</option>
          </select></label>
          <button className="explore__primary" type="submit" disabled={busy || journalEnabled === null}>Load data</button>
          {busy ? <button type="button" onClick={cancel}>Cancel</button> : null}
        </form>
        <p className="explore__hint">{source === "archive" ? "Completed durable jobs, including jobs no longer retained in memory." : distributed ? "Queued and running durable jobs across the cluster." : "Active and recent jobs still held in memory, including ephemeral jobs."} Payloads are excluded. Loads are manual and capped at 32 MiB.</p>
        <p className="explore__status" role="status">{status}</p>
        {dataset ? <p className="explore__hint">Loaded {dataset.source === "archive" ? "durable history" : "retained jobs"}: {formatTimestamp(dataset.from)} – {formatTimestamp(dataset.to)}. States reflect each page's read time. Duration includes queue time.</p> : null}
        {dataset?.limited ? <p className="explore__notice">This dataset is partial: the row or byte limit was reached. Queries cover only these newest loaded jobs. Narrow the time range for complete statistics.</p> : null}
        <div className="explore__query-heading"><label htmlFor="query-sql">SQL query</label>
          <select aria-label="Query template" defaultValue="" onChange={event => { const preset = QUERY_PRESETS.find(item => item.name === event.target.value); if (preset) setSql(preset.sql); event.target.value = ""; }} disabled={busy}>
            <option value="" disabled>Choose a query…</option>{QUERY_PRESETS.map(preset => <option key={preset.name}>{preset.name}</option>)}
          </select>
        </div>
        <textarea id="query-sql" spellCheck={false} value={sql} disabled={busy} onChange={event => setSql(event.target.value)} onKeyDown={event => {
          if ((event.ctrlKey || event.metaKey) && event.key === "Enter") { event.preventDefault(); void run(); }
        }} />
        <div className="explore__actions"><button className="explore__primary" disabled={busy || !dataset} onClick={() => void run()}>Run query</button>
          <span>SELECT / WITH · Ctrl/⌘ + Enter · 15-second limit</span>
        </div>
        <details className="explore__schema"><summary>Available tables and columns</summary>
          <p><code>job_id, state, interface_name, source, durability, created_at_ms, updated_at_ms, duration_ms, current_attempt_id, latest_worker_id, attempt_count, last_error, metadata</code></p>
          <p>Timestamps are Unix milliseconds. Include <code>job_id</code> in results to open task details.</p>
          <p>Query tags in <code>job_metadata (job_id, key, value)</code>, for example:</p>
          <code>SELECT jobs.* FROM jobs JOIN job_metadata USING (job_id) WHERE key = 'tag' AND value = 'value'</code>
        </details>
        {error ? <p className="explore__error" role="alert">{error}</p> : null}
        {result ? <>
          <div className="explore__results-heading"><h2>Results <span>{result.rows.length.toLocaleString()} rows · {result.elapsedMs.toFixed(1)} ms</span></h2>
            <label className="explore__find">Find in results<input type="search" value={filter} onChange={event => { setFilter(event.target.value); setPage(0); }} /></label>
            <button onClick={exportCsv} disabled={!filtered.length || busy}>Export CSV</button>
          </div>
          {result.truncated || result.clippedCells ? <p className="explore__notice">Result limited to {MAX_RESULT_ROWS.toLocaleString()} rows / 4 MiB. {result.clippedCells ? `${result.clippedCells} large cells shortened. ` : ""}Use filters or aggregation to narrow the query. Export includes the displayed results.</p> : null}
          <div className="explore__table"><table><thead><tr>{jobColumn >= 0 ? <th>Task</th> : null}{result.columns.map((column, index) => <th key={index}>{column}</th>)}</tr></thead>
            <tbody>{filtered.slice(page * 50, (page + 1) * 50).map((row, index) => <tr key={index} className={jobColumn >= 0 && row[jobColumn] === selectedJob ? "is-selected" : ""}>
              {jobColumn >= 0 ? <td>{row[jobColumn] ? <button onClick={() => setSelectedJob(row[jobColumn])}>Inspect</button> : null}</td> : null}
              {row.map((value, column) => <td key={column} title={value ?? undefined}>{value === null ? <span className="explore__null">NULL</span> : value}</td>)}
            </tr>)}</tbody></table>
            {!filtered.length ? <p>No matching rows.</p> : null}
          </div>
          <div className="explore__pagination"><span>{filtered.length.toLocaleString()} matching rows · Page {page + 1} of {pageCount}</span>
            <button disabled={page === 0} onClick={() => setPage(value => value - 1)}>Previous</button>
            <button disabled={page + 1 >= pageCount} onClick={() => setPage(value => value + 1)}>Next</button>
          </div>
        </> : null}
      </div>
    </section>
    {selectedJob ? <div className="explore__inspector"><button className="explore__close" onClick={() => setSelectedJob(null)}>Close details</button><JobInspector detail={detail} /></div> : null}
  </div>;
}
