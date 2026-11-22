import { lazy, Suspense, useCallback, useDeferredValue, useEffect, useMemo, useRef, useState } from "react";

import { AuthenticationError, cancelJob, clearConsoleToken, getJob, getJobs, getOverview, getWorkers, hasConsoleToken, setConsoleToken } from "./api";
import {
  ALL_JOB_STATES, buildUrlSearch, DEFAULT_TIME_FILTER, DETAIL_POLL_MS, HISTORY_STATES,
  JOBS_ACTIVE_POLL_MS, JOBS_HISTORY_POLL_MS, OVERVIEW_POLL_MS, parseUrlState, QUEUE_STATES,
  routePath, WORKERS_POLL_MS, type ConsoleTab, type JobScope, type JobStatusFilter, type Theme, type TimeFilterState,
} from "./app-constants";
import { ConnectWorkerModal } from "./ConnectWorkerModal";
import { asErrorMessage, formatPool, sortJobs, sortWorkers } from "./format";
import { InterfacesPane } from "./InterfacesPane";
import { TIME_PRESET_MS } from "./JobsHistogram";
import { JobsPane } from "./JobsPane";
import { OverviewPane } from "./OverviewPane";
import { JobInspector } from "./pane-inspector";
import { PoolsPane, poolKey, type PoolFilterSeed } from "./PoolsPane";
import { Sidebar } from "./Sidebar";
import type { JobDetailResponse, JobsHistogram, JobState, JobSummary, OverviewResponse, WorkerSummary, WorkersResponse } from "./types";
import { usePollingEffect } from "./usePollingEffect";
import { WorkersPane, type HealthFilter, type WorkerFilterSeed } from "./WorkersPane";

const ExplorePane = lazy(() => import("./ExplorePane"));
const THEME_KEY = "anyserve.console.theme";
interface JobsPageState {
  loadedKey: string | null;
  page1: JobSummary[];
  extra: JobSummary[];
  page1Next: string | null;
  extraNext: string | null;
  total: number | null;
  histogram: JobsHistogram | null;
  loadingMore: boolean;
}
const EMPTY_JOBS: JobsPageState = {
  loadedKey: null,
  page1: [],
  extra: [],
  page1Next: null,
  extraNext: null,
  total: null,
  histogram: null,
  loadingMore: false,
};

function readTheme(): Theme {
  try { const s = localStorage.getItem(THEME_KEY); if (s === "dark" || s === "light") return s; } catch { /* ignore */ }
  return "light";
}
const toOptions = (values: Iterable<string>, allLabel: string) => [
  { value: "all", label: allLabel },
  ...Array.from(new Set(values)).sort().map((v) => ({ value: v, label: v })),
];

function sameJobs(a: JobSummary[], b: JobSummary[]): boolean {
  if (a.length !== b.length) return false;
  for (let i = 0; i < a.length; i += 1) {
    if (
      a[i].job_id !== b[i].job_id ||
      a[i].state !== b[i].state ||
      a[i].updated_at_ms !== b[i].updated_at_ms ||
      a[i].attempt_count !== b[i].attempt_count ||
      a[i].latest_worker_id !== b[i].latest_worker_id
    ) {
      return false;
    }
  }
  return true;
}

function sameWorkers(a: WorkerSummary[], b: WorkerSummary[]): boolean {
  if (a.length !== b.length) return false;
  for (let i = 0; i < a.length; i += 1) {
    if (
      a[i].worker_id !== b[i].worker_id ||
      a[i].session_id !== b[i].session_id ||
      a[i].healthy !== b[i].healthy ||
      a[i].draining !== b[i].draining ||
      a[i].connected !== b[i].connected ||
      a[i].active_leases !== b[i].active_leases ||
      a[i].last_seen_at_ms !== b[i].last_seen_at_ms
    ) {
      return false;
    }
  }
  return true;
}

function sameHistogram(a: JobsHistogram | null, b: JobsHistogram | null): boolean {
  if (a === b) return true;
  if (!a || !b) return false;
  if (a.start_ms !== b.start_ms || a.end_ms !== b.end_ms || a.buckets.length !== b.buckets.length) return false;
  for (let i = 0; i < a.buckets.length; i += 1) {
    const x = a.buckets[i];
    const y = b.buckets[i];
    if (
      x.start_ms !== y.start_ms ||
      x.active !== y.active ||
      x.succeeded !== y.succeeded ||
      x.failed !== y.failed ||
      x.cancelled !== y.cancelled
    ) {
      return false;
    }
  }
  return true;
}

export default function App() {
  const init = useMemo(() => parseUrlState(window.location.pathname, window.location.search), []);
  const [activeTab, setActiveTab] = useState<ConsoleTab>(init.tab);
  const [jobScope, setJobScope] = useState<JobScope>(init.jobScope);
  const [theme, setTheme] = useState<Theme>(readTheme);

  const [overview, setOverview] = useState<OverviewResponse | null>(null);
  const [workersResponse, setWorkersResponse] = useState<WorkersResponse | null>(null);
  const [workers, setWorkers] = useState<WorkerSummary[]>([]);
  const [knownWorkers, setKnownWorkers] = useState<string[]>(() => (init.worker !== "all" ? [init.worker] : []));
  const [jobsPage, setJobsPage] = useState<JobsPageState>(EMPTY_JOBS);

  const [filters, setFilters] = useState({
    status: init.status as JobStatusFilter,
    iface: init.iface,
    ns: init.ns,
    worker: init.worker,
    q: init.q,
  });
  const [serverSearch, setServerSearch] = useState(() => init.q.trim());
  const [timeFilter, setTimeFilter] = useState<TimeFilterState>(init.time);
  const deferredSearch = useDeferredValue(filters.q);

  const initialPinned = init.tab === "jobs" ? init.pinnedJobId : null;
  const [pinnedJobByScope, setPinnedJobByScope] = useState<Record<JobScope, string | null>>({
    all: init.jobScope === "all" ? initialPinned : null,
    queue: init.jobScope === "queue" ? initialPinned : null,
    history: init.jobScope === "history" ? initialPinned : null,
  });
  const [selectedJobByScope, setSelectedJobByScope] = useState<Record<JobScope, string | null>>({
    all: init.jobScope === "all" ? initialPinned : null,
    queue: init.jobScope === "queue" ? initialPinned : null,
    history: init.jobScope === "history" ? initialPinned : null,
  });

  const [selectedInterface, setSelectedInterface] = useState<string | null>(init.selectedInterface);
  const [selectedPoolKey, setSelectedPoolKey] = useState<string | null>(init.selectedPoolKey);
  const [poolFilterSeed, setPoolFilterSeed] = useState<PoolFilterSeed | null>(null);
  const [selectedWorkerId, setSelectedWorkerId] = useState<string | null>(init.selectedWorkerId);
  const [workerFilterSeed, setWorkerFilterSeed] = useState<WorkerFilterSeed | null>(null);
  const [connectOpen, setConnectOpen] = useState(false);

  const [jobDetail, setJobDetail] = useState<JobDetailResponse | null>(null);
  const [isCancelling, setIsCancelling] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [authRequired, setAuthRequired] = useState(false);
  const [authenticated, setAuthenticated] = useState(hasConsoleToken);
  const [token, setToken] = useState("");
  const loadMoreAbort = useRef<AbortController | null>(null);
  const navSeedSeq = useRef(0);

  useEffect(() => {
    document.documentElement.setAttribute("data-theme", theme);
    try { localStorage.setItem(THEME_KEY, theme); } catch { /* ignore */ }
  }, [theme]);

  useEffect(() => {
    const onPop = () => {
      const p = parseUrlState(window.location.pathname, window.location.search);
      loadMoreAbort.current?.abort();
      loadMoreAbort.current = null;
      setJobsPage((cur) => (cur.extra.length === 0 && !cur.extraNext && !cur.loadingMore ? cur : { ...cur, extra: [], extraNext: null, loadingMore: false }));
      setActiveTab(p.tab);
      setJobScope(p.jobScope);
      setFilters({ status: p.status, iface: p.iface, ns: p.ns, worker: p.worker, q: p.q });
      setServerSearch(p.q.trim());
      setTimeFilter(p.time);
      if (p.pinnedJobId) {
        setPinnedJobByScope((c) => ({ ...c, [p.jobScope]: p.pinnedJobId }));
        setSelectedJobByScope((c) => ({ ...c, [p.jobScope]: p.pinnedJobId }));
      }
      if (p.selectedInterface !== null) setSelectedInterface(p.selectedInterface);
      if (p.selectedPoolKey !== null) setSelectedPoolKey(p.selectedPoolKey);
      if (p.selectedWorkerId !== null) setSelectedWorkerId(p.selectedWorkerId);
    };
    window.addEventListener("popstate", onPop);
    return () => window.removeEventListener("popstate", onPop);
  }, []);

  useEffect(() => {
    const search = buildUrlSearch({
      tab: activeTab,
      status: filters.status,
      iface: filters.iface,
      ns: filters.ns,
      worker: filters.worker,
      q: filters.q,
      time: timeFilter,
      pinnedJobId: pinnedJobByScope[jobScope],
      selectedInterface,
      selectedPoolKey,
      selectedWorkerId,
    });
    const target = `${routePath(activeTab, jobScope)}${search}`;
    const current = `${window.location.pathname}${window.location.search}`;
    if (current !== target) window.history.replaceState(null, "", target);
  }, [activeTab, jobScope, filters, timeFilter, pinnedJobByScope, selectedInterface, selectedPoolKey, selectedWorkerId]);

  useEffect(() => () => loadMoreAbort.current?.abort(), []);

  useEffect(() => {
    const trimmed = filters.q.trim();
    if (trimmed === "") {
      setServerSearch("");
      return undefined;
    }
    const timer = window.setTimeout(() => setServerSearch(trimmed), 150);
    return () => window.clearTimeout(timer);
  }, [filters.q]);

  const rememberWorkers = useCallback((ids: Array<string | null | undefined>) => {
    setKnownWorkers((prev) => {
      const missing = ids.filter((id): id is string => Boolean(id && !prev.includes(id)));
      return missing.length === 0 ? prev : [...prev, ...missing];
    });
  }, []);

  const handleError = useCallback((err: unknown) => {
    setError(asErrorMessage(err));
    if (err instanceof AuthenticationError) setAuthRequired(true);
  }, []);

  function resetExtraJobs() {
    loadMoreAbort.current?.abort();
    loadMoreAbort.current = null;
    setJobsPage((p) => (p.extra.length === 0 && !p.extraNext && !p.loadingMore ? p : { ...p, extra: [], extraNext: null, loadingMore: false }));
  }

  const polling = !authRequired;
  const activeSelectedJobId = activeTab === "jobs" ? selectedJobByScope[jobScope] : null;
  const effectiveStates: JobState[] =
    filters.status !== "all"
      ? [filters.status]
      : jobScope === "queue"
        ? QUEUE_STATES
        : jobScope === "history"
          ? HISTORY_STATES
          : ALL_JOB_STATES;
  const timeKey =
    timeFilter.preset === "custom"
      ? `custom:${timeFilter.fromMs ?? ""}:${timeFilter.toMs ?? ""}`
      : timeFilter.preset;
  const timeWindowMs =
    timeFilter.preset !== "all" && timeFilter.preset !== "custom"
      ? TIME_PRESET_MS[timeFilter.preset]
      : undefined;
  const jobsApiFilters = useMemo(
    () => ({
      interface: filters.iface,
      namespace: filters.ns,
      worker: filters.worker,
      ...(serverSearch ? { q: serverSearch } : {}),
      ...(timeWindowMs != null ? { windowMs: timeWindowMs } : {}),
      ...(timeFilter.preset === "custom" && timeFilter.fromMs != null ? { fromMs: timeFilter.fromMs } : {}),
      ...(timeFilter.preset === "custom" && timeFilter.toMs != null ? { toMs: timeFilter.toMs } : {}),
    }),
    [filters.iface, filters.ns, filters.worker, serverSearch, timeWindowMs, timeFilter],
  );
  const jobsQueryKey = `${jobScope}|${filters.status}|${filters.iface}|${filters.ns}|${filters.worker}|${serverSearch}|${timeKey}`;

  usePollingEffect(
    getOverview,
    (next) => { setOverview(next); setError(null); },
    handleError,
    activeTab === "overview" || (activeTab === "jobs" && jobScope !== "history") ? OVERVIEW_POLL_MS : 2_000,
    [activeTab, jobScope],
    polling,
  );

  usePollingEffect(
    (signal) => getJobs(effectiveStates, 100, 0, signal, { ...jobsApiFilters, buckets: 32 }),
    (res) => {
      const sorted = sortJobs(res.jobs);
      const nextTotal = res.total ?? null;
      const nextHistogram = res.histogram ?? null;
      rememberWorkers(sorted.map((j) => j.latest_worker_id));
      setJobsPage((p) =>
        p.loadedKey === jobsQueryKey &&
        p.page1Next === res.next_page_token &&
        p.total === nextTotal &&
        sameHistogram(p.histogram, nextHistogram) &&
        sameJobs(p.page1, sorted)
          ? p
          : {
              ...p,
              loadedKey: jobsQueryKey,
              page1: sorted,
              page1Next: res.next_page_token,
              total: nextTotal,
              histogram: nextHistogram,
            },
      );
      setError(null);
    },
    handleError,
    jobScope === "history" ? JOBS_HISTORY_POLL_MS : JOBS_ACTIVE_POLL_MS,
    [jobScope, filters.status, filters.iface, filters.ns, filters.worker, serverSearch, timeKey],
    polling && activeTab === "jobs",
  );

  usePollingEffect(
    getWorkers,
    (res) => {
      const sorted = sortWorkers(res.workers);
      rememberWorkers(sorted.map((w) => w.worker_id));
      setWorkersResponse((cur) =>
        cur && cur.total === res.total && cur.healthy === res.healthy && cur.draining === res.draining && cur.disconnected === res.disconnected && sameWorkers(cur.workers, sorted)
          ? cur
          : res,
      );
      setWorkers((cur) => (sameWorkers(cur, sorted) ? cur : sorted));
      setError(null);
    },
    handleError,
    WORKERS_POLL_MS,
    [],
    polling && (activeTab === "workers" || activeTab === "interfaces" || activeTab === "pools"),
  );

  const jobsQueryMatches = jobsPage.loadedKey === jobsQueryKey;
  const loadedJobs = useMemo(() => {
    if (jobsPage.loadedKey === null) return [];
    if (jobsPage.extra.length === 0) return jobsPage.page1;
    const seen = new Set<string>();
    return [...jobsPage.page1, ...jobsPage.extra].filter((j) => {
      const k = `${j.namespace ?? "default"}::${j.job_id}`;
      if (seen.has(k)) return false;
      seen.add(k);
      return true;
    });
  }, [jobsPage.loadedKey, jobsPage.page1, jobsPage.extra]);

  const filterOptions = useMemo(() => ({
    iface: toOptions([...(overview?.interfaces.map((i) => i.name) ?? []), ...loadedJobs.map((j) => j.interface_name), ...(filters.iface !== "all" ? [filters.iface] : [])], "All interfaces"),
    ns: toOptions([...(overview?.pools?.map((p) => p.namespace) ?? []), ...workers.map((w) => w.namespace).filter(Boolean) as string[], ...loadedJobs.map((j) => j.namespace).filter(Boolean) as string[], ...(filters.ns !== "all" ? [filters.ns] : [])], "All namespaces"),
    worker: toOptions([...knownWorkers, ...(filters.worker !== "all" ? [filters.worker] : [])], "All workers"),
  }), [overview, workers, knownWorkers, loadedJobs, filters.iface, filters.ns, filters.worker]);

  const visibleJobs = useMemo(() => {
    const q = deferredSearch.trim().toLowerCase();
    return loadedJobs.filter((j) => {
      if (effectiveStates.length > 0 && !effectiveStates.includes(j.state)) return false;
      if (filters.iface !== "all" && j.interface_name !== filters.iface) return false;
      if (filters.ns !== "all" && (j.namespace ?? "default") !== filters.ns) return false;
      if (filters.worker !== "all" && j.latest_worker_id !== filters.worker) return false;
      return (
        !q ||
        [j.job_id, j.interface_name, j.namespace, j.source, j.last_error, j.error_code, j.latest_worker_id, ...Object.values(j.metadata)].some((f) =>
          (f ?? "").toLowerCase().includes(q),
        )
      );
    });
  }, [loadedJobs, effectiveStates, filters.iface, filters.ns, filters.worker, deferredSearch]);

  const selectedJob = useMemo(() => visibleJobs.find((j) => j.job_id === activeSelectedJobId) ?? null, [visibleJobs, activeSelectedJobId]);
  const activeSelectedNs = selectedJob?.namespace && selectedJob.namespace !== "default" ? selectedJob.namespace : undefined;
  const activeJobKey = selectedJob ? `${selectedJob.namespace ?? "default"}::${selectedJob.job_id}` : activeSelectedJobId;

  useEffect(() => setJobDetail(null), [activeSelectedJobId]);
  const selectedJobDetail =
    jobDetail &&
    jobDetail.job.job_id === activeSelectedJobId &&
    (!selectedJob || (jobDetail.job.namespace ?? "default") === (selectedJob.namespace ?? "default"))
      ? jobDetail
      : null;

  usePollingEffect(
    (signal) => getJob(activeSelectedJobId!, signal, activeSelectedNs),
    (next) => { setJobDetail(next); setError(null); },
    (err) => { setJobDetail(null); handleError(err); },
    jobScope === "queue" ? DETAIL_POLL_MS : 2_000,
    [activeTab, jobScope, activeJobKey],
    polling && activeTab === "jobs" && activeSelectedJobId !== null && Boolean(selectedJob) && !(selectedJobDetail && HISTORY_STATES.includes(selectedJobDetail.job.state)),
  );

  useEffect(() => {
    if (activeTab !== "jobs" || !jobsQueryMatches || selectedJob) return;
    const nextId = visibleJobs[0]?.job_id ?? null;
    if (selectedJobByScope[jobScope] !== nextId) setSelectedJobByScope((c) => ({ ...c, [jobScope]: nextId }));
    if (pinnedJobByScope[jobScope] !== null) setPinnedJobByScope((c) => ({ ...c, [jobScope]: null }));
  }, [activeTab, jobsQueryMatches, jobScope, visibleJobs, selectedJob, selectedJobByScope, pinnedJobByScope]);

  async function handleLoadMoreJobs() {
    const cursor = jobsPage.extra.length > 0 ? jobsPage.extraNext : jobsPage.page1Next;
    if (!cursor || jobsPage.loadingMore) return;
    const controller = new AbortController();
    loadMoreAbort.current?.abort();
    loadMoreAbort.current = controller;
    setJobsPage((p) => ({ ...p, loadingMore: true }));
    try {
      const res = await getJobs(effectiveStates, 100, cursor, controller.signal, jobsApiFilters);
      if (!controller.signal.aborted) {
        rememberWorkers(res.jobs.map((j) => j.latest_worker_id));
        setJobsPage((p) => ({ ...p, extra: [...p.extra, ...res.jobs], extraNext: res.next_page_token, total: res.total ?? p.total }));
      }
      setError(null);
    } catch (err) {
      if (!controller.signal.aborted) handleError(err);
    } finally {
      if (loadMoreAbort.current === controller) { loadMoreAbort.current = null; setJobsPage((p) => ({ ...p, loadingMore: false })); }
    }
  }

  const handleCancel = useCallback(async (jobId: string, namespace?: string) => {
    try {
      setIsCancelling(true);
      const ns = namespace && namespace !== "default" ? namespace : undefined;
      const res = ns ? await cancelJob(jobId, ns) : await cancelJob(jobId);
      const replace = (list: JobSummary[]) => list.map((j) => (j.job_id === res.job.job_id ? res.job : j));
      setJobsPage((p) => ({ ...p, page1: replace(p.page1), extra: replace(p.extra) }));
      setJobDetail((cur) =>
        cur && cur.job.job_id === res.job.job_id
          ? { ...cur, job: { ...cur.job, state: res.job.state, updated_at_ms: res.job.updated_at_ms, last_error: res.job.last_error, error_code: res.job.error_code, error_retryable: res.job.error_retryable } }
          : cur,
      );
    } catch (err) { handleError(err); } finally { setIsCancelling(false); }
  }, [handleError]);

  const navigateTo = useCallback((nextTab: ConsoleTab, nextScope?: JobScope) => {
    const scope = nextScope ?? jobScope;
    if (nextScope) {
      if (nextScope !== jobScope) resetExtraJobs();
      setJobScope(nextScope);
      if (nextScope === "all") {
        setFilters((f) => (f.status === "all" ? f : { ...f, status: "all" }));
      } else if (nextScope === "queue") {
        setFilters((f) => (f.status !== "all" && !QUEUE_STATES.includes(f.status) ? { ...f, status: "all" } : f));
      } else if (nextScope === "history") {
        setFilters((f) => (f.status !== "all" && !HISTORY_STATES.includes(f.status) ? { ...f, status: "all" } : f));
      }
    }
    setActiveTab(nextTab);
    const nextPath = routePath(nextTab, scope);
    if (window.location.pathname !== nextPath) window.history.pushState(null, "", nextPath);
  }, [jobScope]);

  const handleStatusFilterChange = useCallback((nextStatus: JobStatusFilter) => {
    resetExtraJobs();
    if (nextStatus !== "all") {
      if (jobScope === "queue" && !QUEUE_STATES.includes(nextStatus)) setJobScope("all");
      else if (jobScope === "history" && !HISTORY_STATES.includes(nextStatus)) setJobScope("all");
    }
    setFilters((f) => ({ ...f, status: nextStatus }));
  }, [jobScope]);

  const jumpToJobs = useCallback((opts: { scope?: JobScope; status?: JobStatusFilter; iface?: string; ns?: string; worker?: string; jobId?: string }) => {
    const scope = opts.scope ?? "all";
    resetExtraJobs();
    if (opts.worker && opts.worker !== "all") rememberWorkers([opts.worker]);
    setFilters({ status: opts.status ?? "all", iface: opts.iface ?? "all", ns: opts.ns ?? "all", worker: opts.worker ?? "all", q: "" });
    setServerSearch("");
    setTimeFilter(DEFAULT_TIME_FILTER);
    setPinnedJobByScope((c) => ({ ...c, [scope]: opts.jobId ?? null }));
    if (opts.jobId) setSelectedJobByScope((c) => ({ ...c, [scope]: opts.jobId! }));
    setJobScope(scope);
    setActiveTab("jobs");
    const nextPath = routePath("jobs", scope);
    if (window.location.pathname !== nextPath) window.history.pushState(null, "", nextPath);
  }, [rememberWorkers]);

  const openInterface = useCallback((name: string) => {
    setSelectedInterface(name);
    navigateTo("interfaces");
  }, [navigateTo]);

  const openWorker = useCallback((workerId: string) => {
    setSelectedWorkerId(workerId);
    navigateTo("workers");
  }, [navigateTo]);

  const openWorkersHealth = useCallback((health: HealthFilter) => {
    navSeedSeq.current += 1;
    setWorkerFilterSeed({ health, seq: navSeedSeq.current });
    navigateTo("workers");
  }, [navigateTo]);

  const openPoolKey = useCallback((key: string) => {
    setSelectedPoolKey(key);
    navigateTo("pools");
  }, [navigateTo]);

  const openPoolsByTarget = useCallback((opts: { ns?: string; iface?: string; pool?: string }) => {
    const match = (overview?.pools ?? []).find(
      (p) =>
        (!opts.ns || p.namespace === opts.ns) &&
        (!opts.iface || p.interface === opts.iface) &&
        (opts.pool == null || formatPool(p.pool) === formatPool(opts.pool)),
    );
    if (match) {
      setSelectedPoolKey(poolKey(match));
      navSeedSeq.current += 1;
      setPoolFilterSeed({ ns: match.namespace, iface: match.interface, seq: navSeedSeq.current });
    } else {
      navSeedSeq.current += 1;
      setPoolFilterSeed({ ns: opts.ns, iface: opts.iface, seq: navSeedSeq.current });
    }
    navigateTo("pools");
  }, [overview?.pools, navigateTo]);

  const filterJobNamespace = useCallback((ns: string) => {
    if (activeTab === "jobs" && filters.ns === ns) {
      openPoolsByTarget({ ns });
    } else {
      jumpToJobs({ ns, scope: activeTab === "jobs" ? jobScope : "all" });
    }
  }, [activeTab, filters.ns, jobScope, jumpToJobs, openPoolsByTarget]);

  const handleDisconnect = useCallback(() => {
    clearConsoleToken();
    setAuthenticated(false); setOverview(null); setWorkers([]); setWorkersResponse(null);
    setJobsPage(EMPTY_JOBS); setJobDetail(null); setError(null); setAuthRequired(true);
  }, []);

  const handleToggleTheme = useCallback(() => setTheme((c) => (c === "light" ? "dark" : "light")), []);
  const handleSelectTab = useCallback((tab: ConsoleTab) => navigateTo(tab), [navigateTo]);

  if (authRequired) {
    return (
      <main className="auth">
        <form className="auth__form" onSubmit={(e) => { e.preventDefault(); setConsoleToken(token); setToken(""); setAuthenticated(true); setError(null); setAuthRequired(false); }}>
          <div className="auth__head"><h1>AnyServe Console</h1><p className="auth__sub">Authenticate with a client token to inspect the control plane.</p></div>
          <label htmlFor="client-token">Client token</label>
          <input id="client-token" type="password" autoComplete="off" value={token} onChange={(e) => setToken(e.target.value)} required />
          {error ? <p className="auth__error" role="alert">{error}</p> : null}
          <button type="submit">Connect</button>
        </form>
      </main>
    );
  }

  const jobStats =
    jobScope === "queue"
      ? [
          { label: "Active", value: overview?.queue.active ?? 0, state: "all" as const, active: filters.status === "all" },
          { label: "Queued", value: overview?.queue.queued ?? 0, state: "queued" as const, active: filters.status === "queued", tone: "warn" as const },
          { label: "Running", value: overview?.queue.running ?? 0, state: "running" as const, active: filters.status === "running", tone: "warn" as const },
        ]
      : jobScope === "history"
        ? [
            { label: "Succeeded", value: overview?.history.succeeded ?? 0, state: "succeeded" as const, active: filters.status === "succeeded", tone: "ok" as const },
            { label: "Failed", value: overview?.history.failed ?? 0, state: "failed" as const, active: filters.status === "failed", tone: "danger" as const },
            { label: "Cancelled", value: overview?.history.cancelled ?? 0, state: "cancelled" as const, active: filters.status === "cancelled", tone: "danger" as const },
            { label: "Total", value: overview?.history.total ?? 0, state: "all" as const, active: filters.status === "all" },
          ]
        : [
            { label: "Queued", value: overview?.queue.queued ?? 0, state: "queued" as const, active: filters.status === "queued", tone: "warn" as const },
            { label: "Running", value: overview?.queue.running ?? 0, state: "running" as const, active: filters.status === "running", tone: "warn" as const },
            { label: "Succeeded", value: overview?.history.succeeded ?? 0, state: "succeeded" as const, active: filters.status === "succeeded", tone: "ok" as const },
            { label: "Failed", value: overview?.history.failed ?? 0, state: "failed" as const, active: filters.status === "failed", tone: "danger" as const },
            { label: "Cancelled", value: overview?.history.cancelled ?? 0, state: "cancelled" as const, active: filters.status === "cancelled", tone: "danger" as const },
          ];

  return (
    <div className="app-shell">
      <Sidebar
        activeTab={activeTab}
        onSelectTab={handleSelectTab}
        overview={overview}
        theme={theme}
        onToggleTheme={handleToggleTheme}
        authenticated={authenticated}
        onDisconnect={handleDisconnect}
      />
      <div className="workspace">
        {error ? <div className="banner" role="alert">{error}</div> : null}
        <main className="layout">
          {activeTab === "overview" ? (
            <OverviewPane
              overview={overview}
              onNavigate={handleSelectTab}
              onOpenJobs={(scope, status) => (status ? jumpToJobs({ scope, status }) : navigateTo("jobs", scope))}
              onOpenWorkersHealth={openWorkersHealth}
              onOpenInterface={openInterface}
              onOpenPool={openPoolKey}
              onOpenPoolByTier={(iface, pool) => openPoolsByTarget({ iface, pool })}
            />
          ) : null}
          {activeTab === "jobs" ? (
            <JobsPane
              scope={jobScope} onScopeChange={(next) => navigateTo("jobs", next)} stats={jobStats} jobs={visibleJobs} totalLoaded={loadedJobs.length}
              totalMatching={jobsQueryMatches && filters.q.trim() === serverSearch ? jobsPage.total : null}
              histogram={jobsPage.histogram}
              histogramRefreshing={!jobsQueryMatches && jobsPage.loadedKey !== null}
              timeFilter={timeFilter}
              onTimeFilterChange={(next) => { resetExtraJobs(); setTimeFilter(next); }}
              selectedJobId={selectedJob?.job_id ?? null}
              onSelect={(id) => {
                setPinnedJobByScope((c) => ({ ...c, [jobScope]: id }));
                setSelectedJobByScope((c) => ({ ...c, [jobScope]: id }));
              }}
              onOpenInterface={openInterface} onOpenWorker={openWorker} onFilterNamespace={filterJobNamespace}
              statusFilter={filters.status} onStatusFilterChange={handleStatusFilterChange}
              interfaceFilter={filters.iface} interfaceOptions={filterOptions.iface} onInterfaceFilterChange={(iface) => { resetExtraJobs(); setFilters((f) => ({ ...f, iface })); }}
              namespaceFilter={filters.ns} namespaceOptions={filterOptions.ns} onNamespaceFilterChange={(ns) => { resetExtraJobs(); setFilters((f) => ({ ...f, ns })); }}
              workerFilter={filters.worker} workerOptions={filterOptions.worker} onWorkerFilterChange={(worker) => { resetExtraJobs(); setFilters((f) => ({ ...f, worker })); }}
              searchQuery={filters.q} onSearchQueryChange={(q) => { if (!q.trim()) { resetExtraJobs(); setServerSearch(""); } setFilters((f) => ({ ...f, q })); }}
              onResetAllFilters={() => {
                resetExtraJobs();
                setFilters({ status: "all", iface: "all", ns: "all", worker: "all", q: "" });
                setServerSearch("");
                setTimeFilter(DEFAULT_TIME_FILTER);
                setPinnedJobByScope((c) => ({ ...c, [jobScope]: null }));
              }}
              hasMore={jobsQueryMatches && (jobsPage.extra.length > 0 ? jobsPage.extraNext : jobsPage.page1Next) != null} loadingMore={jobsPage.loadingMore} onLoadMore={() => void handleLoadMoreJobs()}
              inspector={
                <JobInspector
                  detail={selectedJob ? selectedJobDetail : null}
                  isCancelling={isCancelling}
                  onCancel={handleCancel}
                  onOpenWorker={openWorker}
                  onOpenInterface={openInterface}
                  onFilterNamespace={filterJobNamespace}
                />
              }
            />
          ) : null}
          {activeTab === "interfaces" ? (
            <InterfacesPane
              interfaces={overview?.interfaces ?? []} pools={overview?.pools ?? []} workers={workers}
              selectedInterface={selectedInterface} onSelectInterface={setSelectedInterface}
              onViewJobs={(iface) => jumpToJobs({ iface })} onOpenPool={openPoolKey}
              onOpenPoolByTier={(iface, pool) => openPoolsByTarget({ iface, pool })} onOpenWorker={openWorker}
            />
          ) : null}
          {activeTab === "pools" ? (
            <PoolsPane
              pools={overview?.pools ?? []} interfaces={overview?.interfaces ?? []} workers={workers}
              selectedPoolKey={selectedPoolKey} poolFilterSeed={poolFilterSeed} onSelectPool={setSelectedPoolKey}
              onViewJobs={(iface, ns) => jumpToJobs({ iface, ns })} onOpenInterface={openInterface} onOpenWorker={openWorker}
            />
          ) : null}
          {activeTab === "workers" ? (
            <WorkersPane
              overview={overview} workersResponse={workersResponse} workers={workers}
              selectedWorkerId={selectedWorkerId} workerFilterSeed={workerFilterSeed} onSelectWorker={setSelectedWorkerId}
              onViewJobsForWorker={(worker) => jumpToJobs({ worker })}
              onViewJobsForNamespace={(ns) => jumpToJobs({ ns })}
              onOpenInterface={openInterface}
              onOpenPool={(ns, iface, pool) => openPoolsByTarget({ ns, iface, pool })}
              onOpenConnect={() => setConnectOpen(true)}
            />
          ) : null}
          {activeTab === "explore" ? (
            <Suspense fallback={<section className="panel panel--full"><div className="empty">Loading…</div></section>}>
              <ExplorePane
                journalEnabled={overview ? overview.runtime.journal !== "disabled" : null}
                onAuthenticationError={handleError}
                onOpenWorker={openWorker}
                onOpenInterface={openInterface}
                onFilterNamespace={(ns) => jumpToJobs({ ns })}
                onOpenJobInJobs={(jobId, ns) => jumpToJobs({ jobId, ns, scope: "all" })}
              />
            </Suspense>
          ) : null}
        </main>
      </div>
      <ConnectWorkerModal open={connectOpen} controlPlane={overview?.control_plane} onClose={() => setConnectOpen(false)} />
    </div>
  );
}

export { poolKey };
