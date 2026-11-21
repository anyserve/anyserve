import { lazy, Suspense, useEffect, useMemo, useRef, useState } from "react";

import {
  AuthenticationError,
  cancelJob,
  clearConsoleToken,
  getJob,
  getJobs,
  getOverview,
  getWorkers,
  hasConsoleToken,
  setConsoleToken,
} from "./api";
import {
  ALL_JOB_STATES,
  DETAIL_POLL_MS,
  HISTORY_STATES,
  JOBS_ACTIVE_POLL_MS,
  JOBS_HISTORY_POLL_MS,
  JOB_SCOPE_PATHS,
  OVERVIEW_POLL_MS,
  QUEUE_STATES,
  TAB_PATHS,
  WORKERS_POLL_MS,
  routeFromPathname,
  type ConsoleTab,
  type JobScope,
} from "./app-constants";
import { asErrorMessage, sortJobs, sortWorkers, truncateMiddle } from "./format";
import { t, type Lang, type Theme } from "./i18n";
import { InterfacesPane } from "./InterfacesPane";
import { JobsPane } from "./JobsPane";
import { OverviewPane } from "./OverviewPane";
import { JobInspector } from "./pane-inspector";
import { PoolsPane, poolKey } from "./PoolsPane";
import { Sidebar } from "./Sidebar";
import type {
  JobDetailResponse,
  JobSummary,
  OverviewResponse,
  WorkerSummary,
  WorkersResponse,
} from "./types";
import { usePollingEffect } from "./usePollingEffect";
import { WorkersPane } from "./WorkersPane";

const ExplorePane = lazy(() => import("./ExplorePane"));
const THEME_STORAGE_KEY = "anyserve.console.theme";

function readInitialTheme(): Theme {
  try {
    const saved = localStorage.getItem(THEME_STORAGE_KEY);
    if (saved === "dark" || saved === "light") return saved;
  } catch {
    // Ignore storage access errors.
  }
  return "light";
}

function statesForScope(scope: JobScope) {
  if (scope === "queue") return QUEUE_STATES;
  if (scope === "history") return HISTORY_STATES;
  return ALL_JOB_STATES;
}

function pathForRoute(tab: ConsoleTab, jobScope: JobScope): string {
  if (tab === "jobs") {
    return JOB_SCOPE_PATHS[jobScope];
  }
  return TAB_PATHS[tab];
}

export default function App() {
  const initialRoute = useMemo(() => routeFromPathname(window.location.pathname), []);
  const [activeTab, setActiveTab] = useState<ConsoleTab>(initialRoute.tab);
  const [jobScope, setJobScope] = useState<JobScope>(initialRoute.jobScope ?? "all");

  const [lang, setLang] = useState<Lang>("en");
  const [theme, setTheme] = useState<Theme>(() => readInitialTheme());

  const [overview, setOverview] = useState<OverviewResponse | null>(null);
  const [workersResponse, setWorkersResponse] = useState<WorkersResponse | null>(null);
  const [workers, setWorkers] = useState<WorkerSummary[]>([]);

  const [page1Jobs, setPage1Jobs] = useState<JobSummary[]>([]);
  const [jobsLoadedKey, setJobsLoadedKey] = useState<string | null>(null);
  const [extraJobs, setExtraJobs] = useState<JobSummary[]>([]);
  const [page1NextToken, setPage1NextToken] = useState<string | null>(null);
  const [extraNextToken, setExtraNextToken] = useState<string | null>(null);
  const [loadingMore, setLoadingMore] = useState(false);

  const [interfaceFilter, setInterfaceFilter] = useState<string>("all");
  const [namespaceFilter, setNamespaceFilter] = useState<string>("all");
  const [workerFilter, setWorkerFilter] = useState<string>("all");
  const [jobSearch, setJobSearch] = useState<string>("");

  const [selectedJobByScope, setSelectedJobByScope] = useState<Record<JobScope, string | null>>({
    all: null,
    queue: null,
    history: null,
  });
  const [selectedInterface, setSelectedInterface] = useState<string | null>(null);
  const [selectedPoolKey, setSelectedPoolKey] = useState<string | null>(null);
  const [selectedWorkerId, setSelectedWorkerId] = useState<string | null>(null);

  const [jobDetail, setJobDetail] = useState<JobDetailResponse | null>(null);
  const [isCancelling, setIsCancelling] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [authRequired, setAuthRequired] = useState(false);
  const [authenticated, setAuthenticated] = useState(() => hasConsoleToken());
  const [token, setToken] = useState("");
  const loadMoreAbort = useRef<AbortController | null>(null);

  useEffect(() => {
    document.documentElement.setAttribute("data-theme", theme);
    try {
      localStorage.setItem(THEME_STORAGE_KEY, theme);
    } catch {
      // Ignore storage access errors.
    }
  }, [theme]);

  useEffect(() => {
    document.documentElement.lang = lang;
  }, [lang]);

  useEffect(() => {
    const route = routeFromPathname(window.location.pathname);
    setActiveTab(route.tab);
    if (route.jobScope) {
      setJobScope(route.jobScope);
    }

    const expectedPath = pathForRoute(route.tab, route.jobScope ?? "all");
    if (window.location.pathname !== expectedPath) {
      window.history.replaceState(null, "", expectedPath);
    }

    function handlePopState() {
      const next = routeFromPathname(window.location.pathname);
      setActiveTab(next.tab);
      if (next.jobScope) {
        setJobScope(next.jobScope);
      }
    }

    window.addEventListener("popstate", handlePopState);
    return () => {
      window.removeEventListener("popstate", handlePopState);
    };
  }, []);

  useEffect(() => () => {
    loadMoreAbort.current?.abort();
  }, []);

  function handleError(nextError: unknown) {
    setError(asErrorMessage(nextError));
    if (nextError instanceof AuthenticationError) {
      setAuthRequired(true);
    }
  }

  function resetJobPagination() {
    loadMoreAbort.current?.abort();
    loadMoreAbort.current = null;
    setExtraJobs([]);
    setExtraNextToken(null);
    setLoadingMore(false);
  }

  const polling = !authRequired;
  const activeSelectedJobId = activeTab === "jobs" ? selectedJobByScope[jobScope] : null;

  usePollingEffect(
    getOverview,
    (nextOverview) => {
      setOverview(nextOverview);
      setError(null);
    },
    handleError,
    activeTab === "overview" || (activeTab === "jobs" && jobScope !== "history")
      ? OVERVIEW_POLL_MS
      : 2_000,
    [activeTab, jobScope],
    polling,
  );

  const jobsQueryKey = `${jobScope}|${interfaceFilter}|${namespaceFilter}`;
  const jobsQueryMatches = jobsLoadedKey === jobsQueryKey;

  usePollingEffect(
    (signal) =>
      getJobs(statesForScope(jobScope), 100, 0, signal, {
        interface: interfaceFilter,
        namespace: namespaceFilter,
      }),
    (response) => {
      setPage1Jobs(sortJobs(response.jobs));
      setPage1NextToken(response.next_page_token);
      setJobsLoadedKey(jobsQueryKey);
      setError(null);
    },
    handleError,
    jobScope === "history" ? JOBS_HISTORY_POLL_MS : JOBS_ACTIVE_POLL_MS,
    [jobScope, interfaceFilter, namespaceFilter],
    polling && activeTab === "jobs",
  );

  usePollingEffect(
    getWorkers,
    (response) => {
      setWorkersResponse(response);
      setWorkers(sortWorkers(response.workers));
      setError(null);
    },
    handleError,
    WORKERS_POLL_MS,
    [],
    polling &&
      (activeTab === "overview" ||
        activeTab === "workers" ||
        activeTab === "interfaces" ||
        activeTab === "pools"),
  );

  const loadedJobs = useMemo(() => {
    if (!jobsQueryMatches) return [];
    if (extraJobs.length === 0) return page1Jobs;
    const seen = new Set<string>();
    const combined: JobSummary[] = [];
    for (const job of page1Jobs) {
      const k = `${job.namespace ?? "default"}::${job.job_id}`;
      if (!seen.has(k)) {
        seen.add(k);
        combined.push(job);
      }
    }
    for (const job of extraJobs) {
      const k = `${job.namespace ?? "default"}::${job.job_id}`;
      if (!seen.has(k)) {
        seen.add(k);
        combined.push(job);
      }
    }
    return combined;
  }, [jobsQueryMatches, page1Jobs, extraJobs]);

  const interfaceFilterOptions = useMemo(() => {
    const names = new Set<string>();
    for (const item of overview?.interfaces ?? []) names.add(item.name);
    for (const job of loadedJobs) names.add(job.interface_name);
    if (interfaceFilter !== "all") names.add(interfaceFilter);
    return [
      { value: "all", label: t(lang, "jobs.filter.allInterfaces") },
      ...Array.from(names)
        .sort()
        .map((name) => ({ value: name, label: name })),
    ];
  }, [overview, loadedJobs, interfaceFilter, lang]);

  const namespaceFilterOptions = useMemo(() => {
    const names = new Set<string>();
    for (const p of overview?.pools ?? []) names.add(p.namespace);
    for (const w of workers) {
      if (w.namespace) names.add(w.namespace);
    }
    for (const job of loadedJobs) {
      if (job.namespace) names.add(job.namespace);
    }
    if (namespaceFilter !== "all") names.add(namespaceFilter);
    return [
      { value: "all", label: t(lang, "jobs.filter.allNamespaces") },
      ...Array.from(names)
        .sort()
        .map((ns) => ({ value: ns, label: ns })),
    ];
  }, [overview, workers, loadedJobs, namespaceFilter, lang]);

  const workerFilterOptions = useMemo(() => {
    const ids = new Set<string>();
    for (const worker of workers) ids.add(worker.worker_id);
    for (const job of loadedJobs) {
      if (job.latest_worker_id) ids.add(job.latest_worker_id);
    }
    if (workerFilter !== "all") ids.add(workerFilter);
    return [
      { value: "all", label: t(lang, "jobs.filter.allWorkers") },
      ...Array.from(ids)
        .sort()
        .map((workerId) => ({
          value: workerId,
          label: truncateMiddle(workerId, 28),
        })),
    ];
  }, [loadedJobs, workers, workerFilter, lang]);

  const visibleJobs = useMemo(() => {
    const query = jobSearch.trim().toLowerCase();
    return loadedJobs.filter((job) => {
      if (workerFilter !== "all" && job.latest_worker_id !== workerFilter) return false;
      if (!query) return true;
      return (
        job.job_id.toLowerCase().includes(query) ||
        job.interface_name.toLowerCase().includes(query) ||
        (job.namespace ?? "").toLowerCase().includes(query) ||
        (job.source ?? "").toLowerCase().includes(query) ||
        (job.last_error ?? "").toLowerCase().includes(query) ||
        (job.error_code ?? "").toLowerCase().includes(query) ||
        (job.latest_worker_id ?? "").toLowerCase().includes(query)
      );
    });
  }, [loadedJobs, workerFilter, jobSearch]);

  const selectedJob = useMemo(
    () => visibleJobs.find((job) => job.job_id === activeSelectedJobId) ?? null,
    [visibleJobs, activeSelectedJobId],
  );

  const activeSelectedNamespace =
    selectedJob?.namespace && selectedJob.namespace !== "default"
      ? selectedJob.namespace
      : undefined;

  useEffect(() => {
    setJobDetail(null);
  }, [activeSelectedJobId, activeSelectedNamespace]);

  const selectedJobDetail = jobDetail?.job.job_id === activeSelectedJobId ? jobDetail : null;

  usePollingEffect(
    (signal) => getJob(activeSelectedJobId!, signal, activeSelectedNamespace),
    (nextDetail) => {
      setJobDetail(nextDetail);
      setError(null);
    },
    (nextError) => {
      setJobDetail(null);
      handleError(nextError);
    },
    jobScope === "queue" ? DETAIL_POLL_MS : 2_000,
    [activeTab, jobScope, activeSelectedJobId, activeSelectedNamespace],
    polling &&
      activeTab === "jobs" &&
      activeSelectedJobId !== null &&
      !(selectedJobDetail && HISTORY_STATES.includes(selectedJobDetail.job.state)),
  );

  useEffect(() => {
    if (
      workerFilter !== "all" &&
      !workerFilterOptions.some((option) => option.value === workerFilter)
    ) {
      setWorkerFilter("all");
    }
  }, [workerFilter, workerFilterOptions]);

  useEffect(() => {
    if (activeTab !== "jobs" || !jobsQueryMatches) return;
    if (!selectedJob) {
      const nextId = visibleJobs[0]?.job_id ?? null;
      if (selectedJobByScope[jobScope] !== nextId) {
        setSelectedJobByScope((current) => ({ ...current, [jobScope]: nextId }));
      }
    }
  }, [activeTab, jobsQueryMatches, jobScope, visibleJobs, selectedJob, selectedJobByScope]);

  async function handleLoadMoreJobs() {
    const tokenCursor = extraJobs.length > 0 ? extraNextToken : page1NextToken;
    if (!tokenCursor || loadingMore) return;
    const controller = new AbortController();
    loadMoreAbort.current?.abort();
    loadMoreAbort.current = controller;
    try {
      setLoadingMore(true);
      const response = await getJobs(statesForScope(jobScope), 100, tokenCursor, controller.signal, {
        interface: interfaceFilter,
        namespace: namespaceFilter,
      });
      if (controller.signal.aborted) return;
      setExtraJobs((current) => [...current, ...response.jobs]);
      setExtraNextToken(response.next_page_token);
      setError(null);
    } catch (nextError) {
      if (!controller.signal.aborted) {
        handleError(nextError);
      }
    } finally {
      if (loadMoreAbort.current === controller) {
        loadMoreAbort.current = null;
        setLoadingMore(false);
      }
    }
  }

  async function handleCancel(jobId: string, namespace?: string) {
    try {
      setIsCancelling(true);
      const nsArg = namespace && namespace !== "default" ? namespace : undefined;
      const response = nsArg ? await cancelJob(jobId, nsArg) : await cancelJob(jobId);
      setPage1Jobs((current) =>
        current.map((job) => (job.job_id === response.job.job_id ? response.job : job)),
      );
      setExtraJobs((current) =>
        current.map((job) => (job.job_id === response.job.job_id ? response.job : job)),
      );
      setJobDetail((current) =>
        current && current.job.job_id === response.job.job_id
          ? {
              ...current,
              job: {
                ...current.job,
                state: response.job.state,
                updated_at_ms: response.job.updated_at_ms,
                last_error: response.job.last_error,
                error_code: response.job.error_code,
                error_retryable: response.job.error_retryable,
              },
            }
          : current,
      );
    } catch (nextError) {
      handleError(nextError);
    } finally {
      setIsCancelling(false);
    }
  }

  function navigateTo(nextTab: ConsoleTab, nextScope?: JobScope) {
    const resolvedScope = nextScope ?? jobScope;
    if (nextScope) {
      if (nextScope !== jobScope) {
        resetJobPagination();
      }
      setJobScope(nextScope);
    }
    setActiveTab(nextTab);
    const nextPath = pathForRoute(nextTab, resolvedScope);
    if (window.location.pathname !== nextPath) {
      window.history.pushState(null, "", nextPath);
    }
  }

  function handleScopeChange(nextScope: JobScope) {
    if (nextScope === jobScope) return;
    resetJobPagination();
    setJobScope(nextScope);
    const nextPath = JOB_SCOPE_PATHS[nextScope];
    if (window.location.pathname !== nextPath) {
      window.history.pushState(null, "", nextPath);
    }
  }

  function handleInterfaceFilterChange(nextValue: string) {
    resetJobPagination();
    setInterfaceFilter(nextValue);
  }

  function handleNamespaceFilterChange(nextValue: string) {
    resetJobPagination();
    setNamespaceFilter(nextValue);
  }

  function handleDisconnect() {
    clearConsoleToken();
    setAuthenticated(false);
    setOverview(null);
    setWorkers([]);
    setWorkersResponse(null);
    setPage1Jobs([]);
    resetJobPagination();
    setJobDetail(null);
    setError(null);
    setAuthRequired(true);
  }

  if (authRequired) {
    return (
      <main className="auth">
        <form
          className="auth__form"
          onSubmit={(event) => {
            event.preventDefault();
            setConsoleToken(token);
            setToken("");
            setAuthenticated(true);
            setError(null);
            setAuthRequired(false);
          }}
        >
          <div className="auth__head">
            <h1>{t(lang, "auth.title")}</h1>
            <p className="auth__sub">{t(lang, "auth.subtitle")}</p>
          </div>
          <label htmlFor="client-token">{t(lang, "auth.tokenLabel")}</label>
          <input
            id="client-token"
            type="password"
            autoComplete="off"
            value={token}
            onChange={(event) => setToken(event.target.value)}
            required
          />
          {error ? (
            <p className="auth__error" role="alert">
              {error}
            </p>
          ) : null}
          <button type="submit">{t(lang, "auth.connect")}</button>
        </form>
      </main>
    );
  }

  const jobStats =
    jobScope === "history"
      ? [
          { label: t(lang, "overview.card.succeeded"), value: overview?.history.succeeded ?? 0 },
          { label: t(lang, "overview.card.failed"), value: overview?.history.failed ?? 0 },
          { label: t(lang, "overview.card.cancelled"), value: overview?.history.cancelled ?? 0 },
          { label: t(lang, "common.all"), value: overview?.history.total ?? 0 },
        ]
      : [
          { label: t(lang, "overview.card.active"), value: overview?.queue.active ?? 0 },
          { label: t(lang, "overview.card.queued"), value: overview?.queue.queued ?? 0 },
          { label: t(lang, "overview.card.running"), value: overview?.queue.running ?? 0 },
          { label: t(lang, "overview.card.succeeded"), value: overview?.history.succeeded ?? 0 },
        ];

  const activeNextPageToken = extraJobs.length > 0 ? extraNextToken : page1NextToken;

  return (
    <div className="app-shell">
      <Sidebar
        activeTab={activeTab}
        onSelectTab={(tab) => navigateTo(tab)}
        overview={overview}
        lang={lang}
        onToggleLang={() => setLang((current) => (current === "en" ? "zh" : "en"))}
        theme={theme}
        onToggleTheme={() => setTheme((current) => (current === "light" ? "dark" : "light"))}
        authenticated={authenticated}
        onDisconnect={handleDisconnect}
      />

      <div className="workspace">
        {error ? (
          <div className="banner" role="alert">
            {error}
          </div>
        ) : null}

        <main className="layout">
          {activeTab === "overview" ? (
            <OverviewPane
              overview={overview}
              workersResponse={workersResponse}
              lang={lang}
              onNavigate={(tab) => navigateTo(tab)}
              onOpenInterface={(name) => {
                setSelectedInterface(name);
                navigateTo("interfaces");
              }}
              onOpenPool={(key) => {
                setSelectedPoolKey(key);
                navigateTo("pools");
              }}
            />
          ) : null}

          {activeTab === "jobs" ? (
            <JobsPane
              scope={jobScope}
              onScopeChange={handleScopeChange}
              stats={jobStats}
              jobs={visibleJobs}
              totalLoaded={loadedJobs.length}
              selectedJobId={selectedJob?.job_id ?? null}
              onSelect={(jobId) =>
                setSelectedJobByScope((current) => ({ ...current, [jobScope]: jobId }))
              }
              interfaceFilter={interfaceFilter}
              interfaceOptions={interfaceFilterOptions}
              onInterfaceFilterChange={handleInterfaceFilterChange}
              namespaceFilter={namespaceFilter}
              namespaceOptions={namespaceFilterOptions}
              onNamespaceFilterChange={handleNamespaceFilterChange}
              workerFilter={workerFilter}
              workerOptions={workerFilterOptions}
              onWorkerFilterChange={setWorkerFilter}
              searchQuery={jobSearch}
              onSearchQueryChange={setJobSearch}
              hasMore={activeNextPageToken != null}
              loadingMore={loadingMore}
              onLoadMore={() => void handleLoadMoreJobs()}
              lang={lang}
              inspector={
                <JobInspector
                  detail={selectedJob ? selectedJobDetail : null}
                  isCancelling={isCancelling}
                  onCancel={handleCancel}
                  lang={lang}
                />
              }
            />
          ) : null}

          {activeTab === "interfaces" ? (
            <InterfacesPane
              interfaces={overview?.interfaces ?? []}
              pools={overview?.pools ?? []}
              workers={workers}
              selectedInterface={selectedInterface}
              onSelectInterface={setSelectedInterface}
              onViewJobs={(iface) => {
                resetJobPagination();
                setInterfaceFilter(iface);
                setNamespaceFilter("all");
                setWorkerFilter("all");
                navigateTo("jobs", "all");
              }}
              onOpenPool={(key) => {
                setSelectedPoolKey(key);
                navigateTo("pools");
              }}
              onOpenWorker={(workerId) => {
                setSelectedWorkerId(workerId);
                navigateTo("workers");
              }}
              lang={lang}
            />
          ) : null}

          {activeTab === "pools" ? (
            <PoolsPane
              pools={overview?.pools ?? []}
              interfaces={overview?.interfaces ?? []}
              workers={workers}
              selectedPoolKey={selectedPoolKey}
              onSelectPool={(key) => setSelectedPoolKey(key)}
              onViewJobs={(iface, ns) => {
                resetJobPagination();
                setInterfaceFilter(iface);
                setNamespaceFilter(ns);
                setWorkerFilter("all");
                navigateTo("jobs", "all");
              }}
              onOpenWorker={(workerId) => {
                setSelectedWorkerId(workerId);
                navigateTo("workers");
              }}
              lang={lang}
            />
          ) : null}

          {activeTab === "workers" ? (
            <WorkersPane
              overview={overview}
              workersResponse={workersResponse}
              workers={workers}
              selectedWorkerId={selectedWorkerId}
              onSelectWorker={setSelectedWorkerId}
              onViewJobsForWorker={(workerId) => {
                resetJobPagination();
                setWorkerFilter(workerId);
                setInterfaceFilter("all");
                setNamespaceFilter("all");
                navigateTo("jobs", "all");
              }}
              onOpenInterface={(iface) => {
                setSelectedInterface(iface);
                navigateTo("interfaces");
              }}
              lang={lang}
            />
          ) : null}

          {activeTab === "explore" ? (
            <Suspense
              fallback={
                <section className="panel panel--full">
                  <div className="empty">{t(lang, "common.loading")}</div>
                </section>
              }
            >
              <ExplorePane
                journalEnabled={overview ? overview.runtime.journal !== "disabled" : null}
                onAuthenticationError={handleError}
                lang={lang}
              />
            </Suspense>
          ) : null}
        </main>
      </div>
    </div>
  );
}

export { poolKey };
