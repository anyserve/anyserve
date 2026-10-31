import { lazy, Suspense, useEffect, useMemo, useState } from "react";

import {
  AuthenticationError,
  cancelJob,
  getJob,
  getJobs,
  getOverview,
  getWorkers,
  setConsoleToken,
} from "./api";
import {
  CONSOLE_TABS,
  DETAIL_POLL_MS,
  HISTORY_POLL_MS,
  HISTORY_STATES,
  OVERVIEW_POLL_MS,
  QUEUE_POLL_MS,
  QUEUE_STATES,
  TAB_LABELS,
  TAB_PATHS,
  WORKERS_POLL_MS,
  type ConsoleTab,
} from "./app-constants";
import { asErrorMessage, formatTimestamp, readTabFromLocation, sortJobs, sortWorkers, truncateMiddle } from "./format";
import { HistoryPane, JobsPane, JobInspector, WorkersPane } from "./panes";
import { usePollingEffect } from "./usePollingEffect";
import type { JobDetailResponse, JobSummary, OverviewResponse, WorkerSummary } from "./types";

const ExplorePane = lazy(() => import("./ExplorePane"));

export default function App() {
  const [activeTab, setActiveTab] = useState<ConsoleTab>(() => readTabFromLocation());
  const [overview, setOverview] = useState<OverviewResponse | null>(null);
  const [queueJobs, setQueueJobs] = useState<JobSummary[]>([]);
  const [historyJobs, setHistoryJobs] = useState<JobSummary[]>([]);
  const [workers, setWorkers] = useState<WorkerSummary[]>([]);
  const [historyWorkerFilter, setHistoryWorkerFilter] = useState<string>("all");
  const [selectedQueueJobId, setSelectedQueueJobId] = useState<string | null>(null);
  const [selectedHistoryJobId, setSelectedHistoryJobId] = useState<string | null>(null);
  const [jobDetail, setJobDetail] = useState<JobDetailResponse | null>(null);
  const [isCancelling, setIsCancelling] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [authRequired, setAuthRequired] = useState(false);
  const [token, setToken] = useState("");

  function handleError(nextError: unknown) {
    setError(asErrorMessage(nextError));
    if (nextError instanceof AuthenticationError) {
      setAuthRequired(true);
    }
  }

  const polling = !authRequired;
  const activeSelectedJobId =
    activeTab === "history"
      ? selectedHistoryJobId
      : activeTab === "queue"
        ? selectedQueueJobId
        : null;

  useEffect(() => {
    const nextTab = readTabFromLocation();
    setActiveTab(nextTab);

    if (window.location.pathname !== TAB_PATHS[nextTab]) {
      window.history.replaceState(null, "", TAB_PATHS[nextTab]);
    }

    function handlePopState() {
      setActiveTab(readTabFromLocation());
    }

    window.addEventListener("popstate", handlePopState);
    return () => {
      window.removeEventListener("popstate", handlePopState);
    };
  }, []);

  usePollingEffect(
    getOverview,
    (nextOverview) => {
      setOverview(nextOverview);
      setError(null);
    },
    handleError,
    activeTab === "queue" ? OVERVIEW_POLL_MS : 2_000,
    [activeTab],
    polling,
  );

  usePollingEffect(
    (signal) => getJobs(QUEUE_STATES, 100, 0, signal),
    (response) => {
      setQueueJobs(sortJobs(response.jobs));
      setError(null);
    },
    handleError,
    QUEUE_POLL_MS,
    [],
    polling && activeTab === "queue",
  );

  usePollingEffect(
    (signal) => getJobs(HISTORY_STATES, 100, 0, signal),
    (response) => {
      setHistoryJobs(sortJobs(response.jobs));
      setError(null);
    },
    handleError,
    HISTORY_POLL_MS,
    [],
    polling && activeTab === "history",
  );

  usePollingEffect(
    getWorkers,
    (response) => {
      setWorkers(sortWorkers(response.workers));
      setError(null);
    },
    handleError,
    WORKERS_POLL_MS,
    [],
    polling && activeTab === "workers",
  );

  useEffect(() => {
    setJobDetail(null);
  }, [activeSelectedJobId]);

  const selectedJobDetail = jobDetail?.job.job_id === activeSelectedJobId ? jobDetail : null;

  usePollingEffect(
    (signal) => getJob(activeSelectedJobId!, signal),
    (nextDetail) => {
      setJobDetail(nextDetail);
      setError(null);
    },
    (nextError) => {
      setJobDetail(null);
      handleError(nextError);
    },
    activeTab === "queue" ? DETAIL_POLL_MS : 2_000,
    [activeTab, activeSelectedJobId],
    polling && activeSelectedJobId !== null &&
      !(selectedJobDetail && HISTORY_STATES.includes(selectedJobDetail.job.state)),
  );

  const queueSelection = useMemo(
    () => queueJobs.find((job) => job.job_id === selectedQueueJobId) ?? null,
    [queueJobs, selectedQueueJobId],
  );
  const historyWorkerOptions = useMemo(() => {
    const ids = new Set<string>();
    for (const worker of workers) {
      ids.add(worker.worker_id);
    }
    for (const job of historyJobs) {
      if (job.latest_worker_id) {
        ids.add(job.latest_worker_id);
      }
    }

    return [
      { value: "all", label: "All workers" },
      ...Array.from(ids)
        .sort()
        .map((workerId) => ({
          value: workerId,
          label: truncateMiddle(workerId, 28),
        })),
    ];
  }, [historyJobs, workers]);
  const filteredHistoryJobs = useMemo(() => {
    if (historyWorkerFilter === "all") {
      return historyJobs;
    }
    return historyJobs.filter((job) => job.latest_worker_id === historyWorkerFilter);
  }, [historyJobs, historyWorkerFilter]);
  const historySelection = useMemo(
    () => filteredHistoryJobs.find((job) => job.job_id === selectedHistoryJobId) ?? null,
    [filteredHistoryJobs, selectedHistoryJobId],
  );

  useEffect(() => {
    if (
      historyWorkerFilter !== "all" &&
      !historyWorkerOptions.some((option) => option.value === historyWorkerFilter)
    ) {
      setHistoryWorkerFilter("all");
    }
  }, [historyWorkerFilter, historyWorkerOptions]);

  useEffect(() => {
    if (activeTab === "workers" || activeTab === "explore") {
      return;
    }

    if (activeTab === "history") {
      if (filteredHistoryJobs.length === 0) {
        setSelectedHistoryJobId(null);
        return;
      }

      if (
        !selectedHistoryJobId ||
        !filteredHistoryJobs.some((job) => job.job_id === selectedHistoryJobId)
      ) {
        setSelectedHistoryJobId(filteredHistoryJobs[0].job_id);
      }
      return;
    }

    if (queueJobs.length === 0) {
      setSelectedQueueJobId(null);
      return;
    }

    if (!selectedQueueJobId || !queueJobs.some((job) => job.job_id === selectedQueueJobId)) {
      setSelectedQueueJobId(queueJobs[0].job_id);
    }
  }, [activeTab, filteredHistoryJobs, queueJobs, selectedHistoryJobId, selectedQueueJobId]);

  async function handleCancel(jobId: string) {
    try {
      setIsCancelling(true);
      const response = await cancelJob(jobId);
      setQueueJobs((current) =>
        current.filter((job) => job.job_id !== response.job.job_id),
      );
      setHistoryJobs((current) => sortJobs([response.job, ...current]));
      setSelectedHistoryJobId(response.job.job_id);
      setSelectedQueueJobId((current) =>
        current === response.job.job_id ? null : current,
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

  function handleTabChange(nextTab: ConsoleTab) {
    if (nextTab === activeTab) {
      return;
    }

    setActiveTab(nextTab);
    window.history.pushState(null, "", TAB_PATHS[nextTab]);
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
            setError(null);
            setAuthRequired(false);
          }}
        >
          <h1>Anyserve Console</h1>
          <label htmlFor="client-token">Client token</label>
          <input
            id="client-token"
            type="password"
            autoComplete="off"
            value={token}
            onChange={(event) => setToken(event.target.value)}
            required
          />
          {error ? <p role="alert">{error}</p> : null}
          <button type="submit">Connect</button>
        </form>
      </main>
    );
  }

  return (
    <div className="shell">
      <header className="topbar">
        <div className="topbar__brand">
          <span className="topbar__mark" aria-hidden="true" />
          Anyserve
        </div>
        <nav className="tabs" aria-label="Console sections">
          {CONSOLE_TABS.map((tab) => (
            <button
              key={tab}
              type="button"
              className={`tabs__item${activeTab === tab ? " is-active" : ""}`}
              onClick={() => handleTabChange(tab)}
            >
              {TAB_LABELS[tab]}
            </button>
          ))}
        </nav>
        <div className="topbar__status">
          <span className={`chip${overview ? " chip--live" : ""}`}>
            {overview?.runtime.label ?? "Not connected"}
          </span>
          {overview ? <span className="topbar__refreshed">{formatTimestamp(overview.refreshed_at_ms)}</span> : null}
        </div>
      </header>

      {error ? (
        <div className="banner" role="alert">
          {error}
        </div>
      ) : null}

      <main className="layout">
        {activeTab === "explore" ? <Suspense fallback={<section className="panel">Loading Explore…</section>}>
          <ExplorePane journalEnabled={overview ? overview.runtime.journal === "sqlite" : null} onAuthenticationError={handleError} />
        </Suspense> : null}
        {activeTab === "queue" ? (
          <JobsPane
            title="Queue"
            meta={`${queueJobs.length} active jobs`}
            stats={[
              { label: "Active", value: overview?.queue.active ?? 0 },
              { label: "Queued", value: overview?.queue.queued ?? 0 },
              { label: "Running", value: overview?.queue.running ?? 0 },
            ]}
            jobs={queueJobs}
            selectedJobId={queueSelection?.job_id ?? null}
            emptyMessage="No active jobs"
            onSelect={setSelectedQueueJobId}
            inspector={
              queueSelection ? (
                <JobInspector
                  detail={selectedJobDetail}
                  isCancelling={isCancelling}
                  onCancel={handleCancel}
                />
              ) : null
            }
          />
        ) : null}

        {activeTab === "workers" ? (
          <WorkersPane overview={overview} workers={workers} />
        ) : null}

        {activeTab === "history" ? (
          <HistoryPane
            overview={overview}
            jobs={filteredHistoryJobs}
            totalJobs={historyJobs.length}
            selectedJobId={historySelection?.job_id ?? null}
            detail={historySelection ? selectedJobDetail : null}
            workerFilter={historyWorkerFilter}
            workerOptions={historyWorkerOptions}
            onWorkerFilterChange={setHistoryWorkerFilter}
            onSelect={setSelectedHistoryJobId}
          />
        ) : null}
      </main>
    </div>
  );
}
