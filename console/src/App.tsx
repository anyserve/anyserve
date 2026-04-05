import { useEffect, useMemo, useState } from "react";

import { cancelJob, getJob, getJobs, getOverview, getWorkers } from "./api";
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
    async () => {
      try {
        const nextOverview = await getOverview();
        setOverview(nextOverview);
        setError(null);
      } catch (nextError) {
        setError(asErrorMessage(nextError));
      }
    },
    activeTab === "queue" ? OVERVIEW_POLL_MS : 2_000,
    [activeTab],
  );

  usePollingEffect(
    async () => {
      try {
        const response = await getJobs(QUEUE_STATES, 100, 0);
        setQueueJobs(sortJobs(response.jobs));
        setError(null);
      } catch (nextError) {
        setError(asErrorMessage(nextError));
      }
    },
    activeTab === "queue" ? QUEUE_POLL_MS : 3_000,
    [activeTab],
  );

  usePollingEffect(
    async () => {
      try {
        const response = await getJobs(HISTORY_STATES, 100, 0);
        setHistoryJobs(sortJobs(response.jobs));
        setError(null);
      } catch (nextError) {
        setError(asErrorMessage(nextError));
      }
    },
    activeTab === "history" ? 2_000 : HISTORY_POLL_MS,
    [activeTab],
  );

  usePollingEffect(
    async () => {
      try {
        const response = await getWorkers();
        setWorkers(sortWorkers(response.workers));
        setError(null);
      } catch (nextError) {
        setError(asErrorMessage(nextError));
      }
    },
    activeTab === "workers" ? 2_000 : WORKERS_POLL_MS,
    [activeTab],
  );

  useEffect(() => {
    if (!activeSelectedJobId) {
      setJobDetail(null);
    }
  }, [activeSelectedJobId]);

  usePollingEffect(
    async () => {
      if (!activeSelectedJobId) {
        setJobDetail(null);
        return;
      }

      try {
        const nextDetail = await getJob(activeSelectedJobId);
        setJobDetail(nextDetail);
        setError(null);
      } catch (nextError) {
        setJobDetail(null);
        setError(asErrorMessage(nextError));
      }
    },
    activeTab === "queue" ? DETAIL_POLL_MS : 2_000,
    [activeTab, activeSelectedJobId],
    activeSelectedJobId !== null,
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
    if (activeTab === "workers") {
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
      setError(asErrorMessage(nextError));
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

  return (
    <div className="shell">
      <header className="topbar">
        <div className="topbar__row">
          <div className="topbar__brand">
            <div className="topbar__title">Anyserve Console</div>
            <div className="topbar__meta">
              <span>Mode</span>
              <strong>{overview?.runtime.label ?? "Loading"}</strong>
            </div>
          </div>

          <div className="topbar__status">
            <span className="topbar__live">Live</span>
            <span>Refreshed</span>
            <strong>{formatTimestamp(overview?.refreshed_at_ms ?? null)}</strong>
          </div>
        </div>

        <div className="topbar__row topbar__row--nav">
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
        </div>
      </header>

      {error ? (
        <div className="banner" role="alert">
          {error}
        </div>
      ) : null}

      <main className="layout">
        {activeTab === "queue" ? (
          <JobsPane
            title="Queue"
            meta={`${queueJobs.length} active jobs`}
            stats={[
              { label: "Active", value: overview?.queue.active ?? 0 },
              { label: "Pending", value: overview?.queue.pending ?? 0 },
              { label: "Leased", value: overview?.queue.leased ?? 0 },
              { label: "Running", value: overview?.queue.running ?? 0 },
            ]}
            jobs={queueJobs}
            selectedJobId={queueSelection?.job_id ?? null}
            emptyMessage="No active jobs"
            onSelect={setSelectedQueueJobId}
            inspector={
              queueSelection ? (
                <JobInspector
                  detail={jobDetail}
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
            detail={historySelection ? jobDetail : null}
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
