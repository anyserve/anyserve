import { useEffect, useMemo, useState } from "react";

import { cancelJob, getJob, getJobs, getOverview, getWorkers } from "./api";
import type {
  JobDetailResponse,
  JobState,
  JobSummary,
  OverviewResponse,
  WorkerSummary,
} from "./types";

type ConsoleTab = "queue" | "workers" | "history";

const QUEUE_STATES: JobState[] = ["pending", "leased", "running"];
const HISTORY_STATES: JobState[] = ["succeeded", "failed", "cancelled"];
const QUEUE_POLL_MS = 800;
const OVERVIEW_POLL_MS = 1_000;
const DETAIL_POLL_MS = 1_000;
const WORKERS_POLL_MS = 4_000;
const HISTORY_POLL_MS = 6_000;

const timeFormatter = new Intl.DateTimeFormat(undefined, {
  month: "short",
  day: "2-digit",
  hour: "2-digit",
  minute: "2-digit",
  second: "2-digit",
});

const TAB_LABELS: Record<ConsoleTab, string> = {
  queue: "Queue",
  workers: "Workers",
  history: "History",
};

const TAB_PATHS: Record<ConsoleTab, string> = {
  queue: "/queue",
  workers: "/workers",
  history: "/history",
};

export default function App() {
  const [activeTab, setActiveTab] = useState<ConsoleTab>(() => readTabFromLocation());
  const [overview, setOverview] = useState<OverviewResponse | null>(null);
  const [queueJobs, setQueueJobs] = useState<JobSummary[]>([]);
  const [historyJobs, setHistoryJobs] = useState<JobSummary[]>([]);
  const [workers, setWorkers] = useState<WorkerSummary[]>([]);
  const [historyWorkerFilter, setHistoryWorkerFilter] = useState<string>("all");
  const [selectedJobId, setSelectedJobId] = useState<string | null>(null);
  const [jobDetail, setJobDetail] = useState<JobDetailResponse | null>(null);
  const [isCancelling, setIsCancelling] = useState(false);
  const [error, setError] = useState<string | null>(null);

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

  useEffect(() => {
    let cancelled = false;

    async function refreshOverview() {
      try {
        const nextOverview = await getOverview();
        if (cancelled) {
          return;
        }
        setOverview(nextOverview);
        setError(null);
      } catch (nextError) {
        if (!cancelled) {
          setError(asErrorMessage(nextError));
        }
      }
    }

    void refreshOverview();
    const intervalId = window.setInterval(() => {
      void refreshOverview();
    }, activeTab === "queue" ? OVERVIEW_POLL_MS : 2_000);

    return () => {
      cancelled = true;
      window.clearInterval(intervalId);
    };
  }, [activeTab]);

  useEffect(() => {
    let cancelled = false;

    async function refreshQueue() {
      try {
        const response = await getJobs(QUEUE_STATES, 100, 0);
        if (cancelled) {
          return;
        }
        setQueueJobs(sortJobs(response.jobs));
        setError(null);
      } catch (nextError) {
        if (!cancelled) {
          setError(asErrorMessage(nextError));
        }
      }
    }

    void refreshQueue();
    const intervalId = window.setInterval(() => {
      void refreshQueue();
    }, activeTab === "queue" ? QUEUE_POLL_MS : 3_000);

    return () => {
      cancelled = true;
      window.clearInterval(intervalId);
    };
  }, [activeTab]);

  useEffect(() => {
    let cancelled = false;

    async function refreshHistory() {
      try {
        const response = await getJobs(HISTORY_STATES, 100, 0);
        if (cancelled) {
          return;
        }
        setHistoryJobs(sortJobs(response.jobs));
        setError(null);
      } catch (nextError) {
        if (!cancelled) {
          setError(asErrorMessage(nextError));
        }
      }
    }

    void refreshHistory();
    const intervalId = window.setInterval(() => {
      void refreshHistory();
    }, activeTab === "history" ? 2_000 : HISTORY_POLL_MS);

    return () => {
      cancelled = true;
      window.clearInterval(intervalId);
    };
  }, [activeTab]);

  useEffect(() => {
    let cancelled = false;

    async function refreshWorkers() {
      try {
        const response = await getWorkers();
        if (cancelled) {
          return;
        }
        setWorkers(sortWorkers(response.workers));
        setError(null);
      } catch (nextError) {
        if (!cancelled) {
          setError(asErrorMessage(nextError));
        }
      }
    }

    void refreshWorkers();
    const intervalId = window.setInterval(() => {
      void refreshWorkers();
    }, activeTab === "workers" ? 2_000 : WORKERS_POLL_MS);

    return () => {
      cancelled = true;
      window.clearInterval(intervalId);
    };
  }, [activeTab]);

  useEffect(() => {
    if (!selectedJobId) {
      setJobDetail(null);
      return;
    }
    const jobId = selectedJobId;

    let cancelled = false;

    async function refreshDetail() {
      try {
        const nextDetail = await getJob(jobId);
        if (cancelled) {
          return;
        }
        setJobDetail(nextDetail);
        setError(null);
      } catch (nextError) {
        if (!cancelled) {
          setJobDetail(null);
          setError(asErrorMessage(nextError));
        }
      }
    }

    void refreshDetail();
    const intervalId = window.setInterval(() => {
      void refreshDetail();
    }, activeTab === "queue" ? DETAIL_POLL_MS : 2_000);

    return () => {
      cancelled = true;
      window.clearInterval(intervalId);
    };
  }, [activeTab, selectedJobId]);

  const queueSelection = useMemo(
    () => queueJobs.find((job) => job.job_id === selectedJobId) ?? null,
    [queueJobs, selectedJobId],
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
    () => filteredHistoryJobs.find((job) => job.job_id === selectedJobId) ?? null,
    [filteredHistoryJobs, selectedJobId],
  );
  const selectedSummary = activeTab === "history" ? historySelection : queueSelection;

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

    const dataset = activeTab === "history" ? filteredHistoryJobs : queueJobs;
    if (dataset.length === 0) {
      setSelectedJobId(null);
      return;
    }

    if (!selectedJobId || !dataset.some((job) => job.job_id === selectedJobId)) {
      setSelectedJobId(dataset[0].job_id);
    }
  }, [activeTab, filteredHistoryJobs, queueJobs, selectedJobId]);

  async function handleCancel(jobId: string) {
    try {
      setIsCancelling(true);
      const response = await cancelJob(jobId);
      setQueueJobs((current) =>
        current.filter((job) => job.job_id !== response.job.job_id),
      );
      setHistoryJobs((current) => sortJobs([response.job, ...current]));
      setSelectedJobId(response.job.job_id);
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
            {(Object.keys(TAB_LABELS) as ConsoleTab[]).map((tab) => (
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
          <QueuePane
            overview={overview}
            jobs={queueJobs}
            selectedJobId={selectedSummary?.job_id ?? null}
            detail={jobDetail}
            isCancelling={isCancelling}
            onSelect={setSelectedJobId}
            onCancel={handleCancel}
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
            selectedJobId={selectedSummary?.job_id ?? null}
            detail={jobDetail}
            workerFilter={historyWorkerFilter}
            workerOptions={historyWorkerOptions}
            onWorkerFilterChange={setHistoryWorkerFilter}
            onSelect={setSelectedJobId}
          />
        ) : null}
      </main>
    </div>
  );
}

function QueuePane(props: {
  overview: OverviewResponse | null;
  jobs: JobSummary[];
  selectedJobId: string | null;
  detail: JobDetailResponse | null;
  isCancelling: boolean;
  onSelect: (jobId: string) => void;
  onCancel: (jobId: string) => Promise<void>;
}) {
  const { overview, jobs, selectedJobId, detail, isCancelling, onSelect, onCancel } = props;

  return (
    <div className="content-grid">
      <section className="panel">
        <div className="stat-row">
          <StatCard label="Active" value={overview?.queue.active ?? 0} />
          <StatCard label="Pending" value={overview?.queue.pending ?? 0} />
          <StatCard label="Leased" value={overview?.queue.leased ?? 0} />
          <StatCard label="Running" value={overview?.queue.running ?? 0} />
        </div>

        <PanelHeader
          title="Queue"
          meta={`${jobs.length} active jobs`}
        />
        <JobsTable
          jobs={jobs}
          selectedJobId={selectedJobId}
          emptyMessage="No active jobs"
          onSelect={onSelect}
        />
      </section>

      <JobInspector
        detail={detail}
        isCancelling={isCancelling}
        onCancel={onCancel}
      />
    </div>
  );
}

function WorkersPane(props: {
  overview: OverviewResponse | null;
  workers: WorkerSummary[];
}) {
  const { overview, workers } = props;

  return (
    <section className="panel panel--full">
      <div className="stat-row">
        <StatCard label="Workers" value={overview?.workers.total ?? 0} />
        <StatCard label="Healthy" value={overview?.workers.healthy ?? 0} />
        <StatCard label="Expired" value={overview?.workers.expired ?? 0} />
        <StatCard label="Active leases" value={overview?.workers.active_leases ?? 0} />
      </div>

      <PanelHeader
        title="Workers"
        meta={
          overview
            ? `Capacity ${formatCapacityMap(overview.workers.available_capacity)} available`
            : "Loading"
        }
      />
      <WorkersTable workers={workers} />
    </section>
  );
}

function HistoryPane(props: {
  overview: OverviewResponse | null;
  jobs: JobSummary[];
  totalJobs: number;
  selectedJobId: string | null;
  detail: JobDetailResponse | null;
  workerFilter: string;
  workerOptions: Array<{ value: string; label: string }>;
  onWorkerFilterChange: (value: string) => void;
  onSelect: (jobId: string) => void;
}) {
  const {
    overview,
    jobs,
    totalJobs,
    selectedJobId,
    detail,
    workerFilter,
    workerOptions,
    onWorkerFilterChange,
    onSelect,
  } = props;
  const meta =
    workerFilter === "all"
      ? `${jobs.length} recent jobs`
      : `${jobs.length} of ${totalJobs} jobs`;

  return (
    <div className="content-grid">
      <section className="panel">
        <div className="stat-row">
          <StatCard label="Total" value={overview?.history.total ?? 0} />
          <StatCard label="Succeeded" value={overview?.history.succeeded ?? 0} />
          <StatCard label="Failed" value={overview?.history.failed ?? 0} />
          <StatCard label="Cancelled" value={overview?.history.cancelled ?? 0} />
        </div>

        <PanelHeader title="History" meta={meta} />
        <div className="panel__toolbar">
          <label className="field">
            <span className="field__label">Worker</span>
            <select
              className="field__control"
              value={workerFilter}
              onChange={(event) => onWorkerFilterChange(event.target.value)}
            >
              {workerOptions.map((option) => (
                <option key={option.value} value={option.value}>
                  {option.label}
                </option>
              ))}
            </select>
          </label>
        </div>
        <JobsTable
          jobs={jobs}
          selectedJobId={selectedJobId}
          emptyMessage="No historical jobs"
          onSelect={onSelect}
        />
      </section>

      <JobInspector detail={detail} />
    </div>
  );
}

function PanelHeader(props: { title: string; meta: string }) {
  return (
    <div className="panel__header">
      <h1>{props.title}</h1>
      <span>{props.meta}</span>
    </div>
  );
}

function StatCard(props: { label: string; value: number }) {
  return (
    <div className="stat-card">
      <span>{props.label}</span>
      <strong>{formatNumber(props.value)}</strong>
    </div>
  );
}

function JobsTable(props: {
  jobs: JobSummary[];
  selectedJobId: string | null;
  emptyMessage: string;
  onSelect: (jobId: string) => void;
}) {
  const { jobs, selectedJobId, emptyMessage, onSelect } = props;

  if (jobs.length === 0) {
    return <EmptyState message={emptyMessage} />;
  }

  return (
    <div className="table-wrap">
      <table className="table">
        <thead>
          <tr>
            <th>Status</th>
            <th>Job</th>
            <th>Interface</th>
            <th>Worker</th>
            <th>Attempts</th>
            <th>Updated</th>
          </tr>
        </thead>
        <tbody>
          {jobs.map((job) => {
            const isSelected = job.job_id === selectedJobId;
            return (
              <tr
                key={job.job_id}
                className={isSelected ? "is-selected" : ""}
                onClick={() => onSelect(job.job_id)}
              >
                <td>
                  <StateBadge state={job.state} />
                </td>
                <td>
                  <div className="table__primary">{truncateMiddle(job.job_id, 28)}</div>
                  <div className="table__secondary">
                    {job.source ?? job.metadata.request_id ?? "direct"}
                  </div>
                </td>
                <td>{job.interface_name}</td>
                <td>{job.latest_worker_id ?? "Unassigned"}</td>
                <td>{job.attempt_count}</td>
                <td>{formatTimestamp(job.updated_at_ms)}</td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

function WorkersTable(props: { workers: WorkerSummary[] }) {
  const { workers } = props;

  if (workers.length === 0) {
    return <EmptyState message="No workers registered" />;
  }

  return (
    <div className="table-wrap">
      <table className="table">
        <thead>
          <tr>
            <th>Status</th>
            <th>Worker</th>
            <th>Interfaces</th>
            <th>Leases</th>
            <th>Capacity</th>
            <th>Last seen</th>
          </tr>
        </thead>
        <tbody>
          {workers.map((worker) => (
            <tr key={worker.worker_id}>
              <td>
                <WorkerBadge healthy={worker.healthy} />
              </td>
              <td>
                <div className="table__primary">
                  {truncateMiddle(worker.worker_id, 28)}
                </div>
                <div className="table__secondary">
                  {formatCapacityMap(worker.attributes)}
                </div>
              </td>
              <td>{worker.interfaces.join(", ") || "None"}</td>
              <td>
                {worker.active_leases} / {worker.max_active_leases}
              </td>
              <td>
                {formatCapacityMap(worker.available_capacity)}
                <div className="table__secondary">
                  total {formatCapacityMap(worker.total_capacity)}
                </div>
              </td>
              <td>{formatTimestamp(worker.last_seen_at_ms)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function JobInspector(props: {
  detail: JobDetailResponse | null;
  isCancelling?: boolean;
  onCancel?: (jobId: string) => Promise<void>;
}) {
  const { detail, isCancelling = false, onCancel } = props;

  if (!detail) {
    return (
      <aside className="panel panel--aside">
        <div className="panel__header">
          <h1>Inspector</h1>
          <span>Select a job</span>
        </div>
        <EmptyState message="Job details appear here." />
      </aside>
    );
  }

  const canCancel = QUEUE_STATES.includes(detail.job.state);

  return (
    <aside className="panel panel--aside">
      <div className="panel__header panel__header--stack">
        <div>
          <h1>{truncateMiddle(detail.job.job_id, 34)}</h1>
          <span>{detail.job.interface_name}</span>
        </div>

        <div className="inspector__actions">
          <StateBadge state={detail.job.state} />
          {canCancel && onCancel ? (
            <button
              type="button"
              className="button"
              disabled={isCancelling}
              onClick={() => void onCancel(detail.job.job_id)}
            >
              {isCancelling ? "Cancelling" : "Cancel job"}
            </button>
          ) : null}
        </div>
      </div>

      <div className="meta-grid">
        <MetaItem label="Source" value={detail.job.source ?? "direct"} />
        <MetaItem
          label="Worker"
          value={detail.latest_worker?.worker_id ?? latestAttemptWorker(detail) ?? "Unassigned"}
        />
        <MetaItem label="Updated" value={formatTimestamp(detail.job.updated_at_ms)} />
        <MetaItem label="Duration" value={formatDuration(detail.job.duration_ms)} />
      </div>

      <InspectorSection title="Demand">
        <InlineList
          values={[
            `required ${formatCapacityMap(detail.job.demand.required_capacity)}`,
            `attrs ${formatCapacityMap(detail.job.demand.required_attributes)}`,
            `priority ${detail.job.policy.priority}`,
          ]}
        />
      </InspectorSection>

      <InspectorSection title="Request">
        {detail.job.params_preview ? (
          <>
            <PreviewMeta
              values={[
                `${formatNumber(detail.job.params_size_bytes)} B`,
                detail.job.params_truncated ? "truncated" : "full",
              ]}
            />
            <CodeBlock value={detail.job.params_preview} />
          </>
        ) : (
          <EmptyState message="No inline request preview." compact />
        )}
      </InspectorSection>

      <InspectorSection title="Inputs">
        <ObjectRefList
          objects={detail.job.inputs}
          emptyMessage="No persisted inputs"
        />
      </InspectorSection>

      <InspectorSection title="Outputs">
        <ObjectRefList
          objects={detail.job.outputs}
          emptyMessage="No persisted outputs"
        />
      </InspectorSection>

      <InspectorSection title="Response">
        {detail.job.output_preview ? (
          <>
            <PreviewMeta
              values={[
                `${formatNumber(detail.job.output_preview.size_bytes)} B`,
                detail.job.output_preview.content_type ?? "unknown content-type",
                detail.job.output_preview.truncated ? "truncated" : "full",
              ]}
            />
            {detail.job.output_preview.preview ? (
              <CodeBlock value={detail.job.output_preview.preview} />
            ) : (
              <EmptyState message="Response preview unavailable." compact />
            )}
          </>
        ) : (
          <EmptyState message="No response preview." compact />
        )}
      </InspectorSection>

      {detail.job.last_error ? (
        <InspectorSection title="Error">
          <CodeBlock value={detail.job.last_error} />
        </InspectorSection>
      ) : null}

      <InspectorSection title="Attempts">
        <Timeline
          items={detail.attempts.map((attempt) => ({
            key: attempt.attempt_id,
            title: `${attempt.state} · ${attempt.worker_id ?? "unassigned"}`,
            subtitle: formatTimestamp(attempt.created_at_ms),
            meta:
              attempt.duration_ms != null
                ? formatDuration(attempt.duration_ms)
                : attempt.lease_id ?? "pending",
          }))}
          emptyMessage="No attempts"
        />
      </InspectorSection>

      <InspectorSection title="Streams">
        <Timeline
          items={detail.streams.map((stream) => ({
            key: stream.stream_id,
            title: `${stream.stream_name} · ${stream.state}`,
            subtitle: `${stream.scope} / ${stream.direction}`,
            meta: `seq ${stream.last_sequence}`,
          }))}
          emptyMessage="No streams"
        />
      </InspectorSection>

      <InspectorSection title="Events">
        <Timeline
          items={detail.events.map((event) => ({
            key: String(event.sequence),
            title: event.kind,
            subtitle: formatTimestamp(event.created_at_ms),
            meta: `${formatNumber(event.payload_size_bytes)} B`,
          }))}
          emptyMessage="No events"
        />
      </InspectorSection>
    </aside>
  );
}

function InspectorSection(props: { title: string; children: React.ReactNode }) {
  return (
    <section className="inspector__section">
      <div className="inspector__label">{props.title}</div>
      {props.children}
    </section>
  );
}

function MetaItem(props: { label: string; value: string }) {
  return (
    <div className="meta-item">
      <span>{props.label}</span>
      <strong>{props.value}</strong>
    </div>
  );
}

function InlineList(props: { values: string[] }) {
  return (
    <div className="inline-list">
      {props.values.map((value) => (
        <span key={value}>{value}</span>
      ))}
    </div>
  );
}

function PreviewMeta(props: { values: string[] }) {
  return (
    <div className="preview-meta">
      {props.values.map((value) => (
        <span key={value}>{value}</span>
      ))}
    </div>
  );
}

function ObjectRefList(props: {
  objects: Array<{
    kind: "inline" | "uri";
    size_bytes: number | null;
    uri: string | null;
    content_type: string | null;
    preview: string | null;
    truncated: boolean;
    metadata: Record<string, string>;
  }>;
  emptyMessage: string;
}) {
  if (props.objects.length === 0) {
    return <EmptyState message={props.emptyMessage} compact />;
  }

  return (
    <div className="timeline">
      {props.objects.map((object, index) => (
        <div key={`${object.kind}-${object.uri ?? index}`} className="timeline__item">
          <div className="timeline__title">
            {object.kind}
            {object.content_type ? ` · ${object.content_type}` : ""}
          </div>
          <div className="timeline__subtitle">
            {object.uri ?? formatObjectMeta(object)}
          </div>
          {object.preview ? <CodeBlock value={object.preview} /> : null}
          {!object.preview && object.uri == null ? (
            <div className="timeline__meta">Preview unavailable</div>
          ) : null}
          {object.truncated ? <div className="timeline__meta">Preview truncated</div> : null}
        </div>
      ))}
    </div>
  );
}

function Timeline(props: {
  items: Array<{ key: string; title: string; subtitle: string; meta: string }>;
  emptyMessage: string;
}) {
  if (props.items.length === 0) {
    return <EmptyState message={props.emptyMessage} compact />;
  }

  return (
    <div className="timeline">
      {props.items.map((item) => (
        <div key={item.key} className="timeline__item">
          <div className="timeline__title">{item.title}</div>
          <div className="timeline__subtitle">{item.subtitle}</div>
          <div className="timeline__meta">{item.meta}</div>
        </div>
      ))}
    </div>
  );
}

function EmptyState(props: { message: string; compact?: boolean }) {
  return (
    <div className={`empty${props.compact ? " empty--compact" : ""}`}>
      {props.message}
    </div>
  );
}

function CodeBlock(props: { value: string }) {
  return <pre className="code-block">{props.value}</pre>;
}

function StateBadge(props: { state: JobState }) {
  return <span className={`badge badge--${props.state}`}>{props.state}</span>;
}

function WorkerBadge(props: { healthy: boolean }) {
  return (
    <span className={`badge ${props.healthy ? "badge--healthy" : "badge--expired"}`}>
      {props.healthy ? "healthy" : "expired"}
    </span>
  );
}

function sortJobs(jobs: JobSummary[]): JobSummary[] {
  return [...jobs].sort((left, right) => right.updated_at_ms - left.updated_at_ms);
}

function sortWorkers(workers: WorkerSummary[]): WorkerSummary[] {
  return [...workers].sort((left, right) => {
    if (left.healthy !== right.healthy) {
      return Number(right.healthy) - Number(left.healthy);
    }
    return right.last_seen_at_ms - left.last_seen_at_ms;
  });
}

function formatTimestamp(value: number | null): string {
  if (!value) {
    return "N/A";
  }
  const date = new Date(value);
  const milliseconds = String(date.getMilliseconds()).padStart(3, "0");
  const parts = timeFormatter.formatToParts(date);

  return parts
    .flatMap((part) =>
      part.type === "second"
        ? [part.value, ".", milliseconds]
        : [part.value],
    )
    .join("");
}

function formatDuration(value: number | null): string {
  if (value == null) {
    return "N/A";
  }
  if (value < 1_000) {
    return `${value} ms`;
  }
  const seconds = value / 1_000;
  if (seconds < 60) {
    return `${seconds.toFixed(1)} s`;
  }
  const minutes = Math.floor(seconds / 60);
  return `${minutes}m ${(seconds % 60).toFixed(0)}s`;
}

function formatNumber(value: number): string {
  return new Intl.NumberFormat().format(value);
}

function formatCapacityMap(value: Record<string, string | number>): string {
  const entries = Object.entries(value);
  if (entries.length === 0) {
    return "N/A";
  }
  return entries.map(([key, item]) => `${key}:${item}`).join(" ");
}

function formatObjectMeta(value: {
  size_bytes: number | null;
  metadata: Record<string, string>;
}): string {
  const parts = [];
  if (value.size_bytes != null) {
    parts.push(`${formatNumber(value.size_bytes)} B`);
  }
  const metadataEntries = Object.entries(value.metadata)
    .filter(([key]) => key !== "content_type")
    .map(([key, item]) => `${key}:${item}`);
  if (metadataEntries.length > 0) {
    parts.push(metadataEntries.join(" "));
  }
  return parts.join(" · ") || "No metadata";
}

function truncateMiddle(value: string, maxLength: number): string {
  if (value.length <= maxLength) {
    return value;
  }
  const half = Math.max(4, Math.floor((maxLength - 1) / 2));
  return `${value.slice(0, half)}…${value.slice(-half)}`;
}

function latestAttemptWorker(detail: JobDetailResponse): string | null {
  for (let index = detail.attempts.length - 1; index >= 0; index -= 1) {
    const workerId = detail.attempts[index]?.worker_id;
    if (workerId) {
      return workerId;
    }
  }
  return null;
}

function asErrorMessage(error: unknown): string {
  if (error instanceof Error) {
    return error.message;
  }
  return "Unknown error";
}

function readTabFromLocation(): ConsoleTab {
  const pathname = window.location.pathname.replace(/\/+$/, "") || "/";
  const match = (Object.entries(TAB_PATHS) as Array<[ConsoleTab, string]>).find(
    ([, path]) => pathname === path,
  );
  return match?.[0] ?? "queue";
}
