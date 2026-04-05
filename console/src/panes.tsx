import type { ReactNode } from "react";

import { type PaneStat } from "./app-constants";
import { formatCapacityMap, formatNumber } from "./format";
import type { JobDetailResponse, JobSummary, OverviewResponse, WorkerSummary } from "./types";
import { JobsTable, WorkersTable } from "./pane-tables";
import { JobInspector as JobInspectorPanel } from "./pane-inspector";

export function JobsPane(props: {
  title: string;
  meta: string;
  stats: PaneStat[];
  jobs: JobSummary[];
  selectedJobId: string | null;
  emptyMessage: string;
  onSelect: (jobId: string) => void;
  inspector?: ReactNode;
  toolbar?: ReactNode;
}) {
  const {
    title,
    meta,
    stats,
    jobs,
    selectedJobId,
    emptyMessage,
    onSelect,
    inspector,
    toolbar,
  } = props;

  return (
    <div className={`content-grid${inspector ? "" : " content-grid--single"}`}>
      <section className="panel">
        <StatsRow stats={stats} />
        <PanelHeader title={title} meta={meta} />
        {toolbar}
        <JobsTable jobs={jobs} selectedJobId={selectedJobId} emptyMessage={emptyMessage} onSelect={onSelect} />
      </section>

      {inspector ? inspector : null}
    </div>
  );
}

export function WorkersPane(props: {
  overview: OverviewResponse | null;
  workers: WorkerSummary[];
}) {
  const { overview, workers } = props;
  const healthyWorkers = workers.filter((worker) => worker.healthy);
  const expiredWorkers = workers.filter((worker) => !worker.healthy);

  return (
    <section className="panel panel--full">
      <StatsRow
        stats={[
          { label: "Workers", value: overview?.workers.total ?? 0 },
          { label: "Healthy", value: overview?.workers.healthy ?? 0 },
          { label: "Expired", value: overview?.workers.expired ?? 0 },
          { label: "Active leases", value: overview?.workers.active_leases ?? 0 },
        ]}
      />

      <PanelHeader
        title="Workers"
        meta={
          overview
            ? `Capacity ${formatCapacityMap(overview.workers.available_capacity)} available`
            : "Loading"
        }
      />

      <div className="workers-stack">
        <WorkerSection
          title="Healthy workers"
          meta={
            overview
              ? `${healthyWorkers.length} workers · ${overview.workers.active_leases} active leases`
              : `${healthyWorkers.length} workers`
          }
        >
          <WorkersTable workers={healthyWorkers} emptyMessage="No healthy workers" />
        </WorkerSection>

        <WorkerSection
          title="Expired workers"
          meta={
            expiredWorkers.length > 0
              ? "Retained for history, not eligible for new leases"
              : "No expired workers"
          }
        >
          <WorkersTable workers={expiredWorkers} emptyMessage="No expired workers" />
        </WorkerSection>
      </div>
    </section>
  );
}

export function HistoryPane(props: {
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
    <JobsPane
      title="History"
      meta={meta}
      stats={[
        { label: "Total", value: overview?.history.total ?? 0 },
        { label: "Succeeded", value: overview?.history.succeeded ?? 0 },
        { label: "Failed", value: overview?.history.failed ?? 0 },
        { label: "Cancelled", value: overview?.history.cancelled ?? 0 },
      ]}
      jobs={jobs}
      selectedJobId={selectedJobId}
      emptyMessage="No historical jobs"
      onSelect={onSelect}
      toolbar={
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
      }
      inspector={<JobInspectorPanel detail={detail} />}
    />
  );
}

export { JobInspector } from "./pane-inspector";

function StatsRow(props: { stats: PaneStat[] }) {
  return (
    <div className="stat-row">
      {props.stats.map((stat) => (
        <StatCard key={stat.label} label={stat.label} value={stat.value} />
      ))}
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

function WorkerSection(props: {
  title: string;
  meta: string;
  children: ReactNode;
}) {
  return (
    <section className="worker-section">
      <div className="worker-section__header">
        <h2>{props.title}</h2>
        <span>{props.meta}</span>
      </div>
      {props.children}
    </section>
  );
}
