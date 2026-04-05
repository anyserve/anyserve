import { formatCapacityMap, formatTimestamp, truncateMiddle } from "./format";
import type { JobState, JobSummary, WorkerSummary } from "./types";

export function JobsTable(props: {
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
      <table className="table table--interactive">
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
                  <div className="table__primary table__mono">
                    {truncateMiddle(job.job_id, 28)}
                  </div>
                  <div className="table__secondary">
                    {job.source ?? job.metadata.request_id ?? "direct"}
                  </div>
                </td>
                <td>{job.interface_name}</td>
                <td className="table__mono">
                  {job.latest_worker_id ? truncateMiddle(job.latest_worker_id, 28) : "Unassigned"}
                </td>
                <td>{job.attempt_count}</td>
                <td className="table__mono table__timestamp">
                  {formatTimestamp(job.updated_at_ms)}
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

export function WorkersTable(props: { workers: WorkerSummary[]; emptyMessage: string }) {
  const { workers, emptyMessage } = props;

  if (workers.length === 0) {
    return <EmptyState message={emptyMessage} compact />;
  }

  return (
    <div className="table-wrap">
      <table className="table table--static">
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
                <div className="table__primary table__mono">
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
              <td className="table__mono">
                {formatCapacityMap(worker.available_capacity)}
                <div className="table__secondary">
                  total {formatCapacityMap(worker.total_capacity)}
                </div>
              </td>
              <td className="table__mono table__timestamp">
                {formatTimestamp(worker.last_seen_at_ms)}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

export function EmptyState(props: { message: string; compact?: boolean }) {
  return (
    <div className={`empty${props.compact ? " empty--compact" : ""}`}>
      {props.message}
    </div>
  );
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
