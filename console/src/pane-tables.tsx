import type { ReactNode } from "react";

import type { PaneStat } from "./app-constants";
import { formatDuration, formatNumber, formatShortTime, formatTimestamp } from "./format";
import { IconSearch } from "./icons";
import type { InterfaceSummary, JobState, JobSummary, PoolTierPolicy, WorkerSummary } from "./types";

export function StatsRow({ stats }: { stats: PaneStat[] }) {
  return (
    <div className="stat-row" style={{ gridTemplateColumns: `repeat(${Math.max(1, stats.length)}, minmax(0, 1fr))` }}>
      {stats.map((stat) => (
        <div key={stat.label} className="stat-card">
          <span>{stat.label}</span>
          <strong>{typeof stat.value === "number" ? formatNumber(stat.value) : stat.value}</strong>
        </div>
      ))}
    </div>
  );
}

export function PanelHeader(props: { title: string; meta?: string | null; actions?: ReactNode }) {
  return (
    <div className="panel__header">
      <div className="panel__header-title">
        <h1>{props.title}</h1>
        {props.meta ? <span>{props.meta}</span> : null}
      </div>
      {props.actions ? <div className="panel__header-actions">{props.actions}</div> : null}
    </div>
  );
}

export function SelectField(props: {
  label: string;
  value: string;
  options: Array<{ value: string; label: string }>;
  onChange: (value: string) => void;
}) {
  return (
    <label className="field">
      <span className="field__label">{props.label}</span>
      <select className="field__control" value={props.value} onChange={(e) => props.onChange(e.target.value)}>
        {props.options.map((opt) => (
          <option key={opt.value} value={opt.value}>{opt.label}</option>
        ))}
      </select>
    </label>
  );
}

export function SearchInput(props: { value: string; placeholder: string; onChange: (value: string) => void }) {
  return (
    <div className="search-box">
      <IconSearch className="search-box__icon" />
      <input
        type="search"
        className="search-box__input"
        value={props.value}
        onChange={(e) => props.onChange(e.target.value)}
        placeholder={props.placeholder}
        aria-label={props.placeholder}
      />
    </div>
  );
}

export function EmptyState({ message, inline }: { message: string; inline?: boolean }) {
  return <div className={inline ? "empty-inline" : "empty"}>{message}</div>;
}

export function StateBadge({ state }: { state: JobState }) {
  return (
    <span className={`badge badge--${state}`}>
      <i className="badge__dot" aria-hidden="true" />
      {state}
    </span>
  );
}

export function WorkerStateBadge({ worker }: { worker: Pick<WorkerSummary, "healthy" | "connected" | "draining"> }) {
  const kind = !worker.connected ? "disconnected" : worker.draining ? "draining" : worker.healthy ? "healthy" : "unavailable";
  return (
    <span className={`badge badge--worker-${kind}`}>
      <i className="badge__dot" aria-hidden="true" />
      {kind}
    </span>
  );
}

export function ProgressMeter({ percent, tone }: { percent: number; tone?: "accent" | "warn" | "danger" | "ok" }) {
  const clamped = Math.max(0, Math.min(100, percent));
  const resolved = tone ?? (clamped >= 90 ? "danger" : clamped >= 75 ? "warn" : "accent");
  return (
    <div className="meter" role="progressbar" aria-valuenow={Math.round(clamped)} aria-valuemin={0} aria-valuemax={100}>
      <div className={`meter__fill meter__fill--${resolved}`} style={{ width: `${clamped}%` }} />
    </div>
  );
}

export function TierChain({ tiers, highlightPool }: { tiers: PoolTierPolicy[]; highlightPool?: string }) {
  return (
    <div className="tier-chain">
      {tiers.map((tier, idx) => (
        <div key={`${tier.pool}-${idx}`} className={`tier-node${highlightPool === tier.pool ? " is-current" : ""}`}>
          <span className="tier-node__rank">#{idx + 1}</span>
          <div className="tier-node__body">
            <strong className="mono">{tier.pool}</strong>
            <span>
              {tier.spillover_after_ms === 0 ? "Primary" : `Spillover after ${formatDuration(tier.spillover_after_ms)}`}
              {tier.reconnect_grace_ms != null ? ` · Reconnect grace ${formatDuration(tier.reconnect_grace_ms)}` : ""}
            </span>
          </div>
        </div>
      ))}
    </div>
  );
}

export function WorkerMiniList(props: {
  workers: WorkerSummary[];
  emptyMessage: string;
  showLeases?: boolean;
  onSelect: (workerId: string) => void;
}) {
  if (props.workers.length === 0) return <EmptyState message={props.emptyMessage} inline />;
  return (
    <div className="mini-list">
      {props.workers.map((worker) => (
        <button key={worker.worker_id} type="button" className="mini-row" onClick={() => props.onSelect(worker.worker_id)}>
          <span className="mono ellipsis" title={worker.worker_id}>{worker.worker_id}</span>
          <span className="mini-row__meta">
            {props.showLeases ? <span className="num">{worker.active_leases}/{worker.max_concurrency}</span> : null}
            {!props.showLeases && worker.pool ? <span className="tag">{worker.pool}</span> : null}
            <WorkerStateBadge worker={worker} />
          </span>
        </button>
      ))}
    </div>
  );
}

export function JobsTable(props: {
  jobs: JobSummary[];
  selectedJobId: string | null;
  emptyMessage: string;
  onSelect: (jobId: string) => void;
}) {
  if (props.jobs.length === 0) return <EmptyState message={props.emptyMessage} />;
  return (
    <div className="table-wrap">
      <table className="table table--fixed table--interactive">
        <thead>
          <tr>
            <th style={{ width: "17%" }}>Status</th>
            <th style={{ width: "23%" }}>Job</th>
            <th style={{ width: "20%" }}>Interface</th>
            <th style={{ width: "16%" }}>Worker</th>
            <th style={{ width: "11%" }}>Attempts</th>
            <th style={{ width: "13%" }}>Updated</th>
          </tr>
        </thead>
        <tbody>
          {props.jobs.map((job) => {
            const ns = job.namespace && job.namespace !== "default" ? job.namespace : null;
            return (
              <tr
                key={`${job.namespace ?? "default"}::${job.job_id}`}
                className={job.job_id === props.selectedJobId ? "is-selected" : ""}
                onClick={() => props.onSelect(job.job_id)}
              >
                <td>
                  <div className="cell-stack">
                    <StateBadge state={job.state} />
                    {job.error_code ? <span className="tag tag--danger ellipsis">{job.error_code}</span> : null}
                  </div>
                </td>
                <td>
                  <div className="table__primary mono ellipsis" title={job.job_id}>{job.job_id}</div>
                  <div className="table__secondary">
                    <span className="ellipsis">{job.source ?? job.metadata.request_id ?? "direct"}</span>
                    {ns ? <span className="tag">{ns}</span> : null}
                    {job.durability ? <span className="tag">{job.durability}</span> : null}
                  </div>
                </td>
                <td className="mono ellipsis" title={job.interface_name}>{job.interface_name}</td>
                <td className="mono ellipsis" title={job.latest_worker_id ?? "Unassigned"}>
                  {job.latest_worker_id ?? "Unassigned"}
                </td>
                <td className="num">{job.attempt_count}</td>
                <td>
                  <div className="mono table__timestamp" title={formatTimestamp(job.updated_at_ms)}>
                    {formatShortTime(job.updated_at_ms)}
                  </div>
                  {job.duration_ms != null ? <div className="table__secondary num">{formatDuration(job.duration_ms)}</div> : null}
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

export function InterfacesTable(props: {
  interfaces: InterfaceSummary[];
  selectedName?: string | null;
  onSelect?: (name: string) => void;
  compact?: boolean;
}) {
  const { interfaces, selectedName, onSelect, compact = false } = props;
  if (interfaces.length === 0) return <EmptyState message="No interfaces match the filter" inline={compact} />;
  return (
    <div className="table-wrap">
      <table className={`table table--fixed${onSelect ? " table--interactive" : ""}`}>
        <thead>
          <tr>
            <th style={{ width: compact ? "26%" : "21%" }}>Interface</th>
            <th style={{ width: compact ? "12.5%" : "9%" }}>Queued</th>
            <th style={{ width: compact ? "14%" : "10%" }}>Running</th>
            {!compact ? <th style={{ width: "14.5%" }}>Oldest wait</th> : null}
            <th style={{ width: compact ? "14.5%" : "11%" }}>Sessions</th>
            <th style={{ width: compact ? "9.5%" : "13%" }}>{compact ? "Free" : "Free slots"}</th>
            <th style={{ width: compact ? "23.5%" : "21.5%" }}>Policy</th>
          </tr>
        </thead>
        <tbody>
          {interfaces.map((item) => {
            const tiers = item.policy?.pool_tiers ?? [];
            const tip = tiers.map((t, i) => `${i > 0 ? "→ " : ""}${t.pool}${t.spillover_after_ms > 0 ? ` (${formatDuration(t.spillover_after_ms)})` : ""}`).join(" ");
            return (
              <tr
                key={item.name}
                className={selectedName === item.name ? "is-selected" : ""}
                onClick={onSelect ? () => onSelect(item.name) : undefined}
              >
                <td className="table__primary mono ellipsis" title={item.name}>{item.name}</td>
                <td className="num">{formatNumber(item.queued)}</td>
                <td className="num">{formatNumber(item.running)}</td>
                {!compact ? <td className="num">{formatDuration(item.oldest_queued_age_ms)}</td> : null}
                <td className="num">{formatNumber(item.sessions)}</td>
                <td className="num">{formatNumber(item.free_slots)}</td>
                <td>
                  {tiers.length > 0 ? (
                    <div className="tier-chain-inline" title={tip}>
                      {tiers.map((tier, idx) => (
                        <span key={`${tier.pool}-${idx}`} className="tag ellipsis">
                          {idx > 0 ? "→ " : ""}
                          {tier.pool}
                          {!compact && tier.spillover_after_ms > 0 ? ` (${formatDuration(tier.spillover_after_ms)})` : ""}
                        </span>
                      ))}
                    </div>
                  ) : (
                    <span className="muted">
                      {item.policy?.max_queue != null ? `max=${item.policy.max_queue}` : "Default"}
                    </span>
                  )}
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}
