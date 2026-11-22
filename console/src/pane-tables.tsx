import { memo, useEffect, useRef, useState, type ReactNode } from "react";

import type { JobStatusFilter, PaneStat } from "./app-constants";
import { formatDuration, formatNumber, formatPool, formatShortTime, formatTimestamp } from "./format";
import { IconCheck, IconClose, IconCopy, IconSearch } from "./icons";
import type { InterfaceSummary, JobState, JobSummary, PoolTierPolicy, WorkerSummary } from "./types";

export function CopyButton({ text, label = "Copy", resetKey }: { text: string | (() => string); label?: string; resetKey?: string }) {
  const [copied, setCopied] = useState(false);
  const timerRef = useRef<number | null>(null);

  useEffect(() => {
    setCopied(false);
    if (timerRef.current != null) {
      window.clearTimeout(timerRef.current);
      timerRef.current = null;
    }
  }, [resetKey]);

  useEffect(() => () => {
    if (timerRef.current != null) window.clearTimeout(timerRef.current);
  }, []);

  async function handleCopy() {
    try {
      await navigator.clipboard?.writeText(typeof text === "function" ? text() : text);
      setCopied(true);
      if (timerRef.current != null) window.clearTimeout(timerRef.current);
      timerRef.current = window.setTimeout(() => setCopied(false), 1500);
    } catch {
      // Ignore clipboard unavailability in headless or non-HTTPS contexts.
    }
  }

  return (
    <button type="button" className="button button--sm" onClick={() => void handleCopy()}>
      {copied ? <IconCheck /> : <IconCopy />}
      <span>{copied ? "Copied" : label}</span>
    </button>
  );
}

export function StatsRow({ stats, onSelectState }: { stats: PaneStat[]; onSelectState?: (state: JobStatusFilter) => void }) {
  return (
    <div className="stat-row" style={{ gridTemplateColumns: `repeat(${Math.max(1, stats.length)}, minmax(0, 1fr))` }}>
      {stats.map((stat) => {
        const cls = `stat-card${stat.tone ? ` tone-${stat.tone}` : ""}${stat.active ? " is-active" : ""}`;
        if (onSelectState && stat.state != null) {
          const targetState = stat.active && stat.state !== "all" ? "all" : stat.state;
          return (
            <button
              key={stat.label}
              type="button"
              className={`${cls} stat-card--interactive`}
              aria-pressed={Boolean(stat.active)}
              onClick={() => onSelectState(targetState)}
            >
              <span>{stat.label}</span>{" "}
              <strong>{typeof stat.value === "number" ? formatNumber(stat.value) : stat.value}</strong>
            </button>
          );
        }
        return (
          <div key={stat.label} className={cls}>
            <span>{stat.label}</span>{" "}
            <strong>{typeof stat.value === "number" ? formatNumber(stat.value) : stat.value}</strong>
          </div>
        );
      })}
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
  const currentLabel = props.options.find((opt) => opt.value === props.value)?.label ?? props.value;
  return (
    <label className="field">
      <span className="field__label">{props.label}</span>
      <select className="field__control" value={props.value} title={currentLabel} onChange={(e) => props.onChange(e.target.value)}>
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
      {props.value ? (
        <button
          type="button"
          className="search-box__clear"
          aria-label="Clear search"
          onClick={() => props.onChange("")}
        >
          <IconClose width={12} height={12} />
        </button>
      ) : null}
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

export function TierChain({
  tiers,
  highlightPool,
  onSelectPool,
}: {
  tiers: PoolTierPolicy[];
  highlightPool?: string;
  onSelectPool?: (poolName: string) => void;
}) {
  return (
    <div className="tier-chain">
      {tiers.map((tier, idx) => {
        const poolName = formatPool(tier.pool);
        const isCurrent = highlightPool != null && formatPool(highlightPool) === poolName;
        const body = (
          <>
            <span className="tier-node__rank">#{idx + 1}</span>
            <div className="tier-node__body">
              <strong className="mono">{poolName}</strong>
              <span>
                {tier.spillover_after_ms === 0 ? "Primary" : `Spillover after ${formatDuration(tier.spillover_after_ms)}`}
                {tier.reconnect_grace_ms != null ? ` · Reconnect grace ${formatDuration(tier.reconnect_grace_ms)}` : ""}
              </span>
            </div>
          </>
        );
        return onSelectPool ? (
          <button
            key={`${tier.pool}-${idx}`}
            type="button"
            className={`tier-node tier-node--interactive${isCurrent ? " is-current" : ""}`}
            onClick={() => onSelectPool(tier.pool)}
          >
            {body}
          </button>
        ) : (
          <div key={`${tier.pool}-${idx}`} className={`tier-node${isCurrent ? " is-current" : ""}`}>
            {body}
          </div>
        );
      })}
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
            {!props.showLeases ? <span className="tag">{formatPool(worker.pool)}</span> : null}
            <WorkerStateBadge worker={worker} />
          </span>
        </button>
      ))}
    </div>
  );
}

export const JobsTable = memo(function JobsTable(props: {
  jobs: JobSummary[];
  selectedJobId: string | null;
  emptyMessage: string;
  onSelect: (jobId: string) => void;
  onOpenInterface?: (iface: string) => void;
  onOpenWorker?: (workerId: string) => void;
  onFilterNamespace?: (namespace: string) => void;
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
                    {ns ? (
                      props.onFilterNamespace ? (
                        <button
                          type="button"
                          className="tag tag--link"
                          onClick={(e) => {
                            e.stopPropagation();
                            props.onFilterNamespace!(ns);
                          }}
                        >
                          {ns}
                        </button>
                      ) : (
                        <span className="tag">{ns}</span>
                      )
                    ) : null}
                    {job.durability ? <span className="tag">{job.durability}</span> : null}
                  </div>
                </td>
                <td>
                  {props.onOpenInterface ? (
                    <button
                      type="button"
                      className="cell-link mono ellipsis"
                      title={`Open interface ${job.interface_name}`}
                      onClick={(e) => {
                        e.stopPropagation();
                        props.onOpenInterface!(job.interface_name);
                      }}
                    >
                      {job.interface_name}
                    </button>
                  ) : (
                    <span className="mono ellipsis" title={job.interface_name}>{job.interface_name}</span>
                  )}
                </td>
                <td>
                  {job.latest_worker_id && props.onOpenWorker ? (
                    <button
                      type="button"
                      className="cell-link mono ellipsis"
                      title={`Open worker ${job.latest_worker_id}`}
                      onClick={(e) => {
                        e.stopPropagation();
                        props.onOpenWorker!(job.latest_worker_id!);
                      }}
                    >
                      {job.latest_worker_id}
                    </button>
                  ) : (
                    <span className="mono ellipsis" title={job.latest_worker_id ?? "Unassigned"}>
                      {job.latest_worker_id ?? "Unassigned"}
                    </span>
                  )}
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
});

export const InterfacesTable = memo(function InterfacesTable(props: {
  interfaces: InterfaceSummary[];
  selectedName?: string | null;
  onSelect?: (name: string) => void;
  onOpenPool?: (interfaceName: string, poolName: string) => void;
  compact?: boolean;
}) {
  const { interfaces, selectedName, onSelect, onOpenPool, compact = false } = props;
  if (interfaces.length === 0) return <EmptyState message="No interfaces match the filter" inline={compact} />;
  return (
    <div className="table-wrap">
      <table className={`table table--fixed${compact ? " table--compact" : ""}${onSelect ? " table--interactive" : ""}`}>
        <thead>
          <tr>
            <th style={{ width: compact ? "26.5%" : "24%" }}>Interface</th>
            <th style={{ width: compact ? "11%" : "8.5%" }}>Queued</th>
            <th style={{ width: compact ? "12.5%" : "9.5%" }}>Running</th>
            {!compact ? <th style={{ width: "9%" }}>Oldest</th> : null}
            <th style={{ width: compact ? "13%" : "10%" }}>Sessions</th>
            <th style={{ width: compact ? "8.5%" : "8%" }}>Free</th>
            <th style={{ width: compact ? "28.5%" : "31%" }}>Policy</th>
          </tr>
        </thead>
        <tbody>
          {interfaces.map((item) => {
            const tiers = item.policy?.pool_tiers ?? [];
            const tip = tiers.map((t, i) => `${i > 0 ? "→ " : ""}${formatPool(t.pool)}${t.spillover_after_ms > 0 ? ` (${formatDuration(t.spillover_after_ms)})` : ""}`).join(" ");
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
                      {tiers.map((tier, idx) => {
                        const label = `${idx > 0 ? "→ " : ""}${formatPool(tier.pool)}${!compact && tier.spillover_after_ms > 0 ? ` (${formatDuration(tier.spillover_after_ms)})` : ""}`;
                        return onOpenPool ? (
                          <button
                            key={`${tier.pool}-${idx}`}
                            type="button"
                            className="tag tag--link ellipsis"
                            onClick={(e) => {
                              e.stopPropagation();
                              onOpenPool(item.name, tier.pool);
                            }}
                          >
                            {label}
                          </button>
                        ) : (
                          <span key={`${tier.pool}-${idx}`} className="tag ellipsis">{label}</span>
                        );
                      })}
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
});
