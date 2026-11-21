import type { ReactNode } from "react";

import type { PaneStat } from "./app-constants";
import {
  formatDuration,
  formatNumber,
  formatTimestamp,
  truncateMiddle,
} from "./format";
import { t, type Lang } from "./i18n";
import type { InterfaceSummary, JobState, JobSummary, WorkerSummary } from "./types";

export function StatsRow(props: { stats: PaneStat[] }) {
  return (
    <div
      className="stat-row"
      style={{ gridTemplateColumns: `repeat(${Math.max(1, props.stats.length)}, minmax(0, 1fr))` }}
    >
      {props.stats.map((stat) => (
        <div key={stat.label} className="stat-card">
          <span>{stat.label}</span>
          <strong>
            {typeof stat.value === "number" ? formatNumber(stat.value) : stat.value}
          </strong>
        </div>
      ))}
    </div>
  );
}

export function PanelHeader(props: {
  title: string;
  meta: string;
  actions?: ReactNode;
}) {
  return (
    <div className="panel__header">
      <div className="panel__header-title">
        <h1>{props.title}</h1>
        <span>{props.meta}</span>
      </div>
      {props.actions ? <div className="panel__header-actions">{props.actions}</div> : null}
    </div>
  );
}

export function JobsTable(props: {
  jobs: JobSummary[];
  selectedJobId: string | null;
  emptyMessage: string;
  onSelect: (jobId: string) => void;
  lang?: Lang;
}) {
  const { jobs, selectedJobId, emptyMessage, onSelect, lang = "en" } = props;

  if (jobs.length === 0) {
    return <EmptyState message={emptyMessage} />;
  }

  return (
    <div className="table-wrap">
      <table className="table table--interactive">
        <thead>
          <tr>
            <th>{t(lang, "jobs.table.status")}</th>
            <th>{t(lang, "jobs.table.job")}</th>
            <th>{t(lang, "jobs.table.interface")}</th>
            <th>{t(lang, "jobs.table.worker")}</th>
            <th>{t(lang, "jobs.table.attempts")}</th>
            <th>{t(lang, "jobs.table.updated")}</th>
          </tr>
        </thead>
        <tbody>
          {jobs.map((job) => {
            const isSelected = job.job_id === selectedJobId;
            const namespace = job.namespace && job.namespace !== "default" ? job.namespace : null;
            return (
              <tr
                key={`${job.namespace ?? "default"}::${job.job_id}`}
                className={isSelected ? "is-selected" : ""}
                onClick={() => onSelect(job.job_id)}
              >
                <td>
                  <div className="cell-stack">
                    <StateBadge state={job.state} lang={lang} />
                    {job.error_code ? (
                      <span className="pill-sub pill-sub--danger">{job.error_code}</span>
                    ) : null}
                  </div>
                </td>
                <td>
                  <div className="table__primary table__mono">
                    {truncateMiddle(job.job_id, 28)}
                  </div>
                  <div className="table__secondary">
                    <span>{job.source ?? job.metadata.request_id ?? t(lang, "common.direct")}</span>
                    {namespace ? <span className="tag-inline">{namespace}</span> : null}
                    {job.durability ? (
                      <span className="tag-inline">{job.durability}</span>
                    ) : null}
                  </div>
                </td>
                <td className="table__mono">{job.interface_name}</td>
                <td className="table__mono">
                  {job.latest_worker_id
                    ? truncateMiddle(job.latest_worker_id, 24)
                    : t(lang, "common.unassigned")}
                </td>
                <td className="num">{job.attempt_count}</td>
                <td>
                  <div className="table__mono table__timestamp">
                    {formatTimestamp(job.updated_at_ms)}
                  </div>
                  {job.duration_ms != null ? (
                    <div className="table__secondary num">
                      {formatDuration(job.duration_ms)}
                    </div>
                  ) : null}
                </td>
              </tr>
            );
          })}
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

export function StateBadge(props: { state: JobState; lang?: Lang }) {
  const lang = props.lang ?? "en";
  return (
    <span className={`badge badge--${props.state}`}>
      <i className="badge__dot" aria-hidden="true" />
      {t(lang, `state.${props.state}`)}
    </span>
  );
}

export function WorkerStateBadge(props: {
  worker: Pick<WorkerSummary, "healthy" | "connected" | "draining">;
  lang?: Lang;
}) {
  const { worker, lang = "en" } = props;
  const kind = !worker.connected
    ? "disconnected"
    : worker.draining
      ? "draining"
      : worker.healthy
        ? "healthy"
        : "unavailable";
  return (
    <span className={`badge badge--worker-${kind}`}>
      <i className="badge__dot" aria-hidden="true" />
      {t(lang, `worker.${kind}`)}
    </span>
  );
}

export function ProgressMeter(props: {
  percent: number;
  tone?: "accent" | "warn" | "danger" | "ok";
}) {
  const clamped = Math.max(0, Math.min(100, props.percent));
  const tone =
    props.tone ?? (clamped >= 90 ? "danger" : clamped >= 75 ? "warn" : "accent");
  return (
    <div className="meter" role="progressbar" aria-valuenow={Math.round(clamped)} aria-valuemin={0} aria-valuemax={100}>
      <div className={`meter__fill meter__fill--${tone}`} style={{ width: `${clamped}%` }} />
    </div>
  );
}

export function InterfacesTable(props: {
  interfaces: InterfaceSummary[];
  selectedName?: string | null;
  onSelect?: (name: string) => void;
  lang?: Lang;
  compact?: boolean;
}) {
  const { interfaces, selectedName, onSelect, lang = "en", compact = false } = props;

  if (interfaces.length === 0) {
    return <EmptyState message={t(lang, "interfaces.empty")} compact={compact} />;
  }

  return (
    <div className="table-wrap">
      <table className={`table${onSelect ? " table--interactive" : ""}`}>
        <thead>
          <tr>
            <th>{t(lang, "interfaces.table.name")}</th>
            <th>{t(lang, "interfaces.table.queued")}</th>
            <th>{t(lang, "interfaces.table.running")}</th>
            {!compact ? <th>{t(lang, "interfaces.table.oldestWait")}</th> : null}
            <th>{t(lang, "interfaces.table.sessions")}</th>
            <th>{t(lang, "interfaces.table.freeSlots")}</th>
            <th>{t(lang, "interfaces.table.policy")}</th>
          </tr>
        </thead>
        <tbody>
          {interfaces.map((item) => {
            const isSelected = selectedName === item.name;
            const tiers = item.policy?.pool_tiers ?? [];
            return (
              <tr
                key={item.name}
                className={isSelected ? "is-selected" : ""}
                onClick={onSelect ? () => onSelect(item.name) : undefined}
              >
                <td className="table__primary table__mono">{item.name}</td>
                <td className="num">{formatNumber(item.queued)}</td>
                <td className="num">{formatNumber(item.running)}</td>
                {!compact ? (
                  <td className="num">{formatDuration(item.oldest_queued_age_ms)}</td>
                ) : null}
                <td className="num">{formatNumber(item.sessions)}</td>
                <td className="num">{formatNumber(item.free_slots)}</td>
                <td>
                  {item.policy ? (
                    <div className="tier-chain-inline">
                      {tiers.length > 0 ? (
                        tiers.map((tier, idx) => (
                          <span key={`${tier.pool}-${idx}`} className="tag-inline">
                            {idx > 0 ? "→ " : ""}
                            {tier.pool}
                            {tier.spillover_after_ms > 0
                              ? ` (${formatDuration(tier.spillover_after_ms)})`
                              : ""}
                          </span>
                        ))
                      ) : (
                        <span className="table__secondary">
                          {item.policy.max_queue != null ? `q=${item.policy.max_queue}` : t(lang, "common.default")}
                        </span>
                      )}
                    </div>
                  ) : (
                    <span className="table__secondary">{t(lang, "common.default")}</span>
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
