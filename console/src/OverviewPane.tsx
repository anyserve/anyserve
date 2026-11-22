import type { ConsoleTab, JobScope } from "./app-constants";
import { budgetPercent, formatBytes, formatCapacityMap, formatDuration, formatMicros, formatNumber, formatPool } from "./format";
import { IconArrowRight } from "./icons";
import { EmptyState, InterfacesTable, PanelHeader, ProgressMeter } from "./pane-tables";
import type { JobState, OverviewResponse } from "./types";

interface OverviewPaneProps {
  overview: OverviewResponse | null;
  onNavigate: (tab: ConsoleTab) => void;
  onOpenJobs: (scope: JobScope, status?: JobState) => void;
  onOpenWorkersHealth: (health: "all" | "draining") => void;
  onOpenInterface: (interfaceName: string) => void;
  onOpenPool: (poolKey: string) => void;
  onOpenPoolByTier: (interfaceName: string, poolName: string) => void;
}

export function OverviewPane({
  overview,
  onNavigate,
  onOpenJobs,
  onOpenWorkersHealth,
  onOpenInterface,
  onOpenPool,
  onOpenPoolByTier,
}: OverviewPaneProps) {
  const cp = overview?.control_plane;
  const health = overview?.health;
  const budgets = overview?.budgets;
  const js = overview?.journal_stats;
  const pools = overview?.pools ?? [];
  const interfaces = overview?.interfaces ?? [];
  const draining = overview?.workers.draining;
  const journalStatus = health?.journal.status ?? (overview?.runtime.journal === "disabled" ? "disabled" : "healthy");
  const healthDot = !overview ? "offline" : health && !health.live ? "bad" : health && !health.ready ? "warn" : "ok";
  const healthLabel = !overview ? "Connecting" : health && !health.live ? "Not live" : health && !health.ready ? "Not ready" : "Ready";

  const cpItems = cp
    ? [
        { label: "gRPC listen", value: cp.grpc_listen, mono: true, tag: `TLS: ${cp.grpc_tls_enabled ? "On" : "Off"}` },
        { label: "Console listen", value: cp.console_listen, mono: true, tag: `Auth: ${cp.auth_enabled ? "On" : "Off"}` },
        { label: "Leader generation", value: `#${formatNumber(cp.generation)}` },
        { label: "Priority / Max attempts", value: `${cp.priority_levels} / ${cp.default_max_attempts}` },
        { label: "Heartbeat / Timeout", value: `${formatDuration(cp.heartbeat_interval_ms)} / ${formatDuration(cp.heartbeat_timeout_ms)}` },
        { label: "Reconnect grace", value: cp.reconnect_grace_ms != null ? formatDuration(cp.reconnect_grace_ms) : "Default" },
        ...(js
          ? [
              { label: "Pending ops", value: formatNumber(js.pending_ops) },
              { label: "Committed batches / ops", value: `${formatNumber(js.committed_batches)} / ${formatNumber(js.committed_ops)}` },
              { label: "Last batch / Latency", value: `${formatNumber(js.last_batch_size)} ops · ${formatMicros(js.last_commit_micros)}` },
            ]
          : []),
      ]
    : [];

  return (
    <div className="overview-page">
      <section className="panel overview-hero">
        <PanelHeader title="Overview" />
        <div className="kpi-grid">
          <div className="kpi-card">
            <div className="kpi-card__head">
              <span>Queue</span>
              <button type="button" className="link-inline" onClick={() => onOpenJobs("queue")}>View all <IconArrowRight /></button>
            </div>
            <div className="kpi-card__value num">{formatNumber(overview?.queue.active ?? 0)}</div>
            <div className="kpi-card__sub">
              <button type="button" className="stat-link" onClick={() => onOpenJobs("queue", "queued")}>
                Queued <strong className="num">{formatNumber(overview?.queue.queued ?? 0)}</strong>
              </button>
              <button type="button" className="stat-link" onClick={() => onOpenJobs("queue", "running")}>
                Running <strong className="num">{formatNumber(overview?.queue.running ?? 0)}</strong>
              </button>
            </div>
          </div>

          <div className="kpi-card">
            <div className="kpi-card__head">
              <span>Outcomes</span>
              <div className="kpi-card__head-right">
                {cp ? (
                  <button type="button" className="tag tag--link num" title="Query retained jobs in Explore" onClick={() => onNavigate("explore")}>
                    Retained {formatNumber(cp.retained_jobs)}
                  </button>
                ) : null}
                <button type="button" className="link-inline" onClick={() => onOpenJobs("history")}>History <IconArrowRight /></button>
              </div>
            </div>
            <div className="kpi-card__value num">{formatNumber(overview?.history.total ?? 0)}</div>
            <div className="kpi-card__sub">
              <button type="button" className="stat-link tone-ok" onClick={() => onOpenJobs("history", "succeeded")}>
                Succeeded <strong className="num">{formatNumber(overview?.history.succeeded ?? 0)}</strong>
              </button>
              <button type="button" className="stat-link tone-danger" onClick={() => onOpenJobs("history", "failed")}>
                Failed <strong className="num">{formatNumber(overview?.history.failed ?? 0)}</strong>
              </button>
              <button type="button" className="stat-link" onClick={() => onOpenJobs("history", "cancelled")}>
                Cancelled <strong className="num">{formatNumber(overview?.history.cancelled ?? 0)}</strong>
              </button>
            </div>
          </div>

          <div className="kpi-card">
            <div className="kpi-card__head">
              <span>Workers &amp; Sessions</span>
              <button type="button" className="link-inline" onClick={() => onOpenWorkersHealth("all")}>View all <IconArrowRight /></button>
            </div>
            <div className="kpi-card__value num">{overview ? `${formatNumber(overview.workers.healthy)} / ${formatNumber(overview.workers.total)}` : "0 / 0"}</div>
            <div className="kpi-card__sub">
              {cp ? <span>Sessions <strong className="num">{formatNumber(cp.sessions)}/{formatNumber(cp.max_sessions)}</strong></span> : null}
              <span>Active leases <strong className="num">{formatNumber(overview?.workers.active_leases ?? 0)}</strong></span>
              {draining != null ? (
                draining > 0 ? (
                  <button type="button" className="stat-link tone-warn" onClick={() => onOpenWorkersHealth("draining")}>
                    Draining <strong className="num">{formatNumber(draining)}</strong>
                  </button>
                ) : (
                  <span>Draining <strong className="num">0</strong></span>
                )
              ) : null}
            </div>
            {overview && Object.keys(overview.workers.total_capacity).length > 0 ? (
              <div className="kpi-card__extra">Capacity: <code>{formatCapacityMap(overview.workers.total_capacity)}</code></div>
            ) : null}
          </div>

          <div className="kpi-card">
            <div className="kpi-card__head">
              <span>Runtime Health</span>
            </div>
            <div className="status-inline">
              <span className={`status-dot status-dot--${healthDot}`} aria-hidden="true" />
              <strong>{healthLabel}</strong>
              {cp ? <span className="muted">· Gen #{cp.generation}</span> : null}
            </div>
            <div className="kpi-card__sub">
              <span className={health?.live === false ? "tone-danger" : undefined}>Live <strong>{health == null ? (overview ? "Yes" : "N/A") : health.live ? "Yes" : "No"}</strong></span>
              <span className={health?.ready === false ? "tone-warn" : undefined}>Ready <strong>{health == null ? (overview ? "Yes" : "N/A") : health.ready ? "Yes" : "No"}</strong></span>
              <span className={journalStatus === "unavailable" ? "tone-danger" : undefined}>
                Journal <strong>{journalStatus === "healthy" ? "Healthy" : journalStatus === "disabled" ? "Disabled" : "Unavailable"}</strong>
              </span>
            </div>
            {health?.journal.message ? <div className="kpi-card__extra tone-danger">{health.journal.message}</div> : null}
          </div>
        </div>
      </section>

      <div className="overview-grid-2">
        <section className="panel">
          <PanelHeader title="Memory Byte Budgets" />
          <div className="panel__body">
            {budgets ? (
              <div className="budget-list">
                {[
                  { label: "Input bytes", used: budgets.input_bytes, max: budgets.max_pending_bytes },
                  { label: "Result bytes", used: budgets.result_bytes, max: budgets.max_result_bytes },
                  { label: "Event bytes", used: budgets.event_bytes, max: budgets.max_event_bytes },
                ].map((b) => {
                  const pct = budgetPercent(b.used, b.max);
                  return (
                    <div key={b.label} className="budget-row">
                      <div className="budget-row__head">
                        <span className="budget-row__label">{b.label}</span>
                        <span className="budget-row__meta num">{formatBytes(b.used)} / {formatBytes(b.max)} ({pct.toFixed(1)}%)</span>
                      </div>
                      <ProgressMeter percent={pct} />
                    </div>
                  );
                })}
              </div>
            ) : <EmptyState message="Budget metrics unavailable on this server payload." inline />}
          </div>
        </section>

        <section className="panel">
          <PanelHeader title="Control Plane & Journal" />
          <div className="panel__body">
            {cp ? (
              <div className="cp-grid">
                {cpItems.map((item) => (
                  <div key={item.label} className="meta-item">
                    <span>{item.label}</span>
                    <strong className={item.mono ? "meta-item__value mono" : "meta-item__value"}>
                      {item.value}
                      {item.tag ? <> <em className="tag">{item.tag}</em></> : null}
                    </strong>
                  </div>
                ))}
              </div>
            ) : <EmptyState message="Control plane metadata unavailable on this server payload." inline />}
          </div>
        </section>
      </div>

      <div className="overview-grid-2">
        <section className="panel">
          <PanelHeader
            title="Interfaces"
            meta={formatNumber(interfaces.length)}
            actions={<button type="button" className="link-inline" onClick={() => onNavigate("interfaces")}>View all <IconArrowRight /></button>}
          />
          {interfaces.length === 0 ? (
            <div className="panel__body"><EmptyState message="No active or configured interfaces" inline /></div>
          ) : <InterfacesTable interfaces={interfaces.slice(0, 8)} onSelect={onOpenInterface} onOpenPool={onOpenPoolByTier} compact />}
        </section>

        <section className="panel">
          <PanelHeader
            title="Pools"
            meta={formatNumber(pools.length)}
            actions={<button type="button" className="link-inline" onClick={() => onNavigate("pools")}>View all <IconArrowRight /></button>}
          />
          {pools.length === 0 ? (
            <div className="panel__body"><EmptyState message="No active worker pools" inline /></div>
          ) : (
            <div className="table-wrap">
              <table className="table table--fixed table--interactive">
                <thead>
                  <tr>
                    <th style={{ width: "24%" }}>Pool</th>
                    <th style={{ width: "34%" }}>Interface</th>
                    <th style={{ width: "14%" }}>Sessions</th>
                    <th style={{ width: "14%" }}>Leases</th>
                    <th style={{ width: "14%" }}>Desired</th>
                  </tr>
                </thead>
                <tbody>
                  {pools.slice(0, 8).map((pool) => {
                    const key = `${pool.namespace}::${pool.interface}::${pool.pool}`;
                    const poolName = formatPool(pool.pool);
                    return (
                      <tr key={key} onClick={() => onOpenPool(key)}>
                        <td className="table__primary mono ellipsis" title={poolName}>{poolName}</td>
                        <td>
                          <div className="cell-inline" title={`${pool.namespace} / ${pool.interface}`}>
                            {pool.namespace !== "default" ? <span className="tag">{pool.namespace}</span> : null}
                            <button
                              type="button"
                              className="cell-link mono ellipsis"
                              onClick={(e) => {
                                e.stopPropagation();
                                onOpenInterface(pool.interface);
                              }}
                            >
                              {pool.interface}
                            </button>
                          </div>
                        </td>
                        <td className="num">{formatNumber(pool.sessions)}</td>
                        <td className="num">{formatNumber(pool.active_leases)} / {formatNumber(pool.active_leases + pool.free_slots)}</td>
                        <td className="num">
                          {pool.desired_additional_slots > 0 ? <span className="tag tag--warn">+{formatNumber(pool.desired_additional_slots)}</span> : "0"}
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          )}
        </section>
      </div>
    </div>
  );
}
