import type { ConsoleTab } from "./app-constants";
import {
  budgetPercent,
  formatBytes,
  formatCapacityMap,
  formatDuration,
  formatMicros,
  formatNumber,
  formatTimestamp,
} from "./format";
import { t, type Lang } from "./i18n";
import { IconArrowRight } from "./icons";
import { EmptyState, InterfacesTable, PanelHeader, ProgressMeter } from "./pane-tables";
import type { OverviewResponse, WorkersResponse } from "./types";

interface OverviewPaneProps {
  overview: OverviewResponse | null;
  workersResponse: WorkersResponse | null;
  lang: Lang;
  onNavigate: (tab: ConsoleTab) => void;
  onOpenInterface: (interfaceName: string) => void;
  onOpenPool: (poolKey: string) => void;
}

export function OverviewPane(props: OverviewPaneProps) {
  const { overview, workersResponse, lang, onNavigate, onOpenInterface, onOpenPool } = props;
  const cp = overview?.control_plane;
  const health = overview?.health;
  const budgets = overview?.budgets;
  const journalStats = overview?.journal_stats;
  const pools = overview?.pools ?? [];
  const interfaces = overview?.interfaces ?? [];

  const drainingCount =
    workersResponse?.draining ??
    workersResponse?.workers.filter((w) => w.connected && w.draining).length ??
    null;

  const journalStatus =
    health?.journal.status ??
    (overview?.runtime.journal === "disabled" ? "disabled" : "healthy");

  return (
    <div className="overview-page">
      <section className="panel overview-hero">
        <PanelHeader
          title={t(lang, "overview.title")}
          meta={t(lang, "overview.subtitle")}
          actions={
            overview ? (
              <span className="pill-meta">
                {overview.runtime.label} · {formatTimestamp(overview.refreshed_at_ms)}
              </span>
            ) : null
          }
        />

        <div className="kpi-grid">
          {/* Queue */}
          <div className="kpi-card">
            <div className="kpi-card__head">
              <span>{t(lang, "overview.card.queue")}</span>
              <button
                type="button"
                className="link-inline"
                onClick={() => onNavigate("jobs")}
              >
                {t(lang, "common.viewAll")}
                <IconArrowRight />
              </button>
            </div>
            <div className="kpi-card__value num">
              {formatNumber(overview?.queue.active ?? 0)}
            </div>
            <div className="kpi-card__sub">
              <span>
                {t(lang, "overview.card.queued")}{" "}
                <strong className="num">{formatNumber(overview?.queue.queued ?? 0)}</strong>
              </span>
              <span>
                {t(lang, "overview.card.running")}{" "}
                <strong className="num">{formatNumber(overview?.queue.running ?? 0)}</strong>
              </span>
            </div>
          </div>

          {/* Outcomes */}
          <div className="kpi-card">
            <div className="kpi-card__head">
              <span>{t(lang, "overview.card.outcomes")}</span>
              {cp ? (
                <span className="tag-inline num">
                  {t(lang, "overview.card.retained")} {formatNumber(cp.retained_jobs)}
                </span>
              ) : null}
            </div>
            <div className="kpi-card__value num">
              {formatNumber(overview?.history.total ?? 0)}
            </div>
            <div className="kpi-card__sub">
              <span className="tone-ok">
                {t(lang, "overview.card.succeeded")}{" "}
                <strong className="num">{formatNumber(overview?.history.succeeded ?? 0)}</strong>
              </span>
              <span className="tone-danger">
                {t(lang, "overview.card.failed")}{" "}
                <strong className="num">{formatNumber(overview?.history.failed ?? 0)}</strong>
              </span>
              <span>
                {t(lang, "overview.card.cancelled")}{" "}
                <strong className="num">{formatNumber(overview?.history.cancelled ?? 0)}</strong>
              </span>
            </div>
          </div>

          {/* Workers & Sessions */}
          <div className="kpi-card">
            <div className="kpi-card__head">
              <span>{t(lang, "overview.card.fleet")}</span>
              <button
                type="button"
                className="link-inline"
                onClick={() => onNavigate("workers")}
              >
                {t(lang, "common.viewAll")}
                <IconArrowRight />
              </button>
            </div>
            <div className="kpi-card__value num">
              {overview ? `${formatNumber(overview.workers.healthy)} / ${formatNumber(overview.workers.total)}` : "0 / 0"}
            </div>
            <div className="kpi-card__sub">
              {cp ? (
                <span>
                  {t(lang, "overview.card.sessions")}{" "}
                  <strong className="num">
                    {formatNumber(cp.sessions)}/{formatNumber(cp.max_sessions)}
                  </strong>
                </span>
              ) : null}
              <span>
                {t(lang, "overview.card.activeLeases")}{" "}
                <strong className="num">{formatNumber(overview?.workers.active_leases ?? 0)}</strong>
              </span>
              {drainingCount != null ? (
                <span className={drainingCount > 0 ? "tone-warn" : undefined}>
                  {t(lang, "overview.card.draining")}{" "}
                  <strong className="num">{formatNumber(drainingCount)}</strong>
                </span>
              ) : null}
            </div>
            {overview && Object.keys(overview.workers.total_capacity).length > 0 ? (
              <div className="kpi-card__extra">
                {t(lang, "overview.card.totalCapacity")}:{" "}
                <code>{formatCapacityMap(overview.workers.total_capacity)}</code>
              </div>
            ) : null}
          </div>

          {/* Runtime Health */}
          <div className="kpi-card">
            <div className="kpi-card__head">
              <span>{t(lang, "overview.card.health")}</span>
              {cp ? (
                <span className="tag-inline num">
                  {t(lang, "sidebar.leaderGen")} #{cp.generation}
                </span>
              ) : null}
            </div>
            <div className="health-pills">
              <span
                className={`badge ${
                  health == null || health.live ? "badge--worker-healthy" : "badge--failed"
                }`}
              >
                <i className="badge__dot" aria-hidden="true" />
                {t(lang, "overview.card.live")}:{" "}
                {health == null
                  ? overview
                    ? t(lang, "common.yes")
                    : t(lang, "common.na")
                  : health.live
                    ? t(lang, "common.yes")
                    : t(lang, "common.no")}
              </span>
              <span
                className={`badge ${
                  health == null || health.ready ? "badge--worker-healthy" : "badge--queued"
                }`}
              >
                <i className="badge__dot" aria-hidden="true" />
                {t(lang, "overview.card.ready")}:{" "}
                {health == null
                  ? overview
                    ? t(lang, "common.yes")
                    : t(lang, "common.na")
                  : health.ready
                    ? t(lang, "common.yes")
                    : t(lang, "common.no")}
              </span>
            </div>
            <div className="kpi-card__sub">
              <span>
                {t(lang, "overview.card.journal")}:{" "}
                <strong
                  className={
                    journalStatus === "unavailable"
                      ? "tone-danger"
                      : journalStatus === "healthy"
                        ? "tone-ok"
                        : ""
                  }
                >
                  {t(lang, `overview.journal.${journalStatus}`)}
                </strong>
              </span>
            </div>
            {health?.journal.message ? (
              <div className="kpi-card__extra tone-danger">{health.journal.message}</div>
            ) : null}
          </div>
        </div>
      </section>

      <div className="overview-grid-2">
        {/* Memory Byte Budgets */}
        <section className="panel">
          <PanelHeader
            title={t(lang, "overview.budgets.title")}
            meta={t(lang, "overview.budgets.subtitle")}
          />
          <div className="panel__body">
            {budgets ? (
              <div className="budget-list">
                <BudgetRow
                  label={t(lang, "overview.budgets.input")}
                  used={budgets.input_bytes}
                  max={budgets.max_pending_bytes}
                />
                <BudgetRow
                  label={t(lang, "overview.budgets.result")}
                  used={budgets.result_bytes}
                  max={budgets.max_result_bytes}
                />
                <BudgetRow
                  label={t(lang, "overview.budgets.event")}
                  used={budgets.event_bytes}
                  max={budgets.max_event_bytes}
                />
              </div>
            ) : (
              <EmptyState message={t(lang, "overview.budgets.unavailable")} compact />
            )}
          </div>
        </section>

        {/* Control Plane & Journal */}
        <section className="panel">
          <PanelHeader
            title={t(lang, "overview.cp.title")}
            meta={t(lang, "overview.cp.subtitle")}
          />
          <div className="panel__body">
            {cp ? (
              <div className="cp-grid">
                <div className="cp-item">
                  <span>{t(lang, "overview.cp.grpcListen")}</span>
                  <strong className="table__mono">
                    {cp.grpc_listen}{" "}
                    <em className="tag-inline">
                      TLS: {t(lang, cp.grpc_tls_enabled ? "common.enabled" : "common.disabled")}
                    </em>
                  </strong>
                </div>
                <div className="cp-item">
                  <span>{t(lang, "overview.cp.consoleListen")}</span>
                  <strong className="table__mono">
                    {cp.console_listen}{" "}
                    <em className="tag-inline">
                      Auth: {t(lang, cp.auth_enabled ? "common.enabled" : "common.disabled")}
                    </em>
                  </strong>
                </div>
                <div className="cp-item">
                  <span>{t(lang, "overview.cp.generation")}</span>
                  <strong className="num">#{formatNumber(cp.generation)}</strong>
                </div>
                <div className="cp-item">
                  <span>
                    {t(lang, "overview.cp.priorityLevels")} / {t(lang, "overview.cp.defaultMaxAttempts")}
                  </span>
                  <strong className="num">
                    {cp.priority_levels} / {cp.default_max_attempts}
                  </strong>
                </div>
                <div className="cp-item">
                  <span>{t(lang, "overview.cp.heartbeat")}</span>
                  <strong className="num">
                    {formatDuration(cp.heartbeat_interval_ms)} / {formatDuration(cp.heartbeat_timeout_ms)}
                  </strong>
                </div>
                <div className="cp-item">
                  <span>{t(lang, "overview.cp.reconnectGrace")}</span>
                  <strong className="num">
                    {cp.reconnect_grace_ms != null
                      ? formatDuration(cp.reconnect_grace_ms)
                      : t(lang, "common.default")}
                  </strong>
                </div>
                <div className="cp-item">
                  <span>{t(lang, "overview.cp.journalMode")}</span>
                  <strong>{overview?.runtime.label ?? t(lang, "common.na")}</strong>
                </div>
                {journalStats ? (
                  <>
                    <div className="cp-item">
                      <span>{t(lang, "overview.cp.pendingOps")}</span>
                      <strong className="num">{formatNumber(journalStats.pending_ops)}</strong>
                    </div>
                    <div className="cp-item">
                      <span>
                        {t(lang, "overview.cp.committedBatches")} / {t(lang, "overview.cp.committedOps")}
                      </span>
                      <strong className="num">
                        {formatNumber(journalStats.committed_batches)} / {formatNumber(journalStats.committed_ops)}
                      </strong>
                    </div>
                    <div className="cp-item">
                      <span>{t(lang, "overview.cp.lastBatch")}</span>
                      <strong className="num">
                        {formatNumber(journalStats.last_batch_size)} ops ·{" "}
                        {formatMicros(journalStats.last_commit_micros)}
                      </strong>
                    </div>
                  </>
                ) : null}
              </div>
            ) : (
              <EmptyState message={t(lang, "overview.cp.unavailable")} compact />
            )}
          </div>
        </section>
      </div>

      <div className="overview-grid-2">
        {/* Live Interfaces preview */}
        <section className="panel">
          <PanelHeader
            title={t(lang, "overview.interfaces.title")}
            meta={`${formatNumber(interfaces.length)}`}
            actions={
              <button
                type="button"
                className="link-inline"
                onClick={() => onNavigate("interfaces")}
              >
                {t(lang, "common.viewAll")}
                <IconArrowRight />
              </button>
            }
          />
          {interfaces.length === 0 ? (
            <div className="panel__body">
              <EmptyState message={t(lang, "overview.interfaces.empty")} compact />
            </div>
          ) : (
            <InterfacesTable
              interfaces={interfaces.slice(0, 8)}
              onSelect={onOpenInterface}
              lang={lang}
              compact
            />
          )}
        </section>

        {/* Live Pools preview */}
        <section className="panel">
          <PanelHeader
            title={t(lang, "overview.pools.title")}
            meta={`${formatNumber(pools.length)}`}
            actions={
              <button
                type="button"
                className="link-inline"
                onClick={() => onNavigate("pools")}
              >
                {t(lang, "common.viewAll")}
                <IconArrowRight />
              </button>
            }
          />
          {pools.length === 0 ? (
            <div className="panel__body">
              <EmptyState message={t(lang, "overview.pools.empty")} compact />
            </div>
          ) : (
            <div className="table-wrap">
              <table className="table table--interactive">
                <thead>
                  <tr>
                    <th>{t(lang, "pools.table.pool")}</th>
                    <th>{t(lang, "pools.table.namespace")} / {t(lang, "pools.table.interface")}</th>
                    <th>{t(lang, "pools.table.sessions")}</th>
                    <th>{t(lang, "pools.table.leases")}</th>
                    <th>{t(lang, "pools.table.desired")}</th>
                  </tr>
                </thead>
                <tbody>
                  {pools.slice(0, 8).map((pool) => {
                    const key = `${pool.namespace}::${pool.interface}::${pool.pool}`;
                    return (
                      <tr key={key} onClick={() => onOpenPool(key)}>
                        <td className="table__primary table__mono">{pool.pool}</td>
                        <td>
                          <span className="tag-inline">{pool.namespace}</span>{" "}
                          <span className="table__mono">{pool.interface}</span>
                        </td>
                        <td className="num">{formatNumber(pool.sessions)}</td>
                        <td className="num">
                          {formatNumber(pool.active_leases)} /{" "}
                          {formatNumber(pool.active_leases + pool.free_slots)}
                        </td>
                        <td className="num">
                          {pool.desired_additional_slots > 0 ? (
                            <span className="pill-sub pill-sub--warn">
                              +{formatNumber(pool.desired_additional_slots)}
                            </span>
                          ) : (
                            "0"
                          )}
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

function BudgetRow(props: { label: string; used: number; max: number }) {
  const pct = budgetPercent(props.used, props.max);
  return (
    <div className="budget-row">
      <div className="budget-row__head">
        <span className="budget-row__label">{props.label}</span>
        <span className="budget-row__meta num">
          {formatBytes(props.used)} / {formatBytes(props.max)} ({pct.toFixed(1)}%)
        </span>
      </div>
      <ProgressMeter percent={pct} />
    </div>
  );
}
