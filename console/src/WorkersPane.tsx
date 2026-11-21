import { useMemo, useState } from "react";

import {
  formatCapacityMap,
  formatNumber,
  formatPercent,
  formatTimestamp,
  truncateMiddle,
} from "./format";
import { t, type Lang } from "./i18n";
import { IconSearch } from "./icons";
import {
  InspectorGroup,
  InspectorSection,
  KeyValueGrid,
  MetaItem,
} from "./pane-inspector-details";
import {
  EmptyState,
  PanelHeader,
  ProgressMeter,
  StatsRow,
  WorkerStateBadge,
} from "./pane-tables";
import type { OverviewResponse, WorkerSummary, WorkersResponse } from "./types";

type HealthFilter = "all" | "healthy" | "draining" | "disconnected";

interface WorkersPaneProps {
  overview: OverviewResponse | null;
  workersResponse: WorkersResponse | null;
  workers: WorkerSummary[];
  selectedWorkerId: string | null;
  onSelectWorker: (workerId: string) => void;
  onViewJobsForWorker: (workerId: string) => void;
  onOpenInterface: (interfaceName: string) => void;
  lang: Lang;
}

export function WorkersPane(props: WorkersPaneProps) {
  const {
    overview,
    workersResponse,
    workers,
    selectedWorkerId,
    onSelectWorker,
    onViewJobsForWorker,
    onOpenInterface,
    lang,
  } = props;

  const [healthFilter, setHealthFilter] = useState<HealthFilter>("all");
  const [poolFilter, setPoolFilter] = useState("all");
  const [interfaceFilter, setInterfaceFilter] = useState("all");
  const [search, setSearch] = useState("");

  const poolOptions = useMemo(() => {
    const set = new Set<string>();
    for (const w of workers) {
      if (w.pool) set.add(w.pool);
    }
    return Array.from(set).sort();
  }, [workers]);

  const interfaceOptions = useMemo(() => {
    const set = new Set<string>();
    for (const w of workers) {
      for (const iface of w.interfaces) set.add(iface);
    }
    return Array.from(set).sort();
  }, [workers]);

  const filtered = useMemo(() => {
    const q = search.trim().toLowerCase();
    return workers.filter((w) => {
      if (healthFilter === "healthy" && !w.healthy) return false;
      if (healthFilter === "draining" && !(w.connected && w.draining)) return false;
      if (healthFilter === "disconnected" && w.connected) return false;
      if (poolFilter !== "all" && (w.pool ?? "default") !== poolFilter) return false;
      if (interfaceFilter !== "all" && !w.interfaces.includes(interfaceFilter)) return false;
      if (
        q &&
        !w.worker_id.toLowerCase().includes(q) &&
        !(w.session_id ?? "").toLowerCase().includes(q) &&
        !(w.pool ?? "").toLowerCase().includes(q) &&
        !(w.namespace ?? "").toLowerCase().includes(q)
      ) {
        return false;
      }
      return true;
    });
  }, [workers, healthFilter, poolFilter, interfaceFilter, search]);

  const activeWorker =
    filtered.find((w) => w.worker_id === selectedWorkerId) ?? filtered[0] ?? null;

  const totalCount = workersResponse?.total ?? overview?.workers.total ?? workers.length;
  const healthyCount =
    workersResponse?.healthy ??
    overview?.workers.healthy ??
    workers.filter((w) => w.healthy).length;
  const drainingCount =
    workersResponse?.draining ?? workers.filter((w) => w.connected && w.draining).length;
  const disconnectedCount =
    workersResponse?.disconnected ?? workers.filter((w) => !w.connected).length;

  return (
    <div className="content-grid">
      <section className="panel">
        <StatsRow
          stats={[
            { label: t(lang, "workers.stat.total"), value: totalCount },
            { label: t(lang, "workers.stat.healthy"), value: healthyCount },
            { label: t(lang, "workers.stat.draining"), value: drainingCount },
            { label: t(lang, "workers.stat.disconnected"), value: disconnectedCount },
            { label: t(lang, "workers.stat.leases"), value: overview?.workers.active_leases ?? 0 },
          ]}
        />

        <PanelHeader
          title={t(lang, "workers.title")}
          meta={
            overview
              ? `${formatNumber(filtered.length)} · ${formatCapacityMap(overview.workers.total_capacity)}`
              : t(lang, "workers.subtitle")
          }
        />

        <div className="panel__toolbar panel__toolbar--wrap">
          <div className="seg-control" role="group" aria-label="Worker health filter">
            {(["all", "healthy", "draining", "disconnected"] as const).map((item) => (
              <button
                key={item}
                type="button"
                className={`seg-control__btn${healthFilter === item ? " is-active" : ""}`}
                aria-pressed={healthFilter === item}
                onClick={() => setHealthFilter(item)}
              >
                {t(lang, `workers.filter.${item}`)}
              </button>
            ))}
          </div>

          <div className="toolbar-filters">
            <label className="field">
              <span className="field__label">{t(lang, "workers.filter.pool")}</span>
              <select
                className="field__control"
                value={poolFilter}
                onChange={(event) => setPoolFilter(event.target.value)}
              >
                <option value="all">{t(lang, "workers.filter.allPools")}</option>
                {poolOptions.map((pool) => (
                  <option key={pool} value={pool}>
                    {pool}
                  </option>
                ))}
              </select>
            </label>

            <label className="field">
              <span className="field__label">{t(lang, "workers.filter.interface")}</span>
              <select
                className="field__control"
                value={interfaceFilter}
                onChange={(event) => setInterfaceFilter(event.target.value)}
              >
                <option value="all">{t(lang, "workers.filter.allInterfaces")}</option>
                {interfaceOptions.map((iface) => (
                  <option key={iface} value={iface}>
                    {iface}
                  </option>
                ))}
              </select>
            </label>

            <div className="search-box">
              <IconSearch className="search-box__icon" />
              <input
                type="search"
                className="search-box__input"
                value={search}
                onChange={(event) => setSearch(event.target.value)}
                placeholder={t(lang, "workers.filter.searchPlaceholder")}
                aria-label={t(lang, "workers.filter.searchPlaceholder")}
              />
            </div>
          </div>
        </div>

        {filtered.length === 0 ? (
          <EmptyState message={t(lang, "workers.empty")} />
        ) : (
          <div className="table-wrap">
            <table className="table table--interactive">
              <thead>
                <tr>
                  <th>{t(lang, "workers.table.status")}</th>
                  <th>{t(lang, "workers.table.worker")}</th>
                  <th>{t(lang, "workers.table.pool")}</th>
                  <th>{t(lang, "workers.table.interfaces")}</th>
                  <th>{t(lang, "workers.table.leases")}</th>
                  <th>{t(lang, "workers.table.utilization")}</th>
                  <th>{t(lang, "workers.table.lastSeen")}</th>
                </tr>
              </thead>
              <tbody>
                {filtered.map((worker) => {
                  const isSelected = activeWorker?.worker_id === worker.worker_id;
                  const utilPct = Math.max(0, Math.min(100, (worker.utilization ?? 0) * 100));
                  return (
                    <tr
                      key={worker.session_id ?? worker.worker_id}
                      className={isSelected ? "is-selected" : ""}
                      onClick={() => onSelectWorker(worker.worker_id)}
                    >
                      <td>
                        <WorkerStateBadge worker={worker} lang={lang} />
                      </td>
                      <td>
                        <div className="table__primary table__mono">
                          {truncateMiddle(worker.worker_id, 26)}
                        </div>
                        {worker.session_id ? (
                          <div className="table__secondary table__mono">
                            {truncateMiddle(worker.session_id, 22)}
                          </div>
                        ) : null}
                      </td>
                      <td>
                        <div className="cell-stack">
                          <span className="tag-inline">{worker.namespace ?? "default"}</span>
                          <span className="table__mono table__secondary">
                            {worker.pool ?? "default"}
                          </span>
                        </div>
                      </td>
                      <td className="table__mono">
                        {worker.interfaces.join(", ") || t(lang, "common.none")}
                      </td>
                      <td className="num">
                        <div>
                          {worker.active_leases} / {worker.max_concurrency}
                        </div>
                        <div className="table__secondary">{worker.free_slots} free</div>
                      </td>
                      <td>
                        <div className="util-cell">
                          <ProgressMeter percent={utilPct} />
                          <span className="num">{formatPercent(worker.utilization)}</span>
                        </div>
                      </td>
                      <td className="table__mono table__timestamp">
                        {formatTimestamp(worker.last_seen_at_ms)}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        )}
      </section>

      <aside className="panel panel--aside panel--detail">
        {!activeWorker ? (
          <>
            <div className="panel__header">
              <div className="panel__header-title">
                <h1>{t(lang, "workers.inspector.title")}</h1>
                <span>{t(lang, "workers.inspector.select")}</span>
              </div>
            </div>
            <EmptyState message={t(lang, "workers.inspector.empty")} />
          </>
        ) : (
          <>
            <div className="panel__header panel__header--stack">
              <div className="inspector__identity">
                <h1>{truncateMiddle(activeWorker.worker_id, 34)}</h1>
                <span>
                  {activeWorker.namespace ?? "default"} · {activeWorker.pool ?? "default"}
                </span>
              </div>
              <div className="inspector__actions">
                <WorkerStateBadge worker={activeWorker} lang={lang} />
                <button
                  type="button"
                  className="button button--primary"
                  onClick={() => onViewJobsForWorker(activeWorker.worker_id)}
                >
                  {t(lang, "workers.inspector.viewJobs")}
                </button>
              </div>
            </div>

            {!activeWorker.connected && activeWorker.active_leases > 0 ? (
              <p className="inline-notice" role="status">
                {t(lang, "workers.inspector.graceNote")}
              </p>
            ) : null}

            <InspectorGroup
              title={t(lang, "workers.inspector.session")}
              meta={activeWorker.connected ? "gRPC stream active" : "gRPC stream closed"}
            >
              <div className="meta-grid">
                <MetaItem
                  label={t(lang, "workers.inspector.sessionId")}
                  value={activeWorker.session_id ?? t(lang, "common.na")}
                  mono
                />
                <MetaItem
                  label={t(lang, "workers.inspector.transport")}
                  value={`${activeWorker.connected ? "connected" : "disconnected"}${
                    activeWorker.draining ? " · draining" : ""
                  }`}
                />
                <MetaItem
                  label={t(lang, "workers.inspector.connectedAt")}
                  value={formatTimestamp(activeWorker.connected_at_ms)}
                  mono
                />
                <MetaItem
                  label={t(lang, "workers.inspector.lastSeen")}
                  value={formatTimestamp(activeWorker.last_seen_at_ms)}
                  mono
                />
              </div>
            </InspectorGroup>

            <InspectorGroup
              title={t(lang, "workers.inspector.load")}
              meta={`${activeWorker.active_leases} / ${activeWorker.max_concurrency} leases`}
            >
              <div className="meta-grid">
                <MetaItem
                  label={t(lang, "workers.inspector.queued")}
                  value={formatNumber(activeWorker.load?.queued ?? 0)}
                />
                <MetaItem
                  label={t(lang, "workers.inspector.running")}
                  value={formatNumber(activeWorker.load?.running ?? activeWorker.active_leases)}
                />
                <MetaItem
                  label={t(lang, "workers.inspector.utilization")}
                  value={formatPercent(activeWorker.load?.utilization ?? activeWorker.utilization)}
                />
                <MetaItem
                  label={t(lang, "interfaces.table.freeSlots")}
                  value={formatNumber(activeWorker.free_slots)}
                />
              </div>

              {activeWorker.load && Object.keys(activeWorker.load.custom).length > 0 ? (
                <InspectorSection title={t(lang, "workers.inspector.customLoad")}>
                  <KeyValueGrid
                    entries={activeWorker.load.custom}
                    emptyMessage={t(lang, "common.none")}
                  />
                </InspectorSection>
              ) : null}
            </InspectorGroup>

            <InspectorGroup
              title={t(lang, "workers.inspector.capacity")}
              meta={formatCapacityMap(activeWorker.total_capacity)}
            >
              <InspectorSection title={t(lang, "workers.inspector.totalCapacity")}>
                <KeyValueGrid
                  entries={activeWorker.total_capacity}
                  emptyMessage={t(lang, "common.none")}
                />
              </InspectorSection>

              <InspectorSection title={t(lang, "workers.inspector.interfaces")}>
                {activeWorker.interfaces.length === 0 ? (
                  <EmptyState message={t(lang, "common.none")} compact />
                ) : (
                  <div className="chip-list">
                    {activeWorker.interfaces.map((iface) => (
                      <button
                        key={iface}
                        type="button"
                        className="chip-btn"
                        onClick={() => onOpenInterface(iface)}
                      >
                        <span className="table__mono">{iface}</span>
                      </button>
                    ))}
                  </div>
                )}
              </InspectorSection>

              <InspectorSection title={t(lang, "workers.inspector.attributes")}>
                <KeyValueGrid
                  entries={activeWorker.attributes}
                  emptyMessage={t(lang, "common.none")}
                />
              </InspectorSection>
            </InspectorGroup>
          </>
        )}
      </aside>
    </div>
  );
}
