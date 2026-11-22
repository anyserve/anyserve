import { useEffect, useMemo, useRef, useState } from "react";

import { formatCapacityMap, formatNumber, formatPercent, formatPool, formatShortTime, formatTimestamp } from "./format";
import { IconPlus } from "./icons";
import { InspectorGroup, InspectorSection, InspectorShell, KeyValueGrid, MetaGrid } from "./pane-inspector";
import { EmptyState, PanelHeader, ProgressMeter, SearchInput, SelectField, StatsRow, WorkerStateBadge } from "./pane-tables";
import type { OverviewResponse, WorkerSummary, WorkersResponse } from "./types";

export type HealthFilter = "all" | "healthy" | "draining" | "disconnected";

export interface WorkerFilterSeed {
  health: HealthFilter;
  seq: number;
}

interface WorkersPaneProps {
  overview: OverviewResponse | null;
  workersResponse: WorkersResponse | null;
  workers: WorkerSummary[];
  selectedWorkerId: string | null;
  workerFilterSeed?: WorkerFilterSeed | null;
  onSelectWorker: (workerId: string) => void;
  onViewJobsForWorker: (workerId: string) => void;
  onViewJobsForNamespace: (namespace: string) => void;
  onOpenInterface: (interfaceName: string) => void;
  onOpenPool: (namespace: string, interfaceName: string | undefined, pool: string) => void;
  onOpenConnect: () => void;
}

const HEALTH_LABELS: Record<HealthFilter, string> = { all: "All", healthy: "Healthy", draining: "Draining", disconnected: "Disconnected" };

export function WorkersPane(props: WorkersPaneProps) {
  const {
    overview,
    workersResponse,
    workers,
    selectedWorkerId,
    workerFilterSeed,
    onSelectWorker,
    onViewJobsForWorker,
    onViewJobsForNamespace,
    onOpenInterface,
    onOpenPool,
    onOpenConnect,
  } = props;
  const [healthFilter, setHealthFilter] = useState<HealthFilter>("all");
  const [poolFilter, setPoolFilter] = useState("all");
  const [interfaceFilter, setInterfaceFilter] = useState("all");
  const [search, setSearch] = useState("");
  const prevSelectedWorker = useRef<string | null>(selectedWorkerId);

  useEffect(() => {
    if (!workerFilterSeed) return;
    setHealthFilter(workerFilterSeed.health);
    setPoolFilter("all");
    setInterfaceFilter("all");
    setSearch("");
  }, [workerFilterSeed]);

  const poolOptions = useMemo(() => ["all", ...Array.from(new Set(workers.map((w) => formatPool(w.pool)))).sort()], [workers]);
  const interfaceOptions = useMemo(() => ["all", ...Array.from(new Set(workers.flatMap((w) => w.interfaces))).sort()], [workers]);

  const filtered = useMemo(() => {
    const q = search.trim().toLowerCase();
    return workers.filter((w) => {
      if (healthFilter === "healthy" && !w.healthy) return false;
      if (healthFilter === "draining" && !(w.connected && w.draining)) return false;
      if (healthFilter === "disconnected" && w.connected) return false;
      if (poolFilter !== "all" && formatPool(w.pool) !== poolFilter) return false;
      if (interfaceFilter !== "all" && !w.interfaces.includes(interfaceFilter)) return false;
      return !q || [w.worker_id, w.session_id, formatPool(w.pool), w.namespace].some((s) => (s ?? "").toLowerCase().includes(q));
    });
  }, [workers, healthFilter, poolFilter, interfaceFilter, search]);

  useEffect(() => {
    if (selectedWorkerId === prevSelectedWorker.current) return;
    prevSelectedWorker.current = selectedWorkerId;
    if (selectedWorkerId && workers.some((w) => w.worker_id === selectedWorkerId) && !filtered.some((w) => w.worker_id === selectedWorkerId)) {
      setHealthFilter("all");
      setPoolFilter("all");
      setInterfaceFilter("all");
      setSearch("");
    }
  }, [selectedWorkerId, workers, filtered]);

  const activeWorker = filtered.find((w) => w.worker_id === selectedWorkerId) ?? filtered[0] ?? null;

  return (
    <div className="content-grid">
      <section className="panel">
        <StatsRow
          stats={[
            { label: "Total", value: workersResponse?.total ?? overview?.workers.total ?? workers.length },
            { label: "Healthy", value: workersResponse?.healthy ?? overview?.workers.healthy ?? workers.filter((w) => w.healthy).length },
            { label: "Draining", value: workersResponse?.draining ?? workers.filter((w) => w.connected && w.draining).length },
            { label: "Disconnected", value: workersResponse?.disconnected ?? workers.filter((w) => !w.connected).length },
            { label: "Active leases", value: overview?.workers.active_leases ?? 0 },
          ]}
        />
        <PanelHeader
          title="Workers"
          meta={overview && Object.keys(overview.workers.total_capacity).length > 0 ? `${formatNumber(filtered.length)} · ${formatCapacityMap(overview.workers.total_capacity)}` : formatNumber(filtered.length)}
          actions={
            <button type="button" className="button button--primary" onClick={onOpenConnect}>
              <IconPlus />
              <span>Connect Worker</span>
            </button>
          }
        />

        <div className="panel__toolbar panel__toolbar--wrap">
          <div className="seg-control" role="group" aria-label="Worker health filter">
            {(["all", "healthy", "draining", "disconnected"] as const).map((item) => (
              <button key={item} type="button" className={`seg-control__btn${healthFilter === item ? " is-active" : ""}`} aria-pressed={healthFilter === item} onClick={() => setHealthFilter(item)}>
                {HEALTH_LABELS[item]}
              </button>
            ))}
          </div>
          <div className="toolbar-filters">
            <SelectField label="Pool" value={poolFilter} options={poolOptions.map((p) => ({ value: p, label: p === "all" ? "All pools" : p }))} onChange={setPoolFilter} />
            <SelectField label="Interface" value={interfaceFilter} options={interfaceOptions.map((i) => ({ value: i, label: i === "all" ? "All interfaces" : i }))} onChange={setInterfaceFilter} />
            <SearchInput value={search} placeholder="Search worker, session, pool…" onChange={setSearch} />
          </div>
        </div>

        {filtered.length === 0 ? (
          <div className="empty empty--stack">
            <p>{workers.length === 0 ? "No workers currently connected." : "No workers match the current filters"}</p>
            {workers.length === 0 ? (
              <button type="button" className="button button--primary" onClick={onOpenConnect}>
                <IconPlus />
                <span>Connect Worker</span>
              </button>
            ) : null}
          </div>
        ) : (
          <div className="table-wrap">
            <table className="table table--fixed table--interactive">
              <thead>
                <tr>
                  <th style={{ width: "14%" }}>Status</th>
                  <th style={{ width: "23%" }}>Worker</th>
                  <th style={{ width: "15%" }}>Pool</th>
                  <th style={{ width: "21%" }}>Interfaces</th>
                  <th style={{ width: "16%" }}>Occupancy</th>
                  <th style={{ width: "11%" }}>Last seen</th>
                </tr>
              </thead>
              <tbody>
                {filtered.map((worker) => {
                  const slotRatio = worker.max_concurrency > 0 ? worker.active_leases / worker.max_concurrency : 0;
                  const ifaces = worker.interfaces.join(", ") || "None";
                  const poolName = formatPool(worker.pool);
                  const ns = worker.namespace ?? "default";
                  return (
                    <tr key={worker.session_id ?? worker.worker_id} className={activeWorker?.worker_id === worker.worker_id ? "is-selected" : ""} onClick={() => onSelectWorker(worker.worker_id)}>
                      <td><WorkerStateBadge worker={worker} /></td>
                      <td>
                        <div className="table__primary mono ellipsis" title={worker.worker_id}>{worker.worker_id}</div>
                        {worker.session_id ? <div className="table__secondary mono ellipsis" title={worker.session_id}>{worker.session_id}</div> : null}
                      </td>
                      <td>
                        <button
                          type="button"
                          className="cell-link table__primary mono ellipsis"
                          title={`Open pool ${poolName}`}
                          onClick={(e) => {
                            e.stopPropagation();
                            onOpenPool(ns, worker.interfaces[0], worker.pool ?? "");
                          }}
                        >
                          {poolName}
                        </button>
                        {ns !== "default" ? <div className="table__secondary ellipsis">{ns}</div> : null}
                      </td>
                      <td>
                        {worker.interfaces.length === 1 ? (
                          <button
                            type="button"
                            className="cell-link mono ellipsis"
                            title={`Open interface ${worker.interfaces[0]}`}
                            onClick={(e) => {
                              e.stopPropagation();
                              onOpenInterface(worker.interfaces[0]);
                            }}
                          >
                            {worker.interfaces[0]}
                          </button>
                        ) : (
                          <span className="mono ellipsis" title={ifaces}>{ifaces}</span>
                        )}
                      </td>
                      <td>
                        <div className="occupancy-cell" title={`${worker.active_leases} active / ${worker.max_concurrency} max (${worker.free_slots} free)`}>
                          <div className="occupancy-cell__row num">
                            <span>{worker.active_leases} / {worker.max_concurrency}</span>
                            <span className="muted">{formatPercent(slotRatio)}</span>
                          </div>
                          <ProgressMeter percent={slotRatio * 100} />
                        </div>
                      </td>
                      <td className="mono table__timestamp" title={formatTimestamp(worker.last_seen_at_ms)}>{formatShortTime(worker.last_seen_at_ms)}</td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        )}
      </section>

      <InspectorShell
        isEmpty={!activeWorker}
        emptyTitle="Worker & Session Inspector"
        emptyHint="Select a worker"
        emptyMessage="Select a worker to inspect its outbound gRPC session, load, attributes, and capacity."
        title={activeWorker?.worker_id ?? ""}
        subtitle={
          activeWorker ? (
            <span className="inspector__subtitle-row">
              <button
                type="button"
                className="entity-link mono"
                title={`Filter jobs by namespace ${activeWorker.namespace ?? "default"}`}
                onClick={() => onViewJobsForNamespace(activeWorker.namespace ?? "default")}
              >
                {activeWorker.namespace ?? "default"}
              </button>
              <span>·</span>
              <button
                type="button"
                className="entity-link mono"
                title={`Open pool ${formatPool(activeWorker.pool)}`}
                onClick={() => onOpenPool(activeWorker.namespace ?? "default", activeWorker.interfaces[0], activeWorker.pool ?? "")}
              >
                {formatPool(activeWorker.pool)}
              </button>
            </span>
          ) : null
        }
        actions={
          activeWorker ? (
            <>
              <WorkerStateBadge worker={activeWorker} />
              <button type="button" className="button button--primary" onClick={() => onViewJobsForWorker(activeWorker.worker_id)}>
                View Worker Jobs
              </button>
            </>
          ) : null
        }
      >
        {activeWorker ? (
          <>
            {!activeWorker.connected && activeWorker.active_leases > 0 ? (
              <p className="inline-notice" role="status">Session disconnected; active leases survive until reconnect grace expires.</p>
            ) : null}

            <InspectorGroup title="Outbound Session" meta={activeWorker.connected ? "gRPC stream active" : "gRPC stream closed"}>
              <MetaGrid
                items={[
                  { label: "Session ID", value: activeWorker.session_id ?? "N/A", mono: true },
                  { label: "Transport state", value: `${activeWorker.connected ? "connected" : "disconnected"}${activeWorker.draining ? " · draining" : ""}` },
                  { label: "Connected at", value: formatTimestamp(activeWorker.connected_at_ms), mono: true },
                  { label: "Last seen", value: formatTimestamp(activeWorker.last_seen_at_ms), mono: true },
                ]}
              />
            </InspectorGroup>

            <InspectorGroup title="Reported Load" meta={`${activeWorker.active_leases} / ${activeWorker.max_concurrency} leases`}>
              <MetaGrid
                items={[
                  { label: "Worker queued", value: formatNumber(activeWorker.load?.queued ?? 0) },
                  { label: "Worker running", value: formatNumber(activeWorker.load?.running ?? activeWorker.active_leases) },
                  { label: "Utilization", value: formatPercent(activeWorker.load?.utilization ?? activeWorker.utilization) },
                  { label: "Free slots", value: formatNumber(activeWorker.free_slots) },
                ]}
              />
              {activeWorker.load && Object.keys(activeWorker.load.custom).length > 0 ? (
                <InspectorSection title="Custom load metrics">
                  <KeyValueGrid entries={activeWorker.load.custom} emptyMessage="None" />
                </InspectorSection>
              ) : null}
            </InspectorGroup>

            <InspectorGroup title="Capacity & Attributes" meta={formatCapacityMap(activeWorker.total_capacity)}>
              <InspectorSection title="Total capacity">
                <KeyValueGrid entries={activeWorker.total_capacity} emptyMessage="None" />
              </InspectorSection>
              <InspectorSection title="Served interfaces">
                {activeWorker.interfaces.length === 0 ? (
                  <EmptyState message="None" inline />
                ) : (
                  <div className="chip-list">
                    {activeWorker.interfaces.map((iface) => (
                      <button key={iface} type="button" className="chip-btn" onClick={() => onOpenInterface(iface)}>
                        <span className="mono">{iface}</span>
                      </button>
                    ))}
                  </div>
                )}
              </InspectorSection>
              <InspectorSection title="Worker attributes">
                <KeyValueGrid entries={activeWorker.attributes} emptyMessage="None" />
              </InspectorSection>
            </InspectorGroup>
          </>
        ) : null}
      </InspectorShell>
    </div>
  );
}
