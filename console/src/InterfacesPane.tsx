import { useMemo, useState } from "react";

import { formatDuration, formatNumber } from "./format";
import { InspectorGroup, InspectorSection, InspectorShell, MetaGrid } from "./pane-inspector";
import { EmptyState, InterfacesTable, PanelHeader, SearchInput, TierChain, WorkerMiniList } from "./pane-tables";
import type { InterfaceSummary, PoolSummary, WorkerSummary } from "./types";

interface InterfacesPaneProps {
  interfaces: InterfaceSummary[];
  pools: PoolSummary[];
  workers: WorkerSummary[];
  selectedInterface: string | null;
  onSelectInterface: (name: string) => void;
  onViewJobs: (interfaceName: string) => void;
  onOpenPool: (poolKey: string) => void;
  onOpenWorker: (workerId: string) => void;
}

export function InterfacesPane(props: InterfacesPaneProps) {
  const { interfaces, pools, workers, selectedInterface, onSelectInterface, onViewJobs, onOpenPool, onOpenWorker } = props;
  const [search, setSearch] = useState("");

  const filtered = useMemo(() => {
    const q = search.trim().toLowerCase();
    if (!q) return interfaces;
    return interfaces.filter(
      (item) => item.name.toLowerCase().includes(q) || (item.policy?.pool_tiers.some((t) => t.pool.toLowerCase().includes(q)) ?? false),
    );
  }, [interfaces, search]);

  const activeItem = filtered.find((item) => item.name === selectedInterface) ?? filtered[0] ?? null;
  const matchingWorkers = useMemo(() => (activeItem ? workers.filter((w) => w.interfaces.includes(activeItem.name)) : []), [activeItem, workers]);
  const matchingPools = useMemo(() => (activeItem ? pools.filter((p) => p.interface === activeItem.name) : []), [activeItem, pools]);

  return (
    <div className="content-grid">
      <section className="panel">
        <PanelHeader title="Interfaces" meta={formatNumber(filtered.length)} />
        <div className="panel__toolbar">
          <SearchInput value={search} placeholder="Search interface or pool…" onChange={setSearch} />
        </div>
        <InterfacesTable interfaces={filtered} selectedName={activeItem?.name ?? null} onSelect={onSelectInterface} />
      </section>

      <InspectorShell
        isEmpty={!activeItem}
        emptyTitle="Interface Inspector"
        emptyHint="Select an interface"
        emptyMessage="Select an interface to inspect its routing policy, spillover chain, and sessions."
        title={activeItem?.name ?? ""}
        subtitle={activeItem ? `${formatNumber(activeItem.sessions)} Sessions · ${formatNumber(activeItem.free_slots)} Free slots` : null}
        actions={
          activeItem ? (
            <button type="button" className="button button--primary" onClick={() => onViewJobs(activeItem.name)}>
              View Jobs
            </button>
          ) : null
        }
      >
        {activeItem ? (
          <>
            <MetaGrid
              items={[
                { label: "Queued", value: formatNumber(activeItem.queued) },
                { label: "Running", value: formatNumber(activeItem.running) },
                { label: "Oldest wait", value: formatDuration(activeItem.oldest_queued_age_ms) },
                { label: "Free slots", value: formatNumber(activeItem.free_slots) },
              ]}
            />

            <InspectorGroup title="Routing Policy">
              {activeItem.policy ? (
                <>
                  <MetaGrid
                    items={[
                      { label: "Max queue", value: activeItem.policy.max_queue != null ? formatNumber(activeItem.policy.max_queue) : "Default" },
                      {
                        label: "Queue timeout",
                        value: activeItem.policy.queue_timeout_ms != null ? formatDuration(activeItem.policy.queue_timeout_ms) : "Default",
                      },
                      {
                        label: "Max in-flight / key",
                        value: activeItem.policy.max_in_flight_per_key != null ? formatNumber(activeItem.policy.max_in_flight_per_key) : "None",
                      },
                    ]}
                  />
                  <InspectorSection title="Pool Tiers Spillover Chain">
                    {activeItem.policy.pool_tiers.length === 0 ? (
                      <EmptyState message="No pool tier spillover configured; dispatches to any matching worker." inline />
                    ) : (
                      <TierChain tiers={activeItem.policy.pool_tiers} />
                    )}
                  </InspectorSection>
                </>
              ) : (
                <EmptyState message="Using runtime defaults (no explicit interface policy configured)." inline />
              )}
            </InspectorGroup>

            <InspectorSection title="Active Pools">
              {matchingPools.length === 0 ? (
                <EmptyState message="No active pools registered for this interface." inline />
              ) : (
                <div className="chip-list">
                  {matchingPools.map((pool) => {
                    const key = `${pool.namespace}::${pool.interface}::${pool.pool}`;
                    return (
                      <button key={key} type="button" className="chip-btn" onClick={() => onOpenPool(key)}>
                        <span className="mono">{pool.pool}</span>
                        <span className="chip-btn__meta">{pool.namespace} · {pool.sessions}s / {pool.free_slots} free</span>
                      </button>
                    );
                  })}
                </div>
              )}
            </InspectorSection>

            <InspectorSection title="Connected Workers">
              <WorkerMiniList workers={matchingWorkers} emptyMessage="No workers currently serving this interface." onSelect={onOpenWorker} />
            </InspectorSection>
          </>
        ) : null}
      </InspectorShell>
    </div>
  );
}
