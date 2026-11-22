import { useEffect, useMemo, useRef, useState } from "react";

import { formatDuration, formatNumber, formatPool, poolKey, toSelectOptions } from "./format";
import { InspectorSection, InspectorShell, MetaGrid, SubtitlePair } from "./pane-inspector";
import { EmptyState, PanelHeader, SearchInput, SelectField, StatsRow, TierChain, WorkerMiniList } from "./pane-tables";
import type { InterfaceSummary, PoolSummary, WorkerSummary } from "./types";

export interface PoolFilterSeed {
  ns?: string;
  iface?: string;
  seq: number;
}

interface PoolsPaneProps {
  pools: PoolSummary[];
  interfaces: InterfaceSummary[];
  workers: WorkerSummary[];
  selectedPoolKey: string | null;
  poolFilterSeed?: PoolFilterSeed | null;
  onSelectPool: (poolKey: string) => void;
  onViewJobs: (interfaceName: string, namespace: string) => void;
  onOpenInterface: (interfaceName: string) => void;
  onOpenWorker: (workerId: string) => void;
}

export { poolKey };

export function PoolsPane(props: PoolsPaneProps) {
  const { pools, interfaces, workers, selectedPoolKey, poolFilterSeed, onSelectPool, onViewJobs, onOpenInterface, onOpenWorker } = props;
  const [namespaceFilter, setNamespaceFilter] = useState("all");
  const [interfaceFilter, setInterfaceFilter] = useState("all");
  const [search, setSearch] = useState("");
  const prevSelectedKey = useRef<string | null>(selectedPoolKey);

  useEffect(() => {
    if (!poolFilterSeed) return;
    setNamespaceFilter(poolFilterSeed.ns ?? "all");
    setInterfaceFilter(poolFilterSeed.iface ?? "all");
    setSearch("");
  }, [poolFilterSeed]);

  const namespaceOptions = useMemo(() => toSelectOptions(pools.map((p) => p.namespace), "All namespaces"), [pools]);
  const interfaceOptions = useMemo(() => toSelectOptions(pools.map((p) => p.interface), "All interfaces"), [pools]);

  const filtered = useMemo(() => {
    const q = search.trim().toLowerCase();
    return pools.filter((item) => {
      if (namespaceFilter !== "all" && item.namespace !== namespaceFilter) return false;
      if (interfaceFilter !== "all" && item.interface !== interfaceFilter) return false;
      return !q || [formatPool(item.pool), item.interface, item.namespace].some((s) => s.toLowerCase().includes(q));
    });
  }, [pools, namespaceFilter, interfaceFilter, search]);

  useEffect(() => {
    if (selectedPoolKey === prevSelectedKey.current) return;
    prevSelectedKey.current = selectedPoolKey;
    if (selectedPoolKey && pools.some((p) => poolKey(p) === selectedPoolKey) && !filtered.some((p) => poolKey(p) === selectedPoolKey)) {
      setNamespaceFilter("all");
      setInterfaceFilter("all");
      setSearch("");
    }
  }, [selectedPoolKey, pools, filtered]);

  const activePool = filtered.find((item) => poolKey(item) === selectedPoolKey) ?? filtered[0] ?? null;
  const activePolicy = useMemo(() => (activePool ? interfaces.find((i) => i.name === activePool.interface)?.policy ?? null : null), [activePool, interfaces]);
  const matchingWorkers = useMemo(() => {
    if (!activePool) return [];
    return workers.filter(
      (w) => formatPool(w.pool) === formatPool(activePool.pool) && (w.namespace ?? "default") === activePool.namespace && w.interfaces.includes(activePool.interface),
    );
  }, [activePool, workers]);
  const configuredSpillovers = useMemo(() => interfaces.filter((item) => (item.policy?.pool_tiers.length ?? 0) > 0), [interfaces]);

  const totals = useMemo(
    () =>
      filtered.reduce(
        (acc, item) => ({
          sessions: acc.sessions + item.sessions,
          activeLeases: acc.activeLeases + item.active_leases,
          freeSlots: acc.freeSlots + item.free_slots,
          desiredSlots: acc.desiredSlots + item.desired_additional_slots,
        }),
        { sessions: 0, activeLeases: 0, freeSlots: 0, desiredSlots: 0 },
      ),
    [filtered],
  );

  function jumpToTierPool(ifaceName: string, tierPool: string) {
    const match = pools.find((p) => p.interface === ifaceName && formatPool(p.pool) === formatPool(tierPool));
    if (match) {
      onSelectPool(poolKey(match));
    } else {
      setNamespaceFilter("all");
      setInterfaceFilter(ifaceName);
      setSearch("");
    }
  }

  return (
    <div className="content-grid">
      <section className="panel">
        <StatsRow
          stats={[
            { label: "Pools", value: filtered.length },
            { label: "Sessions", value: totals.sessions },
            { label: "Active leases", value: totals.activeLeases },
            { label: "Free slots", value: totals.freeSlots },
            { label: "Desired slots", value: totals.desiredSlots },
          ]}
        />
        <PanelHeader title="Pools" meta={formatNumber(filtered.length)} />

        <div className="panel__toolbar panel__toolbar--wrap">
          <div className="toolbar-filters">
            <SelectField label="Namespace" value={namespaceFilter} options={namespaceOptions} onChange={setNamespaceFilter} />
            <SelectField label="Interface" value={interfaceFilter} options={interfaceOptions} onChange={setInterfaceFilter} />
            <SearchInput value={search} placeholder="Search pool, interface, namespace…" onChange={setSearch} />
          </div>
        </div>

        <div className="pools-stack">
          {filtered.length === 0 ? (
            <EmptyState message="No active pools match the current filters" />
          ) : (
            <div className="table-wrap">
              <table className="table table--fixed table--interactive">
                <thead>
                  <tr>
                    <th style={{ width: "18%" }}>Pool</th>
                    <th style={{ width: "14%" }}>Namespace</th>
                    <th style={{ width: "21%" }}>Interface</th>
                    <th style={{ width: "12%" }}>Sessions</th>
                    <th style={{ width: "11%" }}>Leases</th>
                    <th style={{ width: "10%" }}>Free</th>
                    <th style={{ width: "14%" }}>Desired</th>
                  </tr>
                </thead>
                <tbody>
                  {filtered.map((item) => {
                    const key = poolKey(item);
                    const poolName = formatPool(item.pool);
                    return (
                      <tr key={key} className={activePool && poolKey(activePool) === key ? "is-selected" : ""} onClick={() => onSelectPool(key)}>
                        <td className="table__primary mono ellipsis" title={poolName}>{poolName}</td>
                        <td>
                          <button
                            type="button"
                            className="tag tag--link ellipsis"
                            title={`Filter pools by ${item.namespace}`}
                            onClick={(e) => {
                              e.stopPropagation();
                              setNamespaceFilter(item.namespace);
                            }}
                          >
                            {item.namespace}
                          </button>
                        </td>
                        <td>
                          <button
                            type="button"
                            className="cell-link mono ellipsis"
                            title={`Open interface ${item.interface}`}
                            onClick={(e) => {
                              e.stopPropagation();
                              onOpenInterface(item.interface);
                            }}
                          >
                            {item.interface}
                          </button>
                        </td>
                        <td className="num">{formatNumber(item.sessions)}</td>
                        <td className="num">{formatNumber(item.active_leases)}</td>
                        <td className="num">{formatNumber(item.free_slots)}</td>
                        <td className="num">
                          {item.desired_additional_slots > 0 ? <span className="tag tag--warn">+{formatNumber(item.desired_additional_slots)}</span> : "0"}
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          )}

          {configuredSpillovers.length > 0 ? (
            <section className="sub-section">
              <div className="sub-section__header"><h2>Configured Spillover Tier Policies</h2></div>
              <div className="spillover-rules">
                {configuredSpillovers.map((iface) => (
                  <div key={iface.name} className="spillover-rule-row">
                    <button type="button" className="entity-link mono spillover-rule-row__iface" onClick={() => onOpenInterface(iface.name)}>
                      {iface.name}
                    </button>
                    <div className="tier-chain-inline">
                      {iface.policy!.pool_tiers.map((tier, idx) => (
                        <button
                          key={`${tier.pool}-${idx}`}
                          type="button"
                          className="tag tag--link"
                          onClick={() => jumpToTierPool(iface.name, tier.pool)}
                        >
                          #{idx + 1} {formatPool(tier.pool)}
                          {tier.spillover_after_ms > 0 ? ` · +${formatDuration(tier.spillover_after_ms)}` : ""}
                          {tier.reconnect_grace_ms != null ? ` · grace ${formatDuration(tier.reconnect_grace_ms)}` : ""}
                        </button>
                      ))}
                    </div>
                  </div>
                ))}
              </div>
            </section>
          ) : null}
        </div>
      </section>

      <InspectorShell
        isEmpty={!activePool}
        emptyTitle="Pool Inspector"
        emptyHint="Select a pool"
        emptyMessage="Select a pool entry to inspect autoscaling demand, spillover tier order, and workers."
        title={activePool ? formatPool(activePool.pool) : ""}
        subtitle={
          activePool ? (
            <SubtitlePair
              left={{ text: activePool.namespace, onClick: () => setNamespaceFilter(activePool.namespace) }}
              right={{ text: activePool.interface, onClick: () => onOpenInterface(activePool.interface) }}
            />
          ) : null
        }
        actions={
          activePool ? (
            <button type="button" className="button button--primary" onClick={() => onViewJobs(activePool.interface, activePool.namespace)}>
              View Jobs
            </button>
          ) : null
        }
      >
        {activePool ? (
          <>
            <MetaGrid
              items={[
                { label: "Sessions", value: formatNumber(activePool.sessions) },
                { label: "Active leases", value: formatNumber(activePool.active_leases) },
                { label: "Free slots", value: formatNumber(activePool.free_slots) },
                { label: "Desired additional", value: formatNumber(activePool.desired_additional_slots) },
              ]}
            />
            <InspectorSection title="Spillover Tier Position">
              {activePolicy && activePolicy.pool_tiers.length > 0 ? (
                <TierChain
                  tiers={activePolicy.pool_tiers}
                  highlightPool={activePool.pool}
                  onSelectPool={(poolName) => jumpToTierPool(activePool.interface, poolName)}
                />
              ) : (
                <EmptyState message="No explicit pool_tiers spillover configured for this interface." inline />
              )}
            </InspectorSection>
            <InspectorSection title="Workers in Pool">
              <WorkerMiniList workers={matchingWorkers} emptyMessage="No workers matched for this pool." showLeases onSelect={onOpenWorker} />
            </InspectorSection>
          </>
        ) : null}
      </InspectorShell>
    </div>
  );
}
