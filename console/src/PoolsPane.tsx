import { useMemo, useState } from "react";

import { formatDuration, formatNumber, formatPool } from "./format";
import { InspectorSection, InspectorShell, MetaGrid } from "./pane-inspector";
import { EmptyState, PanelHeader, SearchInput, SelectField, StatsRow, TierChain, WorkerMiniList } from "./pane-tables";
import type { InterfaceSummary, PoolSummary, WorkerSummary } from "./types";

interface PoolsPaneProps {
  pools: PoolSummary[];
  interfaces: InterfaceSummary[];
  workers: WorkerSummary[];
  selectedPoolKey: string | null;
  onSelectPool: (poolKey: string) => void;
  onViewJobs: (interfaceName: string, namespace: string) => void;
  onOpenWorker: (workerId: string) => void;
}

export function poolKey(pool: PoolSummary): string {
  return `${pool.namespace}::${pool.interface}::${pool.pool}`;
}

export function PoolsPane(props: PoolsPaneProps) {
  const { pools, interfaces, workers, selectedPoolKey, onSelectPool, onViewJobs, onOpenWorker } = props;
  const [namespaceFilter, setNamespaceFilter] = useState("all");
  const [interfaceFilter, setInterfaceFilter] = useState("all");
  const [search, setSearch] = useState("");

  const namespaceOptions = useMemo(() => ["all", ...Array.from(new Set(pools.map((p) => p.namespace))).sort()], [pools]);
  const interfaceOptions = useMemo(() => ["all", ...Array.from(new Set(pools.map((p) => p.interface))).sort()], [pools]);

  const filtered = useMemo(() => {
    const q = search.trim().toLowerCase();
    return pools.filter((item) => {
      if (namespaceFilter !== "all" && item.namespace !== namespaceFilter) return false;
      if (interfaceFilter !== "all" && item.interface !== interfaceFilter) return false;
      return !q || [formatPool(item.pool), item.interface, item.namespace].some((s) => s.toLowerCase().includes(q));
    });
  }, [pools, namespaceFilter, interfaceFilter, search]);

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
            <SelectField
              label="Namespace"
              value={namespaceFilter}
              options={namespaceOptions.map((ns) => ({ value: ns, label: ns === "all" ? "All namespaces" : ns }))}
              onChange={setNamespaceFilter}
            />
            <SelectField
              label="Interface"
              value={interfaceFilter}
              options={interfaceOptions.map((iface) => ({ value: iface, label: iface === "all" ? "All interfaces" : iface }))}
              onChange={setInterfaceFilter}
            />
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
                        <td><span className="tag ellipsis" title={item.namespace}>{item.namespace}</span></td>
                        <td className="mono ellipsis" title={item.interface}>{item.interface}</td>
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
                    <span className="mono spillover-rule-row__iface">{iface.name}</span>
                    <div className="tier-chain-inline">
                      {iface.policy!.pool_tiers.map((tier, idx) => (
                        <span key={`${tier.pool}-${idx}`} className="tag">
                          #{idx + 1} {tier.pool}
                          {tier.spillover_after_ms > 0 ? ` · +${formatDuration(tier.spillover_after_ms)}` : ""}
                          {tier.reconnect_grace_ms != null ? ` · grace ${formatDuration(tier.reconnect_grace_ms)}` : ""}
                        </span>
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
        subtitle={activePool ? `${activePool.namespace} · ${activePool.interface}` : null}
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
                <TierChain tiers={activePolicy.pool_tiers} highlightPool={activePool.pool} />
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
