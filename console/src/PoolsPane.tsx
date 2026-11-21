import { useMemo, useState } from "react";

import { formatDuration, formatNumber, truncateMiddle } from "./format";
import { t, type Lang } from "./i18n";
import { IconSearch } from "./icons";
import { InspectorSection, MetaItem } from "./pane-inspector-details";
import { EmptyState, PanelHeader, StatsRow, WorkerStateBadge } from "./pane-tables";
import type { InterfaceSummary, PoolSummary, WorkerSummary } from "./types";

interface PoolsPaneProps {
  pools: PoolSummary[];
  interfaces: InterfaceSummary[];
  workers: WorkerSummary[];
  selectedPoolKey: string | null;
  onSelectPool: (poolKey: string) => void;
  onViewJobs: (interfaceName: string, namespace: string) => void;
  onOpenWorker: (workerId: string) => void;
  lang: Lang;
}

export function poolKey(pool: PoolSummary): string {
  return `${pool.namespace}::${pool.interface}::${pool.pool}`;
}

export function PoolsPane(props: PoolsPaneProps) {
  const {
    pools,
    interfaces,
    workers,
    selectedPoolKey,
    onSelectPool,
    onViewJobs,
    onOpenWorker,
    lang,
  } = props;

  const [namespaceFilter, setNamespaceFilter] = useState("all");
  const [interfaceFilter, setInterfaceFilter] = useState("all");
  const [search, setSearch] = useState("");

  const namespaceOptions = useMemo(() => {
    const set = new Set<string>();
    for (const p of pools) set.add(p.namespace);
    return Array.from(set).sort();
  }, [pools]);

  const interfaceOptions = useMemo(() => {
    const set = new Set<string>();
    for (const p of pools) set.add(p.interface);
    return Array.from(set).sort();
  }, [pools]);

  const filtered = useMemo(() => {
    const q = search.trim().toLowerCase();
    return pools.filter((item) => {
      if (namespaceFilter !== "all" && item.namespace !== namespaceFilter) return false;
      if (interfaceFilter !== "all" && item.interface !== interfaceFilter) return false;
      if (
        q &&
        !item.pool.toLowerCase().includes(q) &&
        !item.interface.toLowerCase().includes(q) &&
        !item.namespace.toLowerCase().includes(q)
      ) {
        return false;
      }
      return true;
    });
  }, [pools, namespaceFilter, interfaceFilter, search]);

  const activePool =
    filtered.find((item) => poolKey(item) === selectedPoolKey) ?? filtered[0] ?? null;

  const activeInterfacePolicy = useMemo(() => {
    if (!activePool) return null;
    return interfaces.find((item) => item.name === activePool.interface)?.policy ?? null;
  }, [activePool, interfaces]);

  const matchingWorkers = useMemo(() => {
    if (!activePool) return [];
    return workers.filter((worker) => {
      const workerPool = worker.pool ?? "default";
      const workerNs = worker.namespace ?? "default";
      return (
        workerPool === activePool.pool &&
        workerNs === activePool.namespace &&
        worker.interfaces.includes(activePool.interface)
      );
    });
  }, [activePool, workers]);

  const configuredSpillovers = useMemo(
    () => interfaces.filter((item) => (item.policy?.pool_tiers.length ?? 0) > 0),
    [interfaces],
  );

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
            { label: t(lang, "pools.stat.pools"), value: filtered.length },
            { label: t(lang, "pools.stat.sessions"), value: totals.sessions },
            { label: t(lang, "pools.stat.activeLeases"), value: totals.activeLeases },
            { label: t(lang, "pools.stat.freeSlots"), value: totals.freeSlots },
            { label: t(lang, "pools.stat.desiredSlots"), value: totals.desiredSlots },
          ]}
        />

        <PanelHeader title={t(lang, "pools.title")} meta={t(lang, "pools.subtitle")} />

        <div className="panel__toolbar panel__toolbar--wrap">
          <div className="toolbar-filters">
            <label className="field">
              <span className="field__label">{t(lang, "pools.filter.namespace")}</span>
              <select
                className="field__control"
                value={namespaceFilter}
                onChange={(event) => setNamespaceFilter(event.target.value)}
              >
                <option value="all">{t(lang, "jobs.filter.allNamespaces")}</option>
                {namespaceOptions.map((ns) => (
                  <option key={ns} value={ns}>
                    {ns}
                  </option>
                ))}
              </select>
            </label>

            <label className="field">
              <span className="field__label">{t(lang, "pools.filter.interface")}</span>
              <select
                className="field__control"
                value={interfaceFilter}
                onChange={(event) => setInterfaceFilter(event.target.value)}
              >
                <option value="all">{t(lang, "jobs.filter.allInterfaces")}</option>
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
                placeholder={t(lang, "pools.filter.searchPlaceholder")}
                aria-label={t(lang, "pools.filter.searchPlaceholder")}
              />
            </div>
          </div>
        </div>

        <div className="pools-stack">
          {filtered.length === 0 ? (
            <EmptyState message={t(lang, "pools.empty")} />
          ) : (
            <div className="table-wrap">
              <table className="table table--interactive">
                <thead>
                  <tr>
                    <th>{t(lang, "pools.table.pool")}</th>
                    <th>{t(lang, "pools.table.namespace")}</th>
                    <th>{t(lang, "pools.table.interface")}</th>
                    <th>{t(lang, "pools.table.sessions")}</th>
                    <th>{t(lang, "pools.table.leases")}</th>
                    <th>{t(lang, "pools.table.freeSlots")}</th>
                    <th>{t(lang, "pools.table.desired")}</th>
                  </tr>
                </thead>
                <tbody>
                  {filtered.map((item) => {
                    const key = poolKey(item);
                    const isSelected = activePool ? poolKey(activePool) === key : false;
                    return (
                      <tr
                        key={key}
                        className={isSelected ? "is-selected" : ""}
                        onClick={() => onSelectPool(key)}
                      >
                        <td className="table__primary table__mono">{item.pool}</td>
                        <td>
                          <span className="tag-inline">{item.namespace}</span>
                        </td>
                        <td className="table__mono">{item.interface}</td>
                        <td className="num">{formatNumber(item.sessions)}</td>
                        <td className="num">{formatNumber(item.active_leases)}</td>
                        <td className="num">{formatNumber(item.free_slots)}</td>
                        <td className="num">
                          {item.desired_additional_slots > 0 ? (
                            <span className="pill-sub pill-sub--warn">
                              +{formatNumber(item.desired_additional_slots)}
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

          {configuredSpillovers.length > 0 ? (
            <section className="sub-section">
              <div className="sub-section__header">
                <h2>{t(lang, "pools.tiersConfig.title")}</h2>
                <span>{t(lang, "pools.tiersConfig.subtitle")}</span>
              </div>
              <div className="spillover-rules">
                {configuredSpillovers.map((iface) => (
                  <div key={iface.name} className="spillover-rule-row">
                    <span className="table__mono spillover-rule-row__iface">{iface.name}</span>
                    <div className="tier-chain-inline">
                      {iface.policy!.pool_tiers.map((tier, idx) => (
                        <span key={`${tier.pool}-${idx}`} className="tag-inline">
                          #{idx + 1} {tier.pool}
                          {tier.spillover_after_ms > 0
                            ? ` · +${formatDuration(tier.spillover_after_ms)}`
                            : ""}
                          {tier.reconnect_grace_ms != null
                            ? ` · grace ${formatDuration(tier.reconnect_grace_ms)}`
                            : ""}
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

      <aside className="panel panel--aside panel--detail">
        {!activePool ? (
          <>
            <div className="panel__header">
              <div className="panel__header-title">
                <h1>{t(lang, "pools.inspector.title")}</h1>
                <span>{t(lang, "pools.inspector.select")}</span>
              </div>
            </div>
            <EmptyState message={t(lang, "pools.inspector.empty")} />
          </>
        ) : (
          <>
            <div className="panel__header panel__header--stack">
              <div className="inspector__identity">
                <h1>{activePool.pool}</h1>
                <span>
                  {activePool.namespace} · {activePool.interface}
                </span>
              </div>
              <div className="inspector__actions">
                <button
                  type="button"
                  className="button button--primary"
                  onClick={() => onViewJobs(activePool.interface, activePool.namespace)}
                >
                  {t(lang, "common.viewJobs")}
                </button>
              </div>
            </div>

            <div className="meta-grid">
              <MetaItem label={t(lang, "pools.table.sessions")} value={formatNumber(activePool.sessions)} />
              <MetaItem
                label={t(lang, "pools.table.leases")}
                value={formatNumber(activePool.active_leases)}
              />
              <MetaItem
                label={t(lang, "pools.table.freeSlots")}
                value={formatNumber(activePool.free_slots)}
              />
              <MetaItem
                label={t(lang, "pools.table.desired")}
                value={formatNumber(activePool.desired_additional_slots)}
              />
            </div>

            <InspectorSection title={t(lang, "pools.inspector.spillover")}>
              {activeInterfacePolicy && activeInterfacePolicy.pool_tiers.length > 0 ? (
                <div className="tier-chain">
                  {activeInterfacePolicy.pool_tiers.map((tier, idx) => {
                    const isCurrent = tier.pool === activePool.pool;
                    return (
                      <div
                        key={`${tier.pool}-${idx}`}
                        className={`tier-node${isCurrent ? " is-current" : ""}`}
                      >
                        <span className="tier-node__rank">#{idx + 1}</span>
                        <div className="tier-node__body">
                          <strong className="table__mono">{tier.pool}</strong>
                          <span>
                            {tier.spillover_after_ms === 0
                              ? t(lang, "interfaces.tiers.primary")
                              : t(lang, "interfaces.tiers.spillover", {
                                  ms: formatDuration(tier.spillover_after_ms),
                                })}
                            {tier.reconnect_grace_ms != null
                              ? ` · ${t(lang, "interfaces.tiers.grace", {
                                  ms: formatDuration(tier.reconnect_grace_ms),
                                })}`
                              : ""}
                          </span>
                        </div>
                      </div>
                    );
                  })}
                </div>
              ) : (
                <EmptyState message={t(lang, "pools.inspector.noPolicy")} compact />
              )}
            </InspectorSection>

            <InspectorSection title={t(lang, "pools.inspector.workers")}>
              {matchingWorkers.length === 0 ? (
                <EmptyState message={t(lang, "pools.inspector.noWorkers")} compact />
              ) : (
                <div className="worker-mini-list">
                  {matchingWorkers.map((worker) => (
                    <button
                      key={worker.worker_id}
                      type="button"
                      className="worker-mini-row"
                      onClick={() => onOpenWorker(worker.worker_id)}
                    >
                      <span className="table__mono">{truncateMiddle(worker.worker_id, 24)}</span>
                      <span className="worker-mini-row__meta">
                        <span className="num">
                          {worker.active_leases}/{worker.max_concurrency}
                        </span>
                        <WorkerStateBadge worker={worker} lang={lang} />
                      </span>
                    </button>
                  ))}
                </div>
              )}
            </InspectorSection>
          </>
        )}
      </aside>
    </div>
  );
}
