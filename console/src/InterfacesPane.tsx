import { useMemo, useState } from "react";

import { formatDuration, formatNumber, truncateMiddle } from "./format";
import { t, type Lang } from "./i18n";
import { IconSearch } from "./icons";
import { InspectorGroup, InspectorSection, MetaItem } from "./pane-inspector-details";
import { EmptyState, InterfacesTable, PanelHeader, WorkerStateBadge } from "./pane-tables";
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
  lang: Lang;
}

export function InterfacesPane(props: InterfacesPaneProps) {
  const {
    interfaces,
    pools,
    workers,
    selectedInterface,
    onSelectInterface,
    onViewJobs,
    onOpenPool,
    onOpenWorker,
    lang,
  } = props;
  const [search, setSearch] = useState("");

  const filtered = useMemo(() => {
    const q = search.trim().toLowerCase();
    if (!q) return interfaces;
    return interfaces.filter((item) => {
      if (item.name.toLowerCase().includes(q)) return true;
      return item.policy?.pool_tiers.some((tier) => tier.pool.toLowerCase().includes(q)) ?? false;
    });
  }, [interfaces, search]);

  const activeItem =
    filtered.find((item) => item.name === selectedInterface) ?? filtered[0] ?? null;

  const matchingWorkers = useMemo(() => {
    if (!activeItem) return [];
    return workers.filter((worker) => worker.interfaces.includes(activeItem.name));
  }, [activeItem, workers]);

  const matchingPools = useMemo(() => {
    if (!activeItem) return [];
    return pools.filter((pool) => pool.interface === activeItem.name);
  }, [activeItem, pools]);

  return (
    <div className="content-grid">
      <section className="panel">
        <PanelHeader
          title={t(lang, "interfaces.title")}
          meta={`${formatNumber(filtered.length)} · ${t(lang, "interfaces.subtitle")}`}
        />

        <div className="panel__toolbar">
          <div className="search-box">
            <IconSearch className="search-box__icon" />
            <input
              type="search"
              className="search-box__input"
              value={search}
              onChange={(event) => setSearch(event.target.value)}
              placeholder={t(lang, "interfaces.searchPlaceholder")}
              aria-label={t(lang, "interfaces.searchPlaceholder")}
            />
          </div>
        </div>

        <InterfacesTable
          interfaces={filtered}
          selectedName={activeItem?.name ?? null}
          onSelect={onSelectInterface}
          lang={lang}
        />
      </section>

      <aside className="panel panel--aside panel--detail">
        {!activeItem ? (
          <>
            <div className="panel__header">
              <div className="panel__header-title">
                <h1>{t(lang, "interfaces.inspector.title")}</h1>
                <span>{t(lang, "interfaces.inspector.select")}</span>
              </div>
            </div>
            <EmptyState message={t(lang, "interfaces.inspector.empty")} />
          </>
        ) : (
          <>
            <div className="panel__header panel__header--stack">
              <div className="inspector__identity">
                <h1>{activeItem.name}</h1>
                <span>
                  {formatNumber(activeItem.sessions)} {t(lang, "interfaces.table.sessions")} ·{" "}
                  {formatNumber(activeItem.free_slots)} {t(lang, "interfaces.table.freeSlots")}
                </span>
              </div>
              <div className="inspector__actions">
                <button
                  type="button"
                  className="button button--primary"
                  onClick={() => onViewJobs(activeItem.name)}
                >
                  {t(lang, "common.viewJobs")}
                </button>
              </div>
            </div>

            <div className="meta-grid">
              <MetaItem label={t(lang, "interfaces.table.queued")} value={formatNumber(activeItem.queued)} />
              <MetaItem label={t(lang, "interfaces.table.running")} value={formatNumber(activeItem.running)} />
              <MetaItem
                label={t(lang, "interfaces.table.oldestWait")}
                value={formatDuration(activeItem.oldest_queued_age_ms)}
              />
              <MetaItem
                label={t(lang, "interfaces.table.freeSlots")}
                value={formatNumber(activeItem.free_slots)}
              />
            </div>

            <InspectorGroup
              title={t(lang, "interfaces.policy.title")}
              meta={t(lang, "interfaces.subtitle")}
            >
              {activeItem.policy ? (
                <>
                  <div className="meta-grid">
                    <MetaItem
                      label={t(lang, "interfaces.policy.maxQueue")}
                      value={
                        activeItem.policy.max_queue != null
                          ? formatNumber(activeItem.policy.max_queue)
                          : t(lang, "common.default")
                      }
                    />
                    <MetaItem
                      label={t(lang, "interfaces.policy.queueTimeout")}
                      value={
                        activeItem.policy.queue_timeout_ms != null
                          ? formatDuration(activeItem.policy.queue_timeout_ms)
                          : t(lang, "common.default")
                      }
                    />
                    <MetaItem
                      label={t(lang, "interfaces.policy.maxInFlight")}
                      value={
                        activeItem.policy.max_in_flight_per_key != null
                          ? formatNumber(activeItem.policy.max_in_flight_per_key)
                          : t(lang, "common.none")
                      }
                    />
                  </div>

                  <InspectorSection title={t(lang, "interfaces.tiers.title")}>
                    {activeItem.policy.pool_tiers.length === 0 ? (
                      <EmptyState message={t(lang, "interfaces.tiers.empty")} compact />
                    ) : (
                      <div className="tier-chain">
                        {activeItem.policy.pool_tiers.map((tier, idx) => (
                          <div key={`${tier.pool}-${idx}`} className="tier-node">
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
                        ))}
                      </div>
                    )}
                  </InspectorSection>
                </>
              ) : (
                <EmptyState message={t(lang, "interfaces.policy.none")} compact />
              )}
            </InspectorGroup>

            <InspectorSection title={t(lang, "interfaces.pools.title")}>
              {matchingPools.length === 0 ? (
                <EmptyState message={t(lang, "interfaces.pools.empty")} compact />
              ) : (
                <div className="chip-list">
                  {matchingPools.map((pool) => {
                    const key = `${pool.namespace}::${pool.interface}::${pool.pool}`;
                    return (
                      <button
                        key={key}
                        type="button"
                        className="chip-btn"
                        onClick={() => onOpenPool(key)}
                      >
                        <span className="table__mono">{pool.pool}</span>
                        <span className="chip-btn__meta">
                          {pool.namespace} · {pool.sessions}s / {pool.free_slots} free
                        </span>
                      </button>
                    );
                  })}
                </div>
              )}
            </InspectorSection>

            <InspectorSection title={t(lang, "interfaces.workers.title")}>
              {matchingWorkers.length === 0 ? (
                <EmptyState message={t(lang, "interfaces.workers.empty")} compact />
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
                        {worker.pool ? <span className="tag-inline">{worker.pool}</span> : null}
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
