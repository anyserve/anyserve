import type { ReactNode } from "react";

import type { JobScope, PaneStat } from "./app-constants";
import { t, type Lang } from "./i18n";
import { IconSearch } from "./icons";
import { JobsTable, PanelHeader, StatsRow } from "./pane-tables";
import type { JobSummary } from "./types";

interface FilterOption {
  value: string;
  label: string;
}

interface JobsPaneProps {
  scope: JobScope;
  onScopeChange: (scope: JobScope) => void;
  stats: PaneStat[];
  jobs: JobSummary[];
  totalLoaded: number;
  selectedJobId: string | null;
  onSelect: (jobId: string) => void;
  interfaceFilter: string;
  interfaceOptions: FilterOption[];
  onInterfaceFilterChange: (value: string) => void;
  namespaceFilter: string;
  namespaceOptions: FilterOption[];
  onNamespaceFilterChange: (value: string) => void;
  workerFilter: string;
  workerOptions: FilterOption[];
  onWorkerFilterChange: (value: string) => void;
  searchQuery: string;
  onSearchQueryChange: (value: string) => void;
  hasMore: boolean;
  loadingMore: boolean;
  onLoadMore: () => void;
  inspector: ReactNode;
  lang: Lang;
}

export function JobsPane(props: JobsPaneProps) {
  const {
    scope,
    onScopeChange,
    stats,
    jobs,
    totalLoaded,
    selectedJobId,
    onSelect,
    interfaceFilter,
    interfaceOptions,
    onInterfaceFilterChange,
    namespaceFilter,
    namespaceOptions,
    onNamespaceFilterChange,
    workerFilter,
    workerOptions,
    onWorkerFilterChange,
    searchQuery,
    onSearchQueryChange,
    hasMore,
    loadingMore,
    onLoadMore,
    inspector,
    lang,
  } = props;

  const isFiltered =
    interfaceFilter !== "all" ||
    namespaceFilter !== "all" ||
    workerFilter !== "all" ||
    searchQuery.trim().length > 0;

  const meta = isFiltered
    ? t(lang, "jobs.count.filtered", { shown: jobs.length, total: totalLoaded })
    : t(lang, "jobs.count.showing", { shown: jobs.length });

  const emptyMessage =
    scope === "queue"
      ? t(lang, "jobs.empty.queue")
      : scope === "history"
        ? t(lang, "jobs.empty.history")
        : t(lang, "jobs.empty.all");

  return (
    <div className="content-grid">
      <section className="panel">
        <StatsRow stats={stats} />
        <PanelHeader title={t(lang, "jobs.title")} meta={meta} />

        <div className="panel__toolbar panel__toolbar--wrap">
          <div className="seg-control" role="group" aria-label="Job state filter">
            {(["all", "queue", "history"] as const).map((item) => (
              <button
                key={item}
                type="button"
                className={`seg-control__btn${scope === item ? " is-active" : ""}`}
                aria-pressed={scope === item}
                onClick={() => onScopeChange(item)}
              >
                {t(lang, `jobs.scope.${item}`)}
              </button>
            ))}
          </div>

          <div className="toolbar-filters">
            <label className="field">
              <span className="field__label">{t(lang, "jobs.filter.interface")}</span>
              <select
                className="field__control"
                value={interfaceFilter}
                onChange={(event) => onInterfaceFilterChange(event.target.value)}
              >
                {interfaceOptions.map((opt) => (
                  <option key={opt.value} value={opt.value}>
                    {opt.label}
                  </option>
                ))}
              </select>
            </label>

            <label className="field">
              <span className="field__label">{t(lang, "jobs.filter.namespace")}</span>
              <select
                className="field__control"
                value={namespaceFilter}
                onChange={(event) => onNamespaceFilterChange(event.target.value)}
              >
                {namespaceOptions.map((opt) => (
                  <option key={opt.value} value={opt.value}>
                    {opt.label}
                  </option>
                ))}
              </select>
            </label>

            <label className="field">
              <span className="field__label">{t(lang, "jobs.filter.worker")}</span>
              <select
                className="field__control"
                value={workerFilter}
                onChange={(event) => onWorkerFilterChange(event.target.value)}
              >
                {workerOptions.map((opt) => (
                  <option key={opt.value} value={opt.value}>
                    {opt.label}
                  </option>
                ))}
              </select>
            </label>

            <div className="search-box">
              <IconSearch className="search-box__icon" />
              <input
                type="search"
                className="search-box__input"
                value={searchQuery}
                onChange={(event) => onSearchQueryChange(event.target.value)}
                placeholder={t(lang, "jobs.filter.searchPlaceholder")}
                aria-label={t(lang, "jobs.filter.searchPlaceholder")}
              />
            </div>
          </div>
        </div>

        <JobsTable
          jobs={jobs}
          selectedJobId={selectedJobId}
          emptyMessage={emptyMessage}
          onSelect={onSelect}
          lang={lang}
        />

        {hasMore ? (
          <div className="panel__footer">
            <button
              type="button"
              className="button"
              disabled={loadingMore}
              onClick={onLoadMore}
            >
              {loadingMore ? t(lang, "jobs.loadingMore") : t(lang, "jobs.loadMore")}
            </button>
          </div>
        ) : null}
      </section>

      {inspector}
    </div>
  );
}
