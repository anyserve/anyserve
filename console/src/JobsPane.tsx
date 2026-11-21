import type { ReactNode } from "react";

import type { JobScope, PaneStat } from "./app-constants";
import { JobsTable, PanelHeader, SearchInput, SelectField, StatsRow } from "./pane-tables";
import type { JobSummary } from "./types";

interface FilterOption { value: string; label: string; }

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
}

const SCOPE_LABELS: Record<JobScope, string> = { all: "All", queue: "Queue", history: "History" };
const EMPTY_MESSAGES: Record<JobScope, string> = { all: "No jobs match the current filters", queue: "No active jobs", history: "No historical jobs" };

export function JobsPane(props: JobsPaneProps) {
  const isFiltered = props.interfaceFilter !== "all" || props.namespaceFilter !== "all" || props.workerFilter !== "all" || props.searchQuery.trim().length > 0;
  const meta = isFiltered ? `${props.jobs.length} of ${props.totalLoaded} loaded` : `${props.jobs.length} shown`;

  return (
    <div className="content-grid">
      <section className="panel">
        <StatsRow stats={props.stats} />
        <PanelHeader title="Jobs" meta={meta} />

        <div className="panel__toolbar panel__toolbar--wrap">
          <div className="seg-control" role="group" aria-label="Job state filter">
            {(["all", "queue", "history"] as const).map((item) => (
              <button key={item} type="button" className={`seg-control__btn${props.scope === item ? " is-active" : ""}`} aria-pressed={props.scope === item} onClick={() => props.onScopeChange(item)}>
                {SCOPE_LABELS[item]}
              </button>
            ))}
          </div>

          <div className="toolbar-filters">
            <SelectField label="Interface" value={props.interfaceFilter} options={props.interfaceOptions} onChange={props.onInterfaceFilterChange} />
            <SelectField label="Namespace" value={props.namespaceFilter} options={props.namespaceOptions} onChange={props.onNamespaceFilterChange} />
            <SelectField label="Worker" value={props.workerFilter} options={props.workerOptions} onChange={props.onWorkerFilterChange} />
            <SearchInput value={props.searchQuery} placeholder="Search job ID, source, error…" onChange={props.onSearchQueryChange} />
          </div>
        </div>

        <JobsTable jobs={props.jobs} selectedJobId={props.selectedJobId} emptyMessage={EMPTY_MESSAGES[props.scope]} onSelect={props.onSelect} />

        {props.hasMore ? (
          <div className="panel__footer">
            <button type="button" className="button" disabled={props.loadingMore} onClick={props.onLoadMore}>
              {props.loadingMore ? "Loading…" : "Load more"}
            </button>
          </div>
        ) : null}
      </section>

      {props.inspector}
    </div>
  );
}
