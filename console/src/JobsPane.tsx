import { useEffect, useState, type ReactNode } from "react";

import {
  DEFAULT_TIME_FILTER, type JobScope, type JobStatusFilter, type PaneStat, type TimeFilterState,
} from "./app-constants";
import { formatNumber, fromDatetimeLocal, toDatetimeLocal } from "./format";
import { JobsHistogram, TIME_PRESETS, TIME_PRESET_MS } from "./JobsHistogram";
import { JobsTable, PanelHeader, SearchInput, SelectField, StatsRow } from "./pane-tables";
import type { JobsHistogram as JobsHistogramData, JobSummary } from "./types";

interface FilterOption { value: string; label: string; }

interface JobsPaneProps {
  scope: JobScope;
  onScopeChange: (scope: JobScope) => void;
  stats: PaneStat[];
  jobs: JobSummary[];
  totalLoaded: number;
  totalMatching: number | null;
  histogram: JobsHistogramData | null;
  histogramRefreshing?: boolean;
  timeFilter: TimeFilterState;
  onTimeFilterChange: (next: TimeFilterState) => void;
  selectedJobId: string | null;
  onSelect: (jobId: string) => void;
  onOpenInterface: (interfaceName: string) => void;
  onOpenWorker: (workerId: string) => void;
  onFilterNamespace: (namespace: string) => void;
  statusFilter: JobStatusFilter;
  onStatusFilterChange: (value: JobStatusFilter) => void;
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
  onResetAllFilters?: () => void;
  hasMore: boolean;
  loadingMore: boolean;
  onLoadMore: () => void;
  inspector: ReactNode;
}

const SCOPE_LABELS: Record<JobScope, string> = { all: "All", queue: "Queue", history: "History" };
const STATUS_OPTIONS: Array<{ value: JobStatusFilter; label: string }> = [
  { value: "all", label: "All statuses" },
  { value: "queued", label: "Queued" },
  { value: "running", label: "Running" },
  { value: "succeeded", label: "Succeeded" },
  { value: "failed", label: "Failed" },
  { value: "cancelled", label: "Cancelled" },
];
const EMPTY_MESSAGES: Record<JobScope, string> = { all: "No jobs match the current filters", queue: "No active jobs", history: "No historical jobs" };

export function JobsPane(props: JobsPaneProps) {
  const [customOpen, setCustomOpen] = useState(false);
  const [draftFrom, setDraftFrom] = useState("");
  const [draftTo, setDraftTo] = useState("");
  const [customError, setCustomError] = useState<string | null>(null);

  useEffect(() => {
    if (props.timeFilter.preset === "all") setCustomOpen(false);
  }, [props.timeFilter.preset]);

  const isFiltered =
    props.statusFilter !== "all" ||
    props.interfaceFilter !== "all" ||
    props.namespaceFilter !== "all" ||
    props.workerFilter !== "all" ||
    props.searchQuery.trim().length > 0 ||
    props.timeFilter.preset !== "all";

  const total = props.totalMatching;
  const meta =
    total != null
      ? props.jobs.length < total
        ? `${formatNumber(props.jobs.length)} of ${formatNumber(total)} ${isFiltered ? "matching" : "jobs"}`
        : `${formatNumber(total)} ${isFiltered ? "matching" : "jobs"}`
      : isFiltered
        ? `${formatNumber(props.jobs.length)} of ${formatNumber(props.totalLoaded)} loaded`
        : `${formatNumber(props.jobs.length)} shown`;

  const remaining = total != null ? Math.max(0, total - props.jobs.length) : null;

  function openCustomEditor() {
    if (customOpen) {
      setCustomOpen(false);
      return;
    }
    const now = Date.now();
    const initialFrom =
      props.timeFilter.fromMs ??
      props.histogram?.start_ms ??
      (props.timeFilter.preset !== "all" && props.timeFilter.preset !== "custom"
        ? now - TIME_PRESET_MS[props.timeFilter.preset]
        : now - 3_600_000);
    const initialTo = props.timeFilter.toMs ?? props.histogram?.end_ms ?? now;
    setDraftFrom(toDatetimeLocal(initialFrom));
    setDraftTo(toDatetimeLocal(initialTo));
    setCustomError(null);
    setCustomOpen(true);
  }

  function applyCustomRange() {
    const fromMs = fromDatetimeLocal(draftFrom);
    const toMs = fromDatetimeLocal(draftTo);
    if (fromMs == null || toMs == null || fromMs >= toMs) {
      setCustomError("Select a valid start time earlier than end time.");
      return;
    }
    setCustomError(null);
    setCustomOpen(false);
    props.onTimeFilterChange({ preset: "custom", fromMs, toMs });
  }

  return (
    <div className="content-grid">
      <section className="panel">
        <StatsRow stats={props.stats} onSelectState={props.onStatusFilterChange} />
        <PanelHeader
          title="Jobs"
          meta={meta}
          actions={
            isFiltered && props.onResetAllFilters ? (
              <button type="button" className="link-inline" onClick={props.onResetAllFilters}>
                Clear filters
              </button>
            ) : null
          }
        />

        <div className="panel__toolbar panel__toolbar--jobs">
          <div className="toolbar-row">
            <div className="seg-control" role="group" aria-label="Job state filter">
              {(["all", "queue", "history"] as const).map((item) => (
                <button
                  key={item}
                  type="button"
                  className={`seg-control__btn${props.scope === item ? " is-active" : ""}`}
                  aria-pressed={props.scope === item}
                  onClick={() => props.onScopeChange(item)}
                >
                  {SCOPE_LABELS[item]}
                </button>
              ))}
            </div>

            <div className="seg-control" role="group" aria-label="Time range">
              {TIME_PRESETS.map((preset) => {
                const isActive =
                  preset.value === "custom"
                    ? props.timeFilter.preset === "custom" || customOpen
                    : props.timeFilter.preset === preset.value;
                return (
                  <button
                    key={preset.value}
                    type="button"
                    className={`seg-control__btn${isActive ? " is-active" : ""}`}
                    aria-pressed={isActive}
                    onClick={() => {
                      if (preset.value === "custom") {
                        openCustomEditor();
                      } else {
                        setCustomOpen(false);
                        props.onTimeFilterChange({ preset: preset.value, fromMs: null, toMs: null });
                      }
                    }}
                  >
                    {preset.label}
                  </button>
                );
              })}
            </div>

            <SearchInput value={props.searchQuery} placeholder="Search job ID, source, error…" onChange={props.onSearchQueryChange} />
          </div>

          <div className="toolbar-filters">
            <SelectField
              label="Status"
              value={props.statusFilter}
              options={STATUS_OPTIONS}
              onChange={(value) => props.onStatusFilterChange(value as JobStatusFilter)}
            />
            <SelectField label="Interface" value={props.interfaceFilter} options={props.interfaceOptions} onChange={props.onInterfaceFilterChange} />
            <SelectField label="Namespace" value={props.namespaceFilter} options={props.namespaceOptions} onChange={props.onNamespaceFilterChange} />
            <SelectField label="Worker" value={props.workerFilter} options={props.workerOptions} onChange={props.onWorkerFilterChange} />
          </div>
        </div>

        {customOpen ? (
          <form
            className="time-custom-bar"
            onSubmit={(e) => {
              e.preventDefault();
              applyCustomRange();
            }}
          >
            <label className="field">
              <span className="field__label">From</span>
              <input
                type="datetime-local"
                step={1}
                className="field__control time-custom-bar__input"
                aria-label="From time"
                value={draftFrom}
                onChange={(e) => setDraftFrom(e.target.value)}
              />
            </label>
            <label className="field">
              <span className="field__label">To</span>
              <input
                type="datetime-local"
                step={1}
                className="field__control time-custom-bar__input"
                aria-label="To time"
                value={draftTo}
                onChange={(e) => setDraftTo(e.target.value)}
              />
            </label>
            <button type="submit" className="button button--primary button--sm">Apply</button>
            <button
              type="button"
              className="button button--sm"
              onClick={() => {
                setCustomOpen(false);
                props.onTimeFilterChange(DEFAULT_TIME_FILTER);
              }}
            >
              Reset
            </button>
            {customError ? <span className="time-custom-bar__error">{customError}</span> : null}
          </form>
        ) : null}

        <JobsHistogram
          histogram={props.histogram}
          timeFilter={props.timeFilter}
          refreshing={props.histogramRefreshing}
          onZoomRange={(fromMs, toMs) => {
            setCustomOpen(false);
            props.onTimeFilterChange({ preset: "custom", fromMs, toMs });
          }}
          onResetTime={() => {
            setCustomOpen(false);
            props.onTimeFilterChange(DEFAULT_TIME_FILTER);
          }}
        />

        <JobsTable
          jobs={props.jobs}
          selectedJobId={props.selectedJobId}
          emptyMessage={EMPTY_MESSAGES[props.scope]}
          onSelect={props.onSelect}
          onOpenInterface={props.onOpenInterface}
          onOpenWorker={props.onOpenWorker}
          onFilterNamespace={props.onFilterNamespace}
        />

        {props.hasMore ? (
          <div className="panel__footer">
            {total != null ? (
              <span className="panel__footer-meta muted num">
                Showing {formatNumber(props.jobs.length)} of {formatNumber(total)}
              </span>
            ) : null}
            <button type="button" className="button" disabled={props.loadingMore} onClick={props.onLoadMore}>
              {props.loadingMore
                ? "Loading…"
                : remaining != null && remaining > 0
                  ? `Load more (${formatNumber(remaining)} remaining)`
                  : "Load more"}
            </button>
          </div>
        ) : props.jobs.length > 0 ? (
          <div className="panel__footer panel__footer--end">
            <span className="panel__footer-meta muted num">
              End of {isFiltered ? "results" : "list"} · all {formatNumber(total ?? props.jobs.length)} {isFiltered ? "matching " : ""}jobs loaded
            </span>
          </div>
        ) : null}
      </section>

      {props.inspector}
    </div>
  );
}
