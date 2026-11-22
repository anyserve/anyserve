import { memo, useEffect, useMemo, useState, type ReactNode } from "react";

import { QUEUE_STATES } from "./app-constants";
import {
  formatBytes,
  formatCapacityMap,
  formatDuration,
  formatNumber,
  formatObjectMeta,
  formatPayloadPreview,
  formatRustDuration,
  formatTimestamp,
  latestAttemptWorker,
} from "./format";
import { CopyButton, EmptyState, PanelHeader, StateBadge } from "./pane-tables";
import type { JobDetailResponse, ObjectRefSummary } from "./types";

const LARGE_PAYLOAD_BYTES = 1024;
const LARGE_PREVIEW_CHARS = 800;

export interface MetaField {
  label: string;
  value: string;
  mono?: boolean;
  tag?: string;
  title?: string;
  onClick?: () => void;
}

export function MetaGrid({ items }: { items: MetaField[] }) {
  return (
    <div className="meta-grid">
      {items.map((item) => (
        <div key={item.label} className="meta-item">
          <span>{item.label}</span>
          <strong className={item.mono ? "meta-item__value mono" : "meta-item__value"}>
            {item.onClick ? (
              <button type="button" className="entity-link" title={item.title} onClick={item.onClick}>
                {item.value}
              </button>
            ) : (
              item.value
            )}
            {item.tag ? <> <em className="tag">{item.tag}</em></> : null}
          </strong>
        </div>
      ))}
    </div>
  );
}

export function InspectorShell(props: {
  title: string;
  subtitle?: ReactNode;
  emptyTitle?: string;
  emptyHint?: string;
  emptyMessage?: string;
  isEmpty?: boolean;
  actions?: ReactNode;
  children?: ReactNode;
}) {
  if (props.isEmpty) {
    return (
      <aside className="panel panel--aside">
        <PanelHeader title={props.emptyTitle ?? props.title} meta={props.emptyHint} />
        <EmptyState message={props.emptyMessage ?? "Select an item to inspect."} />
      </aside>
    );
  }
  return (
    <aside className="panel panel--aside">
      <div className="panel__header panel__header--stack">
        <div className="inspector__identity">
          <h1>{props.title}</h1>
          {props.subtitle ? <div className="inspector__subtitle">{props.subtitle}</div> : null}
        </div>
        {props.actions ? <div className="inspector__actions">{props.actions}</div> : null}
      </div>
      {props.children}
    </aside>
  );
}

export function InspectorSection({ title, children }: { title: string; children: ReactNode }) {
  return <section className="inspector__section"><div className="inspector__label">{title}</div>{children}</section>;
}

export function InspectorGroup({ title, meta, children }: { title: string; meta?: string; children: ReactNode }) {
  return (
    <section className="inspector__group">
      <div className="inspector__group-header"><h2>{title}</h2>{meta ? <span>{meta}</span> : null}</div>
      {children}
    </section>
  );
}

export function KeyValueGrid({ entries, emptyMessage }: { entries: Record<string, string | number>; emptyMessage: string }) {
  const items = Object.entries(entries);
  if (items.length === 0) return <EmptyState message={emptyMessage} inline />;
  return (
    <div className="kv-list">
      {items.map(([k, v]) => (
        <div key={k} className="kv-item"><span className="kv-item__key">{k}</span><code className="kv-item__val">{String(v)}</code></div>
      ))}
    </div>
  );
}

export interface SubtitleItem {
  text: string;
  title?: string;
  onClick?: () => void;
}

function SubtitleNode({ text, title, onClick }: SubtitleItem) {
  return onClick ? (
    <button type="button" className="entity-link mono" title={title} onClick={onClick}>{text}</button>
  ) : (
    <span className="mono">{text}</span>
  );
}

export function SubtitlePair({ left, right }: { left: SubtitleItem; right: SubtitleItem }) {
  return (
    <span className="inspector__subtitle-row">
      <SubtitleNode {...left} />
      <span>·</span>
      <SubtitleNode {...right} />
    </span>
  );
}

function PayloadPreview({
  raw,
  contentType,
  sizeBytes,
  truncated,
  resetKey,
}: {
  raw: string;
  contentType?: string | null;
  sizeBytes?: number | null;
  truncated?: boolean;
  resetKey: string;
}) {
  const effectiveBytes = sizeBytes ?? raw.length;
  const isLarge = Boolean(truncated) || effectiveBytes > LARGE_PAYLOAD_BYTES || raw.length > LARGE_PREVIEW_CHARS;
  const [expanded, setExpanded] = useState(!isLarge);

  useEffect(() => {
    setExpanded(!isLarge);
  }, [resetKey, isLarge]);

  const formatted = useMemo(() => (expanded ? formatPayloadPreview(raw, contentType) : null), [expanded, raw, contentType]);

  return (
    <div className="payload-preview">
      <div className="payload-bar">
        {!expanded ? (
          <span className="muted">
            Large payload ({formatBytes(effectiveBytes)}
            {truncated ? " · truncated preview" : ""}) hidden by default
          </span>
        ) : (
          <span className="muted">{isLarge ? `Showing ${formatBytes(raw.length)} preview` : "Inline preview"}</span>
        )}
        <div className="payload-bar__actions">
          {isLarge ? (
            <button type="button" className="button button--sm" onClick={() => setExpanded((v) => !v)}>
              {expanded ? "Hide preview" : "Show preview"}
            </button>
          ) : null}
          <CopyButton text={() => formatted?.text ?? raw} label={truncated ? "Copy preview" : "Copy"} resetKey={resetKey} />
        </div>
      </div>

      {expanded && formatted ? (
        <>
          {formatted.streamText ? (
            <div>
              <div className="stream-preview__head">Streamed text · {formatted.frameCount} SSE frames</div>
              <pre className="code-block">{formatted.streamText}</pre>
            </div>
          ) : null}
          <pre className="code-block">{formatted.text}</pre>
        </>
      ) : null}
    </div>
  );
}

function ObjectRefList({ objects, resetKey }: { objects: ObjectRefSummary[]; resetKey: string }) {
  return (
    <div className="timeline">
      {objects.map((obj, idx) => (
        <div key={`${obj.kind}-${obj.uri ?? idx}`} className="timeline__item">
          <div className="timeline__title">{obj.kind}{obj.content_type ? ` · ${obj.content_type}` : ""}</div>
          <div className="timeline__subtitle">{obj.uri ?? formatObjectMeta(obj)}</div>
          {obj.preview ? (
            <PayloadPreview
              raw={obj.preview}
              contentType={obj.content_type}
              sizeBytes={obj.size_bytes}
              truncated={obj.truncated}
              resetKey={`${resetKey}:obj:${idx}`}
            />
          ) : null}
          {!obj.preview && obj.uri == null ? <div className="timeline__meta">Preview unavailable</div> : null}
        </div>
      ))}
    </div>
  );
}

interface TimelineItem {
  key: string;
  title: string;
  actor?: string | null;
  actorTitle?: string;
  onActorClick?: () => void;
  subtitle: string;
  meta: string;
  badge?: { label: string; tone: "warn" | "danger" } | null;
  detail?: string | null;
}

function Timeline({ items, emptyMessage }: { items: TimelineItem[]; emptyMessage: string }) {
  if (items.length === 0) return <EmptyState message={emptyMessage} inline />;
  return (
    <div className="timeline">
      {items.map((item) => (
        <div key={item.key} className="timeline__item">
          <div className="timeline__title">
            <span>{item.title}</span>
            {item.actor ? (
              <>
                <span className="muted">·</span>
                {item.onActorClick ? (
                  <button type="button" className="entity-link mono" title={item.actorTitle} onClick={item.onActorClick}>
                    {item.actor}
                  </button>
                ) : (
                  <span className="mono">{item.actor}</span>
                )}
              </>
            ) : null}
            {item.badge ? <span className={`tag tag--${item.badge.tone}`}>{item.badge.label}</span> : null}
          </div>
          <div className="timeline__subtitle">{item.subtitle}</div>
          <div className="timeline__meta">{item.meta}</div>
          {item.detail ? <div className="timeline__error">{item.detail}</div> : null}
        </div>
      ))}
    </div>
  );
}

export const JobInspector = memo(function JobInspector({
  detail,
  isCancelling = false,
  onCancel,
  onOpenWorker,
  onOpenInterface,
  onFilterNamespace,
}: {
  detail: JobDetailResponse | null;
  isCancelling?: boolean;
  onCancel?: (jobId: string, namespace?: string) => Promise<void>;
  onOpenWorker?: (workerId: string) => void;
  onOpenInterface?: (interfaceName: string) => void;
  onFilterNamespace?: (namespace: string) => void;
}) {
  if (!detail) {
    return <InspectorShell isEmpty emptyTitle="Inspector" emptyHint="Select a job" emptyMessage="Job details appear here." title="" />;
  }

  const { job } = detail;
  const { demand, policy } = job;
  const ns = job.namespace || "default";
  const workerId = detail.latest_worker?.worker_id ?? latestAttemptWorker(detail) ?? "Unassigned";
  const durabilityLabel = job.durability ?? policy?.durability?.toLowerCase() ?? "N/A";
  const demandValues = demand
    ? [
        demand.affinity_key && `Affinity key: ${demand.affinity_key}`,
        Object.keys(demand.required_capacity).length > 0 && `Required capacity: ${formatCapacityMap(demand.required_capacity)}`,
        Object.keys(demand.required_attributes).length > 0 && `Required attrs: ${formatCapacityMap(demand.required_attributes)}`,
        Object.keys(demand.preferred_attributes).length > 0 && `Preferred attrs: ${formatCapacityMap(demand.preferred_attributes)}`,
      ].filter((v): v is string => Boolean(v))
    : [];
  const policyValues = policy
    ? [
        `Priority: ${policy.priority}`,
        `Durability: ${durabilityLabel}`,
        `Max attempts: ${policy.max_attempts ?? "Default"}`,
        policy.fairness_key && `Fairness key: ${policy.fairness_key}`,
        policy.concurrency_key && `Concurrency key: ${policy.concurrency_key}`,
        policy.queue_timeout && `Queue timeout: ${formatRustDuration(policy.queue_timeout)}`,
        policy.execution_timeout && `Exec timeout: ${formatRustDuration(policy.execution_timeout)}`,
        policy.total_timeout && `Total timeout: ${formatRustDuration(policy.total_timeout)}`,
        policy.spillover_after && `Spillover after: ${formatRustDuration(policy.spillover_after)}`,
      ].filter((v): v is string => Boolean(v))
    : [];
  const hasParams = Boolean(job.params_preview) || (job.params_size_bytes != null && job.params_size_bytes > 0);
  const showOutputs =
    job.outputs.length > 0 &&
    !(job.output_preview != null && job.outputs.length === 1 && job.outputs[0].kind === "inline");
  const hasPayload = hasParams || job.inputs.length > 0 || showOutputs || Boolean(job.output_preview);

  const subtitle = (
    <SubtitlePair
      left={{
        text: job.interface_name,
        title: `Inspect interface ${job.interface_name}`,
        onClick: onOpenInterface ? () => onOpenInterface(job.interface_name) : undefined,
      }}
      right={{
        text: ns,
        title: `Filter by namespace ${ns}`,
        onClick: onFilterNamespace ? () => onFilterNamespace(ns) : undefined,
      }}
    />
  );

  return (
    <InspectorShell
      title={job.job_id}
      subtitle={subtitle}
      actions={
        <>
          <StateBadge state={job.state} />
          {job.durability ? <span className="tag">{job.durability}</span> : null}
          {QUEUE_STATES.includes(job.state) && onCancel ? (
            <button type="button" className="button button--danger" disabled={isCancelling} onClick={() => void onCancel(job.job_id, job.namespace)}>
              {isCancelling ? "Cancelling" : "Cancel job"}
            </button>
          ) : null}
        </>
      }
    >
      <MetaGrid
        items={[
          { label: "Source", value: job.source ?? "direct" },
          {
            label: "Worker",
            value: workerId,
            mono: true,
            title: workerId !== "Unassigned" ? `Open worker ${workerId}` : undefined,
            onClick: workerId !== "Unassigned" && onOpenWorker ? () => onOpenWorker(workerId) : undefined,
          },
          {
            label: "Namespace",
            value: ns,
            mono: true,
            title: `Filter or open pools for namespace ${ns}`,
            onClick: onFilterNamespace ? () => onFilterNamespace(ns) : undefined,
          },
          { label: "Durability", value: durabilityLabel },
          { label: "Updated", value: formatTimestamp(job.updated_at_ms), mono: true },
          { label: "Duration", value: formatDuration(job.duration_ms) },
        ]}
      />

      {job.last_error || job.error_code ? (
        <InspectorSection title="Error">
          <div className="preview-meta">
            {job.error_code ? <span className="tag tag--danger">{job.error_code}</span> : null}
            {job.error_retryable != null ? (
              <span className={`tag ${job.error_retryable ? "tag--warn" : "tag--danger"}`}>
                {job.error_retryable ? "Retryable" : "Non-retryable"}
              </span>
            ) : null}
          </div>
          {job.last_error ? <pre className="code-block">{job.last_error}</pre> : null}
        </InspectorSection>
      ) : null}

      <InspectorGroup title="Scheduling & Policy">
        <InspectorSection title="Demand">
          {demand ? (
            demandValues.length > 0 ? <div className="inline-list">{demandValues.map((v) => <span key={v}>{v}</span>)}</div> : <EmptyState message="Default demand (no placement constraints)" inline />
          ) : <EmptyState message="Request specification is no longer retained in memory." inline />}
        </InspectorSection>
        <InspectorSection title="Execution Policy">
          {policy ? <div className="inline-list">{policyValues.map((v) => <span key={v}>{v}</span>)}</div> : <EmptyState message="Request specification is no longer retained in memory." inline />}
        </InspectorSection>
      </InspectorGroup>

      <InspectorGroup title="Payload">
        {!hasPayload ? (
          <EmptyState message="No retained request or response payloads." inline />
        ) : (
          <>
            {hasParams ? (
              <InspectorSection title="Request Params">
                <div className="preview-meta"><span>{formatBytes(job.params_size_bytes)}</span><span>{job.params_truncated ? "truncated" : "full"}</span></div>
                {job.params_preview ? (
                  <PayloadPreview
                    raw={job.params_preview}
                    contentType="application/json"
                    sizeBytes={job.params_size_bytes}
                    truncated={job.params_truncated}
                    resetKey={`${job.job_id}:params`}
                  />
                ) : <EmptyState message="Binary or non-text request payload." inline />}
              </InspectorSection>
            ) : null}
            {job.inputs.length > 0 ? <InspectorSection title="Inputs"><ObjectRefList objects={job.inputs} resetKey={`${job.job_id}:in`} /></InspectorSection> : null}
            {showOutputs ? <InspectorSection title="Outputs"><ObjectRefList objects={job.outputs} resetKey={`${job.job_id}:out`} /></InspectorSection> : null}
            {job.output_preview ? (
              <InspectorSection title="Response">
                <div className="preview-meta">
                  <span>{formatBytes(job.output_preview.size_bytes)}</span>
                  <span>{job.output_preview.content_type ?? "text/plain"}</span>
                  <span>{job.output_preview.truncated ? "truncated" : "full"}</span>
                </div>
                {job.output_preview.preview ? (
                  <PayloadPreview
                    raw={job.output_preview.preview}
                    contentType={job.output_preview.content_type}
                    sizeBytes={job.output_preview.size_bytes}
                    truncated={job.output_preview.truncated}
                    resetKey={`${job.job_id}:resp`}
                  />
                ) : <EmptyState message="Binary or unavailable response preview." inline />}
              </InspectorSection>
            ) : null}
          </>
        )}
      </InspectorGroup>

      <InspectorGroup title="Execution">
        <InspectorSection title="Attempts">
          {detail.history_incomplete ? <p className="inline-notice" role="status">Incomplete history: earlier execution events are no longer retained.</p> : null}
          <Timeline
            emptyMessage="No attempts"
            items={detail.attempts.map((a, idx) => {
              const isLastFailed = a.state === "failed" && idx === detail.attempts.length - 1;
              const badge = a.state === "restarted"
                ? { label: "retryable · retried", tone: "warn" as const }
                : isLastFailed && job.error_retryable != null
                  ? { label: job.error_retryable ? "retryable" : "non-retryable", tone: (job.error_retryable ? "warn" : "danger") as "warn" | "danger" }
                  : null;
              return {
                key: a.attempt_id,
                title: `#${idx + 1} ${a.state}`,
                actor: a.worker_id ?? "Unassigned",
                actorTitle: a.worker_id ? `Open worker ${a.worker_id}` : undefined,
                onActorClick: a.worker_id && onOpenWorker ? () => onOpenWorker(a.worker_id!) : undefined,
                subtitle: formatTimestamp(a.created_at_ms),
                meta: a.duration_ms != null ? formatDuration(a.duration_ms) : "in progress",
                badge,
                detail: a.last_error,
              };
            })}
          />
        </InspectorSection>
        <InspectorSection title="Events">
          <Timeline
            emptyMessage="No events"
            items={detail.events.map((e) => ({
              key: String(e.sequence),
              title: `#${e.sequence} · ${e.kind}`,
              subtitle: formatTimestamp(e.created_at_ms),
              meta: `${formatNumber(e.payload_size_bytes)} B`,
            }))}
          />
        </InspectorSection>
      </InspectorGroup>
    </InspectorShell>
  );
});
