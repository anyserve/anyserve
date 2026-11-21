import {
  formatCapacityMap,
  formatDuration,
  formatRustDuration,
  formatTimestamp,
  latestAttemptWorker,
  truncateMiddle,
} from "./format";
import { QUEUE_STATES } from "./app-constants";
import { t, type Lang } from "./i18n";
import { EmptyState, StateBadge } from "./pane-tables";
import {
  CodeBlock,
  InlineList,
  InspectorGroup,
  InspectorSection,
  MetaItem,
  ObjectRefList,
} from "./pane-inspector-details";
import { AttemptsTimeline, EventsTimeline } from "./pane-inspector-timeline";
import type { JobDetailResponse } from "./types";

export function JobInspector(props: {
  detail: JobDetailResponse | null;
  isCancelling?: boolean;
  onCancel?: (jobId: string, namespace?: string) => Promise<void>;
  lang?: Lang;
}) {
  const { detail, isCancelling = false, onCancel, lang = "en" } = props;

  if (!detail) {
    return (
      <aside className="panel panel--aside">
        <div className="panel__header">
          <div className="panel__header-title">
            <h1>{t(lang, "inspector.title")}</h1>
            <span>{t(lang, "inspector.selectJob")}</span>
          </div>
        </div>
        <EmptyState message={t(lang, "inspector.empty")} />
      </aside>
    );
  }

  const { job } = detail;
  const canCancel = QUEUE_STATES.includes(job.state);
  const demand = job.demand;
  const policy = job.policy;
  const durabilityLabel =
    job.durability ??
    (policy?.durability ? policy.durability.toLowerCase() : null) ??
    t(lang, "common.na");

  const demandValues: string[] = [];
  if (demand) {
    if (demand.affinity_key) {
      demandValues.push(`${t(lang, "inspector.affinityKey")}: ${demand.affinity_key}`);
    }
    if (Object.keys(demand.required_capacity).length > 0) {
      demandValues.push(
        `${t(lang, "inspector.requiredCapacity")}: ${formatCapacityMap(demand.required_capacity)}`,
      );
    }
    if (Object.keys(demand.required_attributes).length > 0) {
      demandValues.push(
        `${t(lang, "inspector.requiredAttrs")}: ${formatCapacityMap(demand.required_attributes)}`,
      );
    }
    if (Object.keys(demand.preferred_attributes).length > 0) {
      demandValues.push(
        `${t(lang, "inspector.preferredAttrs")}: ${formatCapacityMap(demand.preferred_attributes)}`,
      );
    }
    if (demandValues.length === 0) {
      demandValues.push(`${t(lang, "inspector.requiredCapacity")}: ${t(lang, "common.none")}`);
    }
  }

  const policyValues: string[] = [];
  if (policy) {
    policyValues.push(`${t(lang, "inspector.priority")}: ${policy.priority}`);
    policyValues.push(`${t(lang, "inspector.durability")}: ${durabilityLabel}`);
    policyValues.push(
      `${t(lang, "inspector.maxAttempts")}: ${policy.max_attempts ?? t(lang, "common.default")}`,
    );
    if (policy.fairness_key) {
      policyValues.push(`${t(lang, "inspector.fairnessKey")}: ${policy.fairness_key}`);
    }
    if (policy.concurrency_key) {
      policyValues.push(`${t(lang, "inspector.concurrencyKey")}: ${policy.concurrency_key}`);
    }
    if (policy.queue_timeout) {
      policyValues.push(
        `${t(lang, "inspector.queueTimeout")}: ${formatRustDuration(policy.queue_timeout)}`,
      );
    }
    if (policy.execution_timeout) {
      policyValues.push(
        `${t(lang, "inspector.execTimeout")}: ${formatRustDuration(policy.execution_timeout)}`,
      );
    }
    if (policy.total_timeout) {
      policyValues.push(
        `${t(lang, "inspector.totalTimeout")}: ${formatRustDuration(policy.total_timeout)}`,
      );
    }
    if (policy.spillover_after) {
      policyValues.push(
        `${t(lang, "inspector.spilloverAfter")}: ${formatRustDuration(policy.spillover_after)}`,
      );
    }
  }

  return (
    <aside className="panel panel--aside panel--detail">
      <div className="panel__header panel__header--stack">
        <div className="inspector__identity">
          <h1>{truncateMiddle(job.job_id, 34)}</h1>
          <span>
            {job.interface_name}
            {job.namespace ? ` · ${job.namespace}` : ""}
          </span>
        </div>

        <div className="inspector__actions">
          <StateBadge state={job.state} lang={lang} />
          {job.durability ? <span className="tag-inline">{job.durability}</span> : null}
          {canCancel && onCancel ? (
            <button
              type="button"
              className="button button--danger"
              disabled={isCancelling}
              onClick={() => void onCancel(job.job_id, job.namespace)}
            >
              {isCancelling ? t(lang, "inspector.cancelling") : t(lang, "inspector.cancelJob")}
            </button>
          ) : null}
        </div>
      </div>

      <div className="meta-grid">
        <MetaItem label={t(lang, "inspector.source")} value={job.source ?? t(lang, "common.direct")} />
        <MetaItem
          label={t(lang, "inspector.worker")}
          value={
            detail.latest_worker?.worker_id ??
            latestAttemptWorker(detail) ??
            t(lang, "common.unassigned")
          }
          mono
        />
        {job.namespace ? (
          <MetaItem label={t(lang, "inspector.namespace")} value={job.namespace} mono />
        ) : null}
        <MetaItem label={t(lang, "inspector.durability")} value={durabilityLabel} />
        <MetaItem label={t(lang, "inspector.updated")} value={formatTimestamp(job.updated_at_ms)} mono />
        <MetaItem label={t(lang, "inspector.duration")} value={formatDuration(job.duration_ms)} />
      </div>

      {job.last_error || job.error_code ? (
        <InspectorSection title={t(lang, "inspector.section.error")}>
          <div className="preview-meta">
            {job.error_code ? <span className="tag-inline tag-inline--danger">{job.error_code}</span> : null}
            {job.error_retryable != null ? (
              <span className="tag-inline">
                {t(lang, job.error_retryable ? "inspector.retryable" : "inspector.nonRetryable")}
              </span>
            ) : null}
          </div>
          {job.last_error ? <CodeBlock value={job.last_error} /> : null}
        </InspectorSection>
      ) : null}

      <InspectorGroup
        title={t(lang, "inspector.group.scheduling")}
        meta={t(lang, "inspector.group.schedulingMeta")}
      >
        <InspectorSection title={t(lang, "inspector.section.demand")}>
          {demand ? (
            <InlineList values={demandValues} />
          ) : (
            <EmptyState message={t(lang, "inspector.specExpired")} compact />
          )}
        </InspectorSection>

        <InspectorSection title={t(lang, "inspector.section.policy")}>
          {policy ? (
            <InlineList values={policyValues} />
          ) : (
            <EmptyState message={t(lang, "inspector.specExpired")} compact />
          )}
        </InspectorSection>
      </InspectorGroup>

      <InspectorGroup
        title={t(lang, "inspector.group.payload")}
        meta={t(lang, "inspector.group.payloadMeta")}
      >
        <InspectorSection title={t(lang, "inspector.section.request")}>
          {job.params_preview ? (
            <>
              <InlineList
                className="preview-meta"
                values={[
                  `${job.params_size_bytes ?? 0} B`,
                  t(lang, job.params_truncated ? "inspector.truncated" : "inspector.full"),
                ]}
              />
              <CodeBlock value={job.params_preview} />
            </>
          ) : (
            <EmptyState message={t(lang, "inspector.noParams")} compact />
          )}
        </InspectorSection>

        <InspectorSection title={t(lang, "inspector.section.inputs")}>
          <ObjectRefList
            objects={job.inputs}
            emptyMessage={t(lang, "inspector.noInputs")}
            lang={lang}
          />
        </InspectorSection>

        <InspectorSection title={t(lang, "inspector.section.outputs")}>
          <ObjectRefList
            objects={job.outputs}
            emptyMessage={t(lang, "inspector.noOutputs")}
            lang={lang}
          />
        </InspectorSection>

        <InspectorSection title={t(lang, "inspector.section.response")}>
          {job.output_preview ? (
            <>
              <InlineList
                className="preview-meta"
                values={[
                  `${job.output_preview.size_bytes} B`,
                  job.output_preview.content_type ?? "unknown content-type",
                  t(lang, job.output_preview.truncated ? "inspector.truncated" : "inspector.full"),
                ]}
              />
              {job.output_preview.preview ? (
                <CodeBlock value={job.output_preview.preview} />
              ) : (
                <EmptyState message={t(lang, "inspector.responseUnavailable")} compact />
              )}
            </>
          ) : (
            <EmptyState message={t(lang, "inspector.noResponse")} compact />
          )}
        </InspectorSection>
      </InspectorGroup>

      <InspectorGroup
        title={t(lang, "inspector.group.execution")}
        meta={t(lang, "inspector.group.executionMeta")}
      >
        <InspectorSection title={t(lang, "inspector.section.attempts")}>
          {detail.history_incomplete ? (
            <p className="inline-notice" role="status">
              {t(lang, "inspector.incompleteHistory")}
            </p>
          ) : null}
          <AttemptsTimeline attempts={detail.attempts} lang={lang} />
        </InspectorSection>

        <InspectorSection title={t(lang, "inspector.section.events")}>
          <EventsTimeline events={detail.events} lang={lang} />
        </InspectorSection>
      </InspectorGroup>
    </aside>
  );
}
