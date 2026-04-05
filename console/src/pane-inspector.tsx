import {
  formatCapacityMap,
  formatDuration,
  formatTimestamp,
  latestAttemptWorker,
  truncateMiddle,
} from "./format";
import { QUEUE_STATES } from "./app-constants";
import { EmptyState } from "./pane-tables";
import {
  CodeBlock,
  InlineList,
  InspectorGroup,
  InspectorSection,
  MetaItem,
  ObjectRefList,
  PreviewMeta,
} from "./pane-inspector-details";
import { AttemptsTimeline, EventsTimeline, StreamsTimeline } from "./pane-inspector-timeline";
import type { JobDetailResponse, JobState } from "./types";

export function JobInspector(props: {
  detail: JobDetailResponse | null;
  isCancelling?: boolean;
  onCancel?: (jobId: string) => Promise<void>;
}) {
  const { detail, isCancelling = false, onCancel } = props;

  if (!detail) {
    return (
      <aside className="panel panel--aside">
        <div className="panel__header">
          <h1>Inspector</h1>
          <span>Select a job</span>
        </div>
        <EmptyState message="Job details appear here." />
      </aside>
    );
  }

  const canCancel = QUEUE_STATES.includes(detail.job.state);

  return (
    <aside className="panel panel--aside panel--detail">
      <div className="panel__header panel__header--stack">
        <div className="inspector__identity">
          <h1>{truncateMiddle(detail.job.job_id, 34)}</h1>
          <span>{detail.job.interface_name}</span>
        </div>

        <div className="inspector__actions">
          <StateBadge state={detail.job.state} />
          {canCancel && onCancel ? (
            <button
              type="button"
              className="button"
              disabled={isCancelling}
              onClick={() => void onCancel(detail.job.job_id)}
            >
              {isCancelling ? "Cancelling" : "Cancel job"}
            </button>
          ) : null}
        </div>
      </div>

      <div className="meta-grid">
        <MetaItem label="Source" value={detail.job.source ?? "direct"} />
        <MetaItem
          label="Worker"
          value={detail.latest_worker?.worker_id ?? latestAttemptWorker(detail) ?? "Unassigned"}
          mono
        />
        <MetaItem label="Updated" value={formatTimestamp(detail.job.updated_at_ms)} mono />
        <MetaItem label="Duration" value={formatDuration(detail.job.duration_ms)} />
      </div>

      <InspectorGroup
        title="Payload"
        meta="Demand, request preview, and persisted artifacts"
      >
        <InspectorSection title="Demand">
          <InlineList
            values={[
              `required ${formatCapacityMap(detail.job.demand.required_capacity)}`,
              `attrs ${formatCapacityMap(detail.job.demand.required_attributes)}`,
              `priority ${detail.job.policy.priority}`,
            ]}
          />
        </InspectorSection>

        <InspectorSection title="Request">
          {detail.job.params_preview ? (
            <>
              <PreviewMeta
                values={[
                  `${detail.job.params_size_bytes} B`,
                  detail.job.params_truncated ? "truncated" : "full",
                ]}
              />
              <CodeBlock value={detail.job.params_preview} />
            </>
          ) : (
            <EmptyState message="No inline request preview." compact />
          )}
        </InspectorSection>

        <InspectorSection title="Inputs">
          <ObjectRefList objects={detail.job.inputs} emptyMessage="No persisted inputs" />
        </InspectorSection>

        <InspectorSection title="Outputs">
          <ObjectRefList objects={detail.job.outputs} emptyMessage="No persisted outputs" />
        </InspectorSection>

        <InspectorSection title="Response">
          {detail.job.output_preview ? (
            <>
              <PreviewMeta
                values={[
                  `${detail.job.output_preview.size_bytes} B`,
                  detail.job.output_preview.content_type ?? "unknown content-type",
                  detail.job.output_preview.truncated ? "truncated" : "full",
                ]}
              />
              {detail.job.output_preview.preview ? (
                <CodeBlock value={detail.job.output_preview.preview} />
              ) : (
                <EmptyState message="Response preview unavailable." compact />
              )}
            </>
          ) : (
            <EmptyState message="No response preview." compact />
          )}
        </InspectorSection>
      </InspectorGroup>

      <InspectorGroup
        title="Execution"
        meta="Attempts, streams, events, and runtime diagnostics"
      >
        <InspectorSection title="Attempts">
          <AttemptsTimeline attempts={detail.attempts} />
        </InspectorSection>

        <InspectorSection title="Streams">
          <StreamsTimeline streams={detail.streams} />
        </InspectorSection>

        <InspectorSection title="Events">
          <EventsTimeline events={detail.events} />
        </InspectorSection>

        {detail.job.last_error ? (
          <InspectorSection title="Error">
            <CodeBlock value={detail.job.last_error} />
          </InspectorSection>
        ) : null}
      </InspectorGroup>
    </aside>
  );
}

function StateBadge(props: { state: JobState }) {
  return <span className={`badge badge--${props.state}`}>{props.state}</span>;
}
