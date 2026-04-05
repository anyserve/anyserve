import { formatDuration, formatNumber, formatTimestamp } from "./format";
import type { AttemptSummary, JobEventSummary, StreamSummary } from "./types";
import { EmptyState } from "./pane-tables";

export function Timeline(props: {
  items: Array<{ key: string; title: string; subtitle: string; meta: string }>;
  emptyMessage: string;
}) {
  if (props.items.length === 0) {
    return <EmptyState message={props.emptyMessage} compact />;
  }

  return (
    <div className="timeline">
      {props.items.map((item) => (
        <div key={item.key} className="timeline__item">
          <div className="timeline__title">{item.title}</div>
          <div className="timeline__subtitle">{item.subtitle}</div>
          <div className="timeline__meta">{item.meta}</div>
        </div>
      ))}
    </div>
  );
}

export function AttemptsTimeline(props: { attempts: AttemptSummary[] }) {
  return (
    <Timeline
      items={props.attempts.map((attempt) => ({
        key: attempt.attempt_id,
        title: `${attempt.state} · ${attempt.worker_id ?? "unassigned"}`,
        subtitle: formatTimestamp(attempt.created_at_ms),
        meta:
          attempt.duration_ms != null ? formatDuration(attempt.duration_ms) : attempt.lease_id ?? "pending",
      }))}
      emptyMessage="No attempts"
    />
  );
}

export function StreamsTimeline(props: { streams: StreamSummary[] }) {
  return (
    <Timeline
      items={props.streams.map((stream) => ({
        key: stream.stream_id,
        title: `${stream.stream_name} · ${stream.state}`,
        subtitle: `${stream.scope} / ${stream.direction}`,
        meta: `seq ${stream.last_sequence}`,
      }))}
      emptyMessage="No streams"
    />
  );
}

export function EventsTimeline(props: { events: JobEventSummary[] }) {
  return (
    <Timeline
      items={props.events.map((event) => ({
        key: String(event.sequence),
        title: event.kind,
        subtitle: formatTimestamp(event.created_at_ms),
        meta: `${formatNumber(event.payload_size_bytes)} B`,
      }))}
      emptyMessage="No events"
    />
  );
}

