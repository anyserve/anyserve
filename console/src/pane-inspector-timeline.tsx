import { formatDuration, formatNumber, formatTimestamp } from "./format";
import { t, type Lang } from "./i18n";
import type { AttemptSummary, JobEventSummary } from "./types";
import { EmptyState } from "./pane-tables";

export function Timeline(props: {
  items: Array<{
    key: string;
    title: string;
    subtitle: string;
    meta: string;
    detail?: string | null;
  }>;
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
          {item.detail ? <div className="timeline__error">{item.detail}</div> : null}
        </div>
      ))}
    </div>
  );
}

export function AttemptsTimeline(props: { attempts: AttemptSummary[]; lang?: Lang }) {
  const lang = props.lang ?? "en";
  return (
    <Timeline
      items={props.attempts.map((attempt) => ({
        key: attempt.attempt_id,
        title: `${attempt.state} · ${attempt.worker_id ?? t(lang, "common.unassigned")}`,
        subtitle: formatTimestamp(attempt.created_at_ms),
        meta:
          attempt.duration_ms != null
            ? formatDuration(attempt.duration_ms)
            : t(lang, "inspector.inProgress"),
        detail: attempt.last_error,
      }))}
      emptyMessage={t(lang, "inspector.noAttempts")}
    />
  );
}

export function EventsTimeline(props: { events: JobEventSummary[]; lang?: Lang }) {
  const lang = props.lang ?? "en";
  return (
    <Timeline
      items={props.events.map((event) => ({
        key: String(event.sequence),
        title: `#${event.sequence} · ${event.kind}`,
        subtitle: formatTimestamp(event.created_at_ms),
        meta: `${formatNumber(event.payload_size_bytes)} B`,
      }))}
      emptyMessage={t(lang, "inspector.noEvents")}
    />
  );
}
