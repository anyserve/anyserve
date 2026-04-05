import type { ReactNode } from "react";

import { formatObjectMeta } from "./format";
import type { ObjectRefSummary } from "./types";
import { EmptyState } from "./pane-tables";

export function InspectorSection(props: { title: string; children: ReactNode }) {
  return (
    <section className="inspector__section">
      <div className="inspector__label">{props.title}</div>
      {props.children}
    </section>
  );
}

export function InspectorGroup(props: {
  title: string;
  meta: string;
  children: ReactNode;
}) {
  return (
    <section className="inspector__group">
      <div className="inspector__group-header">
        <h2>{props.title}</h2>
        <span>{props.meta}</span>
      </div>
      {props.children}
    </section>
  );
}

export function MetaItem(props: { label: string; value: string; mono?: boolean }) {
  return (
    <div className="meta-item">
      <span>{props.label}</span>
      <strong className={props.mono ? "meta-item__value meta-item__value--mono" : "meta-item__value"}>
        {props.value}
      </strong>
    </div>
  );
}

export function InlineList(props: { values: string[] }) {
  return (
    <div className="inline-list">
      {props.values.map((value) => (
        <span key={value}>{value}</span>
      ))}
    </div>
  );
}

export function PreviewMeta(props: { values: string[] }) {
  return (
    <div className="preview-meta">
      {props.values.map((value) => (
        <span key={value}>{value}</span>
      ))}
    </div>
  );
}

export function ObjectRefList(props: {
  objects: ObjectRefSummary[];
  emptyMessage: string;
}) {
  if (props.objects.length === 0) {
    return <EmptyState message={props.emptyMessage} compact />;
  }

  return (
    <div className="timeline">
      {props.objects.map((object, index) => (
        <div key={`${object.kind}-${object.uri ?? index}`} className="timeline__item">
          <div className="timeline__title">
            {object.kind}
            {object.content_type ? ` · ${object.content_type}` : ""}
          </div>
          <div className="timeline__subtitle">
            {object.uri ?? formatObjectMeta(object)}
          </div>
          {object.preview ? <CodeBlock value={object.preview} /> : null}
          {!object.preview && object.uri == null ? (
            <div className="timeline__meta">Preview unavailable</div>
          ) : null}
          {object.truncated ? <div className="timeline__meta">Preview truncated</div> : null}
        </div>
      ))}
    </div>
  );
}

export function CodeBlock(props: { value: string }) {
  return <pre className="code-block">{props.value}</pre>;
}
