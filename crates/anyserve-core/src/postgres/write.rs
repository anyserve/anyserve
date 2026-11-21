use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Weak};

use anyhow::{Context, Result};
use serde_json::Value;
use sqlx::{Postgres as Pg, Row, Transaction};
use tokio::sync::{mpsc, oneshot};

use super::{ClaimedJob, Postgres};
use crate::error::RuntimeError;
use crate::journal::{AcceptRecord, CommitOutcome, JournalOp, StoredJob, StoredSpec, state_text};
use crate::runtime::api::{ConflictPolicy, ErrorInfo, EventKind, JobState, Object, codes};
use crate::runtime::events::{ByteBudget, Permit};

const FRAME_BATCH_MAX: usize = 128;

pub(crate) struct FrameWrite {
    pub job_id: String,
    pub epoch: i64,
    pub attempt_id: String,
    pub sequence: u64,
    pub data: bytes::Bytes,
    pub metadata: crate::runtime::api::Metadata,
    pub progress: bool,
    pub max_events: usize,
    pub max_bytes: usize,
    pub reply: oneshot::Sender<Result<u64, RuntimeError>>,
}

struct PendingFrameInsert {
    kind_json: String,
    data: bytes::Bytes,
    progress: bool,
}

pub(super) async fn run_frame_batcher(pg: Weak<Postgres>, mut rx: mpsc::Receiver<FrameWrite>) {
    while let Some(first) = rx.recv().await {
        let mut batch = vec![first];
        // Yield once so concurrent workers flushing in the same tick coalesce into one commit.
        tokio::task::yield_now().await;
        while batch.len() < FRAME_BATCH_MAX {
            match rx.try_recv() {
                Ok(item) => batch.push(item),
                Err(_) => break,
            }
        }
        let Some(store) = pg.upgrade() else {
            for item in batch {
                let _ = item
                    .reply
                    .send(Err(RuntimeError::unavailable("PostgreSQL store closed")));
            }
            return;
        };
        store.flush_frame_batch(batch).await;
    }
}

pub(super) async fn db_now(tx: &mut Transaction<'_, Pg>) -> Result<i64> {
    Ok(
        sqlx::query_scalar("SELECT (extract(epoch FROM clock_timestamp())*1000)::bigint")
            .fetch_one(&mut **tx)
            .await?,
    )
}

async fn insert_objects(
    tx: &mut Transaction<'_, Pg>,
    id: &str,
    role: &str,
    objects: &[Object],
) -> Result<()> {
    for (idx, object) in objects.iter().enumerate() {
        let (uri, content) = match object {
            Object::Inline { data, .. } => (None, Some(data.as_ref())),
            Object::Uri { uri, .. } => (Some(uri.as_str()), None),
        };
        sqlx::query("INSERT INTO objects(job_id,role,idx,uri,content,metadata_json) VALUES($1,$2,$3,$4,$5,$6)")
            .bind(id).bind(role).bind(idx as i64).bind(uri).bind(content)
            .bind(serde_json::to_string(object.metadata())?).execute(&mut **tx).await?;
    }
    Ok(())
}

pub(super) async fn event(
    tx: &mut Transaction<'_, Pg>,
    id: &str,
    sequence: i64,
    attempt: Option<&str>,
    at: i64,
    kind: EventKind,
) -> Result<()> {
    // Finished stores metadata here; readers attach the single copy of outputs
    // from objects, under the result-byte budget.
    sqlx::query(
        "INSERT INTO events(job_id,sequence,attempt_id,at_ms,kind_json) VALUES($1,$2,$3,$4,$5)",
    )
    .bind(id)
    .bind(sequence)
    .bind(attempt)
    .bind(at)
    .bind(serde_json::to_string(&kind)?)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

const WORKER_OR_TIMEOUT_SQL: &str = "(
    j.attempt_count >= j.max_attempts
    OR j.total_deadline_ms <= $1
    OR (j.state = 'queued' AND CASE
        WHEN j.queued_at_ms > 9223372036854775807 - j.queue_timeout_ms THEN 9223372036854775807
        ELSE j.queued_at_ms + j.queue_timeout_ms END <= $1)
    OR EXISTS (
        SELECT 1 FROM jsonb_array_elements(params.workers) w
        WHERE COALESCE(NULLIF(w->>'namespace', ''), 'default') = j.namespace
          AND w->'interfaces' ? j.interface
          AND (j.required_attributes = '{}'::jsonb OR (w->'attributes') @> j.required_attributes)
          AND (j.spillover_ms IS NULL
               OR j.preferred_attributes = '{}'::jsonb
               OR CASE
                    WHEN j.queued_at_ms > 9223372036854775807 - j.spillover_ms THEN 9223372036854775807
                    ELSE j.queued_at_ms + j.spillover_ms END <= $1
               OR (w->'attributes') @> j.preferred_attributes)
          AND (j.required_capacity = '{}'::jsonb OR NOT EXISTS (
              SELECT 1 FROM jsonb_each_text(j.required_capacity) r
              WHERE r.value::bigint > COALESCE((w->'capacity'->>r.key)::bigint, 0)
          ))
          AND (
              params.policies = '{}'::jsonb
              OR (params.policies -> j.interface -> 'pool_tiers') IS NULL
              OR jsonb_array_length(params.policies -> j.interface -> 'pool_tiers') = 0
              OR EXISTS (
                  SELECT 1 FROM jsonb_array_elements(params.policies -> j.interface -> 'pool_tiers') t
                  WHERE t->>'pool' = COALESCE(w->>'pool', '')
                    AND CASE
                        WHEN j.queued_at_ms > 9223372036854775807 - (t->>'spillover_ms')::bigint THEN 9223372036854775807
                        ELSE j.queued_at_ms + (t->>'spillover_ms')::bigint END <= $1
              )
          )
          AND (
              cardinality(j.failed_workers) = 0
              OR NOT (COALESCE(w->>'worker_id', '') <> '' AND (w->>'worker_id') = ANY(j.failed_workers))
              OR ($1 - j.updated_at_ms) >= 200
              OR NOT EXISTS (
                  SELECT 1 FROM nodes n2
                  WHERE n2.node_id <> $4 AND n2.expires_at > clock_timestamp()
              )
          )
    )
)";

/// Column-by-column B-tree skip-scan over serviceable `(namespace, interface)`
/// pairs on `jobs_claim_fair_groups`, returning up to 64 active `(priority,
/// namespace, interface, fairness_key)` buckets rotated past `cur`.
pub(super) const SKIP_SCAN_SQL: &str = "WITH RECURSIVE
pairs AS MATERIALIZED (
    SELECT ns, iface, ord::bigint AS ord
    FROM UNNEST($1::text[], $2::text[]) WITH ORDINALITY AS p(ns, iface, ord)
),
tp AS MATERIALIZED (
    SELECT sub.prio
    FROM pairs p
    CROSS JOIN LATERAL (
        SELECT j.priority AS prio
        FROM jobs j
        WHERE j.state = 'queued' AND j.owner_id IS NULL
          AND j.namespace = p.ns AND j.interface = p.iface
          AND j.priority <= $3
        ORDER BY j.priority DESC
        LIMIT 1
    ) sub
    ORDER BY sub.prio DESC
    LIMIT 1
),
cur AS MATERIALIZED (
    SELECT c.ord, c.fkey
    FROM tp
    JOIN UNNEST($4::bigint[], $5::bigint[], $6::text[]) AS c(prio, ord, fkey)
      ON c.prio = tp.prio
    LIMIT 1
),
after_cur AS (
    (
        SELECT first_row.ord, first_row.ns, first_row.iface, first_row.fkey, 1 AS depth
        FROM tp
        CROSS JOIN LATERAL (
            WITH same_cur AS MATERIALIZED (
                SELECT c.ord AS ord, p.ns AS ns, p.iface AS iface, nxt.fkey AS fkey
                FROM cur c
                JOIN pairs p ON p.ord = c.ord
                CROSS JOIN LATERAL (
                    SELECT j.fairness_key AS fkey
                    FROM jobs j
                    WHERE j.state = 'queued' AND j.owner_id IS NULL
                      AND j.namespace = p.ns AND j.interface = p.iface
                      AND j.priority = tp.prio
                      AND j.fairness_key > c.fkey
                    ORDER BY j.fairness_key ASC
                    LIMIT 1
                ) nxt
                LIMIT 1
            )
            SELECT ord, ns, iface, fkey FROM same_cur
            UNION ALL
            SELECT later.ord, later.ns, later.iface, later.fkey
            FROM (
                SELECT s.ord, s.ns, s.iface, nxt.fkey
                FROM pairs s
                CROSS JOIN LATERAL (
                    SELECT j.fairness_key AS fkey
                    FROM jobs j
                    WHERE j.state = 'queued' AND j.owner_id IS NULL
                      AND j.namespace = s.ns AND j.interface = s.iface
                      AND j.priority = tp.prio
                    ORDER BY j.fairness_key ASC
                    LIMIT 1
                ) nxt
                WHERE s.ord > COALESCE((SELECT c.ord FROM cur c), 0)
                ORDER BY s.ord ASC
                LIMIT 1
            ) later
            WHERE NOT EXISTS (SELECT 1 FROM same_cur)
        ) first_row
    )
    UNION ALL
    SELECT step.ord, step.ns, step.iface, step.fkey, b.depth + 1
    FROM after_cur b
    CROSS JOIN tp
    CROSS JOIN LATERAL (
        WITH same_pair AS MATERIALIZED (
            SELECT j.fairness_key AS fkey
            FROM jobs j
            WHERE j.state = 'queued' AND j.owner_id IS NULL
              AND j.namespace = b.ns AND j.interface = b.iface
              AND j.priority = tp.prio
              AND j.fairness_key > b.fkey
            ORDER BY j.fairness_key ASC
            LIMIT 1
        )
        SELECT b.ord AS ord, b.ns AS ns, b.iface AS iface, sp.fkey AS fkey
        FROM same_pair sp
        UNION ALL
        SELECT later.ord, later.ns, later.iface, later.fkey
        FROM (
            SELECT s.ord, s.ns, s.iface, nxt.fkey
            FROM pairs s
            CROSS JOIN LATERAL (
                SELECT j.fairness_key AS fkey
                FROM jobs j
                WHERE j.state = 'queued' AND j.owner_id IS NULL
                  AND j.namespace = s.ns AND j.interface = s.iface
                  AND j.priority = tp.prio
                ORDER BY j.fairness_key ASC
                LIMIT 1
            ) nxt
            WHERE s.ord > b.ord
            ORDER BY s.ord ASC
            LIMIT 1
        ) later
        WHERE NOT EXISTS (SELECT 1 FROM same_pair)
    ) step
    WHERE b.depth < 64
),
after_bounded AS MATERIALIZED (
    SELECT ord, ns, iface, fkey, depth FROM after_cur LIMIT 64
),
wrap_cur AS (
    (
        SELECT s.ord, s.ns, s.iface, nxt.fkey, 1 AS depth
        FROM tp
        CROSS JOIN cur c
        CROSS JOIN pairs s
        CROSS JOIN LATERAL (
            SELECT j.fairness_key AS fkey
            FROM jobs j
            WHERE j.state = 'queued' AND j.owner_id IS NULL
              AND j.namespace = s.ns AND j.interface = s.iface
              AND j.priority = tp.prio
            ORDER BY j.fairness_key ASC
            LIMIT 1
        ) nxt
        WHERE (SELECT COUNT(*) FROM after_bounded) < 64
          AND (s.ord < c.ord OR (s.ord = c.ord AND nxt.fkey <= c.fkey))
        ORDER BY s.ord ASC
        LIMIT 1
    )
    UNION ALL
    SELECT step.ord, step.ns, step.iface, step.fkey, b.depth + 1
    FROM wrap_cur b
    CROSS JOIN tp
    CROSS JOIN cur c
    CROSS JOIN LATERAL (
        WITH same_pair AS MATERIALIZED (
            SELECT j.fairness_key AS fkey
            FROM jobs j
            WHERE j.state = 'queued' AND j.owner_id IS NULL
              AND j.namespace = b.ns AND j.interface = b.iface
              AND j.priority = tp.prio
              AND j.fairness_key > b.fkey
            ORDER BY j.fairness_key ASC
            LIMIT 1
        )
        SELECT b.ord AS ord, b.ns AS ns, b.iface AS iface, sp.fkey AS fkey
        FROM same_pair sp
        UNION ALL
        SELECT later.ord, later.ns, later.iface, later.fkey
        FROM (
            SELECT s.ord, s.ns, s.iface, nxt.fkey
            FROM pairs s
            CROSS JOIN LATERAL (
                SELECT j.fairness_key AS fkey
                FROM jobs j
                WHERE j.state = 'queued' AND j.owner_id IS NULL
                  AND j.namespace = s.ns AND j.interface = s.iface
                  AND j.priority = tp.prio
                ORDER BY j.fairness_key ASC
                LIMIT 1
            ) nxt
            WHERE s.ord > b.ord
            ORDER BY s.ord ASC
            LIMIT 1
        ) later
        WHERE NOT EXISTS (SELECT 1 FROM same_pair)
    ) step
    WHERE b.depth < 64
      AND (b.ord < c.ord OR (b.ord = c.ord AND b.fkey < c.fkey))
      AND (step.ord < c.ord OR (step.ord = c.ord AND step.fkey <= c.fkey))
)
SELECT tp.prio, combined.ns, combined.iface, combined.fkey
FROM tp
CROSS JOIN (
    SELECT ns, iface, fkey, 1 AS phase, depth FROM after_bounded
    UNION ALL
    SELECT ns, iface, fkey, 2 AS phase, depth FROM wrap_cur
) combined
ORDER BY combined.phase ASC, combined.depth ASC
LIMIT 64";

type BucketKey = (i64, String, String, String);

struct CandidateRow {
    row: sqlx::postgres::PgRow,
    failure: Option<ErrorInfo>,
    bucket: Option<BucketKey>,
}

fn extract_serviceable_pairs(workers: &Value) -> Vec<(String, String)> {
    let mut pairs = Vec::new();
    if let Some(arr) = workers.as_array() {
        for w in arr {
            let ns = w
                .get("namespace")
                .and_then(Value::as_str)
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .unwrap_or(crate::auth::DEFAULT_NAMESPACE);
            if let Some(ifaces) = w.get("interfaces").and_then(Value::as_array) {
                for iface in ifaces.iter().filter_map(Value::as_str) {
                    if !iface.is_empty() {
                        pairs.push((ns.to_string(), iface.to_string()));
                    }
                }
            }
        }
    }
    pairs.sort_unstable();
    pairs.dedup();
    pairs
}

fn policy_key_cap(policies: &Value, interface: &str) -> usize {
    policies
        .get(interface)
        .and_then(|p| p.get("max_in_flight_per_key"))
        .and_then(Value::as_u64)
        .map(|v| v as usize)
        .unwrap_or(1)
}

fn build_cursor_params(
    cursors: &BTreeMap<i64, (String, String, String)>,
    pairs: &[(String, String)],
) -> (Vec<i64>, Vec<i64>, Vec<String>) {
    let mut cur_prios = Vec::with_capacity(cursors.len());
    let mut cur_ords = Vec::with_capacity(cursors.len());
    let mut cur_fkeys = Vec::with_capacity(cursors.len());
    for (&prio, (c_ns, c_iface, c_fkey)) in cursors {
        match pairs
            .binary_search_by(|(p_ns, p_iface)| p_ns.cmp(c_ns).then_with(|| p_iface.cmp(c_iface)))
        {
            Ok(idx) => {
                cur_prios.push(prio);
                cur_ords.push((idx + 1) as i64);
                cur_fkeys.push(c_fkey.clone());
            }
            Err(ins) if ins > 0 => {
                cur_prios.push(prio);
                cur_ords.push(ins as i64);
                cur_fkeys.push("\u{10ffff}".to_string());
            }
            Err(_) => {}
        }
    }
    (cur_prios, cur_ords, cur_fkeys)
}

async fn fetch_active_concurrency(
    tx: &mut Transaction<'_, Pg>,
    keys: &BTreeSet<(String, String)>,
) -> Result<HashMap<(String, String), usize>> {
    let mut counts_map = HashMap::new();
    if keys.is_empty() {
        return Ok(counts_map);
    }
    let lock_ns: Vec<&str> = keys.iter().map(|(ns, _)| ns.as_str()).collect();
    let lock_ckey: Vec<&str> = keys.iter().map(|(_, ck)| ck.as_str()).collect();
    let counts: Vec<(String, String, i64)> = sqlx::query_as(
        "SELECT j.namespace, j.concurrency_key, COUNT(*)::bigint
         FROM jobs j
         JOIN UNNEST($1::text[], $2::text[]) AS k(ns, ckey)
           ON j.namespace = k.ns AND j.concurrency_key = k.ckey
         WHERE j.concurrency_key IS NOT NULL AND j.concurrency_key <> ''
           AND j.owner_id IS NOT NULL AND j.state IN ('queued','running')
         GROUP BY j.namespace, j.concurrency_key",
    )
    .bind(lock_ns)
    .bind(lock_ckey)
    .fetch_all(&mut **tx)
    .await?;
    for (ns, ckey, cnt) in counts {
        counts_map.insert((ns, ckey), usize::try_from(cnt).unwrap_or(0));
    }
    Ok(counts_map)
}

fn concurrency_advisory_key(namespace: &str, concurrency_key: &str) -> i64 {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(b"anyserve-ckey\0");
    hasher.update(namespace.as_bytes());
    hasher.update(b"\0");
    hasher.update(concurrency_key.as_bytes());
    let digest = hasher.finalize();
    i64::from_le_bytes(digest[..8].try_into().expect("8 bytes"))
}

fn row_claim_failure(row: &sqlx::postgres::PgRow, now: i64) -> Result<Option<ErrorInfo>> {
    let total_deadline: Option<i64> = row.try_get("total_deadline_ms")?;
    let queue_timeout: Option<i64> = row.try_get("queue_timeout_ms")?;
    let queued_at: i64 = row.try_get("queued_at_ms")?;
    Ok(if total_deadline.is_some_and(|t| t <= now) {
        Some(ErrorInfo::new(
            codes::TOTAL_TIMEOUT,
            "job exceeded its total timeout",
            false,
        ))
    } else if row.try_get::<&str, _>("state")? == "queued"
        && queue_timeout.is_some_and(|t| queued_at.saturating_add(t) <= now)
    {
        Some(ErrorInfo::new(
            codes::QUEUE_TIMEOUT,
            "job exceeded its queue timeout",
            false,
        ))
    } else if row.try_get::<i64, _>("attempt_count")? >= row.try_get::<i64, _>("max_attempts")? {
        Some(ErrorInfo::new(
            codes::ATTEMPTS_EXHAUSTED,
            "previous replica exhausted the attempt budget",
            false,
        ))
    } else {
        None
    })
}

fn bucket_of_row(row: &sqlx::postgres::PgRow) -> Result<BucketKey> {
    Ok((
        row.try_get("priority")?,
        row.try_get("namespace")?,
        row.try_get("interface")?,
        row.try_get("fairness_key")?,
    ))
}

#[allow(clippy::too_many_arguments)]
async fn fetch_bucket_rows(
    tx: &mut Transaction<'_, Pg>,
    now: i64,
    workers_json: &str,
    policies_json: &str,
    node_id: &str,
    pass_scan: i64,
    need: usize,
    buckets: &[BucketKey],
    excluded_ckeys: &BTreeSet<(String, String)>,
    seen_ids: &[String],
) -> Result<Vec<(sqlx::postgres::PgRow, BucketKey)>> {
    let build_extra = |next_idx: &mut usize| -> String {
        let mut extra = String::new();
        if !excluded_ckeys.is_empty() {
            let p1 = *next_idx;
            let p2 = *next_idx + 1;
            *next_idx += 2;
            extra.push_str(&format!(
                " AND (j.concurrency_key IS NULL OR j.concurrency_key = ''
                       OR NOT EXISTS (
                           SELECT 1 FROM UNNEST(${p1}::text[], ${p2}::text[]) AS ex(ns_ex, ck_ex)
                           WHERE ex.ns_ex = j.namespace AND ex.ck_ex = j.concurrency_key
                       ))"
            ));
        }
        if !seen_ids.is_empty() {
            let p_seen = *next_idx;
            *next_idx += 1;
            extra.push_str(&format!(" AND NOT (j.job_id = ANY(${p_seen}::text[]))"));
        }
        extra
    };

    if buckets.len() == 1 {
        let mut next_idx = 10;
        let extra = build_extra(&mut next_idx);
        let fast_sql = format!(
            "WITH params AS MATERIALIZED (SELECT $2::jsonb AS workers, $5::jsonb AS policies)
             SELECT j.* FROM jobs j, params
             WHERE j.state = 'queued' AND j.owner_id IS NULL
               AND j.namespace = $6
               AND j.interface = $7
               AND j.priority = $8
               AND j.fairness_key = $9
               AND (j.not_before_ms <= $1 OR j.total_deadline_ms <= $1)
               AND {WORKER_OR_TIMEOUT_SQL}
               {extra}
             ORDER BY j.queued_at_ms ASC, j.job_id ASC
             LIMIT $3
             FOR UPDATE OF j SKIP LOCKED"
        );
        let (prio, ns, iface, fkey) = &buckets[0];
        let mut q = sqlx::query(&fast_sql)
            .bind(now)
            .bind(workers_json)
            .bind(pass_scan)
            .bind(node_id)
            .bind(policies_json)
            .bind(ns)
            .bind(iface)
            .bind(*prio)
            .bind(fkey);
        if !excluded_ckeys.is_empty() {
            let excl_ns: Vec<&str> = excluded_ckeys.iter().map(|(n, _)| n.as_str()).collect();
            let excl_ck: Vec<&str> = excluded_ckeys.iter().map(|(_, c)| c.as_str()).collect();
            q = q.bind(excl_ns).bind(excl_ck);
        }
        if !seen_ids.is_empty() {
            q = q.bind(seen_ids);
        }
        let rows = q.fetch_all(&mut **tx).await?;
        return Ok(rows.into_iter().map(|r| (r, buckets[0].clone())).collect());
    }

    let mut next_idx = 11;
    let extra = build_extra(&mut next_idx);
    let per_bucket = if buckets.len() <= 8 {
        (need as i64).clamp(4, 64)
    } else {
        need.div_ceil(buckets.len()).clamp(1, 64) as i64
    };
    let mut b_prio = Vec::with_capacity(buckets.len());
    let mut b_ns = Vec::with_capacity(buckets.len());
    let mut b_iface = Vec::with_capacity(buckets.len());
    let mut b_fkey = Vec::with_capacity(buckets.len());
    for (p, ns, iface, fk) in buckets {
        b_prio.push(*p);
        b_ns.push(ns.as_str());
        b_iface.push(iface.as_str());
        b_fkey.push(fk.as_str());
    }
    let lateral_sql = format!(
        "WITH params AS MATERIALIZED (SELECT $2::jsonb AS workers, $5::jsonb AS policies)
         SELECT out_job.*
         FROM UNNEST($6::bigint[], $7::text[], $8::text[], $9::text[]) WITH ORDINALITY
              AS b(prio, ns, iface, fkey, ord)
         CROSS JOIN LATERAL (
             SELECT j.job_id,
                    ROW_NUMBER() OVER (ORDER BY j.queued_at_ms ASC, j.job_id ASC) AS rn
             FROM jobs j, params
             WHERE j.state = 'queued' AND j.owner_id IS NULL
               AND j.namespace = b.ns
               AND j.interface = b.iface
               AND j.priority = b.prio
               AND j.fairness_key = b.fkey
               AND (j.not_before_ms <= $1 OR j.total_deadline_ms <= $1)
               AND {WORKER_OR_TIMEOUT_SQL}
               {extra}
             ORDER BY j.queued_at_ms ASC, j.job_id ASC
             LIMIT $10
         ) c
         JOIN jobs out_job ON out_job.job_id = c.job_id
         ORDER BY c.rn ASC, b.ord ASC, out_job.queued_at_ms ASC, out_job.job_id ASC
         LIMIT $3
         FOR UPDATE OF out_job SKIP LOCKED"
    );
    let mut q = sqlx::query(&lateral_sql)
        .bind(now)
        .bind(workers_json)
        .bind(pass_scan)
        .bind(node_id)
        .bind(policies_json)
        .bind(b_prio)
        .bind(b_ns)
        .bind(b_iface)
        .bind(b_fkey)
        .bind(per_bucket);
    if !excluded_ckeys.is_empty() {
        let excl_ns: Vec<&str> = excluded_ckeys.iter().map(|(n, _)| n.as_str()).collect();
        let excl_ck: Vec<&str> = excluded_ckeys.iter().map(|(_, c)| c.as_str()).collect();
        q = q.bind(excl_ns).bind(excl_ck);
    }
    if !seen_ids.is_empty() {
        q = q.bind(seen_ids);
    }
    let rows = q.fetch_all(&mut **tx).await?;
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        let bucket = bucket_of_row(&row)?;
        out.push((row, bucket));
    }
    Ok(out)
}

impl Postgres {
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn enqueue_frame(
        &self,
        id: &str,
        epoch: i64,
        attempt: &str,
        frame: &anyserve_proto::v2::Frame,
        metadata: crate::runtime::api::Metadata,
        max_events: usize,
        max_bytes: usize,
    ) -> Result<u64> {
        let kind = EventKind::Output {
            data: bytes::Bytes::new(),
            metadata: metadata.clone(),
            progress: frame.progress,
        };
        let json = serde_json::to_string(&kind)?;
        if json
            .len()
            .saturating_add(frame.data.len())
            .saturating_add(96)
            > max_bytes
            || max_events == 0
        {
            return Err(
                RuntimeError::exhausted("frame exceeds the durable event log budget").into(),
            );
        }
        self.ensure_healthy()?;
        let (reply_tx, reply_rx) = oneshot::channel();
        self.frame_batch_tx
            .send(FrameWrite {
                job_id: id.to_string(),
                epoch,
                attempt_id: attempt.to_string(),
                sequence: frame.sequence,
                data: frame.data.clone(),
                metadata,
                progress: frame.progress,
                max_events,
                max_bytes,
                reply: reply_tx,
            })
            .await
            .map_err(|_| RuntimeError::unavailable("PostgreSQL frame batcher stopped"))?;
        let seq = reply_rx
            .await
            .map_err(|_| RuntimeError::unavailable("PostgreSQL frame batcher dropped request"))??;
        Ok(seq)
    }

    /// Flushes up to `FRAME_BATCH_MAX` frames in one PostgreSQL transaction.
    /// Groups are iterated in `BTreeMap` `(job_id, epoch, attempt_id)` key order
    /// to avoid multi-row lock-order deadlocks across concurrent batches; a
    /// long-held row lock on one job can therefore delay commits for other jobs
    /// coalesced into the same batch (cross-job head-of-line blocking).
    async fn flush_frame_batch(&self, batch: Vec<FrameWrite>) {
        if let Err(err) = self.ensure_healthy() {
            for item in batch {
                let _ = item.reply.send(Err(err.clone()));
            }
            return;
        }
        let started = std::time::Instant::now();
        let batch_len = batch.len();
        let mut grouped: BTreeMap<(String, i64, String), Vec<FrameWrite>> = BTreeMap::new();
        for item in batch {
            grouped
                .entry((item.job_id.clone(), item.epoch, item.attempt_id.clone()))
                .or_default()
                .push(item);
        }

        let mut tx = match self.pool.begin().await {
            Ok(tx) => tx,
            Err(error) => {
                let err = RuntimeError::unavailable(format!("PostgreSQL: {error:#}"));
                for (_, items) in grouped {
                    for item in items {
                        let _ = item.reply.send(Err(err.clone()));
                    }
                }
                return;
            }
        };

        type DeferredReply = (
            oneshot::Sender<Result<u64, RuntimeError>>,
            Result<u64, RuntimeError>,
        );
        let mut deferred_replies: Vec<DeferredReply> = Vec::with_capacity(batch_len);

        let group_result: Result<(), RuntimeError> = async {
            for ((id, epoch, attempt), items) in grouped {
                let row = sqlx::query(
                    "SELECT last_frame_seq,event_sequence FROM jobs
                     WHERE job_id=$1 AND owner_id=$2 AND ownership_epoch=$3 AND last_attempt_id=$4
                       AND state='running'
                       AND EXISTS (SELECT 1 FROM nodes WHERE node_id=$2 AND expires_at>clock_timestamp())
                     FOR UPDATE",
                )
                .bind(&id)
                .bind(&self.node_id)
                .bind(epoch)
                .bind(&attempt)
                .fetch_optional(&mut *tx)
                .await
                .map_err(|e| RuntimeError::unavailable(format!("PostgreSQL: {e:#}")))?;

                let Some(row) = row else {
                    for item in items {
                        deferred_replies.push((
                            item.reply,
                            Err(RuntimeError::stale("job ownership or attempt changed")),
                        ));
                    }
                    continue;
                };

                let mut last = row
                    .try_get::<i64, _>("last_frame_seq")
                    .map_err(|e| RuntimeError::unavailable(format!("PostgreSQL: {e:#}")))?
                    as u64;
                let mut event_sequence = row
                    .try_get::<i64, _>("event_sequence")
                    .map_err(|e| RuntimeError::unavailable(format!("PostgreSQL: {e:#}")))?;

                let mut max_events = usize::MAX;
                let mut max_bytes = usize::MAX;
                let mut to_insert: Vec<PendingFrameInsert> = Vec::new();
                for item in items {
                    max_events = max_events.min(item.max_events);
                    max_bytes = max_bytes.min(item.max_bytes);
                    if item.sequence <= last {
                        deferred_replies.push((item.reply, Ok(last)));
                        continue;
                    }
                    if item.sequence != last + 1 {
                        deferred_replies.push((
                            item.reply,
                            Err(RuntimeError::invalid("frame sequence is not contiguous")),
                        ));
                        continue;
                    }
                    last = item.sequence;
                    let kind = EventKind::Output {
                        data: bytes::Bytes::new(),
                        metadata: item.metadata,
                        progress: item.progress,
                    };
                    let kind_json = serde_json::to_string(&kind)
                        .map_err(|e| RuntimeError::unavailable(format!("PostgreSQL: {e:#}")))?;
                    let next_insert = PendingFrameInsert {
                        kind_json,
                        data: item.data,
                        progress: item.progress,
                    };
                    if item.progress && to_insert.last().is_some_and(|prev| prev.progress) {
                        *to_insert.last_mut().expect("non-empty") = next_insert;
                    } else {
                        to_insert.push(next_insert);
                    }
                    deferred_replies.push((item.reply, Ok(last)));
                }

                if to_insert.is_empty() {
                    continue;
                }
                let now = db_now(&mut tx)
                    .await
                    .map_err(|e| RuntimeError::unavailable(format!("PostgreSQL: {e:#}")))?;
                for pending in to_insert {
                    event_sequence += 1;
                    sqlx::query(
                        "INSERT INTO events(job_id,sequence,attempt_id,at_ms,kind_json,frame_data)
                         VALUES($1,$2,$3,$4,$5,$6)",
                    )
                    .bind(&id)
                    .bind(event_sequence)
                    .bind(&attempt)
                    .bind(now)
                    .bind(&pending.kind_json)
                    .bind(pending.data.as_ref())
                    .execute(&mut *tx)
                    .await
                    .map_err(|e| RuntimeError::unavailable(format!("PostgreSQL: {e:#}")))?;
                }
                let cutoff: Option<i64> = sqlx::query_scalar(
                    "SELECT MAX(sequence) FROM (
                        SELECT sequence, row_number() OVER (ORDER BY sequence DESC) AS n,
                               SUM(COALESCE(octet_length(frame_data),0)+octet_length(kind_json)+96)
                                   OVER (ORDER BY sequence DESC) AS bytes
                        FROM events WHERE job_id=$1
                     ) retained WHERE n>$2 OR bytes>$3",
                )
                .bind(&id)
                .bind(max_events as i64)
                .bind(max_bytes as i64)
                .fetch_one(&mut *tx)
                .await
                .map_err(|e| RuntimeError::unavailable(format!("PostgreSQL: {e:#}")))?;
                if let Some(cutoff) = cutoff {
                    sqlx::query("DELETE FROM events WHERE job_id=$1 AND sequence<=$2")
                        .bind(&id)
                        .bind(cutoff)
                        .execute(&mut *tx)
                        .await
                        .map_err(|e| RuntimeError::unavailable(format!("PostgreSQL: {e:#}")))?;
                }
                sqlx::query(
                    "UPDATE jobs SET last_frame_seq=$2,event_sequence=$3,
                     events_floor=GREATEST(events_floor,COALESCE($4,0)) WHERE job_id=$1",
                )
                .bind(&id)
                .bind(last as i64)
                .bind(event_sequence)
                .bind(cutoff)
                .execute(&mut *tx)
                .await
                .map_err(|e| RuntimeError::unavailable(format!("PostgreSQL: {e:#}")))?;
            }
            Ok(())
        }
        .await;

        if let Err(err) = group_result {
            drop(tx);
            for (reply, result) in deferred_replies {
                let outcome = match result {
                    Ok(_) => Err(err.clone()),
                    Err(e) => Err(e),
                };
                let _ = reply.send(outcome);
            }
            return;
        }

        match tx.commit().await {
            Ok(()) => {
                {
                    let mut stats = self.stats.lock().expect("PG stats");
                    stats.committed_batches += 1;
                    stats.committed_ops += batch_len as u64;
                    stats.last_batch_size = batch_len;
                    stats.last_commit_micros = started.elapsed().as_micros() as u64;
                }
                for (reply, result) in deferred_replies {
                    let _ = reply.send(result);
                }
            }
            Err(error) => {
                let err = RuntimeError::unavailable(format!("PostgreSQL: {error:#}"));
                for (reply, result) in deferred_replies {
                    let outcome = match result {
                        Ok(_) => Err(err.clone()),
                        Err(e) => Err(e),
                    };
                    let _ = reply.send(outcome);
                }
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn append_frame(
        &self,
        id: &str,
        epoch: i64,
        attempt: &str,
        frame: &anyserve_proto::v2::Frame,
        metadata: crate::runtime::api::Metadata,
        max_events: usize,
        max_bytes: usize,
    ) -> Result<u64> {
        self.enqueue_frame(id, epoch, attempt, frame, metadata, max_events, max_bytes)
            .await
    }

    pub(crate) async fn accept(
        &self,
        record: AcceptRecord,
        conflict: ConflictPolicy,
        queue_timeout_ms: Option<i64>,
    ) -> Result<(StoredJob, bool)> {
        self.ensure_healthy()?;
        let _permit = self
            .operations
            .clone()
            .try_acquire_owned()
            .map_err(|_| RuntimeError::exhausted("PostgreSQL submission queue is full"))?;
        let mut tx = self.pool.begin().await?;
        let spec = &record.spec;
        let (ns, _) = crate::runtime::identity::decode_scoped_id(&record.job_id);
        let fairness_key = if spec.policy.fairness_key.is_empty() {
            crate::runtime::state::DEFAULT_FAIRNESS_KEY
        } else {
            spec.policy.fairness_key.as_str()
        };
        let concurrency_key = (!spec.policy.concurrency_key.is_empty())
            .then_some(spec.policy.concurrency_key.as_str());
        let slim = StoredSpec {
            demand: spec.demand.clone(),
            policy: spec.policy.clone(),
            metadata: spec.metadata.clone(),
        };
        let input_bytes =
            spec.inputs.iter().map(Object::inline_len).sum::<usize>() + spec.params.len();
        let inserted = sqlx::query(
            "WITH stamp AS MATERIALIZED (
            SELECT (extract(epoch FROM clock_timestamp())*1000)::bigint AS now_ms)
            INSERT INTO jobs(job_id,state,spec_hash,spec_json,namespace,interface,fairness_key,concurrency_key,
            required_attributes,preferred_attributes,required_capacity,priority,input_bytes,max_attempts,
            created_at_ms,updated_at_ms,queued_at_ms,queue_timeout_ms,total_deadline_ms,spillover_ms,event_sequence)
            SELECT $1,'queued',$2,$3,$14,$4,$15,$16,$5::jsonb,$6::jsonb,$7::jsonb,$8,$9,$10,
                now_ms,now_ms,now_ms,$11,
                CASE WHEN $12::bigint IS NULL THEN NULL
                     WHEN now_ms>9223372036854775807-$12 THEN 9223372036854775807
                     ELSE now_ms+$12 END,$13,1 FROM stamp
            ON CONFLICT(job_id) DO NOTHING RETURNING *",
        )
        .bind(&record.job_id)
        .bind(&record.spec_hash)
        .bind(serde_json::to_string(&slim)?)
        .bind(&spec.demand.interface)
        .bind(serde_json::to_string(&spec.demand.required_attributes)?)
        .bind(serde_json::to_string(&spec.demand.preferred_attributes)?)
        .bind(serde_json::to_string(&spec.demand.required_capacity)?)
        .bind(spec.policy.priority as i64)
        .bind(input_bytes as i64)
        .bind(record.max_attempts as i64)
        .bind(queue_timeout_ms)
        .bind(
            spec.policy
                .total_timeout
                .map(|d| d.as_millis().min(i64::MAX as u128) as i64),
        )
        .bind(
            spec.policy
                .spillover_after
                .map(|d| d.as_millis().min(i64::MAX as u128) as i64),
        )
        .bind(ns)
        .bind(fairness_key)
        .bind(concurrency_key)
        .fetch_optional(&mut *tx)
        .await?;
        let created = inserted.is_some();
        let row = if let Some(row) = inserted {
            let now: i64 = row.try_get("created_at_ms")?;
            insert_objects(&mut tx, &record.job_id, "input", &spec.inputs).await?;
            if !spec.params.is_empty() {
                insert_objects(
                    &mut tx,
                    &record.job_id,
                    "params",
                    &[Object::inline(spec.params.clone())],
                )
                .await?;
            }
            event(&mut tx, &record.job_id, 1, None, now, EventKind::Accepted).await?;
            row
        } else {
            let row = sqlx::query("SELECT * FROM jobs WHERE job_id=$1")
                .bind(&record.job_id)
                .fetch_one(&mut *tx)
                .await?;
            let hash: &str = row.try_get("spec_hash")?;
            if conflict == ConflictPolicy::Fail || hash != record.spec_hash {
                return Err(RuntimeError::conflict(format!(
                    "job '{}' already exists{}",
                    record.job_id,
                    if hash != record.spec_hash {
                        " with a different specification"
                    } else {
                        ""
                    }
                ))
                .into());
            }
            row
        };
        let job = super::read::pg_job_from_row(&row)?;
        tx.commit().await?;
        Ok((job, created))
    }

    #[cfg(test)]
    pub(crate) async fn claim(
        &self,
        workers: Value,
        limit: usize,
        budget: Arc<ByteBudget>,
    ) -> Result<Vec<ClaimedJob>> {
        self.claim_with_policies(workers, Value::Object(Default::default()), limit, budget)
            .await
    }

    pub(crate) async fn claim_with_policies(
        &self,
        workers: Value,
        policies: Value,
        limit: usize,
        budget: Arc<ByteBudget>,
    ) -> Result<Vec<ClaimedJob>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        self.ensure_healthy()?;
        let mut tx = self.pool.begin().await?;
        let (now, node_alive): (i64, bool) = sqlx::query_as(
            "SELECT (extract(epoch FROM clock_timestamp())*1000)::bigint,
                    EXISTS (SELECT 1 FROM nodes WHERE node_id=$1 AND expires_at>clock_timestamp())",
        )
        .bind(&self.node_id)
        .fetch_one(&mut *tx)
        .await?;
        if !node_alive {
            self.fence();
            return Ok(Vec::new());
        }
        let workers_json = workers.to_string();
        let policies_json = policies.to_string();
        let serviceable_pairs = extract_serviceable_pairs(&workers);
        let pair_ns: Vec<&str> = serviceable_pairs
            .iter()
            .map(|(ns, _)| ns.as_str())
            .collect();
        let pair_iface: Vec<&str> = serviceable_pairs.iter().map(|(_, i)| i.as_str()).collect();
        let scan_limit = limit.min(256) as i64;

        let mut candidate_rows: Vec<CandidateRow> = Vec::new();
        let mut seen_ids: Vec<String> = Vec::new();
        let mut seen_set: BTreeSet<String> = BTreeSet::new();

        // Step A: bounded recovery of jobs whose previous owner node lease expired.
        let orphan_sql = format!(
            "WITH params AS MATERIALIZED (SELECT $2::jsonb AS workers, $5::jsonb AS policies)
             SELECT j.* FROM jobs j, params
             WHERE j.state IN ('queued','running')
               AND j.owner_id IS NOT NULL
               AND NOT EXISTS (
                   SELECT 1 FROM nodes n WHERE n.node_id = j.owner_id AND n.expires_at > clock_timestamp()
               )
               AND (j.not_before_ms <= $1 OR j.total_deadline_ms <= $1)
               AND {WORKER_OR_TIMEOUT_SQL}
             ORDER BY j.updated_at_ms ASC
             LIMIT $3
             FOR UPDATE OF j SKIP LOCKED"
        );
        for row in sqlx::query(&orphan_sql)
            .bind(now)
            .bind(&workers_json)
            .bind(scan_limit)
            .bind(&self.node_id)
            .bind(&policies_json)
            .fetch_all(&mut *tx)
            .await?
        {
            let id: String = row.try_get("job_id")?;
            if seen_set.insert(id.clone()) {
                seen_ids.push(id);
            }
            let failure = row_claim_failure(&row, now)?;
            candidate_rows.push(CandidateRow {
                row,
                failure,
                bucket: None,
            });
        }

        // Step A2: O(expired) sweep of queued unowned jobs past deadline/queue_timeout.
        let remaining_after_orphan = scan_limit.saturating_sub(candidate_rows.len() as i64);
        let now_u64 = now.max(0) as u64;
        let sweep_due = serviceable_pairs.is_empty()
            || now_u64.saturating_sub(self.last_expiry_sweep_ms.load(Ordering::Relaxed)) >= 100;
        if remaining_after_orphan > 0 && sweep_due {
            self.last_expiry_sweep_ms.store(now_u64, Ordering::Relaxed);
            let seen_clause = if seen_ids.is_empty() {
                String::new()
            } else {
                " AND NOT (j.job_id = ANY($3::text[]))".to_string()
            };
            let expiry_sql = format!(
                "SELECT j.* FROM jobs j
                 WHERE j.state = 'queued' AND j.owner_id IS NULL
                   AND (j.total_deadline_ms IS NOT NULL OR j.queue_timeout_ms IS NOT NULL)
                   AND (j.not_before_ms <= $1 OR j.total_deadline_ms <= $1)
                   AND LEAST(
                       COALESCE(j.total_deadline_ms, 9223372036854775807),
                       CASE WHEN j.queue_timeout_ms IS NULL THEN 9223372036854775807
                            WHEN j.queued_at_ms > 9223372036854775807 - j.queue_timeout_ms THEN 9223372036854775807
                            ELSE j.queued_at_ms + j.queue_timeout_ms END
                   ) <= $1
                   {seen_clause}
                 ORDER BY LEAST(
                       COALESCE(j.total_deadline_ms, 9223372036854775807),
                       CASE WHEN j.queue_timeout_ms IS NULL THEN 9223372036854775807
                            WHEN j.queued_at_ms > 9223372036854775807 - j.queue_timeout_ms THEN 9223372036854775807
                            ELSE j.queued_at_ms + j.queue_timeout_ms END
                 ) ASC
                 LIMIT $2
                 FOR UPDATE OF j SKIP LOCKED"
            );
            let mut q = sqlx::query(&expiry_sql)
                .bind(now)
                .bind(remaining_after_orphan);
            if !seen_ids.is_empty() {
                q = q.bind(&seen_ids);
            }
            for row in q.fetch_all(&mut *tx).await? {
                let id: String = row.try_get("job_id")?;
                if seen_set.insert(id.clone()) {
                    seen_ids.push(id);
                }
                let failure = row_claim_failure(&row, now)?;
                candidate_rows.push(CandidateRow {
                    row,
                    failure,
                    bucket: None,
                });
            }
        }

        let mut local_cursors = self
            .fair_cursors
            .lock()
            .expect("fair_cursors poisoned")
            .clone();
        let mut cursor_dirty = false;

        // Steps B & C: bounded column-by-column skip-scan restricted strictly to
        // serviceable (namespace, interface) pairs, with wrap-around cursor and
        // per-pass exclusion of saturated concurrency_keys.
        if !serviceable_pairs.is_empty() && candidate_rows.len() < limit {
            const MAX_PASSES: usize = 8;
            let mut excluded_ckeys: BTreeSet<(String, String)> = BTreeSet::new();
            let mut reserved_ckey_counts: HashMap<(String, String), usize> = HashMap::new();
            let mut max_prio = i64::MAX;
            let mut prio_anchor: Option<(String, String, String)> = None;
            let mut cached_buckets: Option<(i64, bool, Vec<BucketKey>)> = None;

            for c in &candidate_rows {
                if c.failure.is_none()
                    && let Some(ckey) = c
                        .row
                        .try_get::<Option<String>, _>("concurrency_key")?
                        .filter(|k| !k.is_empty())
                {
                    let ns: String = c.row.try_get("namespace")?;
                    *reserved_ckey_counts.entry((ns, ckey)).or_insert(0) += 1;
                }
            }

            for pass in 0..MAX_PASSES {
                if candidate_rows.len() >= limit {
                    break;
                }
                let (top_prio, exhausted_prio, buckets) =
                    if let Some(cached) = cached_buckets.take() {
                        cached
                    } else {
                        let (cur_prios, cur_ords, cur_fkeys) =
                            build_cursor_params(&local_cursors, &serviceable_pairs);
                        let mut buckets: Vec<BucketKey> = sqlx::query_as(SKIP_SCAN_SQL)
                            .bind(&pair_ns)
                            .bind(&pair_iface)
                            .bind(max_prio)
                            .bind(cur_prios)
                            .bind(cur_ords)
                            .bind(cur_fkeys)
                            .fetch_all(&mut *tx)
                            .await?;
                        if buckets.is_empty() {
                            break;
                        }
                        let top_prio = buckets[0].0;
                        let mut exhausted_prio = buckets.len() < 64;
                        if let Some(anchor) = &prio_anchor {
                            if let Some(pos) = buckets.iter().position(|(_, ns, iface, fkey)| {
                                (ns, iface, fkey) == (&anchor.0, &anchor.1, &anchor.2)
                            }) {
                                buckets.truncate(pos);
                                exhausted_prio = true;
                            }
                        } else {
                            prio_anchor = Some((
                                buckets[0].1.clone(),
                                buckets[0].2.clone(),
                                buckets[0].3.clone(),
                            ));
                        }
                        if buckets.is_empty() {
                            if top_prio <= 0 {
                                break;
                            }
                            max_prio = top_prio - 1;
                            prio_anchor = None;
                            continue;
                        }
                        (top_prio, exhausted_prio, buckets)
                    };

                let need = limit - candidate_rows.len();
                let pass_scan = if pass == 0 && excluded_ckeys.is_empty() {
                    need as i64
                } else {
                    need.saturating_mul(2).clamp(need, 128) as i64
                };
                let fetched = fetch_bucket_rows(
                    &mut tx,
                    now,
                    &workers_json,
                    &policies_json,
                    &self.node_id,
                    pass_scan,
                    need,
                    &buckets,
                    &excluded_ckeys,
                    &seen_ids,
                )
                .await?;

                // Pre-filter any saturated concurrency keys via non-locking GROUP BY read.
                let mut check_keys: BTreeSet<(String, String)> = BTreeSet::new();
                for (row, _) in &fetched {
                    let failure = row_claim_failure(row, now)?;
                    if failure.is_none()
                        && let Some(ckey) = row
                            .try_get::<Option<String>, _>("concurrency_key")?
                            .filter(|k| !k.is_empty())
                    {
                        let ns: String = row.try_get("namespace")?;
                        check_keys.insert((ns, ckey));
                    }
                }
                let mut db_counts = if check_keys.is_empty() {
                    HashMap::new()
                } else {
                    fetch_active_concurrency(&mut tx, &check_keys).await?
                };
                for (k, extra) in &reserved_ckey_counts {
                    *db_counts.entry(k.clone()).or_insert(0) += extra;
                }

                let mut newly_excluded = false;
                let mut last_accepted_bucket: Option<BucketKey> = None;
                for (row, bucket) in fetched {
                    let id: String = row.try_get("job_id")?;
                    if seen_set.insert(id.clone()) {
                        seen_ids.push(id);
                    }
                    let failure = row_claim_failure(&row, now)?;
                    if failure.is_none()
                        && let Some(ckey) = row
                            .try_get::<Option<String>, _>("concurrency_key")?
                            .filter(|k| !k.is_empty())
                    {
                        let ns: String = row.try_get("namespace")?;
                        let iface: String = row.try_get("interface")?;
                        let cap = policy_key_cap(&policies, &iface);
                        let key = (ns, ckey);
                        let count = db_counts.entry(key.clone()).or_insert(0);
                        if *count >= cap {
                            if excluded_ckeys.insert(key) {
                                newly_excluded = true;
                            }
                            continue;
                        }
                        *count += 1;
                        *reserved_ckey_counts.entry(key).or_insert(0) += 1;
                    }
                    if candidate_rows.len() < limit {
                        last_accepted_bucket = Some(bucket.clone());
                        candidate_rows.push(CandidateRow {
                            row,
                            failure,
                            bucket: Some(bucket),
                        });
                    }
                }

                if candidate_rows.len() >= limit {
                    if let Some((_, ns, iface, fkey)) = last_accepted_bucket {
                        local_cursors.insert(top_prio, (ns, iface, fkey));
                        cursor_dirty = true;
                    }
                    break;
                }
                if newly_excluded {
                    cached_buckets = Some((top_prio, exhausted_prio, buckets));
                    continue;
                }
                if let Some((_, ns, iface, fkey)) = buckets.last() {
                    local_cursors.insert(top_prio, (ns.clone(), iface.clone(), fkey.clone()));
                    cursor_dirty = true;
                }
                if exhausted_prio {
                    if top_prio <= 0 {
                        break;
                    }
                    max_prio = top_prio - 1;
                    prio_anchor = None;
                }
            }
        }

        // Step D (M1): collect distinct concurrency keys across candidate rows, acquire
        // advisory locks once in strict sorted ascending order, and re-verify counts.
        let mut distinct_ckeys: BTreeSet<(String, String)> = BTreeSet::new();
        for c in &candidate_rows {
            if c.failure.is_none()
                && let Some(ckey) = c
                    .row
                    .try_get::<Option<String>, _>("concurrency_key")?
                    .filter(|k| !k.is_empty())
            {
                let ns: String = c.row.try_get("namespace")?;
                distinct_ckeys.insert((ns, ckey));
            }
        }

        let mut active_concurrency: HashMap<(String, String), usize> = HashMap::new();
        if !distinct_ckeys.is_empty() {
            let mut lock_ids: Vec<i64> = distinct_ckeys
                .iter()
                .map(|(ns, ckey)| concurrency_advisory_key(ns, ckey))
                .collect();
            lock_ids.sort_unstable();
            lock_ids.dedup();
            sqlx::query(
                "SELECT pg_advisory_xact_lock(id)
                 FROM (SELECT UNNEST($1::bigint[]) AS id) s
                 ORDER BY id ASC",
            )
            .bind(lock_ids)
            .execute(&mut *tx)
            .await?;

            active_concurrency = fetch_active_concurrency(&mut tx, &distinct_ckeys).await?;
        }

        let mut claimed = Vec::new();
        let mut last_claimed_bucket: Option<BucketKey> = None;
        for c in candidate_rows {
            if claimed.len() >= limit {
                break;
            }
            let row = c.row;
            let failure = c.failure;
            if failure.is_none()
                && let Some(ckey) = row
                    .try_get::<Option<String>, _>("concurrency_key")?
                    .filter(|k| !k.is_empty())
            {
                let ns: String = row.try_get("namespace")?;
                let iface: String = row.try_get("interface")?;
                let cap = policy_key_cap(&policies, &iface);
                let count = active_concurrency.entry((ns, ckey)).or_insert(0);
                if *count >= cap {
                    continue;
                }
                *count += 1;
            }
            let mut permit = Permit::new(Arc::clone(&budget));
            if failure.is_none()
                && !permit.grow(usize::try_from(row.try_get::<i64, _>("input_bytes")?)?)
            {
                continue;
            }
            if let Some(bucket) = c.bucket {
                last_claimed_bucket = Some(bucket);
            }
            let mut job = super::read::pg_job_from_row(&row)?;
            let was_running = job.state == JobState::Running;
            let queued_at = if was_running {
                now
            } else {
                row.try_get("queued_at_ms")?
            };
            if was_running {
                if let Some(worker_id) = job.last_worker_id.as_ref().filter(|w| !w.is_empty()) {
                    job.failed_workers.insert(worker_id.clone());
                }
                event(
                    &mut tx,
                    &job.job_id,
                    row.try_get::<i64, _>("event_sequence")?
                        .checked_add(1)
                        .context("event sequence overflow")?,
                    job.last_attempt_id.as_deref(),
                    now,
                    EventKind::AttemptRestarted {
                        previous_attempt_id: job.last_attempt_id.clone().unwrap_or_default(),
                        reason: ErrorInfo::new(
                            codes::RECOVERED,
                            "previous replica lease expired",
                            true,
                        ),
                    },
                )
                .await?;
            }
            job.state = JobState::Queued;
            claimed.push(ClaimedJob {
                job,
                epoch: row
                    .try_get::<i64, _>("ownership_epoch")?
                    .checked_add(1)
                    .context("ownership epoch overflow")?,
                queued_at_ms: queued_at as u64,
                input_permit: permit,
                failure,
            });
        }
        if !claimed.is_empty() {
            // The selected rows stay locked until the whole batch is committed.
            let ids: Vec<_> = claimed.iter().map(|c| c.job.job_id.as_str()).collect();
            let changed = sqlx::query(
                "UPDATE jobs SET owner_id=$2,ownership_epoch=ownership_epoch+1,
                state='queued',queued_at_ms=CASE WHEN state='running' THEN $3 ELSE queued_at_ms END,
                event_sequence=event_sequence+CASE WHEN state='running' THEN 1 ELSE 0 END,
                failed_workers=CASE
                    WHEN state='running' AND last_worker_id IS NOT NULL AND last_worker_id <> ''
                         AND NOT (last_worker_id = ANY(failed_workers))
                    THEN array_append(failed_workers, last_worker_id)
                    ELSE failed_workers END
                WHERE job_id=ANY($1)",
            )
            .bind(ids)
            .bind(&self.node_id)
            .bind(now)
            .execute(&mut *tx)
            .await?
            .rows_affected();
            anyhow::ensure!(
                changed == claimed.len() as u64,
                "claim batch changed unexpectedly"
            );
            super::read::load_inputs(&mut tx, &mut claimed).await?;
        }
        if let Err(error) = tx.commit().await {
            // The claim may have committed although no local job was installed.
            // Stop renewal so another node can recover every ambiguous claim.
            self.fence();
            return Err(error.into());
        }
        if cursor_dirty {
            let mut cursors = self.fair_cursors.lock().expect("fair_cursors poisoned");
            if let Some((prio, ns, iface, fkey)) = last_claimed_bucket
                && claimed.len() >= limit
            {
                local_cursors.insert(prio, (ns, iface, fkey));
            }
            *cursors = local_cursors;
        }
        if claimed.len() >= 16 && !self.initial_analyze_done.swap(true, Ordering::Relaxed) {
            let pool = self.pool.clone();
            tokio::spawn(async move {
                let _ = sqlx::query("ANALYZE jobs").execute(&pool).await;
            });
        }
        Ok(claimed)
    }

    pub(crate) async fn release(&self, id: &str, epoch: i64) -> Result<bool> {
        Ok(sqlx::query(
            "UPDATE jobs SET owner_id=NULL WHERE job_id=$1 AND owner_id=$2
            AND ownership_epoch=$3 AND state='queued'",
        )
        .bind(id)
        .bind(&self.node_id)
        .bind(epoch)
        .execute(&self.pool)
        .await?
        .rows_affected()
            == 1)
    }

    pub(super) async fn apply(&self, op: JournalOp) -> CommitOutcome {
        let mut tx = match self.pool.begin().await {
            Ok(tx) => tx,
            Err(e) => return CommitOutcome::RolledBack(e.to_string()),
        };
        let result = self.apply_in_tx(&mut tx, &op).await;
        match result {
            Ok(false) => CommitOutcome::Fenced("job ownership or state changed".into()),
            Err(e) => CommitOutcome::RolledBack(e.to_string()),
            Ok(true) => match tx.commit().await {
                Ok(()) => CommitOutcome::Committed,
                // A lost COMMIT response is ambiguous. Fence this node; the
                // authoritative row/receipt remains readable on every replica.
                Err(e) => CommitOutcome::Unknown(e.to_string()),
            },
        }
    }

    async fn apply_in_tx(&self, tx: &mut Transaction<'_, Pg>, op: &JournalOp) -> Result<bool> {
        self.ensure_healthy()?;
        // IS TRUE keeps point updates from matching partial queue indexes.
        match op {
            JournalOp::Probe => {
                db_now(tx).await?;
                Ok(true)
            }
            JournalOp::Accept(_) => {
                anyhow::bail!("PostgreSQL acceptance uses atomic idempotent submission")
            }
            JournalOp::AttemptStart(record) => {
                let kind = EventKind::Assigned {
                    worker_id: record.worker_id.clone().unwrap_or_default(),
                    attempt_number: record.attempt_number,
                };
                let inserted = sqlx::query("WITH started AS (UPDATE jobs SET state='running',
                    attempt_count=$4,last_attempt_id=$5,last_worker_id=$6,
                    updated_at_ms=(extract(epoch FROM clock_timestamp())*1000)::bigint,
                    event_sequence=event_sequence+1,last_frame_seq=0
                    WHERE job_id=$1 AND owner_id=$2 AND ownership_epoch=$3
                      AND (state='queued') IS TRUE AND attempt_count=$4-1
                      AND EXISTS (SELECT 1 FROM nodes WHERE node_id=$2 AND expires_at>clock_timestamp())
                    RETURNING job_id,event_sequence,last_attempt_id,updated_at_ms)
                    INSERT INTO events(job_id,sequence,attempt_id,at_ms,kind_json)
                    SELECT job_id,event_sequence,last_attempt_id,updated_at_ms,$7 FROM started")
                    .bind(&record.job_id).bind(&self.node_id).bind(record.ownership_epoch)
                    .bind(record.attempt_number as i64).bind(&record.attempt_id).bind(&record.worker_id)
                    .bind(serde_json::to_string(&kind)?).execute(&mut **tx).await?;
                Ok(inserted.rows_affected() == 1)
            }
            JournalOp::Terminal(record) => {
                let retry = record.state == JobState::Queued;
                let state = state_text(record.state);
                let updated: Option<(i64, i64)> = sqlx::query_as(
                    "WITH stamp AS MATERIALIZED (
                    SELECT (extract(epoch FROM clock_timestamp())*1000)::bigint AS now_ms)
                    UPDATE jobs SET state=$4,error_json=$5,
                    updated_at_ms=stamp.now_ms,owner_id=NULL,event_sequence=event_sequence+1,
                    queued_at_ms=stamp.now_ms,not_before_ms=CASE WHEN $6::bigint IS NULL THEN 0
                        WHEN stamp.now_ms>9223372036854775807-$6 THEN 9223372036854775807
                        ELSE stamp.now_ms+$6 END,
                    failed_workers=CASE
                        WHEN $4='queued' AND $7::text IS NOT NULL AND $7 <> ''
                             AND NOT ($7 = ANY(failed_workers))
                        THEN array_append(failed_workers, $7)
                        ELSE failed_workers END
                    FROM stamp
                    WHERE job_id=$1 AND owner_id=$2 AND ownership_epoch=$3 AND (state IN ('queued','running')) IS TRUE
                      AND EXISTS (SELECT 1 FROM nodes WHERE node_id=$2 AND expires_at>clock_timestamp())
                    RETURNING event_sequence,updated_at_ms",
                )
                .bind(&record.job_id)
                .bind(&self.node_id)
                .bind(record.ownership_epoch)
                .bind(state)
                .bind(if retry {
                    None
                } else {
                    record.error.as_ref().map(serde_json::to_string).transpose()?
                })
                .bind(
                    record
                        .retry_at_ms
                        .map(|at| at.saturating_sub(record.at_ms).min(i64::MAX as u64) as i64),
                )
                .bind(record.worker_id.as_deref())
                .fetch_optional(&mut **tx)
                .await?;
                let Some((seq, now)) = updated else {
                    return Ok(false);
                };
                if record.state == JobState::Succeeded {
                    insert_objects(tx, &record.job_id, "output", &record.outputs).await?;
                }
                if let (Some(lease), Some(digest)) =
                    (&record.winning_lease_id, &record.outcome_digest)
                {
                    sqlx::query("INSERT INTO receipts(lease_id,job_id,digest) VALUES($1,$2,$3)")
                        .bind(lease)
                        .bind(&record.job_id)
                        .bind(digest)
                        .execute(&mut **tx)
                        .await?;
                }
                let kind = if retry {
                    EventKind::AttemptRestarted {
                        previous_attempt_id: record.attempt_id.clone().unwrap_or_default(),
                        reason: record.error.clone().context("retry reason")?,
                    }
                } else {
                    EventKind::Finished {
                        state: record.state,
                        outputs: Vec::new(),
                        error: record.error.clone(),
                    }
                };
                event(
                    tx,
                    &record.job_id,
                    seq,
                    record.attempt_id.as_deref(),
                    now,
                    kind,
                )
                .await?;
                Ok(true)
            }
        }
    }

    pub(crate) async fn cancel(&self, id: &str, reason: String) -> Result<()> {
        self.ensure_healthy()?;
        let mut tx = self.pool.begin().await?;
        let now = db_now(&mut tx).await?;
        let error = ErrorInfo::new(
            codes::CANCELLED,
            if reason.trim().is_empty() {
                "cancelled".into()
            } else {
                reason
            },
            false,
        );
        let row = sqlx::query(
            "UPDATE jobs SET state='cancelled',error_json=$2,updated_at_ms=$3,
            owner_id=NULL,ownership_epoch=ownership_epoch+1,event_sequence=event_sequence+1
            WHERE job_id=$1 AND state IN ('queued','running') RETURNING event_sequence,last_attempt_id",
        )
        .bind(id)
        .bind(serde_json::to_string(&error)?)
        .bind(now)
        .fetch_optional(&mut *tx)
        .await?;
        if let Some(row) = row {
            let attempt: Option<String> = row.try_get("last_attempt_id")?;
            event(
                &mut tx,
                id,
                row.try_get("event_sequence")?,
                attempt.as_deref(),
                now,
                EventKind::Finished {
                    state: JobState::Cancelled,
                    outputs: Vec::new(),
                    error: Some(error),
                },
            )
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    pub(crate) async fn purge(&self, before_ms: u64) -> Result<usize> {
        // Bounded cleanup, safe when several replicas run maintenance together.
        let mut removed = 0;
        loop {
            let count = sqlx::query(
                "DELETE FROM jobs WHERE job_id IN (SELECT job_id FROM jobs
            WHERE state IN ('succeeded','failed','cancelled') AND updated_at_ms<$1
            ORDER BY updated_at_ms LIMIT 500 FOR UPDATE SKIP LOCKED)",
            )
            .bind(before_ms.min(i64::MAX as u64) as i64)
            .execute(&self.pool)
            .await?
            .rows_affected() as usize;
            removed += count;
            if count < 500 {
                return Ok(removed);
            }
            tokio::task::yield_now().await;
        }
    }
}
