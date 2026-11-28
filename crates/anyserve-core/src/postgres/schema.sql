-- Schema version 4: single-leader scheduler lease, durable journal, and worker registry.
CREATE TABLE IF NOT EXISTS cluster_schema (version BIGINT PRIMARY KEY);
INSERT INTO cluster_schema VALUES (4) ON CONFLICT DO NOTHING;

CREATE TABLE IF NOT EXISTS scheduler_lease (
    id SMALLINT PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    holder_id TEXT NOT NULL,
    generation BIGINT NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS jobs (
    job_id TEXT COLLATE "C" PRIMARY KEY,
    namespace TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('queued', 'succeeded', 'failed', 'cancelled')),
    spec_hash TEXT NOT NULL,
    spec_json TEXT NOT NULL,
    attempt_count BIGINT NOT NULL DEFAULT 0,
    max_attempts BIGINT NOT NULL,
    created_at_ms BIGINT NOT NULL,
    updated_at_ms BIGINT NOT NULL,
    error_json TEXT,
    winning_lease_id TEXT,
    result_digest TEXT,
    last_attempt_id TEXT,
    last_worker_id TEXT
);

CREATE INDEX IF NOT EXISTS jobs_recovery
    ON jobs(created_at_ms, job_id)
    WHERE state = 'queued';
CREATE UNIQUE INDEX IF NOT EXISTS jobs_winner
    ON jobs(winning_lease_id)
    WHERE winning_lease_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS jobs_history
    ON jobs(created_at_ms DESC, job_id DESC)
    WHERE state IN ('succeeded', 'failed', 'cancelled');
CREATE INDEX IF NOT EXISTS jobs_ns_history
    ON jobs(namespace, created_at_ms DESC, job_id DESC)
    WHERE state IN ('succeeded', 'failed', 'cancelled');
CREATE INDEX IF NOT EXISTS jobs_retention
    ON jobs(updated_at_ms)
    WHERE state IN ('succeeded', 'failed', 'cancelled');

CREATE TABLE IF NOT EXISTS objects (
    job_id TEXT COLLATE "C" NOT NULL
        REFERENCES jobs(job_id) ON DELETE CASCADE DEFERRABLE INITIALLY IMMEDIATE,
    role TEXT NOT NULL,
    idx BIGINT NOT NULL,
    uri TEXT,
    content BYTEA,
    metadata_json TEXT NOT NULL,
    PRIMARY KEY (job_id, role, idx)
);

CREATE TABLE IF NOT EXISTS workers (
    namespace TEXT NOT NULL,
    worker_id TEXT NOT NULL,
    record_json TEXT NOT NULL,
    updated_at_ms BIGINT NOT NULL,
    PRIMARY KEY (namespace, worker_id)
);

CREATE INDEX IF NOT EXISTS workers_updated
    ON workers(updated_at_ms DESC);
