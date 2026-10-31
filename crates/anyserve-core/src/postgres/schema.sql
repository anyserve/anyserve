-- Version 1. Schema creation is serialized only during startup, never scheduling.
CREATE TABLE IF NOT EXISTS cluster_schema (version BIGINT PRIMARY KEY);
INSERT INTO cluster_schema VALUES (1) ON CONFLICT DO NOTHING;
CREATE SEQUENCE IF NOT EXISTS node_generation;
CREATE TABLE IF NOT EXISTS nodes (
    node_id TEXT PRIMARY KEY,
    generation BIGINT NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL
);
CREATE TABLE IF NOT EXISTS jobs (
    job_id TEXT COLLATE "C" PRIMARY KEY,
    state TEXT NOT NULL CHECK (state IN ('queued','running','succeeded','failed','cancelled')),
    spec_hash TEXT NOT NULL,
    spec_json TEXT NOT NULL,
    interface TEXT NOT NULL,
    required_attributes JSONB NOT NULL,
    preferred_attributes JSONB NOT NULL,
    required_capacity JSONB NOT NULL,
    priority BIGINT NOT NULL,
    input_bytes BIGINT NOT NULL,
    attempt_count BIGINT NOT NULL DEFAULT 0,
    max_attempts BIGINT NOT NULL,
    created_at_ms BIGINT NOT NULL,
    updated_at_ms BIGINT NOT NULL,
    queued_at_ms BIGINT NOT NULL,
    not_before_ms BIGINT NOT NULL DEFAULT 0,
    queue_timeout_ms BIGINT,
    total_deadline_ms BIGINT,
    spillover_ms BIGINT,
    owner_id TEXT,
    ownership_epoch BIGINT NOT NULL DEFAULT 0,
    event_sequence BIGINT NOT NULL DEFAULT 0,
    events_floor BIGINT NOT NULL DEFAULT 0,
    last_frame_seq BIGINT NOT NULL DEFAULT 0,
    error_json TEXT,
    last_attempt_id TEXT,
    last_worker_id TEXT
);
CREATE INDEX IF NOT EXISTS jobs_claim ON jobs(interface, priority DESC, created_at_ms, job_id)
    WHERE state IN ('queued','running');
CREATE INDEX IF NOT EXISTS jobs_owner ON jobs(owner_id) WHERE state IN ('queued','running');
CREATE INDEX IF NOT EXISTS jobs_created ON jobs(created_at_ms DESC, job_id DESC);
CREATE INDEX IF NOT EXISTS jobs_history ON jobs(created_at_ms DESC, job_id DESC)
    WHERE state IN ('succeeded','failed','cancelled');
CREATE INDEX IF NOT EXISTS jobs_retention ON jobs(updated_at_ms)
    WHERE state IN ('succeeded','failed','cancelled');
CREATE TABLE IF NOT EXISTS objects (
    job_id TEXT COLLATE "C" NOT NULL REFERENCES jobs(job_id) ON DELETE CASCADE,
    role TEXT NOT NULL,
    idx BIGINT NOT NULL,
    uri TEXT,
    content BYTEA,
    metadata_json TEXT NOT NULL,
    PRIMARY KEY (job_id,role,idx)
);
CREATE TABLE IF NOT EXISTS events (
    job_id TEXT COLLATE "C" NOT NULL REFERENCES jobs(job_id) ON DELETE CASCADE,
    sequence BIGINT NOT NULL,
    attempt_id TEXT,
    at_ms BIGINT NOT NULL,
    kind_json TEXT NOT NULL,
    frame_data BYTEA,
    PRIMARY KEY(job_id,sequence)
);
CREATE TABLE IF NOT EXISTS receipts (
    lease_id TEXT PRIMARY KEY,
    job_id TEXT COLLATE "C" NOT NULL REFERENCES jobs(job_id) ON DELETE CASCADE,
    digest TEXT NOT NULL
);
