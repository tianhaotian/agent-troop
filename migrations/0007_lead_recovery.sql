-- M7B: Lead inbox for explicit context ingestion and takeover recovery.
BEGIN;

CREATE TABLE IF NOT EXISTS lead_inbox (
    id                  TEXT PRIMARY KEY,
    mission_id          TEXT NOT NULL REFERENCES missions(id),
    lead_subtask_id     TEXT NOT NULL REFERENCES subtasks(id),
    source_subtask_id   TEXT NOT NULL UNIQUE REFERENCES subtasks(id),
    kind                TEXT NOT NULL,
    result_ref          TEXT NOT NULL DEFAULT '',
    status              TEXT NOT NULL DEFAULT 'pending',
    ingest_mode         TEXT NOT NULL DEFAULT '',
    ingested_by         TEXT,
    version             BIGINT NOT NULL DEFAULT 0,
    created_at          TIMESTAMPTZ NOT NULL,
    ingested_at         TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_lead_inbox_pending
    ON lead_inbox(lead_subtask_id, created_at) WHERE status = 'pending';

COMMIT;
