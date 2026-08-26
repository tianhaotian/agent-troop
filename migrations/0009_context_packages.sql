-- M7D：按 lease 保存不可变上下文包与审计指纹（设计 §16.2）。

BEGIN;

CREATE TABLE IF NOT EXISTS context_packages (
    id              TEXT PRIMARY KEY,
    lease_id        TEXT NOT NULL UNIQUE REFERENCES leases(id) ON DELETE CASCADE,
    mission_id      TEXT NOT NULL REFERENCES missions(id) ON DELETE CASCADE,
    subtask_id      TEXT NOT NULL REFERENCES subtasks(id) ON DELETE CASCADE,
    payload         JSONB NOT NULL,
    snapshot_hash   TEXT NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_context_packages_subtask
    ON context_packages(subtask_id, created_at);

COMMIT;
