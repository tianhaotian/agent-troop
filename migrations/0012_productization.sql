-- M10：质量申诉审计流。
BEGIN;

CREATE TABLE IF NOT EXISTS quality_appeals (
    id                TEXT PRIMARY KEY,
    artifact_id       TEXT NOT NULL REFERENCES quality_records(artifact_id) ON DELETE CASCADE,
    mission_id        TEXT NOT NULL REFERENCES missions(id) ON DELETE CASCADE,
    appellant_id      TEXT NOT NULL,
    reason            TEXT NOT NULL,
    evidence_refs     JSONB NOT NULL DEFAULT '[]',
    status            TEXT NOT NULL CHECK (status IN ('pending','upheld','overturned')),
    resolution        TEXT NOT NULL DEFAULT '',
    reviewer_id       TEXT NOT NULL DEFAULT '',
    correction_signal TEXT NOT NULL DEFAULT '',
    created_at        TIMESTAMPTZ NOT NULL,
    resolved_at       TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_quality_appeals_mission
    ON quality_appeals(mission_id, status, created_at, id);

COMMIT;
